using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using Harness.SelfTests;
using Harness.Slice;
using Npgsql;
using Reeve.Cluster.Harness.Processes;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Process;

[Collection(StackCollection.Name)]
public sealed class ProcessTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan AckStallAllowance = TimeSpan.FromSeconds(10);

    private const string TwoFlags = """[{"name":"a","enabled":true},{"name":"b","enabled":true}]""";

    [Fact]
    public async Task A_request_fenced_between_two_commits_answers_write_unconfirmed_and_is_not_replayed()
    {
        await using var cluster = await StartPairAsync(r1: o => o.RejoinBackoff = TimeSpan.FromSeconds(30));
        try
        {
            var import = cluster.ThroughAsync("import", HttpMethod.Post, "flags/import?gate=fenced", "r1", Json(TwoFlags));
            await SliceRig.UntilAsync(() => FlagExistsAsync(cluster, "a"), "the first commit");

            Assert.Equal(1, await HarnessOperations.OperatorDelete(cluster.Run, "r1"));
            await SliceRig.UntilAsync(() => IsFencedAsync(cluster["r1"]), "r1 fenced, its steps run");
            await InsertFlagAsync(cluster, "fenced");
            using var response = await import.WaitAsync(Limit);

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("write_unconfirmed", await SliceRig.ErrorCodeAsync(response));
            Assert.True(response.Headers.Contains("Reeve-Seq"));
            var request = await cluster.Balancer.RequestAsync("import");
            Assert.Equal([("r1", (int?)202)], request.Attempts);
            Assert.True(await FlagExistsAsync(cluster, "a"));
            Assert.False(await FlagExistsAsync(cluster, "b"));
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task A_replica_fenced_answer_is_replayed_for_an_idempotent_method_and_not_for_a_POST()
    {
        await using var cluster = await StartPairAsync(r1: o => o.RejoinBackoff = TimeSpan.FromSeconds(30));
        try
        {
            Assert.Equal(1, await HarnessOperations.OperatorDelete(cluster.Run, "r1"));
            await SliceRig.UntilAsync(async () => !await IsReadyAsync(cluster["r1"]), "r1 fenced");

            using var put = await cluster.ThroughAsync("put", HttpMethod.Put, "flags/c", "r1", Json("""{"enabled":true}"""));
            Assert.Equal(HttpStatusCode.OK, put.StatusCode);
            Assert.Equal([("r1", (int?)503), ("r2", 200)], (await cluster.Balancer.RequestAsync("put")).Attempts);
            Assert.True(await FlagExistsAsync(cluster, "c"));

            using var get = await cluster.ThroughAsync("get", HttpMethod.Get, "flags/c", "r1");
            Assert.Equal(HttpStatusCode.OK, get.StatusCode);
            Assert.Equal([("r1", (int?)503), ("r2", 200)], (await cluster.Balancer.RequestAsync("get")).Attempts);

            using var post = await cluster.ThroughAsync("post", HttpMethod.Post, "flags/import", "r1", Json("""[{"name":"d","enabled":true}]"""));
            Assert.Equal(HttpStatusCode.ServiceUnavailable, post.StatusCode);
            Assert.Equal("replica_fenced", await SliceRig.ErrorCodeAsync(post));
            Assert.Equal([("r1", (int?)503)], (await cluster.Balancer.RequestAsync("post")).Attempts);
            Assert.False(await FlagExistsAsync(cluster, "d"));
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task A_write_refused_answer_is_replayed_for_an_idempotent_method_and_not_for_a_POST()
    {
        await using var cluster = await StartPairAsync(r1: o => o.RejoinBackoff = TimeSpan.FromSeconds(30));
        try
        {
            var put = cluster.ThroughAsync("put", HttpMethod.Put, "flags/e?hold=refused", "r1", Json("""{"enabled":true}"""));
            var post = cluster.ThroughAsync("post", HttpMethod.Post, "flags/import?hold=refused", "r1", Json("""[{"name":"f","enabled":true}]"""));
            await SliceRig.UntilAsync(() => HeldAsync(cluster["r1"], 2), "both requests running on r1");

            Assert.Equal(1, await HarnessOperations.OperatorDelete(cluster.Run, "r1"));
            await SliceRig.UntilAsync(() => IsFencedAsync(cluster["r1"]), "r1 fenced, its steps run");
            await InsertFlagAsync(cluster, "refused");
            using var putResponse = await put.WaitAsync(Limit);
            using var postResponse = await post.WaitAsync(Limit);

            Assert.Equal(HttpStatusCode.OK, putResponse.StatusCode);
            Assert.Equal([("r1", (int?)503), ("r2", 200)], (await cluster.Balancer.RequestAsync("put")).Attempts);
            Assert.True(await FlagExistsAsync(cluster, "e"));
            Assert.Equal(HttpStatusCode.ServiceUnavailable, postResponse.StatusCode);
            Assert.Equal("write_refused", await SliceRig.ErrorCodeAsync(postResponse));
            Assert.Equal([("r1", (int?)503)], (await cluster.Balancer.RequestAsync("post")).Attempts);
            Assert.False(await FlagExistsAsync(cluster, "f"));
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task A_PUT_that_outlives_AckTimeout_answers_202_within_the_bound_and_is_not_resent()
    {
        await using var cluster = await StartPairAsync(r2: LongLease);
        try
        {
            var bound = cluster["r1"].Options.AckTimeout;
            Assert.True(bound < cluster["r2"].Options.LeaseTtl, "r2's lease must outlive r1's AckTimeout, or the wait ends by the lease and not by the bound.");
            cluster["r2"].Signal(ProcessSignal.Stop);

            var started = Stopwatch.StartNew();
            using var response = await cluster.ThroughAsync("put", HttpMethod.Put, "flags/x", "r1", Json("""{"enabled":true}"""));
            var elapsed = started.Elapsed;

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("write_unconfirmed", await SliceRig.ErrorCodeAsync(response));
            Assert.True(response.Headers.Contains("Reeve-Seq"));
            Assert.True(elapsed >= bound - TimeSpan.FromMilliseconds(500), $"The answer came after {elapsed}, before the acknowledgement's bound of {bound}.");
            Assert.True(elapsed < ProcessCluster.ProxyReadTimeout, $"The answer came after {elapsed}, past the balancer's read timeout.");
            Assert.Equal([("r1", (int?)202)], (await cluster.Balancer.RequestAsync("put")).Attempts);

            Assert.True(await FlagExistsAsync(cluster, "x"));
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task A_request_that_times_out_at_the_balancer_is_not_sent_again()
    {
        await using var cluster = await StartPairAsync(balancerReadTimeout: TimeSpan.FromSeconds(3));
        try
        {
            cluster["r1"].Signal(ProcessSignal.Stop);

            using var response = await cluster.ThroughAsync("get", HttpMethod.Get, "flags/none", "r1");

            Assert.Equal(HttpStatusCode.GatewayTimeout, response.StatusCode);
            Assert.Equal([("r1", (int?)504)], (await cluster.Balancer.RequestAsync("get")).Attempts);
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task After_a_SIGKILL_writers_acknowledge_within_LeaseTtl_plus_AckPollInterval()
    {
        await using var cluster = await StartPairAsync(r2: o => o.ApplyStallLimit = TimeSpan.FromSeconds(30));
        try
        {
            var r1 = cluster["r1"];
            var bound = r1.Options.LeaseTtl + r1.Options.AckPollInterval;
            Assert.True(cluster["r2"].Options.AckTimeout > bound + AckStallAllowance);

            var killed = Stopwatch.StartNew();
            r1.Signal(ProcessSignal.Kill);
            await r1.WaitForExitAsync(Limit);
            using var response = await cluster.ThroughAsync("put", HttpMethod.Put, "flags/y", "r1", Json("""{"enabled":true}"""));
            var elapsed = killed.Elapsed;

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(elapsed <= bound + AckStallAllowance, $"The write was acknowledged {elapsed} after the kill; the bound is {bound}.");

            var earliest = r1.Options.LeaseTtl - r1.Options.RenewInterval - r1.Options.RenewTimeout - TimeSpan.FromMilliseconds(500);
            Assert.True(elapsed >= earliest, $"The write was acknowledged {elapsed} after the kill, before r1's lease could have run out ({earliest}).");
            var request = await cluster.Balancer.RequestAsync("put");
            Assert.Equal(2, request.Attempts.Count);
            Assert.Equal("r1", request.Attempts[0].Replica);
            Assert.Equal(("r2", (int?)200), request.Attempts[1]);
            Assert.True(await FlagExistsAsync(cluster, "y"));
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    [Fact]
    public async Task A_SIGTERM_drains_the_replica_finishes_the_running_request_and_no_writer_waits()
    {
        await using var cluster = await StartPairAsync(r1: LongLease);
        try
        {
            var r1 = cluster["r1"];
            var import = cluster.ThroughAsync("import", HttpMethod.Post, "flags/import?gate=drained", "r1", Json(TwoFlags));
            await SliceRig.UntilAsync(() => FlagExistsAsync(cluster, "a"), "the first commit");

            r1.Signal(ProcessSignal.Term);
            await SliceRig.UntilAsync(async () => (await cluster.Run.Observer.MemberAsync("r1"))?.ServingSince is null, "r1 left Serving");
            using var get = await cluster.ThroughAsync("get", HttpMethod.Get, "flags/a", "r1");
            Assert.Equal(HttpStatusCode.OK, get.StatusCode);
            Assert.Equal([("r1", (int?)503), ("r2", 200)], (await cluster.Balancer.RequestAsync("get")).Attempts);

            await InsertFlagAsync(cluster, "drained");
            using var response = await import.WaitAsync(Limit);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(await FlagExistsAsync(cluster, "b"));
            Assert.Equal(0, await r1.WaitForExitAsync(Limit));
            Assert.Equal(MemberRowState.Draining, (await cluster.Run.Observer.MemberAsync("r1"))!.State);

            var started = Stopwatch.StartNew();
            using var put = await cluster.ThroughAsync("put", HttpMethod.Put, "flags/z", "r2", Json("""{"enabled":true}"""));
            Assert.Equal(HttpStatusCode.OK, put.StatusCode);
            Assert.True(started.Elapsed < cluster["r2"].Options.AckTimeout, $"The write waited {started.Elapsed} after the drain.");
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    private async Task<ProcessCluster> StartPairAsync(Action<ClusterOptions>? r1 = null, Action<ClusterOptions>? r2 = null, TimeSpan? balancerReadTimeout = null)
    {
        var cluster = await ProcessCluster.CreateAsync(stack.Stack, output.WriteLine, ["r1", "r2"], balancerReadTimeout);
        try
        {
            await cluster.StartProcessAsync("r1", r1);
            await cluster.StartProcessAsync("r2", r2);
            return cluster;
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            await cluster.DisposeAsync();
            throw;
        }
    }

    private static void LongLease(ClusterOptions o)
    {
        o.LeaseTtl = TimeSpan.FromSeconds(60);
        o.SafetyMargin = TimeSpan.FromSeconds(3);
        o.StopBudget = TimeSpan.FromSeconds(5);
        o.RenewTimeout = TimeSpan.FromSeconds(2);
        o.RenewInterval = TimeSpan.FromSeconds(3);
        o.ProxyReadTimeout = TimeSpan.FromSeconds(120);
    }

    private static StringContent Json(string body) => new(body, Encoding.UTF8, "application/json");

    private static async Task<bool> IsReadyAsync(SampleProcess process)
    {
        using var response = await process.Client.GetAsync("health/ready");
        return response.StatusCode == HttpStatusCode.OK;
    }

    private static Task<bool> HeldAsync(SampleProcess process, int requests) => Task.FromResult(process.MarkerCount("hold refused") >= requests);

    private static async Task<bool> IsFencedAsync(SampleProcess process)
    {
        using var response = await process.Client.GetAsync("phase");
        using var status = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return status.RootElement.GetProperty("phase").GetString() == "Fenced";
    }

    private static async Task InsertFlagAsync(ProcessCluster cluster, string name)
    {
        await using var connection = await cluster.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("INSERT INTO sample_flags (name, enabled) VALUES (@name, true) ON CONFLICT DO NOTHING", connection);
        command.Parameters.AddWithValue("name", name);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> FlagExistsAsync(ProcessCluster cluster, string name)
    {
        await using var connection = await cluster.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM sample_flags WHERE name = @name", connection);
        command.Parameters.AddWithValue("name", name);
        return (long)(await command.ExecuteScalarAsync())! > 0;
    }
}
