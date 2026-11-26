using System.Net;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Stimuli;
using Xunit.Abstractions;

namespace Harness.Slice;

[Collection(StackCollection.Name)]
public sealed class SliceTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_request_runs_the_whole_path_join_load_activation_serving_a_write_acknowledged_by_the_own_cursor_and_a_read_through_the_read_index()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http = rig.Client(r1);

        Assert.Equal([ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving], rig.Run.Phases.Path("r1"));
        Assert.Empty(rig.Run.Phases.Discontinuities);
        var row = await rig.Run.Observer.MemberAsync("r1");
        Assert.Equal(MemberRowState.Active, row!.State);
        Assert.NotNull(row.ServingSince);
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("ready")).StatusCode);

        using var put = await http.PutAsync("items/widget/a?value=one", null);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.False(put.Headers.Contains("Reeve-Seq"));
        Assert.False(put.Headers.Contains("Reeve-Replication"));
        var written = await SliceRig.ItemAsync(put);
        Assert.Equal("one", written.Value);
        Assert.True(written.Seq > 0);
        var seq = written.Seq!.Value;

        var waits = rig.Run.Events.WaitSets().Where(w => w.AwaitedSeq == seq).ToList();
        Assert.Equal(["AckBegin", "AckDone"], waits.Select(w => w.Action));
        Assert.All(waits, w => Assert.Equal(["r1"], w.Members));
        Assert.True((await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= seq);
        Assert.True(await AckedSeqAsync(rig) >= seq);

        using var read = await http.GetAsync("items/widget/a");
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        var item = await SliceRig.ItemAsync(read);
        Assert.Equal("one", item.Value);
        Assert.True(item.Head >= seq);
        var generations = rig.Run.Events.GenerationsRead().Where(g => g.Replica == "r1").ToList();
        Assert.Contains(generations, g => g.Head >= seq);

        using var gadget = await http.PutAsync("items/gadget/g?value=two", null);
        Assert.Equal(HttpStatusCode.OK, gadget.StatusCode);
        Assert.Equal("two", (await SliceRig.ItemAsync(await http.GetAsync("items/gadget/g"))).Value);
        Assert.Equal(HttpStatusCode.NotFound, (await http.GetAsync("items/widget/g")).StatusCode);
        var exempt = await (await http.GetAsync("exempt")).Content.ReadAsStringAsync();
        Assert.Contains("ExemptRequest", exempt, StringComparison.Ordinal);
        Assert.Contains("Serving", exempt, StringComparison.Ordinal);
        await using var hub = await rig.ConnectHubAsync(r1);
        Assert.Contains("\"value\":\"three\"", await hub.InvokeAsync("Put", "widget", "h", "three"), StringComparison.Ordinal);
        Assert.Contains("\"value\":\"three\"", await hub.InvokeAsync("Get", "widget", "h"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task No_request_is_served_between_activation_and_Serving_and_the_first_one_after_it_is_served()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        await ExecuteAsync(rig, "INSERT INTO slice_item (type, id, value) VALUES ('widget', 'seed', 's')");
        var r1 = await rig.PrepareAsync("r1");

        using var hold = r1.Faults.HoldNext(MembershipFaultPoints.ActivateCommittedBeforeApplied);
        await r1.StartAsync();
        await r1.Started!.WaitAsync(Limit);
        await hold.WaitReachedAsync(timeout: Limit);
        Assert.Equal(ReplicaPhase.Activated, SliceRig.PhaseOf(r1));
        var http = rig.Client(r1);
        var calls = r1.Services.GetRequiredService<SliceCalls>();

        using var read = await http.GetAsync("items/widget/seed");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, read.StatusCode);
        Assert.Equal("replica_not_serving", await SliceRig.ErrorCodeAsync(read));
        Assert.Equal("1", read.Headers.RetryAfter?.Delta?.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
        using var write = await http.PutAsync("items/widget/new?value=x", null);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, write.StatusCode);
        Assert.Equal("replica_not_serving", await SliceRig.ErrorCodeAsync(write));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await http.GetAsync("ready")).StatusCode);
        await Assert.ThrowsAsync<System.Net.WebSockets.WebSocketException>(() => rig.ConnectHubAsync(r1));
        Assert.Equal(0L, await CountAsync(rig, "SELECT count(*) FROM slice_item WHERE id = 'new'"));

        var exempt = await (await http.GetAsync("exempt")).Content.ReadAsStringAsync();
        Assert.Contains("Activated", exempt, StringComparison.Ordinal);
        Assert.Equal(["exempt"], calls.All);

        hold.Release();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("ready")).StatusCode);
        Assert.Equal("s", (await SliceRig.ItemAsync(await http.GetAsync("items/widget/seed"))).Value);
        Assert.Equal(["exempt", "get widget/seed"], calls.All);
    }

    [Fact]
    public async Task A_write_is_acknowledged_long_before_the_polls_because_the_wake_up_carries_it()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);

        var r1 = await rig.StartAsync("r1", SliceApp.Spec(configure: o => o.ApplyPollInterval = o.AckPollInterval = TimeSpan.FromSeconds(30)));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal("RedisWakeupChannel", r1.Services.GetRequiredService<Reeve.Cluster.Wakeup.IWakeupChannel>().GetType().Name);
        var http = rig.Client(r1);

        var started = System.Diagnostics.Stopwatch.StartNew();
        using var put = await http.PutAsync("items/widget/a?value=one", null);

        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(20), $"The write took {started.Elapsed}; the polls are 30 s.");
        Assert.Equal("one", (await SliceRig.ItemAsync(await http.GetAsync("items/widget/a"))).Value);
    }

    [Fact]
    public async Task A_fence_when_the_proxy_cuts_PostgreSQL_answers_each_request_by_the_rule_closes_the_hub_and_the_replica_rejoins()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http = rig.Client(r1);
        var calls = r1.Services.GetRequiredService<SliceCalls>();
        using (var before = await http.PutAsync("items/widget/before?value=b", null))
        {
            Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        }

        await using var hub = await rig.ConnectHubAsync(r1);
        Assert.Contains("\"value\":\"b\"", await hub.InvokeAsync("Get", "widget", "before"), StringComparison.Ordinal);

        using var beforeAck = r1.Faults.HoldNext(HttpFaultPoints.BeforeAckAwait);
        var inFlight = http.PutAsync("items/widget/inflight?value=i", null);
        await beforeAck.WaitReachedAsync(timeout: Limit);

        await r1.Link!.CutAsync();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Fenced);
        var served = calls.All.Count;

        using var read = await http.GetAsync("items/widget/before");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, read.StatusCode);
        Assert.Equal("replica_fenced", await SliceRig.ErrorCodeAsync(read));
        using var write = await http.PutAsync("items/widget/refused?value=r", null);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, write.StatusCode);
        Assert.Equal("replica_fenced", await SliceRig.ErrorCodeAsync(write));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await http.GetAsync("ready")).StatusCode);
        Assert.Contains("Fenced", await (await http.GetAsync("exempt")).Content.ReadAsStringAsync(), StringComparison.Ordinal);
        Assert.Equal(served + 1, calls.All.Count);
        Assert.Equal("exempt", calls.All[^1]);
        Assert.Equal(0L, await CountAsync(rig, "SELECT count(*) FROM slice_item WHERE id = 'refused'"));
        Assert.True(await hub.ClosedWithinAsync(TimeSpan.FromSeconds(30)));

        beforeAck.Release();
        using var unconfirmed = await inFlight.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal("write_unconfirmed", await SliceRig.ErrorCodeAsync(unconfirmed));
        Assert.True(unconfirmed.Headers.Contains("Reeve-Seq"));
        Assert.Equal(1L, await CountAsync(rig, "SELECT count(*) FROM slice_item WHERE id = 'inflight'"));

        await r1.Link.RestoreAsync();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal(2, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
        Assert.Equal("i", (await SliceRig.ItemAsync(await http.GetAsync("items/widget/inflight"))).Value);
        using var after = await http.PutAsync("items/widget/after?value=a", null);
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
    }

    [Fact]
    public async Task A_fence_with_PostgreSQL_reachable_records_the_fence_and_the_replica_rejoins()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http = rig.Client(r1);
        using (var before = await http.PutAsync("items/widget/before?value=b", null))
        {
            Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        }

        using var rejoin = r1.Faults.HoldNext(MembershipFaultPoints.FenceBeforeStep6);
        await HarnessOperations.OperatorDelete(rig.Run, "r1");
        await rejoin.WaitReachedAsync(timeout: Limit);

        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.FenceHistoryAsync("r1")).Count > 0, "the fence's record");
        var history = Assert.Single(await rig.Run.Observer.FenceHistoryAsync("r1"));
        Assert.Equal(1, history.Epoch);
        Assert.False(string.IsNullOrEmpty(history.Trigger));
        using var refused = await http.GetAsync("items/widget/before");
        Assert.Contains(await SliceRig.ErrorCodeAsync(refused), new[] { "replica_fenced", "replica_not_serving" });
        rejoin.Release();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Contains(ReplicaPhase.Fenced, rig.Run.Phases.Path("r1"));
        Assert.Equal(2, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
        Assert.Equal("b", (await SliceRig.ItemAsync(await http.GetAsync("items/widget/before"))).Value);
    }

    [Fact]
    public async Task A_write_is_not_answered_until_the_replicas_own_applier_has_applied_it()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http = rig.Client(r1);

        using var pass = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = http.PutAsync("items/widget/p?value=x", null);
        await pass.WaitReachedAsync(timeout: Limit);
        Assert.False(await Task.WhenAny(put, Task.Delay(TimeSpan.FromSeconds(3))) == put, "the write was answered before the replica applied it");

        pass.Release();
        using var response = await put.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task A_read_waits_for_the_applier_when_the_log_head_is_ahead_of_the_generation()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http = rig.Client(r1);

        using var pass = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        await using (var write = await HarnessOperations.StartWrite(r1))
        {
            await using (var command = new NpgsqlCommand("INSERT INTO slice_item (type, id, value) VALUES ('widget', 'ri', 'v')", write.Transaction.Connection, write.Transaction))
            {
                await command.ExecuteNonQueryAsync();
            }

            await write.AppendAsync([new ChangeEntry(SliceApp.Widget, "ri")]);
            await write.CommitAsync();
        }

        await pass.WaitReachedAsync(timeout: Limit);
        var get = http.GetAsync("items/widget/ri");
        Assert.False(await Task.WhenAny(get, Task.Delay(TimeSpan.FromSeconds(1))) == get, "the read was answered while the applier was held and the head was ahead of the generation");

        pass.Release();
        using var read = await get.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal("v", (await SliceRig.ItemAsync(read)).Value);
    }

    private static async Task ExecuteAsync(SliceRig rig, string sql)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(SliceRig rig, string sql)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<long> AckedSeqAsync(SliceRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT acked_seq FROM reeve_cluster.change_ack", connection);
        return (long)(await command.ExecuteScalarAsync() ?? 0L);
    }
}
