using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Int.Log;

namespace Int.Http;

public sealed class AcknowledgementFilterIntegrationTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public AcknowledgementFilterIntegrationTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            await item.DisposeAsync();
        }
    }

    private async Task<HttpHostRig> ServingHostAsync(bool ackDatabaseDown = false)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, o => o.ReadIndexTimeout = TimeSpan.FromSeconds(10));
        owned.Add(rig);
        await rig.LoadAsync();
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        var index = new ReadIndex(
            rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), TimeProvider.System,
            NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        var host = new HttpHostRig(rig, index, ackDatabaseDown);
        owned.Add(host);
        return host;
    }

    private static string? Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? string.Join("|", values) : null;

    private static async Task<string?> CodeAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString();

    private static Task<HttpResponseMessage> PostAsync(HttpHostRig host, string query) => host.Client.PostAsync("/write?" + query, null);

    private static async Task<long> HeadSeqAsync(HttpHostRig host) => (await host.Rig.HeadAsync()).Seq;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_handler_that_swallows_the_refusal_or_does_not_still_answers_503_write_refused(bool swallow)
    {
        var host = await ServingHostAsync();
        var request = PostAsync(host, "commits=1&pause=0" + (swallow ? "&swallow" : string.Empty));
        await host.Entered.Task.WaitAsync(Wait);
        await host.FenceAsync();
        host.Finish.SetResult();

        var response = await request.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("write_refused", await CodeAsync(response));
        Assert.Equal(1, response.Headers.RetryAfter?.Delta?.TotalSeconds);
        Assert.Equal(0, await HeadSeqAsync(host));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public async Task A_request_fenced_between_two_commits_answers_202_with_the_seq_of_the_first_and_an_opted_out_one_does_too(bool swallow, bool optOut)
    {
        var host = await ServingHostAsync();
        var request = PostAsync(host, "commits=2&pause=1" + (swallow ? "&swallow" : string.Empty) + (optOut ? "&optout" : string.Empty));
        await host.Entered.Task.WaitAsync(Wait);
        var first = await HeadSeqAsync(host);
        await host.FenceAsync();
        host.Finish.SetResult();

        var response = await request.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(first.ToString(System.Globalization.CultureInfo.InvariantCulture), Header(response, "Reeve-Seq"));
        Assert.Equal("write_unconfirmed", await CodeAsync(response));
        Assert.Equal(first, await HeadSeqAsync(host));
    }

    [Fact]
    public async Task An_opted_out_request_fenced_after_its_only_commit_answers_its_own_status_with_at_commit()
    {
        var host = await ServingHostAsync();
        var request = PostAsync(host, "commits=1&pause=1&optout");
        await host.Entered.Task.WaitAsync(Wait);
        var seq = await HeadSeqAsync(host);
        await host.FenceAsync();
        host.Finish.SetResult();

        var response = await request.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(seq.ToString(System.Globalization.CultureInfo.InvariantCulture), Header(response, "Reeve-Seq"));
        Assert.Equal("at-commit", Header(response, "Reeve-Replication"));
    }

    [Fact]
    public async Task A_request_with_two_commits_is_awaited_once_by_its_highest_seq_and_the_response_starts_after_the_wait()
    {
        var host = await ServingHostAsync();
        var request = host.Client.SendAsync(new HttpRequestMessage(HttpMethod.Post, "/write?commits=2"), HttpCompletionOption.ResponseHeadersRead);
        var highest = 0L;
        for (var waited = TimeSpan.Zero; highest < 2 && waited < Wait; waited += TimeSpan.FromMilliseconds(20))
        {
            await Task.Delay(20);
            highest = await HeadSeqAsync(host);
        }

        await host.PointAsync("http.before-ack-await");
        await host.EventAsync(LogActions.AckBegin);
        Assert.False(request.IsCompleted, "the response started before the write was acknowledged");
        Assert.Equal(["http.before-ack-await"], host.Points.Reached.Where(p => p.StartsWith("http.before-", StringComparison.Ordinal)));
        Assert.Equal(PassOutcome.Published, await host.Rig.PassAsync());

        using var response = await request.WaitAsync(Wait);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var begin = Assert.Single(host.Rig.Events.All, e => e.Action == LogActions.AckBegin);
        Assert.Equal(highest, begin.State![ObservationKeys.AwaitedSeq]);
        Assert.Equal(["http.before-ack-await", "http.before-response-starts"], host.Points.Reached.Where(p => p.StartsWith("http.before-", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task An_Append_followed_by_a_raw_commit_on_a_fencing_replica_answers_202_and_never_503_with_the_database_unreachable()
    {
        var host = await ServingHostAsync(ackDatabaseDown: true);
        var request = PostAsync(host, "commits=1&pauseTx");
        await host.Entered.Task.WaitAsync(Wait);
        await host.FenceAsync();
        host.Finish.SetResult();

        var response = await request.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("write_unconfirmed", await CodeAsync(response));
        Assert.NotNull(Header(response, "Reeve-Seq"));
        Assert.Equal(1, await HeadSeqAsync(host));
    }

    [Fact]
    public async Task The_filter_does_not_wait_inside_an_open_EF_begun_transaction_and_answers_202_unconfirmed()
    {
        var host = await ServingHostAsync();

        var response = await PostAsync(host, "leaveOpen").WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("write_unconfirmed", await CodeAsync(response));
        Assert.NotNull(Header(response, "Reeve-Seq"));
        Assert.DoesNotContain(host.Rig.Events.All, e => e.Action == LogActions.AckBegin);
    }

    [Fact]
    public async Task A_request_that_leaves_open_an_EF_transaction_with_captured_entries_and_a_joining_context_answers_202_without_waiting()
    {
        var host = await ServingHostAsync();

        var response = await host.Client.PostAsync("/ef?capture&leaveOpen", null).WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("write_unconfirmed", await CodeAsync(response));
        Assert.Null(Header(response, "Reeve-Seq"));
        Assert.DoesNotContain(host.Rig.Events.All, e => e.Action == LogActions.AckBegin);
        Assert.Equal(0, await HeadSeqAsync(host));

        await host.LeftEf!.Value.Transaction.CommitAsync();
        Assert.Equal(1, await HeadSeqAsync(host));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_running_requests_EF_commit_after_fence_step_2_is_refused_whatever_it_captured_and_answers_503_write_refused(bool capture)
    {
        var host = await ServingHostAsync();
        var request = host.Client.PostAsync("/ef?pause" + (capture ? "&capture" : string.Empty), null);
        await host.Entered.Task.WaitAsync(Wait);

        await host.FenceAsync();
        host.Finish.TrySetResult();
        var response = await request.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("write_refused", await CodeAsync(response));
        Assert.Equal(0, await HeadSeqAsync(host));
        Assert.Equal(0L, await host.Rig.Log.ScalarAsync<long>("SELECT sum(value)::bigint FROM public.config"));
    }

    private sealed class Disposing(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
