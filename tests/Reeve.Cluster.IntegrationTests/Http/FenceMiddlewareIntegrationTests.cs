using System.Net;
using System.Net.Http.Json;
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

public sealed class FenceMiddlewareIntegrationTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public FenceMiddlewareIntegrationTests(PostgresFixture postgres)
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

    private async Task<HttpHostRig> HostAsync(ReplicaPhase phase, TimeSpan? readIndexTimeout = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, o => o.ReadIndexTimeout = readIndexTimeout ?? TimeSpan.FromSeconds(60));
        owned.Add(rig);
        await rig.LoadAsync();
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        if (phase >= ReplicaPhase.Activated && phase != ReplicaPhase.Fenced)
        {
            rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        }

        if (phase is ReplicaPhase.Serving or ReplicaPhase.Fencing or ReplicaPhase.Fenced)
        {
            rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        }

        var index = new ReadIndex(
            rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), TimeProvider.System,
            NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        var host = new HttpHostRig(rig, index);
        owned.Add(host);
        return host;
    }

    private static async Task<Seen> SeenAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<Seen>())!;
    }

    private static async Task<string?> CodeAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(1, response.Headers.RetryAfter?.Delta?.TotalSeconds);
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString();
    }

    [Fact]
    public async Task A_fenced_replica_whose_cursor_is_behind_answers_an_exempt_endpoint_within_one_round_trip_and_refuses_the_rest()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var rig = host.Rig;
        var before = rig.Store.Current!.Head;
        await rig.WriteAsync(("a", "k"), ("b", "k"));
        var head = await rig.HeadAsync();
        Assert.True(head.Seq > before, "the log is ahead of the replica's cursor");
        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.ApplierStall));
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);

        var exempt = await SeenAsync(await host.Client.GetAsync("/exempt"));

        Assert.DoesNotContain(rig.Events.All, e => e.Action == LogActions.QStart);
        var other = await CodeAsync(await host.Client.GetAsync("/plain"));
        var off = await CodeAsync(await host.Client.GetAsync("/off/plain"));

        Assert.Equal(new Seen("ExemptRequest", before), exempt);
        Assert.Equal("replica_fenced", other);
        Assert.Equal("replica_fenced", off);
    }

    [Fact]
    public async Task No_request_is_served_before_the_replica_applied_the_activation_head_and_CatchingUp_does_not_wait()
    {
        var host = await HostAsync(ReplicaPhase.CatchingUp);
        var rig = host.Rig;
        await rig.WriteAsync(("a", "k"));

        var plain = await CodeAsync(await host.Client.GetAsync("/plain"));
        var exempt = await SeenAsync(await host.Client.GetAsync("/exempt"));

        Assert.Equal("replica_not_serving", plain);
        Assert.Equal("ExemptRequest", exempt.Kind);
        Assert.DoesNotContain(rig.Events.All, e => e.Action == LogActions.QStart);
    }

    [Fact]
    public async Task A_serving_replica_holds_a_request_until_the_applier_applied_a_write_committed_before_it()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var rig = host.Rig;
        var old = rig.Store.Current!.Head;
        await rig.WriteAsync(("a", "k"));

        var request = host.Client.GetAsync("/plain");
        await host.EventAsync(LogActions.QStart);
        Assert.False(request.IsCompleted, "the request waits for the write that committed before it");
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        var seen = await SeenAsync(await request.WaitAsync(Wait));
        Assert.Equal("ClientRequest", seen.Kind);
        Assert.True(seen.PinnedHead > old);
        Assert.Equal(rig.Store.Current!.Head, seen.PinnedHead);
    }

    [Fact]
    public async Task An_exempt_request_on_a_serving_replica_waits_on_the_read_index_and_does_not_where_its_group_turned_it_off()
    {
        var host = await HostAsync(ReplicaPhase.Serving, readIndexTimeout: TimeSpan.FromSeconds(1));
        var rig = host.Rig;
        var old = rig.Store.Current!.Head;
        await rig.WriteAsync(("a", "k"));

        var waits = await CodeAsync(await host.Client.GetAsync("/exempt").WaitAsync(Wait));
        var off = await SeenAsync(await host.Client.GetAsync("/off/exempt"));

        Assert.Equal("replica_behind", waits);
        Assert.Equal(new Seen("ExemptRequest", old), off);
    }

    [Fact]
    public async Task A_request_that_outlives_a_publication_finishes_on_the_generation_it_pinned()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var rig = host.Rig;
        var old = rig.Store.Current!.Head;
        var request = host.Client.GetAsync("/slow");
        await host.Entered.Task.WaitAsync(Wait);

        await rig.WriteAsync(("a", "k"), ("b", "k"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.True(rig.Store.Current!.Head > old);
        host.Finish.SetResult();

        var seen = await SeenAsync(await request.WaitAsync(Wait));
        Assert.Equal(old, seen.PinnedHeadAfter);
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
