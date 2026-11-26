using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Int.Log;

namespace Int.Http;

public sealed class DrainIntegrationTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public DrainIntegrationTests(PostgresFixture postgres)
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

    [Fact]
    public async Task A_write_in_flight_at_a_graceful_stop_is_acknowledged_normally_and_the_draining_write_comes_after_its_response()
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, o => o.ProxyReadTimeout = TimeSpan.FromSeconds(30));
        owned.Add(rig);
        await rig.LoadAsync();
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        var index = new ReadIndex(
            rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), TimeProvider.System,
            NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        var host = new HttpHostRig(rig, index);
        owned.Add(host);
        var store = new OrderedStore(rig.Members, host);
        var points = new PointProbe();
        var drain = new DrainCoordinator(
            rig.Gate, host.Services.GetServices<IDrainParticipant>(), store, Options.Create(rig.Options), TimeProvider.System, points,
            rig.Events, NullLogger<DrainCoordinator>.Instance);

        var request = host.Client.PostAsync("/write?commits=1&pause=1", null);
        await host.Entered.Task.WaitAsync(Wait);
        var stop = Task.Run(drain.StopAsync);
        for (var waited = TimeSpan.Zero; rig.Gate.Phase != ReplicaPhase.Draining && waited < Wait; waited += TimeSpan.FromMilliseconds(10))
        {
            await Task.Delay(10);
        }

        Assert.Equal(ReplicaPhase.Draining, rig.Gate.Phase);

        using (var late = await host.Client.GetAsync("/plain").WaitAsync(Wait))
        {
            Assert.Equal(HttpStatusCode.ServiceUnavailable, late.StatusCode);
            Assert.Equal("replica_not_serving", JsonDocument.Parse(await late.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
        }

        await WaitForAsync(() => points.Reached.Contains(MembershipFaultPoints.DrainBeforeStep4.Name));
        await Task.Delay(100);
        Assert.False(stop.IsCompleted, "the drain did not wait for the request running at step 1");
        Assert.Empty(store.Draining);
        Assert.DoesNotContain(MembershipFaultPoints.DrainBeforeDrainingWrite.Name, points.Reached);

        host.Finish.SetResult();
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        using var response = await request.WaitAsync(Wait);
        await stop.WaitAsync(Wait);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal([true], store.Draining);
        Assert.Equal(ReplicaPhase.Exited, rig.Gate.Phase);
        Assert.Single(rig.Events.All, e => e.Action == LogActions.AckDone);
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (!condition())
        {
            await Task.Delay(10, bound.Token);
        }
    }

    private sealed class OrderedStore(MemberStore inner, HttpHostRig host) : IMemberStore
    {
        public List<bool> Draining { get; } = [];

        public Task<JoinAnswer?> JoinAsync(JoinRequest request, CancellationToken cancellationToken) => inner.JoinAsync(request, cancellationToken);

        public Task<RenewalAnswer> RenewAsync(string replicaId, long epoch, TimeSpan leaseTtl, CancellationToken cancellationToken) =>
            inner.RenewAsync(replicaId, epoch, leaseTtl, cancellationToken);

        public async Task<bool> WriteDrainingAsync(string replicaId, long epoch, CancellationToken cancellationToken)
        {
            Draining.Add(host.ResponseStarted.Task.IsCompleted);
            var written = await inner.WriteDrainingAsync(replicaId, epoch, cancellationToken);
            Assert.True(written);
            return written;
        }

        public Task<bool> ClearServingSinceAsync(string replicaId, long epoch, CancellationToken cancellationToken) =>
            inner.ClearServingSinceAsync(replicaId, epoch, cancellationToken);
    }
}
