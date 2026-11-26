using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Int.Membership;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Int.Owned;

public sealed class PlacerHostTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private OwnedDatabase db = null!;
    private IHost host = null!;

    public PlacerHostTests(PostgresFixture postgres) => this.postgres = postgres;

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        var startupBound = TimeSpan.Zero;
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            startupBound = StartupBound.For(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        host = builder.Build();
        await host.StartAsync().WaitAsync(startupBound);
    }

    public async Task DisposeAsync()
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(bound.Token);
        host.Dispose();
    }

    [Fact]
    public async Task A_host_takes_over_the_absent_placer_row_and_places_the_free_row_sweep_on_itself()
    {
        var runtime = host.Services.GetRequiredService<UnitRuntime>();
        using var bound = new CancellationTokenSource(Wait);
        while (!(runtime.Holds(Placement.PlacerKind, Placement.PlacerKind) && runtime.Holds(FreeRowSweep.Kind, FreeRowSweep.Kind)))
        {
            await Task.Delay(50, bound.Token);
        }

        var store = host.Services.GetRequiredService<OwnedStore>();
        var epoch = host.Services.GetRequiredService<LocalPhaseGate>().Epoch;
        var placer = await store.ReadAsync(Placement.PlacerKind, Placement.PlacerKind, bound.Token);
        var sweep = await store.ReadAsync(FreeRowSweep.Kind, FreeRowSweep.Kind, bound.Token);

        Assert.Equal((OwnedCondition.Owned, "r1", epoch), (placer.Condition, placer.Row?.OwnerReplica, placer.Row?.OwnerEpoch));
        Assert.Equal((OwnedCondition.Owned, "r1", epoch), (sweep.Condition, sweep.Row?.OwnerReplica, sweep.Row?.OwnerEpoch));
        Assert.True(sweep.Row!.Value.Token > placer.Row!.Value.Token, "the placer assigns the sweep under a tenure it took first");
    }
}
