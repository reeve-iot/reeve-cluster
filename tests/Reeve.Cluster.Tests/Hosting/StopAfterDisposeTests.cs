using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Unit.Hosting;

public sealed class StopAfterDisposeTests
{
    private sealed class OneReconciler : IReconciler
    {
        public string AggregateType => "one";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("one"));
    }

    private sealed class StubLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }

    public static TheoryData<Type> Services => new() { typeof(CommandPickup), typeof(PlacerAdopter), typeof(UnitRuntime), typeof(FenceCoordinator) };

    [Theory]
    [MemberData(nameof(Services))]
    public async Task A_service_stopped_twice_or_after_its_disposal_stops_without_throwing(Type type)
    {
        var services = new ServiceCollection().AddLogging().AddSingleton<IHostApplicationLifetime, StubLifetime>().AddReeveCluster(
            new ClusterPorts([typeof(OneReconciler)], typeof(NoFenceSink)),
            o =>
            {
                o.ReplicaId = "replica-1";
                o.ConnectionString = "Host=db;Database=app";
                o.ReconcileTimeout = TimeSpan.FromSeconds(10);
            });
        await using var provider = services.BuildServiceProvider();
        var service = (IHostedService)provider.GetRequiredService(type);

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);
        ((IDisposable)service).Dispose();

        await service.StopAsync(CancellationToken.None);
    }
}
