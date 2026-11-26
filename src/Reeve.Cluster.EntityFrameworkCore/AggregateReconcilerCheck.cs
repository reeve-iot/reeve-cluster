using Microsoft.Extensions.Hosting;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed class AggregateReconcilerCheck(IClusterMapping mapping, IEnumerable<IReconciler> reconcilers) : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        var registered = reconcilers.Select(r => r.AggregateType).ToHashSet(StringComparer.Ordinal);
        var missing = mapping.Entities.Select(e => e.AggregateType).Distinct(StringComparer.Ordinal).Where(t => !registered.Contains(t)).ToArray();
        if (missing.Length > 0)
        {
            throw new ClusterConfigurationException(
                ClusterConfigurationReason.AggregateTypeWithoutReconciler,
                $"Mapped aggregate types have no reconciler: {string.Join(", ", missing)}.");
        }

        return Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
