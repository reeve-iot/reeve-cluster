using Microsoft.Extensions.Options;

namespace Reeve.Cluster.Status;

internal sealed class ClusterStatus(IStatusStore store, IOptions<ClusterOptions> options, TimeProvider time) : IClusterStatus
{
    public async Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken = default)
    {
        var budget = options.Value.ActionTimeBudget;
        if (budget is not { } limit)
        {
            return await store.ReadAsync(cancellationToken).ConfigureAwait(false);
        }

        using var timeout = new CancellationTokenSource(limit, time);
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await store.ReadAsync(bound.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"The status could not be read within ActionTimeBudget ({limit}).", exception);
        }
    }
}
