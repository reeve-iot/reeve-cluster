using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Reeve.Cluster.Owned;

internal sealed class FreeRowSweep(OwnedStore store, IOptions<ClusterOptions> options, TimeProvider time, ILogger<FreeRowSweep> logger) : IOwnedUnit
{
    public const string Kind = "reeve.free-row-sweep";

    internal const int PassLimit = 1000;

    public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await PassAsync(stop).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException && !stop.IsCancellationRequested)
            {
                logger.LogWarning(exception, "A pass of the free-row sweep failed; the next pass reads again.");
            }

            await Task.Delay(options.Value.RetentionInterval, time, stop).ConfigureAwait(false);
        }
    }

    public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

    internal async Task<int> PassAsync(CancellationToken cancellationToken)
    {
        var bound = options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(2);
        IReadOnlyList<(string Kind, string Key, long Token)> rows;
        using (var timer = new CancellationTokenSource(bound, time))
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token))
        {
            rows = await store.ReadSweepableAsync(PassLimit, linked.Token).ConfigureAwait(false);
        }

        var deleted = 0;
        foreach (var (kind, key, token) in rows)
        {
            using var timer = new CancellationTokenSource(bound, time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            if (await store.SweepAsync(kind, key, token, linked.Token).ConfigureAwait(false))
            {
                deleted++;
            }
        }

        if (deleted > 0)
        {
            logger.LogInformation("The free-row sweep deleted {Count} rows.", deleted);
        }

        return deleted;
    }
}
