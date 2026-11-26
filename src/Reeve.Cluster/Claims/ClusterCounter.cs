using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Reeve.Cluster.Claims;

internal sealed partial class ClusterCounter(ICounterRows rows, IOptions<ClusterOptions> options, TimeProvider time, ILogger<ClusterCounter> logger)
    : IClusterCounter
{
    public async Task<CounterResult> IncrementAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        var incremented = await StatementBound.RunAsync(bound => rows.IncrementAsync(key, bound), options, time, cancellationToken).ConfigureAwait(false);
        if (incremented.Cancelled)
        {
            LogUnknown(logger, key);
            return new CounterResult.Unknown();
        }

        return new CounterResult.Incremented(incremented.Value);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The increment of counter {Key} was cancelled by the library's bound before it returned; it may or may not be counted.")]
    private static partial void LogUnknown(ILogger logger, string key);
}
