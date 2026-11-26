using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Claims;

internal sealed partial class ClusterBudget(
    IBudgetRows rows, IClaimGate gate, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<ClusterBudget> logger)
    : IClusterBudget
{
    public async Task<BudgetResult> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(id);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(period, TimeSpan.FromMilliseconds(1));

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await points.ReachAsync(ClaimsFaultPoints.BudgetBeforeConsume, cancellationToken).ConfigureAwait(false);
        if (gate.CheckServing() is null)
        {
            return new BudgetResult.NotServing();
        }

        var consumed = await StatementBound.RunAsync(bound => rows.ConsumeAsync(key, id, limit, period, bound), options, time, cancellationToken).ConfigureAwait(false);
        if (consumed.Cancelled)
        {
            LogUnknown(logger, key);
        }

        return BudgetDecisions.Verdict(consumed.Value, consumed.Cancelled);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The consume of budget {Key} was cancelled by the library's bound before it returned; it is not a grant.")]
    private static partial void LogUnknown(ILogger logger, string key);
}
