using Microsoft.Extensions.Logging;

namespace Reeve.Cluster.AspNetCore;

internal sealed partial class AcknowledgementStep(IChangeAck ack, IFaultPoints points, ILogger<AcknowledgementStep> logger)
{
    public async ValueTask<Verdict> RunAsync(IClusterScope scope, CancellationToken cancellationToken, Action? beforeWait = null)
    {
        var view = TrackerView.Of(scope);
        var plan = FencedRequestRule.Plan(view);
        Verdict verdict;
        if (plan.Settled is { } settled)
        {
            verdict = settled;
            if (view.OpenTransaction && !view.CommitRefused && !view.OptedOut && !view.Exempt)
            {
                LogOpenTransaction(logger, view.Highest);
            }
        }
        else
        {
            await points.ReachAsync(HttpFaultPoints.BeforeAckAwait, cancellationToken).ConfigureAwait(false);
            beforeWait?.Invoke();
            verdict = FencedRequestRule.Settle(plan.AwaitSeq, await ack.WaitAppliedAsync(plan.AwaitSeq, cancellationToken).ConfigureAwait(false));
        }

        await points.ReachAsync(HttpFaultPoints.BeforeResponseStarts, cancellationToken).ConfigureAwait(false);
        return verdict;
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "A transaction begun through EF is still open with a write in it (highest seq {Seq}): the acknowledgement cannot be awaited inside it, so the request answers as unconfirmed, and the commit that follows the response may still be refused or fail. Commit before the action returns.")]
    private static partial void LogOpenTransaction(ILogger logger, long? seq);
}
