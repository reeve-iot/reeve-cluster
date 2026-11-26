using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Log;

internal sealed class PostCommitWakeup
{
    private readonly IWakeupChannel channel;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;

    public PostCommitWakeup(IWakeupChannel channel, IFaultPoints faultPoints, IModelEventSink events)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);

        this.channel = channel;
        this.faultPoints = faultPoints;
        this.events = events;
    }

    public async ValueTask AfterCommitAsync(long seq, string? xid, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(seq);

        events.Emit(new ModelEvent(LogActions.WCommit, new Dictionary<string, object?>
        {
            [ObservationKeys.Xid] = xid,
            [ObservationKeys.Seq] = seq,
        }));

        try
        {
            await faultPoints.ReachAsync(LogFaultPoints.AfterCommitBeforeWakeup, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }

        await channel.PublishAsync(new Wakeup.Wakeup(WakeupKind.Appended, seq), CancellationToken.None).ConfigureAwait(false);
    }
}
