namespace Reeve.Cluster.Core;

internal readonly record struct OwnRowObservation(bool Present, long Epoch, MemberRowState State, bool LeaseValid)
{
    public static OwnRowObservation Absent => new(false, 0, MemberRowState.Fenced, false);
}

internal enum RejoinAction
{
    AwaitDatabase,

    AwaitLeaseExpiry,

    AwaitBackoff,

    Rejoin,

    ExitHigherEpoch,
}

internal readonly record struct RejoinDecision(RejoinAction Action, TimeSpan Wait)
{
    public MemberTrigger? Trigger => Action switch
    {
        RejoinAction.Rejoin => MemberTrigger.RejoinPermitted,
        RejoinAction.ExitHigherEpoch => MemberTrigger.RowHasHigherEpoch,
        _ => null,
    };

    public static RejoinDecision Decide(
        OwnRowObservation? row,
        long ownEpoch,
        TimeSpan lastSpan,
        TimeSpan sinceFence,
        TimeSpan leaseTtl,
        TimeSpan rejoinBackoff)
    {
        if (row is not { } observed)
        {
            return new RejoinDecision(RejoinAction.AwaitDatabase, TimeSpan.Zero);
        }

        if (observed.Present && observed.Epoch > ownEpoch)
        {
            return new RejoinDecision(RejoinAction.ExitHigherEpoch, TimeSpan.Zero);
        }

        if (observed.Present && observed.Epoch == ownEpoch && observed.LeaseValid
            && observed.State is MemberRowState.Joining or MemberRowState.Active)
        {
            return new RejoinDecision(RejoinAction.AwaitLeaseExpiry, TimeSpan.Zero);
        }

        if (lastSpan < leaseTtl && sinceFence < rejoinBackoff)
        {
            return new RejoinDecision(RejoinAction.AwaitBackoff, rejoinBackoff - sinceFence);
        }

        return new RejoinDecision(RejoinAction.Rejoin, TimeSpan.Zero);
    }
}
