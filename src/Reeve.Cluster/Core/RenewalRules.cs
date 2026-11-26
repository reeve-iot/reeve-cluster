namespace Reeve.Cluster.Core;

internal enum RenewalOutcome
{
    Renewed,

    OtherTimeline,

    Unknown,

    NotMine,
}

internal enum RenewalVerdict
{
    Discarded,

    DeadlineAdvanced,

    DeadlineKept,

    FenceZeroRow,

    FenceOtherTimeline,
}

internal readonly record struct RenewalAnswer(int RowsAffected, long? Timeline);

internal readonly record struct RenewalContext(ReplicaPhase Phase, bool DrainRowWriteStarted, long Epoch, TimeSpan? Deadline);

internal readonly record struct RenewalAttempt(long Epoch, TimeSpan T0);

internal readonly record struct RenewalResolution(RenewalVerdict Verdict, TimeSpan? Deadline);

internal static class RenewalRules
{
    public static RenewalOutcome Classify(RenewalAnswer? answer, bool cancelRequested, TimeSpan elapsed, TimeSpan renewTimeout, long joinedTimeline)
    {
        if (answer is not { } a || cancelRequested || elapsed > renewTimeout)
        {
            return RenewalOutcome.Unknown;
        }

        return a.RowsAffected switch
        {
            1 when a.Timeline is null => throw new ArgumentException("A one-row renewal answer returns the server's timeline.", nameof(answer)),
            1 => a.Timeline == joinedTimeline ? RenewalOutcome.Renewed : RenewalOutcome.OtherTimeline,
            0 => RenewalOutcome.NotMine,
            _ => throw new ArgumentOutOfRangeException(nameof(answer), a.RowsAffected, "A renewal updates one row by primary key."),
        };
    }

    public static bool IsActive(ReplicaPhase phase, bool drainRowWriteStarted) => phase switch
    {
        ReplicaPhase.Loading or ReplicaPhase.CatchingUp or ReplicaPhase.Activated or ReplicaPhase.Serving => true,
        ReplicaPhase.Draining => !drainRowWriteStarted,
        _ => false,
    };

    public static RenewalResolution Resolve(RenewalContext context, RenewalAttempt attempt, RenewalOutcome outcome, LeaseTiming timing)
    {
        if (attempt.Epoch != context.Epoch || !IsActive(context.Phase, context.DrainRowWriteStarted))
        {
            return new RenewalResolution(RenewalVerdict.Discarded, context.Deadline);
        }

        return outcome switch
        {
            RenewalOutcome.Renewed => new RenewalResolution(RenewalVerdict.DeadlineAdvanced, LeaseClock.AdvanceDeadline(context.Deadline, attempt.T0, timing)),
            RenewalOutcome.NotMine => new RenewalResolution(RenewalVerdict.FenceZeroRow, context.Deadline),
            RenewalOutcome.OtherTimeline => new RenewalResolution(RenewalVerdict.FenceOtherTimeline, context.Deadline),
            _ => new RenewalResolution(RenewalVerdict.DeadlineKept, context.Deadline),
        };
    }
}
