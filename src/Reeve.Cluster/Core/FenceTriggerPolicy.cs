using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Core;

internal enum FenceDecision
{
    Ignored,

    Fence,

    FenceThenExit,

    ExitAfterFence,

    ExitNow,
}

internal static class FenceTriggerPolicy
{
    public static FenceDecision Decide(ReplicaPhase phase, FenceTrigger trigger, bool drainRowWriteStarted = false) =>
        Judge(phase, trigger, drainRowWriteStarted).Decision;

    public static FenceVerdict Judge(ReplicaPhase phase, FenceTrigger trigger, bool drainRowWriteStarted = false)
    {
        if (phase == ReplicaPhase.Draining && drainRowWriteStarted)
        {
            return Exits(trigger, FenceDecision.ExitNow);
        }

        switch (phase)
        {
            case ReplicaPhase.Starting:
                return Exits(trigger, FenceDecision.ExitNow);

            case ReplicaPhase.Loading:
            case ReplicaPhase.CatchingUp:
                return trigger == FenceTrigger.ApplierStall ? default : Fences(trigger);

            case ReplicaPhase.Activated:
            case ReplicaPhase.Serving:
                return Fences(trigger);

            case ReplicaPhase.Draining:
                return new(FenceDecision.FenceThenExit, ExitCauses.Drain | (trigger == FenceTrigger.StuckAttempt ? ExitCauses.Stuck : ExitCauses.None));

            case ReplicaPhase.Fencing:
                return Exits(trigger, FenceDecision.ExitAfterFence);

            case ReplicaPhase.Fenced:
                return Exits(trigger, FenceDecision.ExitNow);

            default:
                return default;
        }
    }

    public static string? ExitAction(ExitCauses causes) =>
        causes.HasFlag(ExitCauses.Stuck) ? MembershipActions.StuckExit
        : (causes & (ExitCauses.Fault | ExitCauses.Drain)) != 0 ? MembershipActions.FencedExit
        : null;

    private static FenceVerdict Fences(FenceTrigger trigger) =>
        trigger == FenceTrigger.StuckAttempt ? new(FenceDecision.FenceThenExit, ExitCauses.Stuck) : new(FenceDecision.Fence, ExitCauses.None);

    private static FenceVerdict Exits(FenceTrigger trigger, FenceDecision decision) => trigger switch
    {
        FenceTrigger.StuckAttempt => new(decision, ExitCauses.Stuck),
        FenceTrigger.FatalFault => new(decision, ExitCauses.Fault),
        _ => default,
    };
}

internal readonly record struct FenceVerdict(FenceDecision Decision, ExitCauses Cause);

[Flags]
internal enum ExitCauses
{
    None = 0,

    Stuck = 1,

    Fault = 2,

    Drain = 4,

    Stop = 8,
}
