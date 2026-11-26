using Reeve.Cluster.Core;

namespace Reeve.Cluster.Slots;

internal enum SlotRefusal
{
    None,

    SlotLost,

    OtherTimeline,
}

internal enum SlotHolderOutcome
{
    Passed,

    Refused,

    FenceStartPassed,
}

internal readonly record struct SlotHolderState(
    ReplicaPhase Phase, TimeSpan Now, TimeSpan? FenceStart, bool DrainStep4Passed, long? CurrentEpoch, long GrantedEpoch, bool FenceStartedInGrantedEpoch);

internal static class SlotDecisions
{
    public static SlotHolderOutcome HolderCheck(SlotHolderState state)
    {
        if (state.CurrentEpoch != state.GrantedEpoch || state.FenceStartedInGrantedEpoch)
        {
            return SlotHolderOutcome.Refused;
        }

        switch (state.Phase)
        {
            case ReplicaPhase.Serving:
                return BeforeFenceStart(state);
            case ReplicaPhase.Draining:
                return state.DrainStep4Passed ? SlotHolderOutcome.Passed : BeforeFenceStart(state);
            case ReplicaPhase.Exited:
                return state.DrainStep4Passed ? SlotHolderOutcome.Passed : SlotHolderOutcome.Refused;
            default:
                return SlotHolderOutcome.Refused;
        }
    }

    public static SlotAcquireResult AcquireAnswer(Guid? token, bool cancelledBeforeReturn) => (token, cancelledBeforeReturn) switch
    {
        (_, true) => new SlotAcquireResult.Unknown(),
        ({ } granted, false) => new SlotAcquireResult.Acquired(granted),
        _ => new SlotAcquireResult.Full(),
    };

    public static SlotRenewResult RenewAnswer(bool updated, bool cancelledBeforeReturn) =>
        cancelledBeforeReturn ? SlotRenewResult.Unknown : updated ? SlotRenewResult.Renewed : SlotRenewResult.Ended;

    public static SlotRefusal Refusal(string? sqlState) => sqlState switch
    {
        "RC004" => SlotRefusal.SlotLost,
        "RC002" => SlotRefusal.OtherTimeline,
        _ => SlotRefusal.None,
    };

    private static SlotHolderOutcome BeforeFenceStart(SlotHolderState state) =>
        state.FenceStart is { } start && state.Now < start ? SlotHolderOutcome.Passed : SlotHolderOutcome.FenceStartPassed;
}
