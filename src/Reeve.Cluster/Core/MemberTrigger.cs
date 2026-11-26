namespace Reeve.Cluster.Core;

internal enum MemberTrigger
{
    FenceStart,

    ApplierStall,

    StuckAttempt,

    FatalFault,

    ZeroRowRenewal,

    TimelineChanged,

    ActivationUnanswered,

    JoinSucceeded,

    JoinRefused,

    StateLoaded,

    LoadFailed,

    ApplyFailed,

    ActivationCommitted,

    ActivationAbandoned,

    ActivationRefusedByFloor,

    AppliedToActivationHead,

    FenceCompleted,

    FenceCompletedToExit,

    RejoinPermitted,

    RowHasHigherEpoch,

    GracefulStop,

    DrainCompleted,
}
