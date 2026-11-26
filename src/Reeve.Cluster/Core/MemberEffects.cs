namespace Reeve.Cluster.Core;

[Flags]
internal enum MemberEffects : long
{
    None = 0,

    WriteJoiningRow = 1L << 0,

    StartRenewal = 1L << 1,

    CloseToTraffic = 1L << 2,

    RetryJoin = 1L << 3,

    RaiseReadinessReason = 1L << 4,

    RetainCursor = 1L << 5,

    RetryWithBackoff = 1L << 6,

    AbandonActivation = 1L << 7,

    SetServingSince = 1L << 8,

    OpenToTraffic = 1L << 9,

    EnableServingActions = 1L << 10,

    RunFenceSteps = 1L << 11,

    SkipLeaseRelease = 1L << 12,

    SkipDrainRowWrite = 1L << 13,

    ExitAtFenceEnd = 1L << 14,

    MarkExitAfterFence = 1L << 15,

    ExitProcess = 1L << 16,

    RejoinByJoin = 1L << 17,

    RunDrainSteps1To4 = 1L << 18,

    RunDrainSteps2To4 = 1L << 19,

    AnswerPendingUnconfirmedDraining = 1L << 20,

    WriteDrainingRow = 1L << 21,

    StopRenewal = 1L << 22,

    CompleteFenceFirst = 1L << 23,

    ClearReadinessReason = 1L << 24,
}
