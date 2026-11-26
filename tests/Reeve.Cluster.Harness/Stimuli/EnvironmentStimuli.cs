using static Reeve.Cluster.Harness.Stimuli.HarnessOperation;
using static Reeve.Cluster.Harness.Stimuli.StimulusRow;
using static Reeve.Cluster.Harness.Stimuli.StimulusSubject;

namespace Reeve.Cluster.Harness.Stimuli;

internal static class EnvironmentStimuli
{
    public static readonly StimulusRow Tick = Over(
        EnvironmentActions.Tick, Fleet, TimeAdvance.LetTimePass, "Real time passes. The model's tick constants (lease, renew interval, renew timeout, safety margin) are mapped to the profile's durations by the replay, and an event is asserted inside a window of them, never at an instant.");

    public static readonly StimulusRow Pause = Do(
        EnvironmentActions.Pause, Replica, "Freeze the replica whole: Replica.PauseAsync holds its points, timers and network while its clock runs.",
        PauseReplica);

    public static readonly StimulusRow Resume = Do(
        EnvironmentActions.Resume, Replica, "Thaw it: Replica.ResumeAsync; due timers fire and the held answers arrive.",
        ResumeReplica, enabling: "Pause.");

    public static readonly StimulusRow Expire = Over(
        EnvironmentActions.Expire, Replica, TimeAdvance.LetTimePass, "Let the lease run out on the database's clock: LeaseTtl after the last renewal that executed.",
        enabling: "Nothing renews it: Crash, or Pause with the replica's network held, or a cut proxy.");

    public static readonly StimulusRow Crash = Do(
        EnvironmentActions.Crash, Replica, "Replica.CrashAsync: the process is gone, its lease unreleased, its proxies cut.",
        CrashReplica);

    public static readonly StimulusRow Restart = Do(
        EnvironmentActions.Restart, Replica, "Start a new process under the same replica id: HarnessRun.StartReplicaAsync.",
        RestartReplica, enabling: "Crash, or the fence of a stuck attempt (StuckExit).");

    public static readonly StimulusRow StopEarly = Do(
        EnvironmentActions.StopEarly, Replica, "Stop the replica gracefully (Replica.StopAsync).",
        StopReplicaEarly, enabling: "The replica is in Loading, CatchingUp or Activated: hold it at a LoadDone or Activate point first.");

    public static readonly StimulusRow OperatorDelete = Do(
        EnvironmentActions.OperatorDelete, Replica, "DELETE the replica's row from reeve_cluster.member, as an operator would.",
        HarnessOperation.OperatorDelete);

    public static readonly StimulusRow Restore = Do(
        EnvironmentActions.Restore, Fleet, "Restore the database to a backup below the replicas' cursors: HarnessOperations.RestoreDatabase, with every replica stopped (SIMP-10).",
        RestoreDatabase);

    public static readonly StimulusRow Retention = Over(
        EnvironmentActions.Retention, Fleet, TimeAdvance.LetTimePass, "Set a short RetentionInterval and wait for the prefix delete below the lowest cursor.",
        points: Of(null, LogFaultPoints.RetentionBetweenMinimumAndDelete, LogFaultPoints.RetentionBeforeHorizonUpdate));

    public static readonly StimulusRow FloorRaise = Do(
        EnvironmentActions.FloorRaise, Fleet, "UPDATE reeve_cluster.cluster SET protocol_floor, as the version gate's activation would; it takes no change-log lock.",
        RaiseProtocolFloor);

    public static readonly StimulusRow FixFault = Do(
        EnvironmentActions.FixFault, Fleet, "Deploy the fixed reconciler: the poison write stops failing (HarnessOperations.FixFault clears the scripted throws and hangs not yet used, on every replica).",
        FixReconcilerFault, enabling: "AttemptFail or AttemptHang.");

    public static IReadOnlyList<StimulusRow> All { get; } =
    [
        Tick, Pause, Resume, Expire, Crash, Restart, StopEarly, OperatorDelete, Restore, Retention, FloorRaise, FixFault,
    ];
}
