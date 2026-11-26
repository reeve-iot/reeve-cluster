using Reeve.Cluster.Membership;
using static Reeve.Cluster.Core.MemberEffects;

namespace Reeve.Cluster.Core;

internal enum RowKind
{
    Any,

    Unchanged,

    Free,

    Held,

    Joining,

    Active,

    Fenced,

    Draining,
}

internal readonly record struct MemberTransition(
    string? RowId,
    string? ModelAction,
    ReplicaPhase From,
    MemberTrigger Trigger,
    RowKind FromRow,
    ReplicaPhase To,
    RowKind ToRow,
    MemberEffects Effects,
    bool AfterDrainWrite = false)
{
    public bool IsIgnored => RowId is null;
}

internal static class MemberStateMachine
{
    private const string StopEarly = nameof(StopEarly);

    private static readonly IReadOnlyList<MemberTransition> Cells = Build();

    private static readonly Dictionary<(ReplicaPhase, MemberTrigger, bool), MemberTransition> Index =
        Cells.ToDictionary(c => (c.From, c.Trigger, c.AfterDrainWrite));

    public static IReadOnlyList<MemberTransition> Table => Cells;

    public static MemberTransition Decide(ReplicaPhase phase, MemberTrigger trigger, bool drainRowWriteStarted = false) =>
        Index.TryGetValue((phase, trigger, drainRowWriteStarted && phase == ReplicaPhase.Draining), out var cell)
            ? cell
            : new MemberTransition(null, null, phase, trigger, RowKind.Any, phase, RowKind.Unchanged, None);

    private static List<MemberTransition> Build()
    {
        var cells = new List<MemberTransition>();

        void Cell(string row, string? action, ReplicaPhase from, MemberTrigger trigger, RowKind fromRow, ReplicaPhase to, RowKind toRow, MemberEffects effects) =>
            cells.Add(new MemberTransition(row, action, from, trigger, fromRow, to, toRow, effects));

        Cell("M01", MembershipActions.Join, ReplicaPhase.Starting, MemberTrigger.JoinSucceeded, RowKind.Free, ReplicaPhase.Loading, RowKind.Joining,
            WriteJoiningRow | StartRenewal | CloseToTraffic | ClearReadinessReason);
        Cell("M02", null, ReplicaPhase.Starting, MemberTrigger.JoinRefused, RowKind.Held, ReplicaPhase.Starting, RowKind.Unchanged,
            RetryJoin | RaiseReadinessReason);

        Cell("M03", MembershipActions.FencedExit, ReplicaPhase.Starting, MemberTrigger.FatalFault, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);

        Cell("M04", LogActions.LoadDone, ReplicaPhase.Loading, MemberTrigger.StateLoaded, RowKind.Joining, ReplicaPhase.CatchingUp, RowKind.Joining, RetainCursor);
        Cell("M04", null, ReplicaPhase.Loading, MemberTrigger.LoadFailed, RowKind.Joining, ReplicaPhase.Loading, RowKind.Joining, RetryWithBackoff);
        Cell("M04", null, ReplicaPhase.Loading, MemberTrigger.ApplierStall, RowKind.Joining, ReplicaPhase.Loading, RowKind.Joining, RetryWithBackoff);

        Cell("M05", null, ReplicaPhase.CatchingUp, MemberTrigger.ApplyFailed, RowKind.Joining, ReplicaPhase.CatchingUp, RowKind.Joining, RetryWithBackoff);
        Cell("M05", null, ReplicaPhase.CatchingUp, MemberTrigger.ApplierStall, RowKind.Joining, ReplicaPhase.CatchingUp, RowKind.Joining, RetryWithBackoff);

        Cell("M06", MembershipActions.Activate, ReplicaPhase.CatchingUp, MemberTrigger.ActivationCommitted, RowKind.Joining, ReplicaPhase.Activated, RowKind.Active, ClearReadinessReason);
        Cell("M06", null, ReplicaPhase.CatchingUp, MemberTrigger.ActivationAbandoned, RowKind.Joining, ReplicaPhase.CatchingUp, RowKind.Unchanged, AbandonActivation);
        Cell("M06", null, ReplicaPhase.CatchingUp, MemberTrigger.ActivationRefusedByFloor, RowKind.Joining, ReplicaPhase.CatchingUp, RowKind.Joining, RaiseReadinessReason);

        Cell("M07", MembershipActions.EnterServing, ReplicaPhase.Activated, MemberTrigger.AppliedToActivationHead, RowKind.Active, ReplicaPhase.Serving, RowKind.Active,
            SetServingSince | OpenToTraffic | EnableServingActions);

        var fencing = new[]
        {
            MemberTrigger.FenceStart, MemberTrigger.ApplierStall, MemberTrigger.StuckAttempt, MemberTrigger.FatalFault, MemberTrigger.ZeroRowRenewal,
            MemberTrigger.TimelineChanged, MemberTrigger.ActivationUnanswered,
        };

        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining })
        {
            var rowOfPhase = phase is ReplicaPhase.Loading or ReplicaPhase.CatchingUp ? RowKind.Joining : RowKind.Active;
            foreach (var trigger in fencing)
            {
                if (trigger == MemberTrigger.ApplierStall && phase is ReplicaPhase.Loading or ReplicaPhase.CatchingUp)
                {
                    continue;
                }

                var effects = RunFenceSteps
                    | (trigger == MemberTrigger.ZeroRowRenewal ? SkipLeaseRelease : None)
                    | (phase == ReplicaPhase.Draining ? SkipDrainRowWrite | ExitAtFenceEnd : None)
                    | (trigger == MemberTrigger.StuckAttempt ? ExitAtFenceEnd : None);

                var rowAfter = trigger == MemberTrigger.ZeroRowRenewal ? RowKind.Unchanged : RowKind.Fenced;
                var action = trigger switch
                {
                    MemberTrigger.FenceStart => MembershipActions.FenceAtStart,
                    MemberTrigger.ApplierStall => MembershipActions.FenceStall,
                    MemberTrigger.FatalFault => MembershipActions.FenceAny,
                    MemberTrigger.ActivationUnanswered => MembershipActions.FenceAny,
                    MemberTrigger.ZeroRowRenewal => MembershipActions.RenewAnswer,
                    MemberTrigger.TimelineChanged => MembershipActions.RenewAnswer,
                    _ => MembershipActions.FenceStuck,
                };
                Cell("M08", action, phase, trigger, rowOfPhase, ReplicaPhase.Fencing, rowAfter, effects);
            }
        }

        Cell("M08", MembershipActions.FStep, ReplicaPhase.Fencing, MemberTrigger.FenceCompleted, RowKind.Any, ReplicaPhase.Fenced, RowKind.Unchanged, None);
        Cell("M08", null, ReplicaPhase.Fencing, MemberTrigger.FenceCompletedToExit, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);
        Cell("M08", null, ReplicaPhase.Fencing, MemberTrigger.StuckAttempt, RowKind.Any, ReplicaPhase.Fencing, RowKind.Unchanged, MarkExitAfterFence);
        Cell("M08", null, ReplicaPhase.Fencing, MemberTrigger.FatalFault, RowKind.Any, ReplicaPhase.Fencing, RowKind.Unchanged, MarkExitAfterFence);

        Cell("M09", MembershipActions.Rejoin, ReplicaPhase.Fenced, MemberTrigger.RejoinPermitted, RowKind.Any, ReplicaPhase.Starting, RowKind.Unchanged, RejoinByJoin);
        Cell("M10", MembershipActions.Rejoin, ReplicaPhase.Fenced, MemberTrigger.RowHasHigherEpoch, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);

        Cell("M11", MembershipActions.FencedExit, ReplicaPhase.Fenced, MemberTrigger.FatalFault, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);
        Cell("M11", MembershipActions.StuckExit, ReplicaPhase.Fenced, MemberTrigger.StuckAttempt, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);
        Cell("M11", MembershipActions.StuckExit, ReplicaPhase.Starting, MemberTrigger.StuckAttempt, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);

        Cell("M12", MembershipActions.Drain1, ReplicaPhase.Serving, MemberTrigger.GracefulStop, RowKind.Active, ReplicaPhase.Draining, RowKind.Active, RunDrainSteps1To4);
        Cell("M13", MembershipActions.Drain4, ReplicaPhase.Draining, MemberTrigger.DrainCompleted, RowKind.Active, ReplicaPhase.Exited, RowKind.Draining,
            AnswerPendingUnconfirmedDraining | WriteDrainingRow | StopRenewal | ExitProcess);

        foreach (var trigger in new[] { MemberTrigger.FatalFault, MemberTrigger.StuckAttempt })
        {
            cells.Add(new MemberTransition("M13", null, ReplicaPhase.Draining, trigger, RowKind.Draining, ReplicaPhase.Exited, RowKind.Draining, ExitProcess, AfterDrainWrite: true));
        }

        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp })
        {
            Cell("M14", StopEarly, phase, MemberTrigger.GracefulStop, RowKind.Joining, ReplicaPhase.Exited, RowKind.Draining,
                WriteDrainingRow | StopRenewal | ExitProcess);
        }

        Cell("M15", StopEarly, ReplicaPhase.Activated, MemberTrigger.GracefulStop, RowKind.Active, ReplicaPhase.Exited, RowKind.Draining,
            RunDrainSteps2To4 | AnswerPendingUnconfirmedDraining | WriteDrainingRow | StopRenewal | ExitProcess);

        Cell("M16", null, ReplicaPhase.Starting, MemberTrigger.GracefulStop, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);
        Cell("M16", null, ReplicaPhase.Fencing, MemberTrigger.GracefulStop, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, CompleteFenceFirst | ExitProcess);
        Cell("M16", null, ReplicaPhase.Fenced, MemberTrigger.GracefulStop, RowKind.Any, ReplicaPhase.Exited, RowKind.Unchanged, ExitProcess);

        return cells;
    }
}
