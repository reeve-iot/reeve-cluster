using static Reeve.Cluster.Harness.Stimuli.HarnessOperation;
using static Reeve.Cluster.Harness.Stimuli.StimulusRow;
using static Reeve.Cluster.Harness.Stimuli.StimulusSubject;

namespace Reeve.Cluster.Harness.Stimuli;

internal static class MembershipStimuli
{
    public static readonly StimulusRow Join = At(
        MembershipActions.Join, Replica, "Hold the starting process before and after the compare-and-set that writes its member row.",
        Of(null, MembershipFaultPoints.JoinBefore, MembershipFaultPoints.JoinAfter));

    public static readonly StimulusRow RenewSend = At(
        MembershipActions.RenewSend, Replica, "Hold the renewal timer's attempt before it sends its statement.",
        Of(null, MembershipFaultPoints.RenewSend));

    public static readonly StimulusRow RenewExec = At(
        MembershipActions.RenewExec, Replica, "Hold the attempt after the send and before the database executes the statement; releasing it lets the update run.",
        Of(null, MembershipFaultPoints.RenewExec), enabling: "RenewSend of the same attempt.");

    public static readonly StimulusRow RenewAnswer = At(
        MembershipActions.RenewAnswer, Replica, "Hold the answer after the database executed and before the replica reads it: the late answer of a paused or slow replica.",
        Of(null, MembershipFaultPoints.RenewAnswer), enabling: "RenewExec of the same attempt.");

    public static readonly StimulusRow RenewZeroL = At(
        MembershipActions.RenewZeroL, Replica, "Hold the attempt at the answer and release it: the database answered zero rows, and the replica fences without step 3.",
        Of(null, MembershipFaultPoints.RenewAnswer),
        enabling: "The row is no longer this process's or no longer leased: OperatorDelete, Expire or Restore, or a takeover (HarnessOperations.MutateMemberRow, which has no model action and makes a run unfit for the trace check). The stimulus changes nothing in the database itself.");

    public static readonly StimulusRow ReachFS = Over(
        MembershipActions.ReachFS, Replica, TimeAdvance.ReplicaClock, "Move the replica's monotonic clock past fence start: ClockShift, or let the pause of a Pause row outlast it.",
        enabling: "Pause, when the pause is what carries the clock past fence start.").Proven("I1_the_fence_timer_starts_the_fence_at_fence_start_without_a_check");

    public static readonly StimulusRow FStep = At(
        MembershipActions.FStep, Replica, "Hold the fence before each of steps 1 to 6, around the step-3 lease release and around the fence_history record; release one at a time.",
        Of(
            null,
            MembershipFaultPoints.FenceBeforeStep1,
            MembershipFaultPoints.FenceBeforeStep2,
            MembershipFaultPoints.FenceBeforeStep3,
            MembershipFaultPoints.FenceBeforeStep4,
            MembershipFaultPoints.FenceBeforeStep5,
            MembershipFaultPoints.FenceBeforeStep6,
            MembershipFaultPoints.FenceBeforeLeaseRelease,
            MembershipFaultPoints.FenceAfterLeaseRelease,
            MembershipFaultPoints.FenceBeforeHistoryRecord,
            MembershipFaultPoints.FenceAfterHistoryRecord),
        enabling: "A fence trigger (FenceAtStart, FenceStall, FenceAny, FenceStuck, RenewZeroL) has run step 0.");

    public static readonly StimulusRow FenceAtStart = At(
        MembershipActions.FenceAtStart, Replica, "Hold the fence timer before its call into the gate. Step 0 itself runs inside the gate's flip and cannot be held; a serving check that runs while the timer is held flips it on its own path, so the replica must serve nothing meanwhile.",
        Of(null, MembershipFaultPoints.FenceBeforeStep0), enabling: "ReachFS: the replica's clock has passed fence start.", advance: TimeAdvance.ReplicaClock);

    public static readonly StimulusRow FenceStall = Over(
        MembershipActions.FenceStall, Replica, TimeAdvance.ReplicaClock, "Move the replica's clock past ReconcileTimeout + ApplyStallLimit from the start of the stalled pass.",
        enabling: "AttemptFail or AttemptHang has stalled the applier, in Activated, Serving or Draining.",
        points: Of(null, LogFaultPoints.StallLimitDecision))
        .Proven("Replay_M1_clockack_unanswered_a_stall_whose_step_3_is_not_answered_may_end_the_write_Unconfirmed_Timeout");

    public static readonly StimulusRow FenceAny = Do(
        MembershipActions.FenceAny, Replica, "Report a fatal local fault to the replica's gate (IPhaseGate.ReportFatalFault) at the phase the step is in; in Fencing or Fenced the report is FaultFenced.",
        ReportFatalFault).Proven("FenceAny_a_fatal_fault_reported_to_the_gate_fences_a_replica_that_holds_a_row");

    public static readonly StimulusRow FenceStuck = Over(
        MembershipActions.FenceStuck, Replica, TimeAdvance.ReplicaClock, "Move the replica's clock past the stuck attempt's start + ReconcileTimeout + ApplyStallLimit, whatever ended the applier's wait for it (SIMP-9); in Loading or CatchingUp, where the stall is no trigger.",
        enabling: "AttemptHang with an attempt that ignores its token (HangMode.Stuck, BlockThread or a server-side sleep).",
        points: Of(null, LogFaultPoints.StuckAttemptDecision))
        .Proven("Stuck_in_CatchingUp_an_attempt_that_never_returns_fences_with_FenceStuck_and_ends_the_process_at_its_start_plus_the_allowance");

    public static readonly StimulusRow TimelineFence = Next(
        MembershipActions.TimelineFence, Replica, "Nothing is injected beyond the promotion: the replica's next renewal answer or pass finds another timeline than its join recorded and fences.",
        enabling: "Restore on a new timeline: a promotion of a standby that lacks a suffix, or a switchover.")
        .Proven("I32_TimelineFence_a_pass_that_finds_another_timeline_than_the_joins_fences_and_one_on_it_passes");

    public static readonly StimulusRow FaultFenced = Do(
        MembershipActions.FaultFenced, Replica, "Report a fatal local fault to the replica's gate (IPhaseGate.ReportFatalFault) while it is in Fencing or Fenced (1.3-S1).",
        ReportFatalFaultWhileFenced, enabling: "A fence trigger has run step 0 (Fencing), or the fence has run to step 6 (Fenced).")
        .Proven("S1_a_fault_reported_in_Fenced_exits_at_once");

    public static readonly StimulusRow StuckExit = Over(
        MembershipActions.StuckExit, Replica, TimeAdvance.LetTimePass, "Wait for the process to exit after the fence of a stuck attempt; nothing is injected, the test observes the exit.",
        enabling: "FenceStuck, and the fence's steps to step 6.")
        .Proven("StuckExit_a_stuck_attempt_ends_the_process_after_the_lease_release_with_the_fence_recorded_first");

    public static readonly StimulusRow FencedExit = Next(
        MembershipActions.FencedExit, Replica, "Nothing is injected: the fenced process exits without rejoining; the test observes the exit.",
        enabling: "A fence that took over a drain (Drain1, then a fence trigger), or a fatal local fault reported in Fencing or Fenced (FaultFenced), and the fence has run to step 6.")
        .Proven("FencedExit_a_fatal_fault_reported_in_Fencing_ends_the_process_at_step_6_after_the_steps");

    public static readonly StimulusRow Rejoin = Over(
        MembershipActions.Rejoin, Replica, TimeAdvance.LetTimePass, "Wait out RejoinBackoff (2 s in the short profile) after the fence; the new process joins.",
        enabling: "The replica has fenced and reached Fenced.")
        .Proven("Rejoin_a_fenced_replica_releases_its_lease_records_the_fence_and_rejoins_after_RejoinBackoff_with_a_new_epoch");

    public static readonly StimulusRow Activate = At(
        MembershipActions.Activate, Replica, "Hold the replica before the change-log lock, after the head read, before the update, after it, and before the commit of the activation transaction.",
        Of(
            null,
            MembershipFaultPoints.ActivateBeforeLock,
            MembershipFaultPoints.ActivateAfterHeadRead,
            MembershipFaultPoints.ActivateBeforeUpdate,
            MembershipFaultPoints.ActivateAfterUpdate,
            MembershipFaultPoints.ActivateBeforeCommit),
        enabling: "LoadDone, and the cursor within one pass of the head.");

    public static readonly StimulusRow EnterServing = At(
        MembershipActions.EnterServing, Replica, "Hold the replica between the activation's commit and the applier reaching H; readiness stays false while it is held.",
        Of(null, MembershipFaultPoints.ActivateCommittedBeforeApplied), enabling: "Activate.");

    public static readonly StimulusRow Drain1 = At(
        MembershipActions.Drain1, Replica, "Stop the replica gracefully (StopReplica) and hold it after drain step 1.",
        Of("1.20", MembershipFaultPoints.DrainAfterStep1), StopReplica);

    public static readonly StimulusRow Drain23 = At(
        MembershipActions.Drain23, Replica, "Hold the drain after steps 2 and 3, before step 4's wait for the running requests.",
        Of("1.20", MembershipFaultPoints.DrainBeforeStep4), enabling: "Drain1.");

    public static readonly StimulusRow Drain4 = At(
        MembershipActions.Drain4, Replica, "Hold the drain before the draining write.",
        Of("1.20", MembershipFaultPoints.DrainBeforeDrainingWrite), enabling: "Drain23.");

    public static IReadOnlyList<StimulusRow> All { get; } =
    [
        Join, RenewSend, RenewExec, RenewAnswer, RenewZeroL, ReachFS, FStep, FenceAtStart, FenceStall, FenceAny, FenceStuck,
        TimelineFence, FaultFenced, StuckExit, FencedExit, Rejoin, Activate, EnterServing, Drain1, Drain23, Drain4,
    ];
}
