using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class LocalPhaseGateTests : IDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private const long Epoch = 7;
    private const long Timeline = 1;

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly LocalPhaseGate gate;
    private readonly List<PhaseEvent> phaseEvents = [];
    private readonly List<FenceRequest> fences = [];
    private readonly List<FenceRequest> exits = [];
    private readonly List<string> order = [];

    public LocalPhaseGateTests()
    {
        gate = new LocalPhaseGate(Timing, time, events, NullLogger<LocalPhaseGate>.Instance);
        gate.Subscribe(e => { phaseEvents.Add(e); order.Add($"phase:{e.To}"); });
        gate.OnStepZero(r => order.Add("step0"));
        gate.OnFenceRequested(r => { fences.Add(r); order.Add("fence"); });
        gate.OnExitRequested(r => { exits.Add(r); order.Add("exit"); });
    }

    public void Dispose() => gate.Dispose();

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private void Join() => Assert.True(gate.Joined(Epoch, Timeline, gate.Now));

    private void ToServing() => ToServing(gate);

    private static void ToServing(LocalPhaseGate g)
    {
        Assert.True(g.Joined(Epoch, Timeline, g.Now));
        g.Apply(MemberTrigger.StateLoaded);
        g.Apply(MemberTrigger.ActivationCommitted);
        g.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, g.Phase);
    }

    [Fact]
    public void M01_a_join_enters_Loading_records_its_epoch_and_timeline_and_sets_the_deadline_from_t0()
    {
        time.Advance(S(5));
        var t0 = gate.Now;
        time.Advance(S(1));

        Assert.True(gate.Joined(Epoch, Timeline, t0));

        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.Equal(Epoch, gate.Epoch);
        Assert.Equal(Timeline, gate.JoinedTimeline);
        Assert.Equal(t0 + S(27), gate.Deadline);
        Assert.Equal(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading), Assert.Single(phaseEvents));
        var join = Assert.Single(events.Of(MembershipActions.Join));
        Assert.Equal(Epoch, join.State!["epoch"]);
        Assert.Equal(Timeline, join.State!["timeline"]);
    }

    [Fact]
    public void A_join_is_refused_by_the_gate_outside_Starting()
    {
        Join();

        Assert.False(gate.Joined(Epoch + 1, Timeline, gate.Now));
        Assert.Equal(Epoch, gate.Epoch);
    }

    [Fact]
    public void I1_the_serving_check_refuses_at_and_after_fence_start_and_starts_the_fence()
    {
        using var paused = new LocalPhaseGate(Timing, new TimerlessTimeProvider(time), events, NullLogger<LocalPhaseGate>.Instance);
        var fenced = new List<FenceRequest>();
        paused.OnFenceRequested(fenced.Add);
        ToServing(paused);
        time.Advance(S(22) - TimeSpan.FromTicks(1));
        Assert.Equal(ServingCheckOutcome.Serving, paused.CheckServing().Outcome);
        Assert.Empty(fenced);

        time.Advance(TimeSpan.FromTicks(1));
        var result = paused.CheckServing();

        Assert.Equal(ServingCheckOutcome.FenceStartPassed, result.Outcome);
        Assert.Equal(ReplicaPhase.Fencing, paused.Phase);
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fenced).Trigger);
        Assert.Equal(ServingCheckOutcome.NotServing, paused.CheckServing().Outcome);
        Assert.Single(fenced);
    }

    [Fact]
    public void I1_at_fence_start_the_timer_has_fenced_and_the_check_refuses()
    {
        ToServing();
        time.Advance(S(22));

        Assert.Equal(ServingCheckOutcome.NotServing, gate.CheckServing().Outcome);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Single(fences);
    }

    [Fact]
    public void I1_a_check_that_runs_before_the_timer_after_a_pause_fences_before_the_action()
    {
        using var paused = new LocalPhaseGate(Timing, new TimerlessTimeProvider(time), events, NullLogger<LocalPhaseGate>.Instance);
        var fenced = new List<FenceRequest>();
        paused.OnFenceRequested(fenced.Add);
        ToServing(paused);
        time.Advance(S(60));
        Assert.Equal(ReplicaPhase.Serving, paused.Phase);

        Assert.Equal(ServingCheckOutcome.FenceStartPassed, paused.CheckServing().Outcome);
        Assert.Equal(ReplicaPhase.Fencing, paused.Phase);
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fenced).Trigger);
        Assert.Single(events.Of(MembershipActions.ReachFS));
    }

    [Fact]
    public void I1_the_serving_check_refuses_outside_Serving_without_fencing()
    {
        Assert.Equal(new ServingCheckResult(ServingCheckOutcome.NotServing, ReplicaPhase.Starting), gate.CheckServing());
        Join();
        Assert.Equal(ServingCheckOutcome.NotServing, gate.CheckServing().Outcome);
        Assert.Empty(fences);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    [Fact]
    public void I1_the_fence_timer_starts_the_fence_at_fence_start_without_a_check()
    {
        Join();
        time.Advance(S(22) - TimeSpan.FromTicks(1));
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);

        time.Advance(TimeSpan.FromTicks(1));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fences).Trigger);
        Assert.Single(events.Of(MembershipActions.ReachFS));
        Assert.Null(gate.FenceStartTimestamp);
    }

    [Fact]
    public void I23_a_renewal_moves_fence_start_and_the_timer_with_it()
    {
        Join();
        time.Advance(S(10));
        var verdict = gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Renewed);
        Assert.Equal(RenewalVerdict.DeadlineAdvanced, verdict.Verdict);

        time.Advance(S(21));
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);

        time.Advance(S(1));
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public void Only_a_counted_one_row_renewal_reaches_the_renewal_subscribers()
    {
        Join();
        var renewed = new List<long>();
        using var subscription = gate.OnRenewed(renewed.Add);

        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Unknown);
        gate.ApplyRenewal(new RenewalAttempt(Epoch - 1, gate.Now), RenewalOutcome.Renewed);
        Assert.Empty(renewed);

        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Renewed);
        Assert.Equal([Epoch], renewed);

        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.OtherTimeline);
        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Renewed);
        Assert.Equal([Epoch], renewed);
    }

    [Fact]
    public void I1_entering_Serving_past_fence_start_fences_instead()
    {
        using var paused = new LocalPhaseGate(Timing, new TimerlessTimeProvider(time), events, NullLogger<LocalPhaseGate>.Instance);
        var seen = new List<PhaseEvent>();
        paused.Subscribe(seen.Add);
        Assert.True(paused.Joined(Epoch, Timeline, paused.Now));
        paused.Apply(MemberTrigger.StateLoaded);
        paused.Apply(MemberTrigger.ActivationCommitted);
        time.Advance(S(22));

        var cell = paused.Apply(MemberTrigger.AppliedToActivationHead);

        Assert.True(cell.IsIgnored);
        Assert.Equal(ReplicaPhase.Fencing, paused.Phase);
        Assert.DoesNotContain(seen, e => e.To == ReplicaPhase.Serving);
        Assert.Single(events.Of(MembershipActions.ReachFS));
        Assert.Null(paused.FenceStartTimestamp);
    }

    [Fact]
    public void FenceStartTimestamp_is_fence_start_on_the_time_providers_timestamp_scale()
    {
        Assert.Null(gate.FenceStartTimestamp);
        Join();

        var remaining = time.GetElapsedTime(time.GetTimestamp(), gate.FenceStartTimestamp!.Value);

        Assert.Equal(S(22), remaining);
    }

    [Fact]
    public void I23_a_zero_row_renewal_fences_skipping_step_3_and_leaves_the_deadline()
    {
        Join();
        var deadline = gate.Deadline;

        var r = gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.NotMine);

        Assert.Equal(RenewalVerdict.FenceZeroRow, r.Verdict);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.ZeroRowRenewal, Assert.Single(fences).Trigger);
        Assert.Equal(deadline, gate.Deadline);
        var zero = Assert.Single(events.Of(MembershipActions.RenewZeroL));
        Assert.Equal(Epoch, zero.State!["epoch"]);
        Assert.Equal("zero", zero.State["res"]);
    }

    [Fact]
    public void I32_a_one_row_answer_on_another_timeline_raises_the_fence_trigger_and_does_not_advance_the_deadline()
    {
        Join();
        var deadline = gate.Deadline;
        time.Advance(S(3));

        var r = gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.OtherTimeline);

        Assert.Equal(RenewalVerdict.FenceOtherTimeline, r.Verdict);
        Assert.Equal(deadline, gate.Deadline);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.Timeline, Assert.Single(fences).Trigger);
        Assert.Equal("other", Assert.Single(events.Of(MembershipActions.RenewAnswer)).State!["res"]);
    }

    [Fact]
    public void I23_I33_an_unknown_answer_leaves_the_deadline_and_emits_no_answer()
    {
        Join();
        var deadline = gate.Deadline;
        time.Advance(S(3));

        var r = gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Unknown);

        Assert.Equal(RenewalVerdict.DeadlineKept, r.Verdict);
        Assert.Equal(deadline, gate.Deadline);
        Assert.Empty(events.Of(MembershipActions.RenewAnswer));
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    [Fact]
    public void I13_I23_an_answer_of_an_earlier_epoch_or_after_step_0_counts_for_nothing()
    {
        Join();
        Assert.Equal(RenewalVerdict.Discarded, gate.ApplyRenewal(new RenewalAttempt(Epoch - 1, gate.Now), RenewalOutcome.NotMine).Verdict);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);

        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault));
        var deadline = gate.Deadline;
        time.Advance(S(1));
        Assert.Equal(RenewalVerdict.Discarded, gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.Renewed).Verdict);
        Assert.Equal(deadline, gate.Deadline);
        Assert.Single(fences);
    }

    [Fact]
    public void I23_renewal_runs_only_in_the_phases_that_renew_and_names_the_joined_epoch_and_timeline()
    {
        Assert.False(gate.TryGetRenewal(out _));
        Join();
        Assert.True(gate.TryGetRenewal(out var target));
        Assert.Equal((Epoch, Timeline), (target.Epoch, target.Timeline));

        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault));
        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public void I23_no_attempt_starts_after_the_draining_write_has_started()
    {
        ToServing();
        gate.Apply(MemberTrigger.GracefulStop);
        Assert.True(gate.TryGetRenewal(out _));

        Assert.True(gate.BeginDrainStep4());

        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public void Step_0_flips_the_phase_then_runs_the_hooks_then_hands_the_request_to_the_coordinator()
    {
        Join();
        order.Clear();

        Assert.Equal(FenceDecision.Fence, gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart)));

        Assert.Equal(["phase:Fencing", "step0", "fence"], order);
    }

    [Fact]
    public void I23_a_fence_is_never_undone_a_second_trigger_adds_nothing()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        Assert.Equal(FenceDecision.Ignored, gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal)));
        Assert.Equal(FenceDecision.Ignored, gate.RequestFence(new FenceRequest(FenceTrigger.Timeline)));
        Assert.Single(fences);
        Assert.False(gate.ExitAfterFence);
    }

    [Fact]
    public void A_fence_that_is_not_marked_to_exit_ends_in_Fenced()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal));

        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
        Assert.Empty(exits);
    }

    [Fact]
    public void S1_a_fault_reported_in_Fencing_ends_in_Exited_never_in_a_rejoin()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        Assert.Equal(FenceDecision.ExitAfterFence, gate.ReportFatalFault("disk full"));
        Assert.True(gate.ExitAfterFence);
        Assert.Equal("Fencing", Assert.Single(events.Of(MembershipActions.FaultFenced)).State!["phase"]);
        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.DoesNotContain(phaseEvents, e => e.To == ReplicaPhase.Fenced);
        Assert.True(gate.Apply(MemberTrigger.RejoinPermitted).IsIgnored);
    }

    [Fact]
    public void S1_a_fault_reported_in_Fenced_exits_at_once()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();

        Assert.Equal(FenceDecision.ExitNow, gate.ReportFatalFault("disk full"));

        Assert.Equal("Fenced", Assert.Single(events.Of(MembershipActions.FaultFenced)).State!["phase"]);
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Equal(FenceTrigger.FatalFault, Assert.Single(exits).Trigger);
        Assert.Equal("disk full", exits[0].Detail);
        Assert.True(gate.Apply(MemberTrigger.RejoinPermitted).IsIgnored);
    }

    [Fact]
    public void S1_a_fault_reported_in_Starting_exits_at_once()
    {
        Assert.Equal(FenceDecision.ExitNow, gate.ReportFatalFault("no config"));

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Single(exits);
        Assert.Empty(events.Of(MembershipActions.FaultFenced));
        Assert.Empty(fences);
        Assert.False(gate.Joined(Epoch, Timeline, gate.Now));
    }

    [Fact]
    public void FenceAny_a_fatal_fault_reported_to_the_gate_fences_a_replica_that_holds_a_row()
    {
        Join();

        Assert.Equal(FenceDecision.Fence, gate.ReportFatalFault("cache corrupt"));

        Assert.Equal(FenceTrigger.FatalFault, Assert.Single(fences).Trigger);
        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
    }

    [Fact]
    public void The_serving_stamp_lives_from_M07_until_Serving_is_left_across_a_fence_a_rejoin_and_a_drain()
    {
        Assert.Null(gate.ServingEpoch);
        Assert.True(gate.Joined(1, Timeline, gate.Now));
        AssertStamp(ReplicaPhase.Loading, null);
        gate.Apply(MemberTrigger.StateLoaded);
        AssertStamp(ReplicaPhase.CatchingUp, null);
        gate.Apply(MemberTrigger.ActivationCommitted);
        AssertStamp(ReplicaPhase.Activated, null);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        AssertStamp(ReplicaPhase.Serving, 1);
        Assert.Equal(1, gate.CheckServing().Epoch);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        AssertStamp(ReplicaPhase.Fencing, null);
        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
        AssertStamp(ReplicaPhase.Fenced, null);
        gate.Apply(MemberTrigger.RejoinPermitted);
        AssertStamp(ReplicaPhase.Starting, null);

        Assert.True(gate.Joined(2, Timeline, gate.Now));
        AssertStamp(ReplicaPhase.Loading, null);
        gate.Apply(MemberTrigger.StateLoaded);
        AssertStamp(ReplicaPhase.CatchingUp, null);
        gate.Apply(MemberTrigger.ActivationCommitted);
        AssertStamp(ReplicaPhase.Activated, null);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        AssertStamp(ReplicaPhase.Serving, 2);
        Assert.Equal(2, gate.CheckServing().Epoch);

        gate.Apply(MemberTrigger.GracefulStop);
        AssertStamp(ReplicaPhase.Draining, null);

        void AssertStamp(ReplicaPhase phase, long? stamp)
        {
            Assert.Equal(phase, gate.Phase);
            Assert.Equal(stamp, gate.ServingEpoch);
        }
    }

    [Fact]
    public void S2_a_fence_that_took_over_a_drain_ends_in_Exited_never_in_a_rejoin()
    {
        ToServing();
        gate.Apply(MemberTrigger.GracefulStop);
        Assert.Equal(ReplicaPhase.Draining, gate.Phase);

        Assert.Equal(FenceDecision.FenceThenExit, gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal)));
        Assert.True(gate.ExitAfterFence);
        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());
        Assert.DoesNotContain(phaseEvents, e => e.To == ReplicaPhase.Fenced);
    }

    [Fact]
    public void A_stuck_attempt_in_Starting_or_Fenced_exits_and_one_in_a_fence_marks_it()
    {
        Join();
        Assert.Equal(FenceDecision.FenceThenExit, gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt)));
        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());
    }

    [Fact]
    public void The_fence_triggers_go_through_RequestFence_not_Apply()
    {
        Join();

        Assert.Throws<ArgumentException>(() => gate.Apply(MemberTrigger.ZeroRowRenewal));
        Assert.Throws<ArgumentException>(() => gate.Apply(MemberTrigger.JoinSucceeded));
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    [Fact]
    public void A_rejoin_returns_to_Starting_and_the_next_join_resets_the_fence_marks()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();

        Assert.Equal(ReplicaPhase.Starting, gate.Apply(MemberTrigger.RejoinPermitted).To);
        Assert.Null(gate.FenceStartTimestamp);
        Assert.Null(gate.Deadline);
        Assert.Null(gate.Epoch);
        Assert.Null(gate.JoinedTimeline);

        Assert.True(gate.Joined(Epoch + 1, Timeline, gate.Now));
        Assert.Equal(Epoch + 1, gate.Epoch);
        Assert.False(gate.ExitAfterFence);
        Assert.Equal(FenceDecision.Fence, gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart)));
    }

    [Fact]
    public void ReachFS_is_emitted_once_per_epoch_and_again_after_a_rejoin()
    {
        using var paused = new LocalPhaseGate(Timing, new TimerlessTimeProvider(time), events, NullLogger<LocalPhaseGate>.Instance);
        ToServing(paused);
        time.Advance(S(22));

        Assert.Equal(ServingCheckOutcome.FenceStartPassed, paused.CheckServing().Outcome);
        Assert.Equal(ServingCheckOutcome.NotServing, paused.CheckServing().Outcome);
        Assert.Single(events.Of(MembershipActions.ReachFS));

        paused.CompleteFence();
        paused.Apply(MemberTrigger.RejoinPermitted);
        ToServing(paused);
        time.Advance(S(22));
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, paused.CheckServing().Outcome);
        Assert.Equal(2, events.Of(MembershipActions.ReachFS).Count);
    }

    [Fact]
    public void A_fence_and_an_exit_clear_fence_start_and_a_refusal_changes_no_phase()
    {
        Join();
        Assert.NotNull(gate.FenceStartTimestamp);
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault));
        Assert.Null(gate.FenceStartTimestamp);

        using var other = new LocalPhaseGate(Timing, time, events, NullLogger<LocalPhaseGate>.Instance);
        var seen = new List<PhaseEvent>();
        other.Subscribe(seen.Add);
        Assert.Equal(ReplicaPhase.Starting, other.Apply(MemberTrigger.JoinRefused).To);
        Assert.Empty(seen);
    }

    [Fact]
    public void M16_a_graceful_stop_in_Fencing_lets_the_fence_steps_finish_and_exits_at_step_6()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        var cell = gate.Apply(MemberTrigger.GracefulStop);

        Assert.True(cell.Effects.HasFlag(MemberEffects.CompleteFenceFirst));
        Assert.False(cell.Effects.HasFlag(MemberEffects.WriteDrainingRow));
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(gate.ExitAfterFence);
        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());
    }

    [Fact]
    public async Task NextPhaseChange_completes_when_the_phase_changes()
    {
        var change = gate.NextPhaseChange;
        Assert.False(change.IsCompleted);

        Join();

        await change.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(gate.NextPhaseChange.IsCompleted);
    }

    [Fact]
    public void A_throwing_subscriber_does_not_stop_the_flip_or_the_others()
    {
        gate.Subscribe(_ => throw new InvalidOperationException("bad subscriber"));
        Join();

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Single(fences);
    }

    [Fact]
    public void A_disposed_subscription_receives_nothing()
    {
        var seen = 0;
        var subscription = gate.Subscribe(_ => seen++);
        subscription.Dispose();

        Join();

        Assert.Equal(0, seen);
    }

    [Fact]
    public void Readiness_reasons_are_raised_replaced_by_code_and_cleared()
    {
        var reasons = gate.Readiness;

        reasons.Raise(new ReadinessReason("joining.refused", "first"));
        reasons.Raise(new ReadinessReason("replication.not-synchronous", "second"));
        reasons.Raise(new ReadinessReason("joining.refused", "replaced"));

        Assert.Equal(["joining.refused", "replication.not-synchronous"], reasons.Current.Select(r => r.Code));
        Assert.Equal("replaced", reasons.Current[0].Description);

        reasons.Clear("joining.refused");
        reasons.Clear("not-raised");
        Assert.Equal("replication.not-synchronous", Assert.Single(reasons.Current).Code);
    }
}

internal sealed class TimerlessTimeProvider(FakeTimeProvider inner) : TimeProvider
{
    public override long TimestampFrequency => inner.TimestampFrequency;

    public override long GetTimestamp() => inner.GetTimestamp();

    public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) => new Never();

    private sealed class Never : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

internal sealed class RecordingModelEvents : IModelEventSink
{
    private readonly List<ModelEvent> events = [];

    public IReadOnlyList<ModelEvent> All
    {
        get
        {
            lock (events)
            {
                return [.. events];
            }
        }
    }

    public IReadOnlyList<ModelEvent> Of(string action) => [.. All.Where(e => e.Action == action)];

    public void Emit(in ModelEvent modelEvent)
    {
        lock (events)
        {
            events.Add(modelEvent);
        }
    }
}
