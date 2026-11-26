using CsCheck;
using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Membership;

public sealed class RenewalRulesTests
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private const long Epoch = 7;

    private static RenewalResolution Resolve(
        ReplicaPhase phase,
        bool drainRowWriteStarted,
        RenewalOutcome outcome,
        TimeSpan t0,
        TimeSpan? deadline,
        long attemptEpoch = Epoch,
        long currentEpoch = Epoch) =>
        RenewalRules.Resolve(new RenewalContext(phase, drainRowWriteStarted, currentEpoch, deadline), new RenewalAttempt(attemptEpoch, t0), outcome, Timing);

    private static readonly ReplicaPhase[] Renewing =
        [ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining];

    private static readonly ReplicaPhase[] PhasesAndFencing = [.. Renewing, ReplicaPhase.Fencing];

    private const long Joined = 3;

    private static RenewalOutcome Classify(int? rows, TimeSpan elapsed, long? timeline = Joined, bool cancelRequested = false) =>
        RenewalRules.Classify(rows is { } r ? new RenewalAnswer(r, r == 1 ? timeline : null) : null, cancelRequested, elapsed, Timing.RenewTimeout, Joined);

    [Fact]
    public void One_row_on_the_joined_timeline_within_RenewTimeout_is_renewed()
    {
        Assert.Equal(RenewalOutcome.Renewed, Classify(1, S(0.1)));
        Assert.Equal(RenewalOutcome.Renewed, Classify(1, Timing.RenewTimeout));
    }

    [Fact]
    public void I32_one_row_on_another_timeline_is_its_own_outcome()
    {
        Assert.Equal(RenewalOutcome.OtherTimeline, Classify(1, S(0.1), timeline: Joined + 1));
        Assert.Equal(RenewalOutcome.OtherTimeline, Classify(1, S(0.1), timeline: Joined - 1));
    }

    [Fact]
    public void Zero_rows_within_RenewTimeout_mean_the_row_is_not_this_processs()
    {
        Assert.Equal(RenewalOutcome.NotMine, Classify(0, S(0.1)));
        Assert.Equal(RenewalOutcome.NotMine, Classify(0, Timing.RenewTimeout));
    }

    [Fact]
    public void No_answer_is_unknown()
    {
        Assert.Equal(RenewalOutcome.Unknown, Classify(null, S(2)));
        Assert.Equal(RenewalOutcome.Unknown, Classify(null, S(0.1)));
    }

    [Fact]
    public void An_answer_that_arrives_after_RenewTimeout_is_no_answer_whatever_it_says()
    {
        Assert.Equal(RenewalOutcome.Unknown, Classify(1, Timing.RenewTimeout + Tick));
        Assert.Equal(RenewalOutcome.Unknown, Classify(0, Timing.RenewTimeout + Tick));
        Assert.Equal(RenewalOutcome.Unknown, Classify(1, Timing.RenewTimeout + Tick, timeline: Joined + 1));
    }

    [Fact]
    public void I33_an_answer_to_an_attempt_whose_cancel_was_requested_before_it_returned_is_no_answer_whatever_it_says()
    {
        Assert.Equal(RenewalOutcome.Unknown, Classify(1, S(0.1), cancelRequested: true));
        Assert.Equal(RenewalOutcome.Unknown, Classify(0, S(0.1), cancelRequested: true));
        Assert.Equal(RenewalOutcome.Unknown, Classify(1, S(0.1), timeline: Joined + 1, cancelRequested: true));
    }

    [Fact]
    public void A_primary_key_update_reports_zero_or_one_row_anything_else_is_a_bug()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Classify(2, S(0.1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => Classify(-1, S(0.1)));
    }

    [Fact]
    public void One_row_without_a_timeline_is_a_bug()
    {
        Assert.Throws<ArgumentException>(() => RenewalRules.Classify(new RenewalAnswer(1, null), false, S(0.1), Timing.RenewTimeout, Joined));
    }

    [Fact]
    public void I23_no_attempt_starts_and_no_answer_counts_after_fence_step_0()
    {
        foreach (var phase in new[] { ReplicaPhase.Starting, ReplicaPhase.Fencing, ReplicaPhase.Fenced, ReplicaPhase.Exited })
        {
            Assert.False(RenewalRules.IsActive(phase, drainRowWriteStarted: false), phase.ToString());
            Assert.False(RenewalRules.IsActive(phase, drainRowWriteStarted: true), phase.ToString());
        }
    }

    [Fact]
    public void I23_renewal_runs_in_every_phase_from_Loading_to_Draining()
    {
        foreach (var phase in Renewing)
        {
            Assert.True(RenewalRules.IsActive(phase, drainRowWriteStarted: false), phase.ToString());
        }
    }

    [Fact]
    public void I23_no_attempt_starts_and_no_answer_counts_after_the_draining_write()
    {
        Assert.True(RenewalRules.IsActive(ReplicaPhase.Draining, drainRowWriteStarted: false));
        Assert.False(RenewalRules.IsActive(ReplicaPhase.Draining, drainRowWriteStarted: true));
    }

    [Fact]
    public void I23_an_answer_after_step_0_or_the_draining_write_is_discarded_a_zero_row_answer_included()
    {
        foreach (var outcome in Enum.GetValues<RenewalOutcome>())
        {
            foreach (var (phase, drainWrite) in new[]
            {
                (ReplicaPhase.Fencing, false), (ReplicaPhase.Fenced, false), (ReplicaPhase.Exited, false), (ReplicaPhase.Draining, true),
            })
            {
                var r = Resolve(phase, drainWrite, outcome, S(100), S(120));

                Assert.Equal(RenewalVerdict.Discarded, r.Verdict);
                Assert.Equal(S(120), r.Deadline);
            }
        }
    }

    [Fact]
    public void I23_a_one_row_answer_advances_the_deadline_from_t0()
    {
        foreach (var phase in Renewing)
        {
            var r = Resolve(phase, false, RenewalOutcome.Renewed, S(100), S(120));

            Assert.Equal(RenewalVerdict.DeadlineAdvanced, r.Verdict);
            Assert.Equal(S(100) + S(27), r.Deadline);
        }
    }

    [Fact]
    public void I23_a_one_row_answer_never_moves_the_deadline_back()
    {
        var r = Resolve(ReplicaPhase.Serving, false, RenewalOutcome.Renewed, S(90), S(125));

        Assert.Equal(RenewalVerdict.DeadlineAdvanced, r.Verdict);
        Assert.Equal(S(125), r.Deadline);
    }

    [Fact]
    public void I23_a_renewal_with_no_answer_leaves_the_deadline_where_it_is()
    {
        foreach (var phase in Renewing)
        {
            var r = Resolve(phase, false, RenewalOutcome.Unknown, S(100), S(120));

            Assert.Equal(RenewalVerdict.DeadlineKept, r.Verdict);
            Assert.Equal(S(120), r.Deadline);
        }
    }

    [Fact]
    public void I23_zero_rows_while_the_database_answered_fence_at_once_and_touch_no_deadline()
    {
        foreach (var phase in Renewing)
        {
            var r = Resolve(phase, false, RenewalOutcome.NotMine, S(100), S(120));

            Assert.Equal(RenewalVerdict.FenceZeroRow, r.Verdict);
            Assert.Equal(S(120), r.Deadline);
        }
    }

    [Fact]
    public void I32_one_row_on_another_timeline_fences_and_does_not_advance_the_deadline()
    {
        foreach (var phase in Renewing)
        {
            var r = Resolve(phase, false, RenewalOutcome.OtherTimeline, S(100), S(120));

            Assert.Equal(RenewalVerdict.FenceOtherTimeline, r.Verdict);
            Assert.Equal(S(120), r.Deadline);
        }
    }

    [Fact]
    public void I23_the_first_attempt_after_a_join_with_no_deadline_yet_sets_one()
    {
        var r = Resolve(ReplicaPhase.Loading, false, RenewalOutcome.Renewed, S(100), null);

        Assert.Equal(S(127), r.Deadline);
        Assert.Null(Resolve(ReplicaPhase.Loading, false, RenewalOutcome.Unknown, S(100), null).Deadline);
    }

    [Fact]
    public void I23_an_answer_to_an_attempt_of_an_earlier_epoch_is_discarded_in_every_phase_and_whatever_it_says()
    {
        foreach (var phase in Renewing)
        {
            foreach (var outcome in Enum.GetValues<RenewalOutcome>())
            {
                var r = Resolve(phase, false, outcome, S(100), S(120), attemptEpoch: Epoch - 1, currentEpoch: Epoch);

                Assert.Equal(RenewalVerdict.Discarded, r.Verdict);
                Assert.Equal(S(120), r.Deadline);
            }
        }
    }

    [Fact]
    public void I23_a_zero_row_answer_from_before_step_0_does_not_fence_the_next_epoch_after_a_rejoin()
    {
        var phase = MemberStateMachine.Decide(ReplicaPhase.Serving, MemberTrigger.ApplierStall).To;
        phase = MemberStateMachine.Decide(phase, MemberTrigger.FenceCompleted).To;
        phase = MemberStateMachine.Decide(phase, MemberTrigger.RejoinPermitted).To;
        phase = MemberStateMachine.Decide(phase, MemberTrigger.JoinSucceeded).To;
        Assert.Equal(ReplicaPhase.Loading, phase);

        var outcome = Classify(0, S(1.5));
        var stale = Resolve(phase, false, outcome, S(3600), S(3627), attemptEpoch: 7, currentEpoch: 8);
        var fresh = Resolve(phase, false, outcome, S(3600), S(3627), attemptEpoch: 8, currentEpoch: 8);

        Assert.Equal(RenewalVerdict.Discarded, stale.Verdict);
        Assert.Equal(RenewalVerdict.FenceZeroRow, fresh.Verdict);
    }

    [Fact]
    public void I23_property_the_deadline_changes_only_on_a_one_row_answer_and_never_decreases()
    {
        var step = Gen.Select(
            Gen.Enum<RenewalOutcome>(),
            Gen.Int[0, 100_000].Select(ms => TimeSpan.FromMilliseconds(ms)),
            Gen.Int[0, 5].Select(i => PhasesAndFencing[i]),
            Gen.Bool,
            Gen.Bool);

        step.List[1, 40].Sample(steps =>
        {
            TimeSpan? deadline = null;
            foreach (var (outcome, t0, phase, drainWrite, stale) in steps)
            {
                var r = Resolve(phase, drainWrite, outcome, t0, deadline, attemptEpoch: stale ? Epoch - 1 : Epoch);

                if (r.Verdict == RenewalVerdict.DeadlineAdvanced)
                {
                    Assert.Equal(RenewalOutcome.Renewed, outcome);
                    Assert.False(stale);
                    Assert.True(deadline is null || r.Deadline >= deadline);
                }
                else
                {
                    Assert.Equal(deadline, r.Deadline);
                }

                deadline = r.Deadline;
            }
        }, iter: 2_000);
    }
}
