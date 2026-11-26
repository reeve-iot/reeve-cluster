using Reeve.Cluster;
using Reeve.Cluster.Slots;

namespace Unit.Slots;

public sealed class SlotDecisionsTests
{
    private static readonly Guid Token = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly TimeSpan FenceStart = TimeSpan.FromSeconds(20);

    private static SlotHolderState State(
        ReplicaPhase phase, TimeSpan? now = null, TimeSpan? fenceStart = null, bool drainStep4 = false, long? current = 7, long granted = 7, bool fenced = false) =>
        new(phase, now ?? TimeSpan.FromSeconds(5), fenceStart, drainStep4, current, granted, fenced);

    [Theory]
    [InlineData(ReplicaPhase.Serving)]
    [InlineData(ReplicaPhase.Draining)]
    public void The_holder_check_passes_while_the_member_lease_is_renewed_and_the_clock_is_before_fence_start(ReplicaPhase phase)
    {
        Assert.Equal(SlotHolderOutcome.Passed, SlotDecisions.HolderCheck(State(phase, fenceStart: FenceStart)));
    }

    [Theory]
    [InlineData(ReplicaPhase.Serving, 20)]
    [InlineData(ReplicaPhase.Serving, 25)]
    [InlineData(ReplicaPhase.Draining, 20)]
    [InlineData(ReplicaPhase.Draining, 25)]
    public void I1_the_holder_check_finds_fence_start_passed_while_the_lease_is_renewed_and_asks_for_the_fence(ReplicaPhase phase, int seconds)
    {
        var outcome = SlotDecisions.HolderCheck(State(phase, now: TimeSpan.FromSeconds(seconds), fenceStart: FenceStart));

        Assert.Equal(SlotHolderOutcome.FenceStartPassed, outcome);
    }

    [Theory]
    [InlineData(ReplicaPhase.Serving)]
    [InlineData(ReplicaPhase.Draining)]
    public void A_renewed_lease_with_no_fence_start_fails_closed_and_asks_for_the_fence(ReplicaPhase phase)
    {
        Assert.Equal(SlotHolderOutcome.FenceStartPassed, SlotDecisions.HolderCheck(State(phase, fenceStart: null)));
    }

    [Theory]
    [InlineData(ReplicaPhase.Draining)]
    [InlineData(ReplicaPhase.Exited)]
    public void B1_after_drain_step_4_the_holder_check_passes_whatever_the_clock_says_because_nothing_renews_the_lease(ReplicaPhase phase)
    {
        Assert.Equal(SlotHolderOutcome.Passed, SlotDecisions.HolderCheck(State(phase, now: TimeSpan.FromHours(1), fenceStart: null, drainStep4: true)));
    }

    [Fact]
    public void An_exit_before_drain_step_4_leaves_no_slot_to_act_on()
    {
        Assert.Equal(SlotHolderOutcome.Refused, SlotDecisions.HolderCheck(State(ReplicaPhase.Exited, drainStep4: false)));
    }

    [Theory]
    [InlineData(ReplicaPhase.Starting)]
    [InlineData(ReplicaPhase.Loading)]
    [InlineData(ReplicaPhase.CatchingUp)]
    [InlineData(ReplicaPhase.Activated)]
    [InlineData(ReplicaPhase.Fencing)]
    [InlineData(ReplicaPhase.Fenced)]
    public void The_holder_check_refuses_in_every_other_phase_and_asks_for_no_fence(ReplicaPhase phase)
    {
        Assert.Equal(SlotHolderOutcome.Refused, SlotDecisions.HolderCheck(State(phase, fenceStart: FenceStart)));
    }

    [Theory]
    [InlineData(ReplicaPhase.Serving, false)]
    [InlineData(ReplicaPhase.Draining, false)]
    [InlineData(ReplicaPhase.Draining, true)]
    [InlineData(ReplicaPhase.Exited, true)]
    public void A_slot_of_another_epoch_than_the_current_join_is_refused_in_every_phase_that_otherwise_passes(ReplicaPhase phase, bool drainStep4)
    {
        var state = State(phase, fenceStart: FenceStart, drainStep4: drainStep4, current: 8, granted: 7);

        Assert.Equal(SlotHolderOutcome.Refused, SlotDecisions.HolderCheck(state));
        Assert.Equal(SlotHolderOutcome.Refused, SlotDecisions.HolderCheck(state with { CurrentEpoch = null }));
    }

    [Theory]
    [InlineData(ReplicaPhase.Serving, false)]
    [InlineData(ReplicaPhase.Draining, false)]
    [InlineData(ReplicaPhase.Draining, true)]
    [InlineData(ReplicaPhase.Exited, true)]
    public void A_fence_started_in_the_slots_epoch_refuses_for_good_whatever_the_phase_says(ReplicaPhase phase, bool drainStep4)
    {
        var state = State(phase, fenceStart: FenceStart, drainStep4: drainStep4, fenced: true);

        Assert.Equal(SlotHolderOutcome.Refused, SlotDecisions.HolderCheck(state));
    }

    [Theory]
    [InlineData("RC004", "SlotLost")]
    [InlineData("RC002", "OtherTimeline")]
    [InlineData("RC003", "None")]
    [InlineData("RC001", "None")]
    [InlineData("40001", "None")]
    [InlineData("25P04", "None")]
    [InlineData(null, "None")]
    public void The_database_refusals_map_to_the_slots_two_and_nothing_else(string? sqlState, string expected)
    {
        Assert.Equal(expected, SlotDecisions.Refusal(sqlState).ToString());
    }

    [Fact]
    public void A_written_token_is_acquired_when_no_cancel_came_before_the_call_returned()
    {
        Assert.Equal(new SlotAcquireResult.Acquired(Token), SlotDecisions.AcquireAnswer(Token, cancelledBeforeReturn: false));
    }

    [Fact]
    public void A_statement_that_granted_nothing_answers_full()
    {
        Assert.IsType<SlotAcquireResult.Full>(SlotDecisions.AcquireAnswer(null, cancelledBeforeReturn: false));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void I33_an_acquire_the_librarys_bound_cancelled_is_unknown_whatever_the_statement_returned(bool granted)
    {
        var answer = SlotDecisions.AcquireAnswer(granted ? Token : null, cancelledBeforeReturn: true);

        Assert.IsType<SlotAcquireResult.Unknown>(answer);
        Assert.False(answer.IsAcquired);
    }

    [Theory]
    [InlineData(true, false, SlotRenewResult.Renewed)]
    [InlineData(false, false, SlotRenewResult.Ended)]
    [InlineData(true, true, SlotRenewResult.Unknown)]
    [InlineData(false, true, SlotRenewResult.Unknown)]
    public void A_renewal_is_renewed_ended_or_unknown_when_the_librarys_bound_cancelled_it(bool updated, bool cancelled, SlotRenewResult expected)
    {
        Assert.Equal(expected, SlotDecisions.RenewAnswer(updated, cancelled));
    }

    [Fact]
    public void Only_an_acquired_slot_is_acquired_and_only_it_carries_a_token()
    {
        Assert.True(new SlotAcquireResult.Acquired(Token).IsAcquired);
        Assert.True(new SlotAcquireResult.Acquired(Token).TryGetToken(out var token));
        Assert.Equal(Token, token);

        foreach (SlotAcquireResult other in new SlotAcquireResult[] { new SlotAcquireResult.Full(), new SlotAcquireResult.NotServing(), new SlotAcquireResult.Unknown() })
        {
            Assert.False(other.IsAcquired);
            Assert.False(other.TryGetToken(out var none));
            Assert.Equal(Guid.Empty, none);
        }
    }
}
