using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Membership;

public sealed class RejoinDecisionTests
{
    private const long Own = 7;

    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Backoff = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private static RejoinDecision Decide(
        OwnRowObservation? row,
        TimeSpan? lastSpan = null,
        TimeSpan? sinceFence = null,
        TimeSpan? backoff = null) =>
        RejoinDecision.Decide(row, Own, lastSpan ?? Ttl, sinceFence ?? TimeSpan.Zero, Ttl, backoff ?? Backoff);

    private static OwnRowObservation Row(long epoch, MemberRowState state, bool leaseValid) => new(true, epoch, state, leaseValid);

    [Fact]
    public void While_the_database_does_not_answer_the_replica_stays_fenced_and_waits()
    {
        var d = Decide(null);

        Assert.Equal(RejoinAction.AwaitDatabase, d.Action);
        Assert.Null(d.Trigger);
    }

    [Fact]
    public void A_fenced_row_of_its_own_epoch_is_rejoined_when_no_backoff_applies()
    {
        var d = Decide(Row(Own, MemberRowState.Fenced, leaseValid: false));

        Assert.Equal(RejoinAction.Rejoin, d.Action);
        Assert.Equal(MemberTrigger.RejoinPermitted, d.Trigger);
    }

    [Fact]
    public void A_row_whose_lease_has_expired_is_rejoined_even_when_step_3_never_ran()
    {
        Assert.Equal(RejoinAction.Rejoin, Decide(Row(Own, MemberRowState.Active, leaseValid: false)).Action);
        Assert.Equal(RejoinAction.Rejoin, Decide(Row(Own, MemberRowState.Joining, leaseValid: false)).Action);
    }

    [Fact]
    public void A_joining_or_active_row_of_its_own_epoch_with_a_valid_lease_is_waited_out()
    {
        foreach (var state in new[] { MemberRowState.Joining, MemberRowState.Active })
        {
            var d = Decide(Row(Own, state, leaseValid: true));

            Assert.Equal(RejoinAction.AwaitLeaseExpiry, d.Action);
            Assert.Null(d.Trigger);
        }
    }

    [Fact]
    public void A_fenced_or_draining_row_of_its_own_epoch_is_rejoined_whatever_its_lease_says()
    {
        Assert.Equal(RejoinAction.Rejoin, Decide(Row(Own, MemberRowState.Fenced, leaseValid: true)).Action);
        Assert.Equal(RejoinAction.Rejoin, Decide(Row(Own, MemberRowState.Draining, leaseValid: true)).Action);
    }

    [Fact]
    public void A_row_with_an_older_epoch_a_restore_is_rejoined_the_join_itself_decides_about_its_lease()
    {
        foreach (var state in Enum.GetValues<MemberRowState>())
        {
            Assert.Equal(RejoinAction.Rejoin, Decide(Row(Own - 1, state, leaseValid: true)).Action);
            Assert.Equal(RejoinAction.Rejoin, Decide(Row(0, state, leaseValid: false)).Action);
        }
    }

    [Fact]
    public void An_absent_observation_names_no_row_and_no_lease()
    {
        Assert.False(OwnRowObservation.Absent.Present);
        Assert.False(OwnRowObservation.Absent.LeaseValid);
        Assert.Equal(0, OwnRowObservation.Absent.Epoch);
    }

    [Fact]
    public void An_absent_row_an_operators_delete_is_rejoined()
    {
        Assert.Equal(RejoinAction.Rejoin, Decide(OwnRowObservation.Absent).Action);
    }

    [Fact]
    public void A_row_with_a_higher_epoch_exits_the_process_it_never_takes_the_row_back()
    {
        foreach (var state in Enum.GetValues<MemberRowState>())
        {
            var d = Decide(Row(Own + 1, state, leaseValid: true));

            Assert.Equal(RejoinAction.ExitHigherEpoch, d.Action);
            Assert.Equal(MemberTrigger.RowHasHigherEpoch, d.Trigger);
        }
    }

    [Fact]
    public void A_higher_epoch_exits_even_inside_the_backoff()
    {
        var d = Decide(Row(Own + 1, MemberRowState.Active, leaseValid: true), lastSpan: S(1), sinceFence: S(1));

        Assert.Equal(RejoinAction.ExitHigherEpoch, d.Action);
    }

    [Fact]
    public void A_short_span_waits_out_RejoinBackoff_from_the_fence()
    {
        var row = Row(Own, MemberRowState.Fenced, leaseValid: false);

        var d = Decide(row, lastSpan: S(10), sinceFence: S(20));

        Assert.Equal(RejoinAction.AwaitBackoff, d.Action);
        Assert.Equal(S(40), d.Wait);
        Assert.Null(d.Trigger);
    }

    [Fact]
    public void The_backoff_ends_exactly_when_RejoinBackoff_has_passed()
    {
        var row = Row(Own, MemberRowState.Fenced, leaseValid: false);

        var last = Decide(row, lastSpan: S(10), sinceFence: Backoff - Tick);
        Assert.Equal((RejoinAction.AwaitBackoff, Tick), (last.Action, last.Wait));
        Assert.Equal(RejoinAction.Rejoin, Decide(row, lastSpan: S(10), sinceFence: Backoff).Action);
        Assert.Equal(RejoinAction.Rejoin, Decide(row, lastSpan: S(10), sinceFence: Backoff + Tick).Action);
    }

    [Fact]
    public void A_span_of_LeaseTtl_or_more_needs_no_backoff()
    {
        var row = Row(Own, MemberRowState.Fenced, leaseValid: false);

        var oneTickShort = Decide(row, lastSpan: Ttl - Tick, sinceFence: TimeSpan.Zero);
        Assert.Equal((RejoinAction.AwaitBackoff, Backoff), (oneTickShort.Action, oneTickShort.Wait));
        Assert.Equal(RejoinAction.Rejoin, Decide(row, lastSpan: Ttl, sinceFence: TimeSpan.Zero).Action);
        Assert.Equal(RejoinAction.Rejoin, Decide(row, lastSpan: Ttl + Tick, sinceFence: TimeSpan.Zero).Action);
    }

    [Fact]
    public void A_zero_RejoinBackoff_never_waits()
    {
        var d = Decide(Row(Own, MemberRowState.Fenced, leaseValid: false), lastSpan: TimeSpan.Zero, sinceFence: TimeSpan.Zero, backoff: TimeSpan.Zero);

        Assert.Equal(RejoinAction.Rejoin, d.Action);
    }

    [Fact]
    public void The_backoff_applies_to_a_restored_older_row_and_an_absent_row_too()
    {
        Assert.Equal(RejoinAction.AwaitBackoff, Decide(OwnRowObservation.Absent, lastSpan: S(5), sinceFence: S(5)).Action);
        Assert.Equal(RejoinAction.AwaitBackoff, Decide(Row(Own - 1, MemberRowState.Active, true), lastSpan: S(5), sinceFence: S(5)).Action);
    }

    [Fact]
    public void An_unanswered_database_takes_precedence_over_everything_else()
    {
        Assert.Equal(RejoinAction.AwaitDatabase, Decide(null, lastSpan: S(1), sinceFence: Backoff + S(1)).Action);
    }
}
