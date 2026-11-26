using Reeve.Cluster;
using Reeve.Cluster.Log;

namespace Unit.Log;

public sealed class ApplierFailureRulesTests
{
    private static readonly TimeSpan First = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Cap = TimeSpan.FromSeconds(20);

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(5, 16)]
    [InlineData(6, 20)]
    [InlineData(64, 20)]
    [InlineData(int.MaxValue, 20)]
    public void A_retry_waits_twice_as_long_as_the_one_before_up_to_the_cap(int failures, double seconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(seconds), ApplierFailureRules.RetryDelay(failures, First, Cap));
    }

    [Fact]
    public void The_first_retry_waits_the_first_delay_and_a_cap_below_it_is_the_first_delay()
    {
        Assert.Equal(First, ApplierFailureRules.RetryDelay(1, First, Cap));
        Assert.Equal(First, ApplierFailureRules.RetryDelay(3, First, TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public void No_failure_means_no_retry_delay()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ApplierFailureRules.RetryDelay(0, First, Cap));
    }

    [Theory]
    [InlineData(16, 5, 5)]
    [InlineData(2, 5, 2)]
    [InlineData(5, 5, 5)]
    public void A_retry_while_a_stall_runs_comes_no_later_than_its_deadline_less_ReconcileTimeout(double delay, double untilLastRetry, double expected)
    {
        Assert.Equal(
            TimeSpan.FromSeconds(expected),
            ApplierFailureRules.BeforeStallDeadline(TimeSpan.FromSeconds(delay), TimeSpan.FromSeconds(untilLastRetry)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    public void Once_the_last_retry_before_the_deadline_has_passed_the_backoff_stands(double untilLastRetry)
    {
        Assert.Equal(TimeSpan.FromSeconds(16), ApplierFailureRules.BeforeStallDeadline(TimeSpan.FromSeconds(16), TimeSpan.FromSeconds(untilLastRetry)));
    }

    [Fact]
    public void With_no_stall_running_the_backoff_stands()
    {
        Assert.Equal(TimeSpan.FromSeconds(16), ApplierFailureRules.BeforeStallDeadline(TimeSpan.FromSeconds(16), untilLastRetry: null));
    }

    [Theory]
    [InlineData(ReplicaPhase.Activated)]
    [InlineData(ReplicaPhase.Serving)]
    [InlineData(ReplicaPhase.Draining)]
    public void A_stall_past_the_limit_fences_in_Activated_Serving_and_Draining(ReplicaPhase phase)
    {
        var limit = TimeSpan.FromSeconds(30);

        Assert.Equal(StallVerdict.Fence, ApplierFailureRules.StallLimit(phase, limit, limit));
        Assert.Equal(StallVerdict.Fence, ApplierFailureRules.StallLimit(phase, limit + TimeSpan.FromTicks(1), limit));
        Assert.Equal(StallVerdict.Within, ApplierFailureRules.StallLimit(phase, limit - TimeSpan.FromTicks(1), limit));
    }

    [Theory]
    [InlineData(ReplicaPhase.Loading)]
    [InlineData(ReplicaPhase.CatchingUp)]
    public void A_stall_past_the_limit_keeps_retrying_in_Loading_and_CatchingUp(ReplicaPhase phase)
    {
        var limit = TimeSpan.FromSeconds(30);

        Assert.Equal(StallVerdict.KeepRetrying, ApplierFailureRules.StallLimit(phase, TimeSpan.FromHours(1), limit));
        Assert.Equal(StallVerdict.Within, ApplierFailureRules.StallLimit(phase, TimeSpan.FromSeconds(1), limit));
    }

    [Theory]
    [InlineData(ReplicaPhase.Starting)]
    [InlineData(ReplicaPhase.Fencing)]
    [InlineData(ReplicaPhase.Fenced)]
    [InlineData(ReplicaPhase.Exited)]
    public void A_stall_is_no_trigger_where_the_replica_has_no_serving_epoch(ReplicaPhase phase)
    {
        Assert.Equal(StallVerdict.None, ApplierFailureRules.StallLimit(phase, TimeSpan.FromHours(1), TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void An_applier_that_is_not_stalled_has_nothing_to_decide()
    {
        Assert.Equal(StallVerdict.None, ApplierFailureRules.StallLimit(ReplicaPhase.Serving, stalledFor: null, TimeSpan.FromSeconds(30)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void An_attempt_is_stuck_from_its_start_plus_the_allowance_whatever_ended_the_wait_for_it(bool abandoned)
    {
        var why = abandoned ? AttemptOutcomeKind.Abandoned : AttemptOutcomeKind.TimedOut;
        const long frequency = 1000;
        var allowance = TimeSpan.FromSeconds(30);
        var attempt = new UnreturnedAttempt(new ApplierAttempt(1, 1, "a", started: 5_000, deadline: 15_000), why, Since: 6_000);

        Assert.Equal(35_000, ApplierFailureRules.StuckAt(attempt, allowance, frequency));
        Assert.Null(ApplierFailureRules.FirstStuck([attempt], now: 34_999, allowance, frequency));
        Assert.Same(attempt, ApplierFailureRules.FirstStuck([attempt], now: 35_000, allowance, frequency));
    }

    [Fact]
    public void The_first_stuck_attempt_is_the_one_that_started_first_and_the_next_due_is_the_earliest_start()
    {
        const long frequency = 1000;
        var allowance = TimeSpan.FromSeconds(10);
        var late = new UnreturnedAttempt(new ApplierAttempt(2, 1, "b", started: 9_000, deadline: null), AttemptOutcomeKind.Abandoned, 9_500);
        var early = new UnreturnedAttempt(new ApplierAttempt(1, 1, "a", started: 1_000, deadline: 6_000), AttemptOutcomeKind.TimedOut, 6_000);

        Assert.Same(early, ApplierFailureRules.FirstStuck([late, early], now: 20_000, allowance, frequency));
        Assert.Equal(11_000, ApplierFailureRules.NextStuckAt([late, early], allowance, frequency));
        Assert.Null(ApplierFailureRules.NextStuckAt([], allowance, frequency));
    }
}
