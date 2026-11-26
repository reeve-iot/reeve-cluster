using CsCheck;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class LeaseClockTests
{
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private static readonly LeaseTiming Defaults = new(
        LeaseTtl: TimeSpan.FromSeconds(30),
        SafetyMargin: TimeSpan.FromSeconds(3),
        StopBudget: TimeSpan.FromSeconds(5),
        RenewTimeout: TimeSpan.FromSeconds(2));

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    [Fact]
    public void The_timing_comes_from_the_options()
    {
        var options = new ClusterOptions
        {
            LeaseTtl = S(40),
            SafetyMargin = S(4),
            StopBudget = S(6),
            RenewTimeout = S(1),
        };

        Assert.Equal(new LeaseTiming(S(40), S(4), S(6), S(1)), LeaseTiming.From(options));
    }

    [Fact]
    public void The_timing_of_no_options_is_refused()
    {
        Assert.Throws<ArgumentNullException>(() => LeaseTiming.From(null!));
    }

    [Fact]
    public void The_first_deadline_is_t0_plus_LeaseTtl_minus_SafetyMargin()
    {
        Assert.Equal(S(100) + S(27), LeaseClock.AdvanceDeadline(null, S(100), Defaults));
    }

    [Fact]
    public void A_later_attempt_moves_the_deadline_forward()
    {
        Assert.Equal(S(103) + S(27), LeaseClock.AdvanceDeadline(S(127), S(103), Defaults));
    }

    [Fact]
    public void The_deadline_is_the_maximum_over_attempts_so_an_older_t0_never_moves_it_back()
    {
        var current = LeaseClock.AdvanceDeadline(null, S(106), Defaults);

        Assert.Equal(current, LeaseClock.AdvanceDeadline(current, S(103), Defaults));
        Assert.Equal(current, LeaseClock.AdvanceDeadline(current, S(106), Defaults));
        Assert.Equal(current + Tick, LeaseClock.AdvanceDeadline(current, S(106) + Tick, Defaults));
    }

    [Fact]
    public void Fence_start_is_the_deadline_minus_StopBudget()
    {
        Assert.Equal(S(127) - S(5), LeaseClock.FenceStart(S(127), Defaults));
        Assert.Equal(S(100) + Defaults.LeaseTtl - Defaults.SafetyMargin - Defaults.StopBudget, LeaseClock.FenceStart(LeaseClock.AdvanceDeadline(null, S(100), Defaults)!.Value, Defaults));
    }

    [Fact]
    public void I1_the_serving_check_admits_only_before_fence_start()
    {
        var fenceStart = S(500);

        Assert.Equal(ServingCheckOutcome.Serving, ServingCheck.Evaluate(ReplicaPhase.Serving, fenceStart - Tick, fenceStart));
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, ServingCheck.Evaluate(ReplicaPhase.Serving, fenceStart, fenceStart));
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, ServingCheck.Evaluate(ReplicaPhase.Serving, fenceStart + Tick, fenceStart));
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, ServingCheck.Evaluate(ReplicaPhase.Serving, fenceStart + S(3600), fenceStart));
    }

    [Fact]
    public void I1_a_process_paused_across_fence_start_fails_the_check_though_its_phase_is_still_Serving()
    {
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, ServingCheck.Evaluate(ReplicaPhase.Serving, S(40), S(25)));
    }

    [Fact]
    public void I1_a_phase_other_than_Serving_never_serves_whatever_the_clock_says()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>().Where(p => p != ReplicaPhase.Serving))
        {
            Assert.Equal(ServingCheckOutcome.NotServing, ServingCheck.Evaluate(phase, S(1), S(500)));
            Assert.Equal(ServingCheckOutcome.NotServing, ServingCheck.Evaluate(phase, S(900), S(500)));
            Assert.Equal(ServingCheckOutcome.NotServing, ServingCheck.Evaluate(phase, S(1), null));
        }
    }

    [Fact]
    public void I1_a_Serving_replica_with_no_fence_start_fails_closed_with_the_answer_that_starts_the_fence()
    {
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, ServingCheck.Evaluate(ReplicaPhase.Serving, S(1), null));
    }

    [Fact]
    public void A_replica_may_enter_Serving_only_before_fence_start()
    {
        Assert.True(ServingCheck.MayEnterServing(S(10) - Tick, S(10)));
        Assert.False(ServingCheck.MayEnterServing(S(10), S(10)));
        Assert.False(ServingCheck.MayEnterServing(S(11), S(10)));
        Assert.False(ServingCheck.MayEnterServing(S(1), null));
    }

    [Fact]
    public void I1_property_over_a_run_of_attempts_answered_out_of_order_executed_unexecuted_and_lost()
    {
        var timing = Defaults;
        var rt = timing.RenewTimeout.Ticks;
        var bound = (timing.SafetyMargin.Ticks - 1) / 2;
        var err = Gen.OneOf(Gen.Const(0L), Gen.Const(bound), Gen.Long[0, bound]);
        var attempt = Gen.Select(Gen.Long[0, TimeSpan.FromSeconds(3).Ticks], Gen.Long[0, rt], Gen.Bool, Gen.Bool);

        Gen.Select(attempt.List[1, 12], err, err, err, Gen.Int).Sample(x =>
        {
            var (attempts, dm, ds, df, seed) = x;

            TimeSpan? deadline = LeaseClock.AdvanceDeadline(null, TimeSpan.Zero, timing);
            long maxExec = 0;
            long t = 0;
            var runs = new List<(long T0, bool Executed, bool Answered)>();
            foreach (var (gap, delay, executed, answered) in attempts)
            {
                t += gap;
                runs.Add((t, executed, answered));
                if (executed)
                {
                    maxExec = Math.Max(maxExec, t + delay);
                }
            }

            var rng = new Random(seed);
            foreach (var a in runs.OrderBy(_ => rng.Next()))
            {
                var outcome = a.Executed && a.Answered ? RenewalOutcome.Renewed
                    : !a.Executed && a.Answered ? RenewalOutcome.NotMine
                    : RenewalOutcome.Unknown;
                deadline = RenewalRules.Resolve(
                    new RenewalContext(ReplicaPhase.Serving, false, 1, deadline),
                    new RenewalAttempt(1, TimeSpan.FromTicks(a.T0)),
                    outcome,
                    timing).Deadline;
            }

            var fenceStartTrue = LeaseClock.FenceStart(deadline!.Value, timing).Ticks + dm;
            var expiryTrue = maxExec + timing.LeaseTtl.Ticks - ds - df;
            Assert.True(fenceStartTrue < expiryTrue, $"fence start {fenceStartTrue} not before expiry {expiryTrue}");
        }, iter: 20_000);
    }

    [Fact]
    public void I1_fence_start_lies_before_the_database_expiry_with_the_three_clock_errors_at_their_bounds()
    {
        ValidTimings.SelectMany(timing =>
        {
            var bound = (timing.SafetyMargin.Ticks + 1) / 2;
            var error = Gen.OneOf(Gen.Const(0L), Gen.Const(bound - 1), Gen.Long[0, bound - 1]);
            var execDelay = Gen.OneOf(Gen.Const(0L), Gen.Const(timing.RenewTimeout.Ticks), Gen.Long[0, timing.RenewTimeout.Ticks]);
            return Gen.Select(Gen.Const(timing), error, error, error, execDelay, Gen.Long[0, 1_000_000_000_000]);
        }).Sample(x =>
        {
            var (timing, dm, ds, df, delay, t0Ticks) = x;
            Assert.True(FenceStartPrecedesExpiry(timing, dm, ds, df, delay, t0Ticks));
        }, iter: 20_000);
    }

    [Fact]
    public void I1_the_bound_has_teeth_a_StopBudget_well_below_half_the_margin_can_lose()
    {
        var timing = Defaults with { StopBudget = TimeSpan.FromTicks(Defaults.SafetyMargin.Ticks / 4) };
        var worst = Defaults.SafetyMargin.Ticks / 2 - 1;

        Assert.False(FenceStartPrecedesExpiry(timing, worst, worst, worst, 0, 0));
    }

    private static Gen<LeaseTiming> ValidTimings =>
        Gen.Long[TimeSpan.FromSeconds(10).Ticks, TimeSpan.FromSeconds(300).Ticks].SelectMany(ttl =>
            Gen.Long[TimeSpan.FromMilliseconds(100).Ticks, ttl / 6].SelectMany(margin =>
                Gen.Long[margin / 2 + 1, Math.Max(margin / 2 + 1, ttl / 4)].SelectMany(stop =>
                    Gen.Long[TimeSpan.FromMilliseconds(100).Ticks, ttl / 8].Select(renewTimeout =>
                        new LeaseTiming(TimeSpan.FromTicks(ttl), TimeSpan.FromTicks(margin), TimeSpan.FromTicks(stop), TimeSpan.FromTicks(renewTimeout))))))
            .Where(t => t.LeaseTtl - t.SafetyMargin - t.StopBudget - t.RenewTimeout > TimeSpan.Zero);

    private static bool FenceStartPrecedesExpiry(LeaseTiming timing, long dm, long ds, long df, long delay, long t0Ticks)
    {
        var t0 = TimeSpan.FromTicks(t0Ticks);
        var deadline = LeaseClock.AdvanceDeadline(null, t0, timing)!.Value;
        var fenceStart = LeaseClock.FenceStart(deadline, timing);

        var fenceStartTrue = (fenceStart - t0).Ticks + dm;
        var expiryTrue = delay + timing.LeaseTtl.Ticks - ds - df;

        return fenceStartTrue < expiryTrue;
    }
}
