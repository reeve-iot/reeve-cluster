using System.Diagnostics;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Time;

namespace Harness.SelfTests;

public sealed class FaultPointTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    private static (HarnessFaultPoints Faults, ArrivalLog Log, DriftingTimeProvider Clock) Replica(string id = "r1")
    {
        var clock = new DriftingTimeProvider();
        var log = new ArrivalLog(new RunSequence());
        return (new HarnessFaultPoints(id, clock, log), log, clock);
    }

    [Fact]
    public async Task A_point_with_no_rule_passes_at_once_and_is_recorded()
    {
        var (faults, log, _) = Replica();

        var reached = faults.ReachAsync(LogFaultPoints.PassBegin);

        Assert.True(reached.IsCompletedSuccessfully);
        await reached;
        Assert.Equal("log.applier.pass-begin", Assert.Single(log.Snapshot()).Point);
    }

    [Fact]
    public async Task A_held_point_holds_until_released()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.AfterSnapshotRead);

        var reaching = faults.ReachAsync(LogFaultPoints.AfterSnapshotRead).AsTask();
        await hold.WaitReachedAsync();
        await Task.Delay(150);

        Assert.False(reaching.IsCompleted);
        Assert.Equal(1, hold.HeldNow);

        Assert.True(hold.Release());

        await reaching.WaitAsync(Limit);
        Assert.Equal(0, hold.HeldNow);
        Assert.Equal(1, hold.ReleasedTotal);
    }

    [Fact]
    public async Task A_hold_of_the_next_arrival_lets_the_ones_after_it_pass()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.PassBegin);
        var first = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();
        await hold.WaitReachedAsync();

        var second = faults.ReachAsync(LogFaultPoints.PassBegin);

        Assert.True(second.IsCompletedSuccessfully);
        Assert.False(first.IsCompleted);
        hold.ReleaseAll();
        await first.WaitAsync(Limit);
    }

    [Fact]
    public async Task Arrivals_are_released_oldest_first_and_a_chosen_one_can_go_ahead()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.Hold(LogFaultPoints.PassBegin);
        var order = new List<int>();
        var tasks = new List<Task>();
        for (var i = 0; i < 3; i++)
        {
            var n = i;
            tasks.Add(Task.Run(async () =>
            {
                await faults.ReachAsync(LogFaultPoints.PassBegin);
                lock (order)
                {
                    order.Add(n);
                }
            }));
            await hold.WaitReachedAsync(i + 1);
        }

        Assert.True(hold.ReleaseAt(1));
        await Task.WhenAny(tasks).WaitAsync(Limit);
        hold.Release();
        hold.Release();
        await Task.WhenAll(tasks).WaitAsync(Limit);

        Assert.Equal(3, order.Count);
        Assert.Equal(1, order[0]);
    }

    [Fact]
    public async Task A_hold_can_let_the_first_arrivals_through()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.Hold(LogFaultPoints.PassBegin, skip: 2);

        Assert.True(faults.ReachAsync(LogFaultPoints.PassBegin).IsCompletedSuccessfully);
        Assert.True(faults.ReachAsync(LogFaultPoints.PassBegin).IsCompletedSuccessfully);
        var third = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();
        await hold.WaitReachedAsync();

        Assert.False(third.IsCompleted);
        hold.Release();
        await third.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_held_arrival_that_is_cancelled_leaves_the_queue_and_throws_at_the_point()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.Hold(LogFaultPoints.BeforeReconcilerReturns);
        using var cts = new CancellationTokenSource();
        var reaching = faults.ReachAsync(LogFaultPoints.BeforeReconcilerReturns, cts.Token).AsTask();
        await hold.WaitReachedAsync();

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reaching).WaitAsync(Limit);
        Assert.Equal(0, hold.HeldNow);
        Assert.Equal(1, hold.CancelledTotal);
    }

    [Fact]
    public async Task A_held_arrival_can_be_failed_at_the_point()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.AppendAfter);
        var reaching = faults.ReachAsync(LogFaultPoints.AppendAfter).AsTask();
        await hold.WaitReachedAsync();

        hold.Fail(new IOException("the process died here"));

        var thrown = await Assert.ThrowsAsync<IOException>(() => reaching).WaitAsync(Limit);
        Assert.Equal("the process died here", thrown.Message);
    }

    [Fact]
    public async Task Disposing_a_hold_releases_what_it_holds_and_removes_the_rule()
    {
        var (faults, _, _) = Replica();
        var hold = faults.Hold(LogFaultPoints.PassBegin);
        var reaching = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();
        await hold.WaitReachedAsync();

        hold.Dispose();

        await reaching.WaitAsync(Limit);
        Assert.True(faults.ReachAsync(LogFaultPoints.PassBegin).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Waiting_for_a_point_nobody_reaches_times_out_and_says_what_was_seen()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.Hold(LogFaultPoints.PassBegin);

        var thrown = await Assert.ThrowsAsync<TimeoutException>(() => hold.WaitReachedAsync(1, TimeSpan.FromMilliseconds(100)));

        Assert.Contains("log.applier.pass-begin", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_throw_fires_the_given_number_of_times_then_the_point_passes()
    {
        var (faults, _, _) = Replica();
        using var rule = faults.Throw(LogFaultPoints.AppendBefore, () => new InvalidOperationException("injected"), times: 2);

        await Assert.ThrowsAsync<InvalidOperationException>(() => faults.ReachAsync(LogFaultPoints.AppendBefore).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => faults.ReachAsync(LogFaultPoints.AppendBefore).AsTask());
        await faults.ReachAsync(LogFaultPoints.AppendBefore);
    }

    [Fact]
    public async Task A_delay_runs_on_the_replicas_own_clock()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        var faults = new HarnessFaultPoints("r1", clock, new ArrivalLog(new RunSequence()));
        clock.SetRate(5);
        using var rule = faults.Delay(LogFaultPoints.PassBegin, TimeSpan.FromSeconds(2));

        var reach = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();

        real.Advance(TimeSpan.FromMilliseconds(399));
        Assert.False(reach.IsCompleted);
        real.Advance(TimeSpan.FromMilliseconds(1));
        await reach.WaitAsync(Limit);
    }

    [Fact]
    public async Task An_observer_runs_at_every_arrival_before_the_replica_goes_on()
    {
        var (faults, _, _) = Replica();
        var seen = new List<long>();
        using var rule = faults.Observe(LogFaultPoints.PassBegin, arrival =>
        {
            seen.Add(arrival.Sequence);
            return ValueTask.CompletedTask;
        });

        await faults.ReachAsync(LogFaultPoints.PassBegin);
        await faults.ReachAsync(LogFaultPoints.PassBegin);

        Assert.Equal([1, 2], seen);
    }

    [Fact]
    public async Task Rules_on_one_point_apply_in_the_order_they_were_added()
    {
        var (faults, _, _) = Replica();
        var trace = new List<string>();
        using var a = faults.Observe(LogFaultPoints.PassBegin, _ =>
        {
            trace.Add("observed");
            return ValueTask.CompletedTask;
        });
        using var b = faults.Throw(LogFaultPoints.PassBegin, () => new InvalidOperationException("after the observer"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => faults.ReachAsync(LogFaultPoints.PassBegin).AsTask());

        Assert.Equal(["observed"], trace);
    }

    [Fact]
    public async Task A_name_outside_the_catalogue_is_refused_when_reached_and_when_resolved()
    {
        var (faults, _, _) = Replica();

        await Assert.ThrowsAsync<UnknownFaultPointException>(() => faults.ReachAsync("membership.renew.sent").AsTask());
        Assert.Throws<UnknownFaultPointException>(() => UnknownFaultPointException.Resolve("nope"));
        Assert.Equal(MembershipFaultPoints.RenewSend, UnknownFaultPointException.Resolve("membership.renew.send"));
    }

    [Fact]
    public async Task A_paused_replica_holds_every_point_even_one_with_no_rule_until_resumed()
    {
        var (faults, _, _) = Replica();
        faults.Pause();

        var reaching = faults.ReachAsync(MembershipFaultPoints.RenewSend).AsTask();
        await Task.Delay(150);

        Assert.False(reaching.IsCompleted);

        faults.Resume();
        await reaching.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_pause_does_not_replace_a_hold_the_arrival_still_stops_at_its_rule_after_the_resume()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.PassBegin);
        faults.Pause();
        var reaching = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();

        faults.Resume();
        await hold.WaitReachedAsync();

        Assert.False(reaching.IsCompleted);
        hold.Release();
        await reaching.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_dead_replica_stands_still_and_ignores_cancellation()
    {
        var (faults, _, _) = Replica();
        faults.Abandon();
        using var cts = new CancellationTokenSource();

        var reaching = faults.ReachAsync(MembershipFaultPoints.RenewSend, cts.Token).AsTask();
        cts.Cancel();
        await Task.Delay(150);

        Assert.False(reaching.IsCompleted);
    }

    [Fact]
    public async Task Replicas_share_one_arrival_order_and_each_counts_its_own_arrivals()
    {
        var log = new ArrivalLog(new RunSequence());
        var clock = new DriftingTimeProvider();
        var a = new HarnessFaultPoints("a", clock, log);
        var b = new HarnessFaultPoints("b", clock, log);

        await a.ReachAsync(LogFaultPoints.PassBegin);
        await b.ReachAsync(LogFaultPoints.PassBegin);
        await a.ReachAsync(LogFaultPoints.PassBegin);

        Assert.Equal(["a", "b", "a"], log.Snapshot().Select(x => x.Replica));
        Assert.Equal([1, 2, 3], log.Snapshot().Select(x => x.Sequence));
        Assert.Equal(2, a.Reached(LogFaultPoints.PassBegin));
        Assert.Equal(1, b.Reached(LogFaultPoints.PassBegin));
    }

    [Fact]
    public async Task A_hold_released_during_a_pause_does_not_let_the_arrival_go_on_until_the_resume()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.PassBegin);
        var reaching = faults.ReachAsync(LogFaultPoints.PassBegin).AsTask();
        await hold.WaitReachedAsync();

        faults.Pause();
        hold.Release();
        await Task.Delay(250);

        Assert.False(reaching.IsCompleted);

        faults.Resume();
        await reaching.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_token_cancelled_during_a_pause_is_seen_only_after_the_resume()
    {
        var (faults, _, _) = Replica();
        using var cts = new CancellationTokenSource();
        faults.Pause();
        using var hold = faults.HoldNext(LogFaultPoints.PassBegin);
        var reaching = faults.ReachAsync(LogFaultPoints.PassBegin, cts.Token).AsTask();

        cts.Cancel();
        await Task.Delay(250);

        Assert.False(reaching.IsCompleted);

        faults.Resume();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reaching).WaitAsync(Limit);
    }

    [Fact]
    public async Task A_fault_thrown_at_a_point_during_a_pause_surfaces_after_the_resume()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(LogFaultPoints.AppendAfter);
        var reaching = faults.ReachAsync(LogFaultPoints.AppendAfter).AsTask();
        await hold.WaitReachedAsync();
        faults.Pause();

        hold.Fail(new IOException("late"));
        await Task.Delay(250);

        Assert.False(reaching.IsCompleted);

        faults.Resume();
        await Assert.ThrowsAsync<IOException>(() => reaching).WaitAsync(Limit);
    }

    [Fact]
    public async Task Arrivals_that_came_during_a_pause_meet_a_rule_armed_during_it_in_the_order_they_came()
    {
        var (faults, log, _) = Replica();
        faults.Pause();
        var arrivals = new List<Task>();
        for (var i = 0; i < 4; i++)
        {
            arrivals.Add(faults.ReachAsync(LogFaultPoints.PassBegin).AsTask());
        }

        using var hold = faults.HoldNext(LogFaultPoints.PassBegin);
        faults.Resume();
        await hold.WaitReachedAsync();

        Assert.Equal(log.Snapshot()[0].Sequence, Assert.Single(hold.Held).Sequence);
        hold.Release();
        await Task.WhenAll(arrivals).WaitAsync(Limit);
    }

    [Fact]
    public async Task A_rule_disposed_while_an_arrival_has_claimed_it_lets_the_arrival_pass()
    {
        var (faults, _, _) = Replica();
        HoldRule? hold = null;
        using var observer = faults.Observe(LogFaultPoints.PassBegin, _ =>
        {
            hold!.Dispose();
            return ValueTask.CompletedTask;
        });
        hold = faults.Hold(LogFaultPoints.PassBegin);

        await faults.ReachAsync(LogFaultPoints.PassBegin).AsTask().WaitAsync(Limit);
    }

    [Fact]
    public async Task Disposing_holds_while_arrivals_come_never_strands_one()
    {
        var (faults, _, _) = Replica();

        for (var i = 0; i < 1500; i++)
        {
            var hold = faults.Hold(LogFaultPoints.PassBegin);
            var reaching = Task.Run(() => faults.ReachAsync(LogFaultPoints.PassBegin).AsTask());
            hold.Dispose();
            await reaching.WaitAsync(Limit);
        }
    }

    [Fact]
    public async Task A_release_that_wins_against_a_cancellation_lets_the_arrival_go_on_and_one_that_loses_reports_it()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.Hold(LogFaultPoints.PassBegin);
        using var releasedFirst = new CancellationTokenSource();
        using var cancelledFirst = new CancellationTokenSource();
        var won = faults.ReachAsync(LogFaultPoints.PassBegin, releasedFirst.Token).AsTask();
        await hold.WaitReachedAsync(1);
        var lost = faults.ReachAsync(LogFaultPoints.PassBegin, cancelledFirst.Token).AsTask();
        await hold.WaitReachedAsync(2);

        Assert.True(hold.Release());
        releasedFirst.Cancel();
        cancelledFirst.Cancel();

        await won.WaitAsync(Limit);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lost).WaitAsync(Limit);
        Assert.False(hold.Release());
        Assert.Equal(1, hold.CancelledTotal);
        Assert.Equal(1, hold.ReleasedTotal);
    }

    [Fact]
    public async Task A_dead_replicas_held_arrival_does_not_go_on_when_released_or_cancelled()
    {
        var (faults, _, _) = Replica();
        using var hold = faults.HoldNext(MembershipFaultPoints.RenewSend);
        using var cts = new CancellationTokenSource();
        var reaching = faults.ReachAsync(MembershipFaultPoints.RenewSend, cts.Token).AsTask();
        await hold.WaitReachedAsync();

        faults.Abandon();
        hold.Release();
        cts.Cancel();
        await Task.Delay(250);

        Assert.False(reaching.IsCompleted);
    }

    [Fact]
    public async Task A_dead_replica_reaches_nothing_and_is_not_recorded_as_having()
    {
        var (faults, log, _) = Replica();
        await faults.ReachAsync(LogFaultPoints.PassBegin);

        faults.Abandon();
        _ = faults.ReachAsync(LogFaultPoints.PassBegin);

        Assert.Single(log.Snapshot());
    }
}
