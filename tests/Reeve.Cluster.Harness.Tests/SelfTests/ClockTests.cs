using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Harness.Time;

namespace Harness.SelfTests;

public sealed class ClockTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    [Fact]
    public void A_slow_clock_measures_less_time_than_passed()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        clock.SetRate(0.5);
        var start = clock.GetTimestamp();

        real.Advance(TimeSpan.FromMilliseconds(600));

        Assert.Equal(TimeSpan.FromMilliseconds(300), clock.GetElapsedTime(start));
    }

    [Fact]
    public void A_fast_clock_measures_more_time_than_passed()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        clock.SetRate(3);
        var start = clock.GetTimestamp();

        real.Advance(TimeSpan.FromMilliseconds(400));

        Assert.Equal(TimeSpan.FromMilliseconds(1200), clock.GetElapsedTime(start));
    }

    [Fact]
    public void A_timer_fires_when_the_replicas_clock_says_it_is_due()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        clock.SetRate(2);
        var fired = 0;

        using var timer = clock.CreateTimer(_ => Interlocked.Increment(ref fired), null, TimeSpan.FromMilliseconds(800), Timeout.InfiniteTimeSpan);

        real.Advance(TimeSpan.FromMilliseconds(399));
        Assert.Equal(0, fired);
        real.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(1, fired);
    }

    [Fact]
    public async Task Task_Delay_on_the_clock_follows_its_rate()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        clock.SetRate(4);

        var delay = Task.Delay(TimeSpan.FromSeconds(2), clock);

        real.Advance(TimeSpan.FromMilliseconds(499));
        Assert.False(delay.IsCompleted);
        real.Advance(TimeSpan.FromMilliseconds(1));
        await delay.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_change_of_rate_re_aims_the_timers_already_waiting()
    {
        var clock = new DriftingTimeProvider();
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timer = clock.CreateTimer(_ => fired.TrySetResult(), null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);

        clock.SetRate(100);

        await fired.Task.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_shift_moves_the_timeline_and_fires_what_fell_due()
    {
        var clock = new DriftingTimeProvider();
        var start = clock.GetTimestamp();
        var wall = clock.GetUtcNow();
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timer = clock.CreateTimer(_ => fired.TrySetResult(), null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);

        clock.Shift(TimeSpan.FromSeconds(40));

        await fired.Task.WaitAsync(Limit);
        Assert.True(clock.GetElapsedTime(start) >= TimeSpan.FromSeconds(40));
        Assert.True(clock.GetUtcNow() - wall >= TimeSpan.FromSeconds(40));
    }

    [Fact]
    public void A_wall_step_moves_the_wall_clock_and_not_the_monotonic_one()
    {
        var clock = new DriftingTimeProvider(new FakeTimeProvider());
        var start = clock.GetTimestamp();
        var wall = clock.GetUtcNow();

        clock.StepWall(TimeSpan.FromMinutes(-5));

        Assert.True(clock.GetUtcNow() - wall < TimeSpan.FromMinutes(-4.9));
        Assert.Equal(TimeSpan.Zero, clock.GetElapsedTime(start));
    }

    [Fact]
    public async Task A_paused_clock_keeps_running_and_its_timers_wait_for_the_resume()
    {
        var clock = new DriftingTimeProvider();
        var start = clock.GetTimestamp();
        var fired = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timer = clock.CreateTimer(_ => fired.TrySetResult(clock.GetElapsedTime(start)), null, TimeSpan.FromMilliseconds(100), Timeout.InfiniteTimeSpan);

        clock.Pause();
        await Task.Delay(500);

        Assert.False(fired.Task.IsCompleted);
        Assert.True(clock.GetElapsedTime(start) >= TimeSpan.FromMilliseconds(450));

        clock.Resume();

        var seen = await fired.Task.WaitAsync(Limit);
        Assert.True(seen >= TimeSpan.FromMilliseconds(450), $"the timer fired at {seen}");
    }

    [Fact]
    public async Task Timers_that_fell_due_in_a_pause_fire_in_due_order_on_resume()
    {
        var clock = new DriftingTimeProvider();
        var order = new List<string>();
        var done = new CountdownEvent(3);

        void Add(string name)
        {
            lock (order)
            {
                order.Add(name);
            }

            done.Signal();
        }

        using var late = clock.CreateTimer(_ => Add("late"), null, TimeSpan.FromMilliseconds(150), Timeout.InfiniteTimeSpan);
        using var early = clock.CreateTimer(_ => Add("early"), null, TimeSpan.FromMilliseconds(50), Timeout.InfiniteTimeSpan);
        using var middle = clock.CreateTimer(_ => Add("middle"), null, TimeSpan.FromMilliseconds(100), Timeout.InfiniteTimeSpan);

        clock.Pause();
        await Task.Delay(400);
        clock.Resume();

        Assert.True(await Task.Run(() => done.Wait(Limit)));
        Assert.Equal(["early", "middle", "late"], order);
    }

    [Fact]
    public void A_periodic_timer_fires_once_after_a_pause_and_then_goes_on()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        var fires = 0;
        using var timer = clock.CreateTimer(_ => Interlocked.Increment(ref fires), null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));

        clock.Pause();
        real.Advance(TimeSpan.FromMilliseconds(700));
        Assert.Equal(0, fires);
        clock.Resume();
        WaitUntil(() => Volatile.Read(ref fires) >= 1);

        Assert.Equal(1, Volatile.Read(ref fires));
        real.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Equal(2, Volatile.Read(ref fires));
        real.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Equal(3, Volatile.Read(ref fires));
    }

    private static void WaitUntil(Func<bool> condition)
    {
        SpinWait.SpinUntil(condition, Limit);
    }

    [Fact]
    public void A_shift_back_is_refused_because_a_monotonic_clock_never_goes_back()
    {
        var clock = new DriftingTimeProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Shift(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void A_disposed_timer_leaves_the_clock()
    {
        var clock = new DriftingTimeProvider();
        var timer = clock.CreateTimer(_ => { }, null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);
        Assert.Equal(1, clock.LiveTimers);

        timer.Dispose();

        Assert.Equal(0, clock.LiveTimers);
    }
}
