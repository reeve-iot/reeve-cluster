using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Harness.Timing;

namespace Harness.SelfTests;

public sealed class StallProbeTests
{
    private static (FakeTimeProvider Clock, StallProbe Probe) Fake()
    {
        var clock = new FakeTimeProvider();
        return (clock, new StallProbe(clock.GetTimestamp, clock.TimestampFrequency));
    }

    [Fact]
    public void A_probe_that_ticks_on_time_sees_no_stall()
    {
        var (clock, probe) = Fake();
        var span = probe.Begin();

        for (var i = 0; i < 100; i++)
        {
            clock.Advance(StallProbe.Interval);
            probe.Tick();
        }

        Assert.Equal(TimeSpan.Zero, span.Largest);
        Assert.False(span.Exceeds(TimeSpan.Zero));
    }

    [Fact]
    public void A_gap_between_ticks_is_a_stall_by_what_it_exceeds_the_interval()
    {
        var (clock, probe) = Fake();
        var span = probe.Begin();

        clock.Advance(StallProbe.Interval);
        probe.Tick();
        clock.Advance(TimeSpan.FromSeconds(3) + StallProbe.Interval);
        probe.Tick();
        clock.Advance(StallProbe.Interval);
        probe.Tick();

        Assert.Equal(TimeSpan.FromSeconds(3), span.Largest);
        Assert.True(span.Exceeds(TimeSpan.FromSeconds(2.9)));
        Assert.False(span.Exceeds(TimeSpan.FromSeconds(3)));
    }

    [Fact]
    public void A_stall_in_progress_counts_before_the_probe_ticks_again()
    {
        var (clock, probe) = Fake();
        var span = probe.Begin();

        clock.Advance(TimeSpan.FromSeconds(2) + StallProbe.Interval);

        Assert.Equal(TimeSpan.FromSeconds(2), span.Largest);
    }

    [Fact]
    public void A_span_sees_only_the_stalls_after_it_began_and_the_part_of_one_that_straddles_it()
    {
        var (clock, probe) = Fake();
        clock.Advance(TimeSpan.FromSeconds(5) + StallProbe.Interval);
        probe.Tick();
        clock.Advance(StallProbe.Interval);
        probe.Tick();

        var after = probe.Begin();
        Assert.Equal(TimeSpan.Zero, after.Largest);

        var straddling = probe.Begin();
        clock.Advance(TimeSpan.FromSeconds(1) + StallProbe.Interval);
        probe.Tick();
        Assert.Equal(TimeSpan.FromSeconds(1), straddling.Largest);
        Assert.Equal(TimeSpan.FromSeconds(5), probe.LargestGap);
    }

    [Fact]
    public void A_stall_measured_elsewhere_is_noted_with_its_source()
    {
        var (clock, probe) = Fake();
        var span = probe.Begin();
        for (var i = 0; i < 300; i++)
        {
            clock.Advance(StallProbe.Interval);
            probe.Tick();
        }

        probe.Note(TimeSpan.FromSeconds(5), "observer connection open");

        Assert.Contains("5 s (observer connection open)", span.Describe(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_probe_of_its_own_runs_on_a_thread_and_stops_when_disposed()
    {
        using var probe = StallProbe.StartNew();
        var span = probe.Begin();

        await Task.Delay(TimeSpan.FromMilliseconds(200));

        Assert.True(span.Largest < TimeSpan.FromSeconds(30), span.Describe());
    }
}
