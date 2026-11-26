using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Timing;
using Xunit.Sdk;

namespace Harness.SelfTests;

public sealed class ProfileAndWindowTests
{
    [Fact]
    public void The_short_profile_passes_the_timing_validator()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        Assert.Empty(OptionsValidator.Validate(options, aspNetCoreInUse: false));
    }

    [Fact]
    public void The_short_profile_is_the_one_the_plan_names()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        Assert.Equal(TimeSpan.FromSeconds(8), options.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(1), options.SafetyMargin);
        Assert.Equal(TimeSpan.FromSeconds(1), options.StopBudget);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.RenewTimeout);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), options.RenewInterval);
    }

    [Fact]
    public void Fence_start_lands_between_the_local_deadline_less_the_stop_budget_and_its_timers_lateness()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        var window = TimingWindows.FenceStart(options, timerLateness: TimeSpan.FromMilliseconds(400));

        Assert.Equal(TimeSpan.FromSeconds(6), window.Earliest);
        Assert.Equal(TimeSpan.FromMilliseconds(6400), window.Latest);
        window.AssertContains(TimeSpan.FromSeconds(6.2));
    }

    [Fact]
    public void Fence_start_always_precedes_the_expiry_the_database_set()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        Assert.True(TimingWindows.FenceStart(options).Latest < options.LeaseTtl);
    }

    [Fact]
    public void A_slow_clock_stretches_the_window_in_real_time()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        var window = TimingWindows.FenceStart(options, TimeSpan.Zero).OnClockOfRate(0.5);

        Assert.Equal(TimeSpan.FromMilliseconds(11950), window.Earliest);
    }

    [Fact]
    public void An_event_outside_its_window_fails_the_test_naming_the_window_and_what_was_seen()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");
        var window = TimingWindows.FenceStart(options);

        var failure = Assert.Throws<WindowViolationException>(() => window.AssertContains(TimeSpan.FromSeconds(4.5)));

        Assert.IsAssignableFrom<XunitException>(failure);
        Assert.Contains("fence start", failure.Message, StringComparison.Ordinal);
        Assert.Contains("4.5", failure.Message, StringComparison.Ordinal);
        Assert.Contains("6", failure.Message, StringComparison.Ordinal);
        Assert.False(window.Contains(TimeSpan.FromSeconds(7)));
    }

    [Fact]
    public void The_outage_the_fleet_rides_out_is_fifteen_seconds_with_the_defaults()
    {
        Assert.Equal(TimeSpan.FromSeconds(15), TimingWindows.GuaranteedOutage(new ClusterOptions()));
    }

    [Fact]
    public void The_acknowledgement_after_a_crash_is_bounded_below_by_the_lease_and_above_by_one_poll_more()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        var window = TimingWindows.AckAfterCrash(options, TimeSpan.Zero);

        Assert.Equal(TimeSpan.FromSeconds(6), window.Earliest);
        Assert.Equal(options.LeaseTtl + options.AckPollInterval, window.Latest);
        Assert.False(window.Contains(TimeSpan.FromSeconds(1)), "an acknowledgement a second after a crash dropped a member that was still leased");
    }

    [Fact]
    public void The_slowest_clock_the_assumptions_allow_loses_half_the_safety_margin_over_a_lease()
    {
        Assert.Equal(0.9375, TimingWindows.SlowestClockRate(TestProfile.Options("r1", "Host=localhost;Username=u")));
        Assert.Equal(1 - 3.0 / 60, TimingWindows.SlowestClockRate(new ClusterOptions()), 10);
    }

    [Fact]
    public void Under_the_short_profile_none_of_the_outages_the_spec_names_is_ridden_out_and_under_the_defaults_they_span_the_regimes()
    {
        var shortProfile = TestProfile.Options("r1", "Host=localhost;Username=u");
        var defaults = new ClusterOptions();
        TestProfile.ApplyLibraryDefaults(defaults);

        Assert.Equal(
            [OutageRegime.Either, OutageRegime.Fences, OutageRegime.Fences, OutageRegime.Fences],
            Reeve.Cluster.Harness.Stack.DatabaseFaults.Outages.Select(outage => OutageRegimes.Of(shortProfile, outage)));
        Assert.Equal(
            [OutageRegime.RiddenOut, OutageRegime.RiddenOut, OutageRegime.Either, OutageRegime.Fences],
            Reeve.Cluster.Harness.Stack.DatabaseFaults.Outages.Select(outage => OutageRegimes.Of(defaults, outage)));
    }

    [Fact]
    public void Outages_for_a_set_of_options_come_one_from_each_regime()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        var outages = Reeve.Cluster.Harness.Stack.DatabaseFaults.OutagesFor(options);

        Assert.Equal(
            [OutageRegime.RiddenOut, OutageRegime.Either, OutageRegime.Fences],
            outages.Select(outage => OutageRegimes.Of(options, outage)));
    }

    [Fact]
    public void The_lease_release_is_bounded_by_the_renew_timeout()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u");

        Assert.Equal(options.RenewTimeout, TimingWindows.LeaseRelease(options, TimeSpan.Zero).Latest);
    }

    [Fact]
    public void The_stall_tolerant_profile_passes_the_timing_validator_and_rides_out_a_five_second_stall()
    {
        var options = TestProfile.Options("r1", "Host=localhost;Username=u", profile: null);

        Assert.Empty(OptionsValidator.Validate(options, aspNetCoreInUse: false));
        Assert.Equal(TimeSpan.FromSeconds(16), TimingWindows.FenceStart(options, TimeSpan.Zero).Earliest);
        Assert.True(options.LeaseTtl - options.SafetyMargin - options.StopBudget - options.RenewInterval - options.RenewTimeout > 2 * TimeSpan.FromSeconds(5));
        Assert.True(options.ApplyStallLimit + options.ReconcileTimeout > 2 * TimeSpan.FromSeconds(5));
        Assert.True(options.AckTimeout > options.LeaseTtl);
    }

    [Fact]
    public void The_tight_profile_is_asked_for_by_name_and_is_the_short_one()
    {
        var tight = new ClusterOptions();
        TestProfile.ApplyTight(tight);
        var shortProfile = TestProfile.Options("r1", "Host=localhost;Username=u");

        Assert.Equal(shortProfile.LeaseTtl, tight.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(2), tight.ApplyStallLimit);
    }

    [Fact]
    public void A_late_event_that_a_stall_accounts_for_is_degraded_and_is_not_a_failure()
    {
        var clock = new FakeTimeProvider();
        var probe = new StallProbe(clock.GetTimestamp, clock.TimestampFrequency);
        var window = TimingWindows.FenceStart(TestProfile.Options("r1", "Host=localhost;Username=u"));
        var lines = new List<string>();
        var span = probe.Begin();

        clock.Advance(TimeSpan.FromSeconds(5) + StallProbe.Interval);
        probe.Tick();

        Assert.False(window.AssertContains(TimeSpan.FromSeconds(10.8), span, lines.Add));
        var line = Assert.Single(lines);
        Assert.StartsWith(Degraded.Marker + " ProfileAndWindowTests." + nameof(A_late_event_that_a_stall_accounts_for_is_degraded_and_is_not_a_failure) + " ", line, StringComparison.Ordinal);
        Assert.Contains("largest stall 5 s", line, StringComparison.Ordinal);
        Assert.Contains(line, File.ReadAllLines(Degraded.FilePath));
    }

    [Fact]
    public void A_late_event_with_no_stall_to_account_for_it_still_fails_and_so_does_an_early_one()
    {
        var clock = new FakeTimeProvider();
        var probe = new StallProbe(clock.GetTimestamp, clock.TimestampFrequency);
        var window = TimingWindows.FenceStart(TestProfile.Options("r1", "Host=localhost;Username=u"));
        var lines = new List<string>();
        var span = probe.Begin();
        clock.Advance(TimeSpan.FromSeconds(1) + StallProbe.Interval);
        probe.Tick();

        Assert.False(window.AssertContains(TimeSpan.FromSeconds(7.7), span, lines.Add));
        var late = Assert.Throws<WindowViolationException>(() => window.AssertContains(TimeSpan.FromSeconds(10), span, lines.Add));
        Assert.Contains("largest stall 1 s", late.Message, StringComparison.Ordinal);
        Assert.Throws<WindowViolationException>(() => window.AssertContains(TimeSpan.FromSeconds(4.5), span, lines.Add));
        Assert.Single(lines);
    }
}
