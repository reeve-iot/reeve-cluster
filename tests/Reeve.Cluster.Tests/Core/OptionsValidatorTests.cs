using CsCheck;
using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Core;

public sealed class OptionsValidatorTests
{
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private static ClusterOptions Valid() => new()
    {
        ReplicaId = "replica-1",
        ConnectionString = "Host=db;Database=app;Username=u",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
    };

    private static ClusterOptions ValidForHttp() => new()
    {
        ReplicaId = "replica-1",
        ConnectionString = "Host=db;Database=app;Username=u",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
        ProxyReadTimeout = TimeSpan.FromSeconds(120),
        ActionTimeBudget = TimeSpan.FromSeconds(15),
    };

    private static IReadOnlyList<OptionsViolation> Rules(ClusterOptions options, bool http = false) =>
        [.. OptionsValidator.Validate(options, http).Select(p => p.Rule)];

    [Fact]
    public void The_defaults_with_the_required_settings_are_valid()
    {
        Assert.Empty(OptionsValidator.Validate(Valid(), aspNetCoreInUse: false));
    }

    [Fact]
    public void The_documented_defaults_are_the_specs()
    {
        var o = Valid();

        Assert.Equal(TimeSpan.FromSeconds(30), o.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(3), o.SafetyMargin);
        Assert.Equal(TimeSpan.FromSeconds(5), o.StopBudget);
        Assert.Equal(TimeSpan.FromSeconds(3), o.RenewTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), o.RenewInterval);
        Assert.Equal(TimeSpan.FromSeconds(20), o.ApplyStallLimit);
        Assert.Equal(TimeSpan.FromSeconds(1), o.AckPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(1), o.ApplyPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(60), o.RetentionInterval);
        Assert.Equal(TimeSpan.FromSeconds(30), o.ReplicationCheckInterval);
        Assert.Equal(TimeSpan.FromSeconds(3), o.StatusReportInterval);
        Assert.Equal(TimeSpan.FromSeconds(10), o.ReadIndexTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), o.FencedTransactionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), o.PlacementInterval);
        Assert.Equal(TimeSpan.FromSeconds(12), o.ReleaseGrace);
        Assert.Equal(TimeSpan.FromSeconds(30), o.StabilisationWindow);
        Assert.Equal(TimeSpan.FromHours(24), o.OwnedRowRetention);
        Assert.Equal(1000, o.HintLimit);
        Assert.Equal(TimeSpan.FromSeconds(60), o.RejoinBackoff);
        Assert.True(o.CheckSynchronousReplication);
    }

    [Fact]
    public void The_AckTimeout_default_with_a_ten_second_ReconcileTimeout_is_the_specs_36_seconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(36), Valid().AckTimeout);
    }

    [Fact]
    public void A_derived_default_follows_the_setting_it_derives_from_until_it_is_set()
    {
        var o = Valid();
        o.AckPollInterval = TimeSpan.FromSeconds(4);
        o.LeaseTtl = TimeSpan.FromSeconds(60);
        o.RenewInterval = TimeSpan.FromSeconds(5);

        Assert.Equal(TimeSpan.FromSeconds(4), o.ApplyPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(60), o.ReplicationCheckInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), o.StatusReportInterval);

        o.ApplyPollInterval = TimeSpan.FromSeconds(9);
        o.AckPollInterval = TimeSpan.FromSeconds(1);

        Assert.Equal(TimeSpan.FromSeconds(9), o.ApplyPollInterval);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void It_refuses_a_missing_ReplicaId(string? replicaId)
    {
        var o = Valid();
        o.ReplicaId = replicaId;

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.ReplicaIdMissing, problem.Rule);
        Assert.Equal("ReplicaId", problem.Setting);
    }

    [Fact]
    public void It_refuses_a_missing_ReconcileTimeout_and_does_not_misreport_its_derived_settings()
    {
        var o = Valid();
        o.ReconcileTimeout = null;

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.ReconcileTimeoutMissing, problem.Rule);
        Assert.Equal("ReconcileTimeout", problem.Setting);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void It_refuses_a_missing_ConnectionString(string? connectionString)
    {
        var o = Valid();
        o.ConnectionString = connectionString;

        Assert.Equal([OptionsViolation.ConnectionStringMissing], Rules(o));
    }

    [Fact]
    public void ProxyReadTimeout_and_ActionTimeBudget_are_required_only_when_AspNetCore_is_in_use()
    {
        var o = Valid();

        Assert.Empty(Rules(o, http: false));
        Assert.Equal(
            [OptionsViolation.ProxyReadTimeoutMissing, OptionsViolation.ActionTimeBudgetMissing],
            Rules(o, http: true));
    }

    [Fact]
    public void It_refuses_ReadIndexTimeout_plus_ActionTimeBudget_plus_AckTimeout_at_or_above_ProxyReadTimeout()
    {
        var o = ValidForHttp();
        var sum = o.ReadIndexTimeout + o.ActionTimeBudget!.Value + o.AckTimeout;

        o.ProxyReadTimeout = sum;
        Assert.Equal([OptionsViolation.HttpBoundExceeded], Rules(o, http: true));

        o.ProxyReadTimeout = sum - Tick;
        Assert.Equal([OptionsViolation.HttpBoundExceeded], Rules(o, http: true));

        o.ProxyReadTimeout = sum + Tick;
        Assert.Empty(Rules(o, http: true));
    }

    [Fact]
    public void The_HTTP_bound_is_not_checked_without_AspNetCore()
    {
        var o = ValidForHttp();
        o.ProxyReadTimeout = TimeSpan.FromSeconds(1);

        Assert.Empty(Rules(o, http: false));
    }

    [Fact]
    public void The_specs_example_keeps_ActionTimeBudget_below_14_seconds_behind_a_60_second_proxy()
    {
        var o = ValidForHttp();
        o.ProxyReadTimeout = TimeSpan.FromSeconds(60);

        o.ActionTimeBudget = TimeSpan.FromSeconds(14) - Tick;
        Assert.Empty(Rules(o, http: true));

        o.ActionTimeBudget = TimeSpan.FromSeconds(14);
        Assert.Equal([OptionsViolation.HttpBoundExceeded], Rules(o, http: true));
    }

    [Fact]
    public void It_refuses_an_AckTimeout_at_or_below_its_relation()
    {
        var o = Valid();
        var bound = o.AckTimeoutRelationBound;

        o.AckTimeout = bound;
        Assert.Equal([OptionsViolation.AckTimeoutTooShort], Rules(o));

        o.AckTimeout = bound - Tick;
        Assert.Equal([OptionsViolation.AckTimeoutTooShort], Rules(o));

        o.AckTimeout = bound + Tick;
        Assert.Empty(Rules(o));
    }

    [Fact]
    public void The_AckTimeout_relation_takes_the_larger_of_LeaseTtl_and_the_stall_budget()
    {
        var o = Valid();
        o.AckTimeout = o.LeaseTtl + o.AckPollInterval + Tick;

        Assert.Equal([OptionsViolation.AckTimeoutTooShort], Rules(o));

        o.ReconcileTimeout = TimeSpan.FromSeconds(3);
        o.AckTimeout = o.LeaseTtl + o.AckPollInterval - Tick;
        Assert.Equal([OptionsViolation.AckTimeoutTooShort], Rules(o));

        o.AckTimeout = o.LeaseTtl + o.AckPollInterval + Tick;
        Assert.Empty(Rules(o));
    }

    [Fact]
    public void It_accepts_the_AckTimeout_default_for_every_valid_RenewTimeout()
    {
        Gen.Select(
                Gen.Int[5, 600],
                Gen.Int[1, 5],
                Gen.Int[1, 10],
                Gen.Int[1, 5],
                Gen.Double[0, 1])
            .Where(t => (t.Item1 - t.Item2 - (t.Item2 / 2.0 + 0.5)) > 1)
            .Sample(t =>
            {
                var o = Valid();
                o.LeaseTtl = TimeSpan.FromSeconds(t.Item1);
                o.SafetyMargin = TimeSpan.FromSeconds(t.Item2);
                o.StopBudget = TimeSpan.FromSeconds(t.Item2 / 2.0 + 0.5);
                o.ReconcileTimeout = TimeSpan.FromSeconds(t.Item3);
                o.AckPollInterval = TimeSpan.FromSeconds(t.Item4);

                var room = o.LeaseTtl - o.SafetyMargin - o.StopBudget;
                o.RenewInterval = TimeSpan.FromTicks(Math.Max(1, (long)(room.Ticks * 0.1)));
                var maxRenewTimeout = room - (3 * o.RenewInterval);
                o.RenewTimeout = TimeSpan.FromTicks(Math.Max(1, (long)(maxRenewTimeout.Ticks * t.Item5)));

                var rules = Rules(o);

                Assert.DoesNotContain(OptionsViolation.AckTimeoutTooShort, rules);
                Assert.Empty(rules);
            }, iter: 2000);
    }

    [Fact]
    public void The_AckTimeout_default_beats_its_relation_by_exactly_the_slack_for_any_setting()
    {
        Gen.Select(Gen.Int[1, 120], Gen.Int[1, 60], Gen.Int[1, 30], Gen.Int[1, 30])
            .Sample(t =>
            {
                var o = Valid();
                o.LeaseTtl = TimeSpan.FromSeconds(t.Item1);
                o.ReconcileTimeout = TimeSpan.FromSeconds(t.Item2);
                o.RenewTimeout = TimeSpan.FromSeconds(t.Item3);
                o.AckPollInterval = TimeSpan.FromSeconds(t.Item4);

                Assert.Equal(o.AckTimeoutRelationBound + ClusterOptions.AckTimeoutDefaultSlack, o.AckTimeout);
                Assert.True(o.AckTimeout > o.AckTimeoutRelationBound);
            }, iter: 500);
    }

    [Fact]
    public void It_refuses_StopBudget_at_or_below_half_of_SafetyMargin()
    {
        var o = Valid();
        o.SafetyMargin = TimeSpan.FromSeconds(4);

        o.StopBudget = TimeSpan.FromSeconds(2);
        Assert.Equal([OptionsViolation.StopBudgetTooShort], Rules(o));

        o.StopBudget = TimeSpan.FromSeconds(1);
        Assert.Contains(OptionsViolation.StopBudgetTooShort, Rules(o));

        o.StopBudget = TimeSpan.FromSeconds(2) + Tick;
        Assert.DoesNotContain(OptionsViolation.StopBudgetTooShort, Rules(o));
    }

    [Fact]
    public void It_refuses_a_RenewInterval_above_a_third_of_what_is_left_of_the_lease()
    {
        var o = Valid();
        var limit = o.RenewIntervalLimit;
        Assert.Equal(TimeSpan.FromTicks(TimeSpan.FromSeconds(19).Ticks / 3), limit);

        o.RenewInterval = limit;
        Assert.Empty(Rules(o));

        o.RenewInterval = limit + Tick;
        Assert.Equal([OptionsViolation.RenewIntervalTooLong], Rules(o));
    }

    [Fact]
    public void It_refuses_a_lease_too_short_for_the_stop_budget_and_the_renew_timeout()
    {
        var o = Valid();
        o.LeaseTtl = o.SafetyMargin + o.StopBudget + o.RenewTimeout;

        Assert.Contains(OptionsViolation.RenewIntervalTooLong, Rules(o));
    }

    [Fact]
    public void It_refuses_an_ApplyStallLimit_below_twice_ReconcileTimeout()
    {
        var o = Valid();

        o.ApplyStallLimit = (2 * o.ReconcileTimeout!.Value) - Tick;
        Assert.Contains(OptionsViolation.ApplyStallLimitTooShort, Rules(o));

        o.ApplyStallLimit = 2 * o.ReconcileTimeout!.Value;
        Assert.DoesNotContain(OptionsViolation.ApplyStallLimitTooShort, Rules(o));
    }

    [Fact]
    public void It_refuses_a_ReleaseGrace_that_is_not_two_StopBudgets_plus_PlacementInterval()
    {
        var o = Valid();
        o.ReleaseGrace = TimeSpan.FromSeconds(12);
        Assert.Empty(Rules(o));

        o.ReleaseGrace = TimeSpan.FromSeconds(12) + Tick;
        Assert.Equal([OptionsViolation.ReleaseGraceMismatch], Rules(o));

        o.ReleaseGrace = TimeSpan.FromSeconds(12) - Tick;
        Assert.Equal([OptionsViolation.ReleaseGraceMismatch], Rules(o));
    }

    [Fact]
    public void The_ReleaseGrace_default_follows_StopBudget_and_PlacementInterval()
    {
        var o = Valid();
        o.StopBudget = TimeSpan.FromSeconds(4);
        o.PlacementInterval = TimeSpan.FromSeconds(3);

        Assert.Equal(TimeSpan.FromSeconds(11), o.ReleaseGrace);
        Assert.Empty(Rules(o));
    }

    [Theory]
    [InlineData(nameof(ClusterOptions.LeaseTtl))]
    [InlineData(nameof(ClusterOptions.SafetyMargin))]
    [InlineData(nameof(ClusterOptions.StopBudget))]
    [InlineData(nameof(ClusterOptions.RenewTimeout))]
    [InlineData(nameof(ClusterOptions.RenewInterval))]
    [InlineData(nameof(ClusterOptions.AckPollInterval))]
    [InlineData(nameof(ClusterOptions.ApplyPollInterval))]
    [InlineData(nameof(ClusterOptions.RetentionInterval))]
    [InlineData(nameof(ClusterOptions.ReplicationCheckInterval))]
    [InlineData(nameof(ClusterOptions.StatusReportInterval))]
    [InlineData(nameof(ClusterOptions.ReadIndexTimeout))]
    [InlineData(nameof(ClusterOptions.FencedTransactionTimeout))]
    [InlineData(nameof(ClusterOptions.PlacementInterval))]
    [InlineData(nameof(ClusterOptions.OwnedRowRetention))]
    public void It_refuses_a_duration_that_is_zero_or_negative(string setting)
    {
        foreach (var bad in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1) })
        {
            var o = Valid();
            typeof(ClusterOptions).GetProperty(setting)!.SetValue(o, bad);

            var problems = OptionsValidator.Validate(o, aspNetCoreInUse: false);

            Assert.Contains(problems, p => p.Rule == OptionsViolation.NotPositive && p.Setting == setting);
        }
    }

    [Fact]
    public void Zero_is_a_valid_RejoinBackoff_and_StabilisationWindow_and_a_negative_one_is_not()
    {
        var zero = Valid();
        zero.RejoinBackoff = TimeSpan.Zero;
        zero.StabilisationWindow = TimeSpan.Zero;
        Assert.Empty(OptionsValidator.Validate(zero, aspNetCoreInUse: false));

        foreach (var setting in new[] { nameof(ClusterOptions.RejoinBackoff), nameof(ClusterOptions.StabilisationWindow) })
        {
            var negative = Valid();
            typeof(ClusterOptions).GetProperty(setting)!.SetValue(negative, TimeSpan.FromSeconds(-1));

            var problem = Assert.Single(OptionsValidator.Validate(negative, aspNetCoreInUse: false));

            Assert.Equal(OptionsViolation.NotPositive, problem.Rule);
            Assert.Equal(setting, problem.Setting);
            Assert.Contains("must not be negative", problem.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void The_HTTP_bound_is_not_judged_on_an_AckTimeout_derived_from_a_missing_ReconcileTimeout()
    {
        var o = ValidForHttp();
        o.ReconcileTimeout = null;
        o.ProxyReadTimeout = TimeSpan.FromSeconds(1);

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: true));

        Assert.Equal(OptionsViolation.ReconcileTimeoutMissing, problem.Rule);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void It_refuses_a_HintLimit_that_is_not_positive(int hintLimit)
    {
        var o = Valid();
        o.HintLimit = hintLimit;

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.NotPositive, problem.Rule);
        Assert.Equal("HintLimit", problem.Setting);
    }

    [Fact]
    public void It_refuses_a_ReconcileTimeout_that_is_not_positive()
    {
        var o = Valid();
        o.ReconcileTimeout = TimeSpan.Zero;

        Assert.Contains(OptionsViolation.NotPositive, Rules(o));
    }

    [Fact]
    public void It_refuses_HTTP_settings_that_are_not_positive()
    {
        var o = ValidForHttp();
        o.ProxyReadTimeout = TimeSpan.Zero;
        o.ActionTimeBudget = TimeSpan.FromSeconds(-1);

        var problems = OptionsValidator.Validate(o, aspNetCoreInUse: true);

        Assert.Contains(problems, p => p is { Rule: OptionsViolation.NotPositive, Setting: "ProxyReadTimeout" });
        Assert.Contains(problems, p => p is { Rule: OptionsViolation.NotPositive, Setting: "ActionTimeBudget" });
    }

    private static readonly TimeSpan TimerLimit = OptionsValidator.MaxTimerDuration;
    private static readonly TimeSpan Millisecond = TimeSpan.FromMilliseconds(1);

    [Fact]
    public async Task The_timer_limit_is_the_longest_wait_Task_Delay_and_a_timer_accept()
    {
        using var cts = new CancellationTokenSource();
        var delay = Task.Delay(TimerLimit, TimeProvider.System, cts.Token);
        TimeProvider.System.CreateTimer(_ => { }, null, TimerLimit, Timeout.InfiniteTimeSpan).Dispose();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delay);

        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = Task.Delay(TimerLimit + Millisecond, TimeProvider.System, CancellationToken.None); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = TimeProvider.System.CreateTimer(_ => { }, null, TimerLimit + Millisecond, Timeout.InfiniteTimeSpan); });
    }

    [Theory]
    [InlineData(nameof(ClusterOptions.LeaseTtl))]
    [InlineData(nameof(ClusterOptions.StopBudget))]
    [InlineData(nameof(ClusterOptions.RenewTimeout))]
    [InlineData(nameof(ClusterOptions.RenewInterval))]
    [InlineData(nameof(ClusterOptions.ReconcileTimeout))]
    [InlineData(nameof(ClusterOptions.ApplyStallLimit))]
    [InlineData(nameof(ClusterOptions.AckTimeout))]
    [InlineData(nameof(ClusterOptions.AckPollInterval))]
    [InlineData(nameof(ClusterOptions.ApplyPollInterval))]
    [InlineData(nameof(ClusterOptions.RetentionInterval))]
    [InlineData(nameof(ClusterOptions.ReplicationCheckInterval))]
    [InlineData(nameof(ClusterOptions.StatusReportInterval))]
    [InlineData(nameof(ClusterOptions.ReadIndexTimeout))]
    [InlineData(nameof(ClusterOptions.FencedTransactionTimeout))]
    [InlineData(nameof(ClusterOptions.PlacementInterval))]
    [InlineData(nameof(ClusterOptions.StabilisationWindow))]
    [InlineData(nameof(ClusterOptions.RejoinBackoff))]
    public void It_refuses_a_timer_duration_one_tick_over_the_limit_and_not_one_on_it(string setting)
    {
        var property = typeof(ClusterOptions).GetProperty(setting)!;

        var onTheLimit = Valid();
        property.SetValue(onTheLimit, TimerLimit);
        Assert.DoesNotContain(OptionsValidator.Validate(onTheLimit, aspNetCoreInUse: false), p => p is { Rule: OptionsViolation.DurationTooLong } && p.Setting == setting);

        var over = Valid();
        property.SetValue(over, TimerLimit + Tick);
        var problems = OptionsValidator.Validate(over, aspNetCoreInUse: false);

        Assert.Contains(problems, p => p is { Rule: OptionsViolation.DurationTooLong } && p.Setting == setting);
        Assert.All(problems, p => Assert.Equal(OptionsViolation.DurationTooLong, p.Rule));
    }

    [Fact]
    public void A_ReconcileTimeout_above_half_the_limit_is_reported_on_the_ApplyStallLimit_it_doubles_into()
    {
        var o = Valid();
        o.ReconcileTimeout = (TimerLimit / 2) + Tick;

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.DurationTooLong, problem.Rule);
        Assert.Equal("ApplyStallLimit", problem.Setting);
    }

    [Fact]
    public void A_ReconcileTimeout_at_half_the_limit_is_valid()
    {
        var o = Valid();
        o.ReconcileTimeout = TimerLimit / 2;
        o.LeaseTtl = TimerLimit / 2;
        o.RenewInterval = TimeSpan.FromSeconds(3);
        o.AckTimeout = TimerLimit;

        Assert.DoesNotContain(OptionsViolation.DurationTooLong, Rules(o));
    }

    [Fact]
    public void A_duration_no_sum_can_hold_is_reported_without_arithmetic_that_overflows()
    {
        var huge = Valid();
        huge.ReconcileTimeout = TimeSpan.MaxValue;
        var stall = Valid();
        stall.ApplyStallLimit = TimeSpan.MaxValue;
        var ack = Valid();
        ack.AckTimeout = TimeSpan.MaxValue;

        Assert.Equal("ReconcileTimeout", Assert.Single(OptionsValidator.Validate(huge, aspNetCoreInUse: false)).Setting);
        Assert.Equal("ApplyStallLimit", Assert.Single(OptionsValidator.Validate(stall, aspNetCoreInUse: false)).Setting);
        Assert.Equal("AckTimeout", Assert.Single(OptionsValidator.Validate(ack, aspNetCoreInUse: false)).Setting);
    }

    [Fact]
    public void A_derived_default_over_the_limit_is_reported_with_the_setting_it_follows()
    {
        var o = Valid();
        o.LeaseTtl = TimerLimit + Tick;

        var problems = OptionsValidator.Validate(o, aspNetCoreInUse: false);

        Assert.Contains(problems, p => p.Setting == "LeaseTtl");
        Assert.Contains(problems, p => p.Setting == "ReplicationCheckInterval");
    }

    [Fact]
    public void A_duration_over_the_limit_stops_the_relations_from_reporting_about_it()
    {
        var o = Valid();
        o.RetentionInterval = TimerLimit + Tick;
        o.StopBudget = TimeSpan.FromSeconds(1);

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal("RetentionInterval", problem.Setting);
    }

    [Fact]
    public void A_duration_no_timer_is_armed_with_may_exceed_the_limit()
    {
        var o = ValidForHttp();
        o.OwnedRowRetention = TimeSpan.FromDays(365);
        o.ProxyReadTimeout = TimeSpan.FromDays(100);
        o.ActionTimeBudget = TimeSpan.FromDays(60);

        Assert.Empty(OptionsValidator.Validate(o, aspNetCoreInUse: true));
    }

    [Theory]
    [InlineData("Host=db;Target Session Attributes=standby")]
    [InlineData("Host=db;Target Session Attributes=prefer-standby")]
    [InlineData("Host=db;Target Session Attributes=any")]
    [InlineData("Host=db;Target Session Attributes=prefer-primary")]
    [InlineData("Host=db;Target Session Attributes=primary")]
    [InlineData("Host=db;Target Session Attributes=read-write")]
    [InlineData("Host=a,b;Target Session Attributes=read-only")]
    [InlineData("Host=a,b;Target Session Attributes=any")]
    [InlineData("Host=a,b;Target Session Attributes=prefer-primary")]
    [InlineData("Host=a,b")]
    public void I24_a_standby_or_any_target_is_refused(string connectionString)
    {
        var o = Valid();
        o.ConnectionString = connectionString;

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.TargetNotPrimary, problem.Rule);
        Assert.Equal("ConnectionString", problem.Setting);
    }

    [Theory]
    [InlineData("Host=db")]
    [InlineData("Host=db;Port=6432;Database=app")]
    [InlineData("Host=a,b;Target Session Attributes=primary")]
    [InlineData("Host=a,b;Target Session Attributes=read-write")]
    public async Task I24_the_primary_is_accepted_and_the_string_builds_the_librarys_data_source(string connectionString)
    {
        var o = Valid();
        o.ConnectionString = connectionString;

        Assert.Empty(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        await using var source = new ClusterDataSource(o);

        Assert.NotNull(source.DataSource);
    }

    [Fact]
    public void I24_a_connection_string_that_does_not_parse_is_refused()
    {
        var o = Valid();
        o.ConnectionString = "this is not a connection string";

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.TargetNotPrimary, problem.Rule);
    }

    [Fact]
    public void PrimaryTarget_names_the_attribute_it_refused()
    {
        var reason = PrimaryTarget.Refusal("Host=db;Target Session Attributes=standby");

        Assert.NotNull(reason);
        Assert.Contains("standby", reason, StringComparison.OrdinalIgnoreCase);
        Assert.Null(PrimaryTarget.Refusal("Host=a,b;Target Session Attributes=primary"));
    }

    [Fact]
    public void It_reports_every_broken_rule_it_can_at_once_not_only_the_first()
    {
        var o = new ClusterOptions { StopBudget = TimeSpan.FromSeconds(1), RenewInterval = TimeSpan.FromMinutes(5) };

        var rules = Rules(o, http: true);

        Assert.Contains(OptionsViolation.ReplicaIdMissing, rules);
        Assert.Contains(OptionsViolation.ConnectionStringMissing, rules);
        Assert.Contains(OptionsViolation.ReconcileTimeoutMissing, rules);
        Assert.Contains(OptionsViolation.ProxyReadTimeoutMissing, rules);
        Assert.Contains(OptionsViolation.ActionTimeBudgetMissing, rules);
        Assert.Contains(OptionsViolation.StopBudgetTooShort, rules);
        Assert.Contains(OptionsViolation.RenewIntervalTooLong, rules);
    }

    [Fact]
    public void Every_problem_carries_a_message_that_names_its_setting()
    {
        var o = new ClusterOptions { StopBudget = TimeSpan.FromSeconds(1), RenewInterval = TimeSpan.FromMinutes(5) };

        foreach (var p in OptionsValidator.Validate(o, aspNetCoreInUse: true))
        {
            Assert.False(string.IsNullOrWhiteSpace(p.Message));
            Assert.Contains(p.Setting, p.Message, StringComparison.Ordinal);
        }
    }
}
