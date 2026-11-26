using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Core;

public sealed class OptionsValidatorMessageTests
{
    private static ClusterOptions Valid() => new()
    {
        ReplicaId = "replica-1",
        ConnectionString = "Host=db",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
        ProxyReadTimeout = TimeSpan.FromSeconds(120),
        ActionTimeBudget = TimeSpan.FromSeconds(15),
    };

    public static TheoryData<string, string, string, string[]> Scenarios() => new()
    {
        { "ReplicaId", nameof(OptionsViolation.ReplicaIdMissing), "ReplicaId", [] },
        { "ConnectionString", nameof(OptionsViolation.ConnectionStringMissing), "ConnectionString", [] },
        { "Target", nameof(OptionsViolation.TargetNotPrimary), "ConnectionString", ["standby"] },
        { "ReconcileTimeout", nameof(OptionsViolation.ReconcileTimeoutMissing), "ReconcileTimeout", [] },
        { "ProxyReadTimeout", nameof(OptionsViolation.ProxyReadTimeoutMissing), "ProxyReadTimeout", [] },
        { "ActionTimeBudget", nameof(OptionsViolation.ActionTimeBudgetMissing), "ActionTimeBudget", [] },
        { "NotPositive", nameof(OptionsViolation.NotPositive), "RetentionInterval", ["00:00:00"] },
        { "HintLimit", nameof(OptionsViolation.NotPositive), "HintLimit", ["-3"] },
        { "DurationTooLong", nameof(OptionsViolation.DurationTooLong), "RetentionInterval", ["Task.Delay", "49.17:02:47.2940000", "60.00:00:00"] },
        { "RenewInterval", nameof(OptionsViolation.RenewIntervalTooLong), "RenewInterval", ["LeaseTtl", "SafetyMargin", "StopBudget", "RenewTimeout"] },
        { "StopBudget", nameof(OptionsViolation.StopBudgetTooShort), "StopBudget", ["SafetyMargin"] },
        { "ApplyStallLimit", nameof(OptionsViolation.ApplyStallLimitTooShort), "ApplyStallLimit", ["ReconcileTimeout"] },
        { "AckTimeout", nameof(OptionsViolation.AckTimeoutTooShort), "AckTimeout", ["LeaseTtl", "ApplyStallLimit", "AckPollInterval"] },
        { "ReleaseGrace", nameof(OptionsViolation.ReleaseGraceMismatch), "ReleaseGrace", ["StopBudget", "PlacementInterval"] },
        { "HttpBound", nameof(OptionsViolation.HttpBoundExceeded), "ProxyReadTimeout", ["ReadIndexTimeout", "ActionTimeBudget", "AckTimeout"] },
        { "CommandPendingTtl", nameof(OptionsViolation.CommandPendingTtlTooShort), "CommandPendingTtl", ["scheduler", "PlacementInterval", "00:00:43"] },
    };

    private static ClusterOptions Break(string scenario)
    {
        var o = Valid();
        switch (scenario)
        {
            case "ReplicaId": o.ReplicaId = null; break;
            case "ConnectionString": o.ConnectionString = null; break;
            case "Target": o.ConnectionString = "Host=db;Target Session Attributes=standby"; break;
            case "ReconcileTimeout": o.ReconcileTimeout = null; break;
            case "ProxyReadTimeout": o.ProxyReadTimeout = null; break;
            case "ActionTimeBudget": o.ActionTimeBudget = null; break;
            case "NotPositive": o.RetentionInterval = TimeSpan.Zero; break;
            case "HintLimit": o.HintLimit = -3; break;
            case "DurationTooLong": o.RetentionInterval = TimeSpan.FromDays(60); break;
            case "RenewInterval": o.RenewInterval = TimeSpan.FromSeconds(30); break;
            case "StopBudget": o.StopBudget = TimeSpan.FromSeconds(1); break;
            case "ApplyStallLimit": o.ApplyStallLimit = TimeSpan.FromSeconds(1); break;
            case "AckTimeout": o.AckTimeout = TimeSpan.FromSeconds(1); break;
            case "ReleaseGrace": o.ReleaseGrace = TimeSpan.FromSeconds(1); break;
            case "HttpBound": o.ProxyReadTimeout = TimeSpan.FromSeconds(30); break;
            case "CommandPendingTtl": o.CommandPendingTtl = TimeSpan.FromSeconds(30); break;
            default: throw new ArgumentOutOfRangeException(nameof(scenario));
        }

        return o;
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void Each_rule_reports_alone_with_a_message_that_names_its_setting_and_terms(
        string scenario, string ruleName, string setting, string[] terms)
    {
        var rule = Enum.Parse<OptionsViolation>(ruleName);
        var kind = new Reeve.Cluster.Owned.UnitKind("scheduler", typeof(object), new UnitKindOptions()) { Keyed = true };
        var problems = OptionsValidator.Validate(Break(scenario), aspNetCoreInUse: true, commandKinds: [kind]);

        var problem = Assert.Single(problems);
        Assert.Equal(rule, problem.Rule);
        Assert.Equal(setting, problem.Setting);
        Assert.Contains(setting, problem.Message, StringComparison.Ordinal);
        foreach (var term in terms)
        {
            Assert.Contains(term, problem.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void The_scenario_table_covers_every_rule()
    {
        Assert.Equal(
            Enum.GetValues<OptionsViolation>().Order(),
            Scenarios().Select(row => Enum.Parse<OptionsViolation>((string)row[1])).Distinct().Order());
    }

    [Fact]
    public void A_duration_that_is_not_positive_stops_the_relations_from_reporting_nonsense_about_it()
    {
        var o = Valid();
        o.LeaseTtl = TimeSpan.Zero;

        var problems = OptionsValidator.Validate(o, aspNetCoreInUse: true);

        Assert.All(problems, p => Assert.Equal(OptionsViolation.NotPositive, p.Rule));
        Assert.Contains(problems, p => p.Setting == "LeaseTtl");
    }

    [Fact]
    public void A_HintLimit_that_is_not_positive_also_stops_the_relations()
    {
        var o = Valid();
        o.HintLimit = 0;
        o.StopBudget = TimeSpan.FromSeconds(1);

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: true));

        Assert.Equal(OptionsViolation.NotPositive, problem.Rule);
    }

    [Fact]
    public void A_negative_backoff_also_stops_the_relations()
    {
        var o = Valid();
        o.RejoinBackoff = TimeSpan.FromSeconds(-1);
        o.StopBudget = TimeSpan.FromSeconds(1);

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: true));

        Assert.Equal("RejoinBackoff", problem.Setting);
    }

    [Fact]
    public void Options_are_required()
    {
        Assert.Throws<ArgumentNullException>(() => OptionsValidator.Validate(null!, aspNetCoreInUse: false));
    }

    [Fact]
    public void A_missing_ReconcileTimeout_does_not_report_relations_that_need_it()
    {
        var o = Valid();
        o.ReconcileTimeout = null;
        o.AckTimeout = TimeSpan.FromSeconds(1);
        o.ApplyStallLimit = TimeSpan.FromSeconds(1);

        var problem = Assert.Single(OptionsValidator.Validate(o, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.ReconcileTimeoutMissing, problem.Rule);
    }

    [Fact]
    public void An_unparsable_connection_string_says_so()
    {
        var reason = PrimaryTarget.Refusal("this is not a connection string");

        Assert.Contains("does not parse", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void A_single_host_with_an_attribute_tells_the_operator_to_remove_it()
    {
        var reason = PrimaryTarget.Refusal("Host=db;Target Session Attributes=primary");

        Assert.Contains("only supported with several hosts", reason, StringComparison.Ordinal);
        Assert.Contains("remove it", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void A_standby_target_on_several_hosts_is_named_and_the_remedy_is_primary()
    {
        var reason = PrimaryTarget.Refusal("Host=a,b;Target Session Attributes=standby");

        Assert.Contains("standby", reason, StringComparison.Ordinal);
        Assert.Contains("needs primary", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void A_host_list_with_no_attribute_tells_the_operator_what_to_set()
    {
        var reason = PrimaryTarget.Refusal("Host=a,b");

        Assert.Contains("Target Session Attributes=primary", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void The_target_attribute_is_matched_without_regard_to_case_or_spaces()
    {
        Assert.Null(PrimaryTarget.Refusal("Host=a,b;Target Session Attributes= Primary "));
        Assert.Null(PrimaryTarget.Refusal("Host=a,b;Target Session Attributes=READ-WRITE"));
        Assert.NotNull(PrimaryTarget.Refusal("Host=a,b;Target Session Attributes=Standby"));
    }

    [Fact]
    public void The_connection_faults_explain_what_to_change()
    {
        Assert.Contains("primary", PhysicalConnectionCheck.Evaluate(true, "0", "0")!.Message, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL 18", PhysicalConnectionCheck.Evaluate(false, null, "0")!.Message, StringComparison.Ordinal);
        Assert.Contains("RESET transaction_timeout", PhysicalConnectionCheck.Evaluate(false, "5s", "0")!.Message, StringComparison.Ordinal);
    }
}
