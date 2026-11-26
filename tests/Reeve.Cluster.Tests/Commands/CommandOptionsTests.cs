using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Unit.Commands;

public sealed class CommandOptionsTests
{
    private static ClusterOptions Valid() => new()
    {
        ReplicaId = "replica-1",
        ConnectionString = "Host=db;Database=app;Username=u",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
    };

    private static UnitKind Kind(string name = "scheduler", TimeSpan? start = null, bool redis = false) =>
        new(name, typeof(object), new UnitKindOptions { StartTime = start ?? TimeSpan.Zero, RedisFence = redis }) { Keyed = true };

    private static IReadOnlyList<OptionsProblem> Validate(ClusterOptions options, params UnitKind[] kinds) =>
        OptionsValidator.Validate(options, aspNetCoreInUse: false, commandKinds: kinds);

    private static TimeSpan Floor(ClusterOptions options, UnitKind kind) => kind.FailoverBound(options) + options.PlacementInterval;

    [Fact]
    public void The_defaults_are_ten_minutes_and_twenty_four_hours()
    {
        var o = Valid();

        Assert.Equal(TimeSpan.FromMinutes(10), o.CommandPendingTtl);
        Assert.Equal(TimeSpan.FromHours(24), o.CommandRetention);
    }

    [Fact]
    public void The_ten_minute_default_passes_for_a_registered_kind_with_an_in_doubt_policy()
    {
        Assert.Empty(Validate(Valid(), Kind()));
    }

    [Fact]
    public void A_value_below_the_failover_bound_plus_the_placement_interval_is_refused_and_the_message_names_the_value_needed()
    {
        var o = Valid();
        var kind = Kind();
        var needed = Floor(o, kind);
        o.CommandPendingTtl = needed - TimeSpan.FromTicks(1);

        var problem = Assert.Single(Validate(o, kind));

        Assert.Equal(OptionsViolation.CommandPendingTtlTooShort, problem.Rule);
        Assert.Equal("CommandPendingTtl", problem.Setting);
        Assert.Contains(needed.ToString(), problem.Message, StringComparison.Ordinal);
        Assert.Contains(o.CommandPendingTtl.ToString(), problem.Message, StringComparison.Ordinal);
        Assert.Contains("scheduler", problem.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_value_on_the_line_is_accepted()
    {
        var o = Valid();
        var kind = Kind();
        o.CommandPendingTtl = Floor(o, kind);

        Assert.Empty(Validate(o, kind));
    }

    [Fact]
    public void The_floor_is_the_highest_of_the_registered_kinds_and_the_message_names_that_kind()
    {
        var o = Valid();
        var slow = Kind("slow", start: TimeSpan.FromMinutes(2));
        var fast = Kind("fast");
        o.CommandPendingTtl = Floor(o, fast);

        var problem = Assert.Single(Validate(o, fast, slow));

        Assert.Contains("slow", problem.Message, StringComparison.Ordinal);
        Assert.Contains(Floor(o, slow).ToString(), problem.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_kind_that_declares_a_redis_fence_has_the_longer_bound()
    {
        var o = Valid();
        var plain = Kind();
        var fenced = Kind("fenced", redis: true);
        o.CommandPendingTtl = Floor(o, plain);

        Assert.Empty(Validate(o, plain));
        Assert.Equal([OptionsViolation.CommandPendingTtlTooShort], Validate(o, fenced).Select(p => p.Rule));
    }

    [Fact]
    public void With_no_kind_that_declares_an_in_doubt_policy_there_is_no_floor()
    {
        var o = Valid();
        o.CommandPendingTtl = TimeSpan.FromSeconds(1);

        Assert.Empty(OptionsValidator.Validate(o, aspNetCoreInUse: false));
        Assert.Empty(Validate(o));
    }

    [Theory]
    [InlineData("CommandPendingTtl")]
    [InlineData("CommandRetention")]
    public void The_two_settings_must_be_positive(string setting)
    {
        var o = Valid();
        if (setting == "CommandPendingTtl")
        {
            o.CommandPendingTtl = TimeSpan.Zero;
        }
        else
        {
            o.CommandRetention = TimeSpan.FromSeconds(-1);
        }

        var problem = Assert.Single(Validate(o, Kind()));

        Assert.Equal(OptionsViolation.NotPositive, problem.Rule);
        Assert.Equal(setting, problem.Setting);
    }

    [Fact]
    public void Start_up_validation_reads_the_registered_command_handlers_and_refuses_a_value_below_the_floor()
    {
        var o = Valid();
        var kind = Kind();
        o.CommandPendingTtl = TimeSpan.FromSeconds(10);
        var validation = new Reeve.Cluster.Hosting.ClusterOptionsValidation(
            [], [kind], [new Microsoft.Extensions.DependencyInjection.CommandHandlerRegistration("scheduler", typeof(object), InDoubtPolicy.Complete)]);

        var result = validation.Validate(null, o);

        Assert.True(result.Failed);
        Assert.Contains(Floor(o, kind).ToString(), result.FailureMessage, StringComparison.Ordinal);
        Assert.True(new Reeve.Cluster.Hosting.ClusterOptionsValidation([], [kind], []).Validate(null, o).Succeeded);
    }

    [Fact]
    public void A_handler_whose_kind_is_not_registered_is_judged_by_the_default_failover_bound()
    {
        var o = Valid();
        o.CommandPendingTtl = TimeSpan.FromSeconds(10);
        var validation = new Reeve.Cluster.Hosting.ClusterOptionsValidation(
            [], [], [new Microsoft.Extensions.DependencyInjection.CommandHandlerRegistration("ghost", typeof(object), InDoubtPolicy.ReRun)]);

        Assert.True(validation.Validate(null, o).Failed);
    }

    [Theory]
    [InlineData("CommandPendingTtl")]
    [InlineData("CommandRetention")]
    public void A_command_age_beyond_the_limit_is_refused_and_one_on_the_limit_is_accepted(string setting)
    {
        var property = typeof(ClusterOptions).GetProperty(setting)!;
        var onTheLimit = Valid();
        property.SetValue(onTheLimit, OptionsValidator.MaxCommandAge);
        var over = Valid();
        property.SetValue(over, OptionsValidator.MaxCommandAge + TimeSpan.FromTicks(1));
        var huge = Valid();
        property.SetValue(huge, TimeSpan.MaxValue);

        Assert.DoesNotContain(OptionsValidator.Validate(onTheLimit, aspNetCoreInUse: false, commandKinds: [Kind()]), p => p.Rule == OptionsViolation.DurationTooLong);
        foreach (var options in new[] { over, huge })
        {
            var problem = Assert.Single(OptionsValidator.Validate(options, aspNetCoreInUse: false, commandKinds: [Kind()]));
            Assert.Equal(OptionsViolation.DurationTooLong, problem.Rule);
            Assert.Equal(setting, problem.Setting);
        }
    }
}
