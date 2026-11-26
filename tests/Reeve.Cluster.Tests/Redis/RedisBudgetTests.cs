using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;
using Unit.Claims;
using Unit.Diagnostics;

namespace Unit.Redis;

[Collection(MetricsCollection.Name)]
public sealed class RedisBudgetTests
{
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(1);

    private readonly ScriptedBudgetRows budgetRows = new();
    private readonly ScriptedCounterRows counterRows = new();
    private readonly ScriptedGate gate = new();

    public static IEnumerable<object[]> Outages() => RedisFailures.Outages();

    private readonly WarningLog log = new();

    private RedisBudget Budget(OnRedisUnavailable policy = OnRedisUnavailable.Refuse) => new(budgetRows, gate, policy, log);

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_consumes_nothing_and_touches_no_key()
    {
        gate.Serving = false;

        Assert.IsType<BudgetResult.NotServing>(await Budget().ConsumeAsync("k", "i1", 3, Period));
        Assert.Empty(budgetRows.Consumes);
    }

    [Fact]
    public async Task A_new_id_within_the_limit_is_granted_with_the_count_and_nothing_is_removed()
    {
        budgetRows.Consume.Returns(new RedisBudgetStatement(true, 2, 600));

        var result = await Budget().ConsumeAsync("k", "i1", 3, Period);

        Assert.Equal(new BudgetResult.Granted(2), result);
        Assert.Equal([("k", "i1", Period)], budgetRows.Consumes);
        Assert.Empty(budgetRows.Undos);
    }

    [Fact]
    public async Task A_retried_id_is_already_counted_and_nothing_is_removed()
    {
        budgetRows.Consume.Returns(new RedisBudgetStatement(false, 3, 600));

        Assert.Equal(new BudgetResult.AlreadyCounted(), await Budget().ConsumeAsync("k", "i1", 3, Period));
        Assert.Empty(budgetRows.Undos);
    }

    [Fact]
    public async Task I16_an_id_that_takes_the_set_past_the_limit_is_exhausted_and_removed_from_its_window()
    {
        budgetRows.Consume.Returns(new RedisBudgetStatement(true, 4, 600));
        budgetRows.Undo.Returns(true);

        var result = await Budget().ConsumeAsync("k", "i4", 3, Period);

        Assert.Equal(new BudgetResult.Exhausted(), result);
        Assert.False(result.IsAllowed);
        Assert.Equal([("k", "i4", 600L)], budgetRows.Undos);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task An_exhausted_id_whose_removal_Redis_did_not_answer_is_still_exhausted(Exception outage)
    {
        budgetRows.Consume.Returns(new RedisBudgetStatement(true, 4, 600));
        budgetRows.Undo.Throws(outage);

        Assert.Equal(new BudgetResult.Exhausted(), await Budget(OnRedisUnavailable.Grant).ConsumeAsync("k", "i4", 3, Period));
    }

    [Fact]
    public async Task An_exhausted_id_whose_removal_Redis_rejected_is_still_exhausted_and_the_rejection_is_logged()
    {
        budgetRows.Consume.Returns(new RedisBudgetStatement(true, 4, 600));
        var rejection = RedisFailures.WrongType();
        budgetRows.Undo.Throws(rejection);

        Assert.Equal(new BudgetResult.Exhausted(), await Budget().ConsumeAsync("k", "i4", 3, Period));
        Assert.Equal([("k", "i4", 600L)], budgetRows.Undos);
        Assert.Same(rejection, Assert.Single(log.Warnings));
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Refuse_answers_an_unavailable_Redis_with_Unknown_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        budgetRows.Consume.Throws(outage);

        var result = await Budget(OnRedisUnavailable.Refuse).ConsumeAsync("k", "i1", 3, Period);

        Assert.IsType<BudgetResult.Unknown>(result);
        Assert.False(result.IsAllowed);
        RedisClaimsTests.AssertUsed(recorder, "budget", "refuse", 1);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Grant_answers_an_unavailable_Redis_with_Granted_of_nothing_counted_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        budgetRows.Consume.Throws(outage);

        var result = await Budget(OnRedisUnavailable.Grant).ConsumeAsync("k", "i1", 3, Period);

        Assert.Equal(new BudgetResult.Granted(RedisDecisions.UncountedGrant), result);
        Assert.True(result.IsAllowed);
        Assert.Empty(budgetRows.Undos);
        RedisClaimsTests.AssertUsed(recorder, "budget", "grant", 1);
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Grant)]
    [InlineData(OnRedisUnavailable.Refuse)]
    public async Task A_rejection_that_is_not_an_outage_is_thrown_whatever_the_policy_and_uses_none(OnRedisUnavailable policy)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        budgetRows.Consume.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Budget(policy).ConsumeAsync("k", "i1", 3, Period));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task The_arguments_are_checked_before_anything_is_sent()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Budget().ConsumeAsync(null!, "i", 3, Period));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Budget().ConsumeAsync("k", null!, 3, Period));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "i", 0, Period));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "i", 3, TimeSpan.Zero));
        Assert.Empty(budgetRows.Consumes);
        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, Assert.Throws<ClusterConfigurationException>(() => Budget(default)).Reason);
    }

    [Fact]
    public async Task A_counter_answers_the_value_of_the_increment()
    {
        counterRows.Increment.Returns(7);

        Assert.Equal(new CounterResult.Incremented(7), await new RedisCounter(counterRows).IncrementAsync("c"));
        Assert.Equal(["c"], counterRows.Increments);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_counter_Redis_did_not_answer_is_Unknown_and_uses_no_policy(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        counterRows.Increment.Throws(outage);

        Assert.IsType<CounterResult.Unknown>(await new RedisCounter(counterRows).IncrementAsync("c"));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task A_counter_rejected_for_its_own_sake_is_thrown()
    {
        counterRows.Increment.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => new RedisCounter(counterRows).IncrementAsync("c"));
        await Assert.ThrowsAsync<ArgumentNullException>(() => new RedisCounter(counterRows).IncrementAsync(null!));
    }

    private sealed class WarningLog : ILogger<RedisBudget>
    {
        public List<Exception?> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(exception);
            }
        }
    }
}
