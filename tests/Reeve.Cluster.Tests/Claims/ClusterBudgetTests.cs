using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClusterBudgetTests
{
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(1);

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedBudgetRows rows = new();
    private readonly ScriptedGate gate = new();
    private readonly HookedPoints points = new();

    private ClusterBudget Budget() =>
        new(rows, gate, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points, NullLogger<ClusterBudget>.Instance);

    [Fact]
    public async Task A_new_id_within_the_limit_is_granted_and_the_statement_gets_the_key_id_limit_and_period()
    {
        rows.Statements.Enqueue(new BudgetStatement(true, 1));

        var result = await Budget().ConsumeAsync("k", "id-1", 5, Period);

        Assert.Equal(new BudgetResult.Granted(1), result);
        Assert.Equal([("k", "id-1", 5, Period)], rows.Calls);
    }

    [Fact]
    public async Task A_retried_id_is_already_counted()
    {
        rows.Statements.Enqueue(new BudgetStatement(false, null));

        Assert.Equal(new BudgetResult.AlreadyCounted(), await Budget().ConsumeAsync("k", "id-1", 5, Period));
    }

    [Fact]
    public async Task A_new_id_over_the_limit_is_exhausted()
    {
        rows.Statements.Enqueue(new BudgetStatement(true, null));

        Assert.Equal(new BudgetResult.Exhausted(), await Budget().ConsumeAsync("k", "id-1", 5, Period));
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_consumes_nothing_and_touches_no_row()
    {
        gate.Serving = false;

        Assert.IsType<BudgetResult.NotServing>(await Budget().ConsumeAsync("k", "id-1", 5, Period));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_statement_across_fence_start_consumes_nothing()
    {
        points.On(ClaimsFaultPoints.BudgetBeforeConsume.Name, () => gate.Serving = false);
        rows.Statements.Enqueue(new BudgetStatement(true, 1));

        Assert.IsType<BudgetResult.NotServing>(await Budget().ConsumeAsync("k", "id-1", 5, Period));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task I33_a_consume_the_librarys_own_bound_cancelled_is_unknown_although_the_statement_counted_the_id()
    {
        rows.OnConsume = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Statements.Enqueue(new BudgetStatement(true, 1));

        var result = await Budget().ConsumeAsync("k", "id-1", 5, Period);

        Assert.Equal(new BudgetResult.Unknown(), result);
        Assert.False(result.IsAllowed);
    }

    [Fact]
    public async Task I33_a_consume_the_caller_cancelled_before_it_returned_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnConsume = () => cancel.Cancel();
        rows.Statements.Enqueue(new BudgetStatement(true, 1));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Budget().ConsumeAsync("k", "id-1", 5, Period, cancel.Token));
    }

    [Fact]
    public async Task A_database_failure_is_thrown_and_is_not_a_grant()
    {
        rows.Failure = new DbFailure("the database is down");

        await Assert.ThrowsAsync<DbFailure>(() => Budget().ConsumeAsync("k", "id-1", 5, Period));
    }

    [Fact]
    public async Task A_consume_needs_a_key_an_id_a_positive_limit_and_a_period_of_a_millisecond()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Budget().ConsumeAsync(null!, "id", 5, Period));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Budget().ConsumeAsync("k", null!, 5, Period));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "id", 0, Period));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "id", 5, TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "id", 5, TimeSpan.FromTicks(9)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Budget().ConsumeAsync("k", "id", 5, TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - 1)));
        Assert.Empty(rows.Calls);
        Assert.Equal(0, gate.Checks);
    }

    private sealed class ScriptedBudgetRows : IBudgetRows
    {
        public Queue<BudgetStatement> Statements { get; } = new();

        public List<(string Key, string Id, int Limit, TimeSpan Period)> Calls { get; } = [];

        public Action? OnConsume { get; set; }

        public Exception? Failure { get; set; }

        public Task<BudgetStatement> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken)
        {
            Calls.Add((key, id, limit, period));
            OnConsume?.Invoke();
            return Failure is null ? Task.FromResult(Statements.Dequeue()) : Task.FromException<BudgetStatement>(Failure);
        }
    }
}
