using System.Text.RegularExpressions;
using System.Transactions;
using Int.Claims;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class BudgetTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private readonly PostgresFixture postgres;

    public BudgetTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task Distinct_ids_are_granted_up_to_the_limit_then_exhausted()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var budget = rig.BudgetOf(a);

        Assert.Equal(new BudgetResult.Granted(1), await budget.ConsumeAsync("k", "i1", 3, Hour));
        Assert.Equal(new BudgetResult.Granted(2), await budget.ConsumeAsync("k", "i2", 3, Hour));
        Assert.Equal(new BudgetResult.Granted(3), await budget.ConsumeAsync("k", "i3", 3, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k", "i4", 3, Hour));

        Assert.Equal(3, await rig.BudgetUsedAsync("k"));
        Assert.Equal(3, await rig.BudgetIdsAsync("k"));
    }

    [Fact]
    public async Task A_retried_consume_of_the_same_id_is_not_counted_twice()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");

        Assert.Equal(new BudgetResult.Granted(1), await rig.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour));
        var retry = await rig.BudgetOf(b).ConsumeAsync("k", "i1", 3, Hour);
        var again = await rig.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour);

        Assert.Equal(new BudgetResult.AlreadyCounted(), retry);
        Assert.Equal(new BudgetResult.AlreadyCounted(), again);
        Assert.True(retry.IsAllowed);
        Assert.Equal(1, await rig.BudgetUsedAsync("k"));
        Assert.Equal(1, await rig.BudgetIdsAsync("k"));
    }

    [Fact]
    public async Task A_retried_id_is_still_counted_after_the_period_filled_up_and_a_refused_id_is_not_recorded()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budget = rig.BudgetOf(await rig.Claims.AddReplicaAsync("a"));
        await budget.ConsumeAsync("k", "i1", 1, Hour);

        Assert.Equal(new BudgetResult.AlreadyCounted(), await budget.ConsumeAsync("k", "i1", 1, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k", "i2", 1, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k", "i2", 1, Hour));
        Assert.Equal(1, await rig.BudgetIdsAsync("k"));
        Assert.Equal(1, await rig.BudgetUsedAsync("k"));
    }

    [Fact]
    public async Task Budgets_of_different_keys_are_apart()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budget = rig.BudgetOf(await rig.Claims.AddReplicaAsync("a"));
        await budget.ConsumeAsync("k1", "i1", 1, Hour);

        Assert.Equal(new BudgetResult.Granted(1), await budget.ConsumeAsync("k2", "i1", 1, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k1", "i2", 1, Hour));
    }

    [Fact]
    public async Task Concurrent_consumes_of_distinct_ids_from_many_replicas_never_exceed_the_limit()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budgets = new List<ClusterBudget>();
        for (var i = 0; i < 4; i++)
        {
            budgets.Add(rig.BudgetOf(await rig.Claims.AddReplicaAsync($"r{i}")));
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 40).Select(i => budgets[i % budgets.Count].ConsumeAsync("k", $"i{i}", 10, Hour)));

        Assert.Equal(10, results.Count(r => r is BudgetResult.Granted));
        Assert.Equal(30, results.Count(r => r is BudgetResult.Exhausted));
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], results.OfType<BudgetResult.Granted>().Select(g => g.Used).Order());
        Assert.Equal(10, await rig.BudgetUsedAsync("k"));
        Assert.Equal(10, await rig.BudgetIdsAsync("k"));
    }

    [Fact]
    public async Task Concurrent_consumes_of_one_id_from_many_replicas_count_it_once()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budgets = new List<ClusterBudget>();
        for (var i = 0; i < 4; i++)
        {
            budgets.Add(rig.BudgetOf(await rig.Claims.AddReplicaAsync($"r{i}")));
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => budgets[i % budgets.Count].ConsumeAsync("k", "same", 5, Hour)));

        Assert.Single(results.OfType<BudgetResult.Granted>());
        Assert.Equal(15, results.Count(r => r is BudgetResult.AlreadyCounted));
        Assert.Equal(1, await rig.BudgetUsedAsync("k"));
    }

    [Fact]
    public async Task The_period_is_a_window_of_the_databases_clock_counted_from_the_epoch()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budget = rig.BudgetOf(await rig.Claims.AddReplicaAsync("a"));

        await budget.ConsumeAsync("k", "i1", 5, Hour);

        var stored = await rig.StoredWindowStartAsync("k");
        Assert.Equal(await rig.WindowStartAsync(3600), stored);
        Assert.Equal(0, stored.Minute + stored.Second);
    }

    [Fact]
    public async Task A_new_period_counts_afresh_after_the_boundary_on_the_databases_clock()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budget = rig.BudgetOf(await rig.Claims.AddReplicaAsync("a"));
        var period = TimeSpan.FromSeconds(2);

        var first = await budget.ConsumeAsync("k", "i1", 1, period);
        var second = await budget.ConsumeAsync("k", "i2", 1, period);
        Assert.True(first is BudgetResult.Granted);
        Assert.True(second is BudgetResult.Exhausted or BudgetResult.Granted);

        BudgetResult? later = null;
        await Poll.UntilAsync(
            async () => (later = await budget.ConsumeAsync("k", "next-" + Guid.NewGuid(), 1, period)) is BudgetResult.Granted, "a new period to grant", TimeSpan.FromSeconds(10));

        Assert.IsType<BudgetResult.Granted>(later);
        Assert.True(await rig.BudgetWindowsAsync("k") >= 2);
    }

    [Theory]
    [InlineData(1_500_000L)]
    [InlineData(100_000L)]
    [InlineData(3_000_000L)]
    [InlineData(604_800_000_000L)]
    public async Task The_window_expression_gives_an_exact_multiple_of_the_period_at_every_instant_around_its_boundaries(long periodMicroseconds)
    {
        var body = Regex.Match(BudgetStore.ConsumeSql, @"WITH w AS \((?<body>.*?)\),\s*fresh AS", RegexOptions.Singleline | RegexOptions.CultureInvariant).Groups["body"].Value;
        Assert.Contains("clock_timestamp()", body, StringComparison.Ordinal);
        const long Base = 1_793_000_000_000_000L;
        var first = Base / periodMicroseconds + 1;
        var instants = new SortedSet<long>();
        for (var j = 0; j < 4; j++)
        {
            var boundary = (first + j) * periodMicroseconds;
            instants.UnionWith([boundary - 1, boundary, boundary + 1, boundary + (periodMicroseconds / 2), boundary + periodMicroseconds - 1]);
        }

        await using var connection = await postgres.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            $"""
            SELECT i.us, w.start - to_timestamp(0)
            FROM (SELECT u.us, to_timestamp(0) + (u.us / 1000000) * interval '1 second' + (u.us % 1000000) * interval '1 microsecond' AS instant
                  FROM unnest(@us::bigint[]) AS u(us)) AS i
            CROSS JOIN LATERAL ({body.Replace("clock_timestamp()", "i.instant", StringComparison.Ordinal)}) AS w
            ORDER BY i.us
            """,
            connection);
        command.Parameters.AddWithValue("us", instants.ToArray());
        command.Parameters.AddWithValue("period", TimeSpan.FromTicks(periodMicroseconds * 10));
        await using var reader = await command.ExecuteReaderAsync();
        var answered = 0;
        while (await reader.ReadAsync())
        {
            var instant = reader.GetInt64(0);
            var start = reader.GetFieldValue<TimeSpan>(1).Ticks / 10;
            Assert.True(start == instant / periodMicroseconds * periodMicroseconds, $"instant {instant} us of a {periodMicroseconds} us period started a window at {start} us");
            answered++;
        }

        Assert.Equal(instants.Count, answered);
    }

    [Fact]
    public async Task Four_replicas_consuming_one_id_under_a_period_of_one_and_a_half_seconds_count_it_once_in_each_of_three_windows()
    {
        const long PeriodMicroseconds = 1_500_000;
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budgets = new List<ClusterBudget>();
        for (var i = 0; i < 4; i++)
        {
            budgets.Add(rig.BudgetOf(await rig.Claims.AddReplicaAsync($"r{i}")));
        }

        var granted = 0;
        var others = 0;
        Task<long> WindowAsync() => rig.Claims.ScalarAsync<long>("SELECT floor(extract(epoch FROM clock_timestamp()) / 1.5)::bigint");
        var lastWindow = -1L;
        for (var round = 0; round < 3; round++)
        {
            var before = lastWindow;
            await Poll.UntilAsync(async () => await WindowAsync() > before, "the database's clock to enter a new window", TimeSpan.FromSeconds(10));
            var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(i => Task.Run(() => budgets[i].ConsumeAsync("k", "same", 100, TimeSpan.FromMilliseconds(1500)))));
            lastWindow = await WindowAsync();
            granted += results.Count(r => r is BudgetResult.Granted);
            others += results.Count(r => r is BudgetResult.AlreadyCounted);
        }

        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT window_start - to_timestamp(0) FROM reeve_cluster.budget_id WHERE key = 'k' AND id = 'same'", connection);
        var starts = new List<long>();
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                starts.Add(reader.GetFieldValue<TimeSpan>(0).Ticks / 10);
            }
        }

        Assert.All(starts, start => Assert.Equal(0, start % PeriodMicroseconds));
        Assert.True(starts.Distinct().Count() >= 3, "three windows were entered");
        Assert.Equal(starts.Count, starts.Distinct().Count());
        Assert.Equal(starts.Count, granted);
        Assert.Equal(12 - granted, others);
        Assert.Equal(starts.Count, await rig.BudgetWindowsAsync("k"));
        Assert.Equal(starts.Count, await rig.BudgetUsedAsync("k"));
    }

    [Fact]
    public async Task A_consume_is_a_transaction_of_its_own_and_survives_the_rollback_of_the_callers()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var budget = rig.BudgetOf(await rig.Claims.AddReplicaAsync("a"));

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            Assert.Equal(new BudgetResult.Granted(1), await budget.ConsumeAsync("k", "i1", 3, Hour));
        }

        Assert.Equal(1, await rig.BudgetUsedAsync("k"));
        Assert.Equal(new BudgetResult.AlreadyCounted(), await budget.ConsumeAsync("k", "i1", 3, Hour));
    }

    [Fact]
    public async Task I16_a_replica_that_does_not_serve_consumes_nothing_and_writes_no_row()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        a.Serving = false;

        Assert.IsType<BudgetResult.NotServing>(await rig.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour));

        Assert.Equal(0, await rig.BudgetIdsAsync("k"));
        Assert.Equal(0, await rig.BudgetWindowsAsync("k"));
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_consume_across_fence_start_consumes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.BudgetBeforeConsume);

        var consume = rig.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour);
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        hold.Release();

        Assert.IsType<BudgetResult.NotServing>(await consume);
        Assert.Equal(0, await rig.BudgetIdsAsync("k"));
    }
}
