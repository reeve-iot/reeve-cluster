using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisBudgetTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private readonly RedisFixture redis;

    public RedisBudgetTests(RedisFixture redis) => this.redis = redis;

    [Fact]
    public async Task Distinct_ids_are_granted_up_to_the_limit_then_exhausted_and_an_exhausted_id_stays_exhausted_on_retry()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var budget = (await rig.AddReplicaAsync("a")).Budget(OnRedisUnavailable.Refuse);

        Assert.Equal(new BudgetResult.Granted(1), await budget.ConsumeAsync("k", "i1", 3, Hour));
        Assert.Equal(new BudgetResult.Granted(2), await budget.ConsumeAsync("k", "i2", 3, Hour));
        Assert.Equal(new BudgetResult.Granted(3), await budget.ConsumeAsync("k", "i3", 3, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k", "i4", 3, Hour));
        Assert.Equal(new BudgetResult.Exhausted(), await budget.ConsumeAsync("k", "i4", 3, Hour));
        Assert.Equal(new BudgetResult.AlreadyCounted(), await budget.ConsumeAsync("k", "i2", 3, Hour));
    }

    [Fact]
    public async Task I16_the_limit_holds_under_concurrent_consumes_from_four_replicas_and_the_set_ends_at_the_limit()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var budgets = new List<RedisBudget>();
        foreach (var id in new[] { "a", "b", "c", "d" })
        {
            budgets.Add((await rig.AddReplicaAsync(id)).Budget(OnRedisUnavailable.Refuse));
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 200).Select(i => budgets[i % 4].ConsumeAsync("k", $"id{i}", 50, Hour)));

        Assert.Equal(50, results.OfType<BudgetResult.Granted>().Count());
        Assert.Equal(150, results.OfType<BudgetResult.Exhausted>().Count());
        Assert.Equal(Enumerable.Range(1, 50), results.OfType<BudgetResult.Granted>().Select(g => g.Used).Order());
        var sets = rig.Direct.Multiplexer.GetServer(rig.Direct.Multiplexer.GetEndPoints()[0]).Keys(pattern: $"{{{rig.Prefix}budget:k}}*").ToList();
        var set = Assert.Single(sets);
        Assert.Equal(50, await rig.Direct.SetLengthAsync(set));
    }

    [Fact]
    public async Task A_retried_consume_of_the_same_id_from_another_replica_is_not_counted_twice()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Budget(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Budget(OnRedisUnavailable.Refuse);

        Assert.Equal(new BudgetResult.Granted(1), await a.ConsumeAsync("k", "i1", 3, Hour));
        Assert.Equal(new BudgetResult.AlreadyCounted(), await b.ConsumeAsync("k", "i1", 3, Hour));
        Assert.Equal(new BudgetResult.Granted(2), await b.ConsumeAsync("k", "i2", 3, Hour));
    }

    [Fact]
    public async Task A_new_window_of_Redis_s_clock_starts_a_new_count()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var budget = (await rig.AddReplicaAsync("a")).Budget(OnRedisUnavailable.Refuse);
        var period = TimeSpan.FromSeconds(1);

        BudgetResult? next = null;
        await budget.ConsumeAsync("k", "i1", 1, period);
        await Poll.UntilAsync(async () => (next = await budget.ConsumeAsync("k", "i2", 1, period)) is BudgetResult.Granted, "the next window admits a new id");
        Assert.Equal(new BudgetResult.Granted(1), next);
    }

    [Fact]
    public async Task A_replica_that_does_not_serve_consumes_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        replica.Serving = false;

        Assert.IsType<BudgetResult.NotServing>(await replica.Budget(OnRedisUnavailable.Grant).ConsumeAsync("k", "i1", 3, Hour));
    }

    [Fact]
    public async Task Concurrent_increments_from_four_replicas_count_every_one_exactly_once()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var counters = new List<RedisCounter>();
        foreach (var id in new[] { "a", "b", "c", "d" })
        {
            counters.Add((await rig.AddReplicaAsync(id)).Counter());
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 100).Select(i => counters[i % 4].IncrementAsync("c")));

        Assert.Equal(Enumerable.Range(1, 100).Select(v => (long)v), results.OfType<CounterResult.Incremented>().Select(r => r.Value).Order());
    }
}
