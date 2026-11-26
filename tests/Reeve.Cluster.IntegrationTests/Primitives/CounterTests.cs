using System.Transactions;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class CounterTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public CounterTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_counter_starts_at_zero_and_each_increment_answers_the_value_after_it()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var counter = rig.CounterOf();

        Assert.Null(await rig.CounterValueAsync("k"));
        Assert.Equal(new CounterResult.Incremented(1), await counter.IncrementAsync("k"));
        Assert.Equal(new CounterResult.Incremented(2), await counter.IncrementAsync("k"));
        Assert.Equal(new CounterResult.Incremented(1), await counter.IncrementAsync("other"));
        Assert.Equal(2, await rig.CounterValueAsync("k"));
    }

    [Fact]
    public async Task Concurrent_increments_from_many_callers_lose_none_and_answer_distinct_values()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var counters = Enumerable.Range(0, 4).Select(_ => rig.CounterOf()).ToList();

        var results = await Task.WhenAll(Enumerable.Range(0, 80).Select(i => counters[i % counters.Count].IncrementAsync("k")));

        Assert.Equal(Enumerable.Range(1, 80).Select(i => (long)i), results.OfType<CounterResult.Incremented>().Select(r => r.Value).Order());
        Assert.Equal(80, await rig.CounterValueAsync("k"));
    }

    [Fact]
    public async Task An_increment_is_a_transaction_of_its_own_and_survives_the_rollback_of_the_callers()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var counter = rig.CounterOf();

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            Assert.Equal(new CounterResult.Incremented(1), await counter.IncrementAsync("k"));
        }

        Assert.Equal(1, await rig.CounterValueAsync("k"));
    }
}
