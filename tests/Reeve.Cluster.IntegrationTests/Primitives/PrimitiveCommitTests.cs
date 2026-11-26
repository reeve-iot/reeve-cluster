using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class PrimitiveCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private readonly PostgresFixture postgres;

    public PrimitiveCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    private async Task<string> StallAsync(PrimitivesRig rig)
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Claims.Db.ConnectionString).Database!;
        await rig.Claims.ExecAsync($"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
        return database;
    }

    [Fact]
    public async Task I33_a_consume_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_unknown_and_a_retry_finds_the_id_counted()
    {
        var time = new FakeTimeProvider();
        await using var setup = await PrimitivesRig.CreateAsync(postgres, Ftt, time);
        var a = await setup.Claims.AddReplicaAsync("a");
        var database = await StallAsync(setup);
        await using var rig = PrimitivesRig.Open(setup.Claims.Db, Ftt, time);

        var consume = rig.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await consume.WaitAsync(Poll.Bound);

        Assert.Equal(new BudgetResult.Unknown(), result);
        Assert.False(result.IsAllowed);
        Assert.Equal(1, await rig.BudgetUsedAsync("k"));
        await SyncStandby.SetAsync(postgres, string.Empty);
        await using var later = PrimitivesRig.Open(setup.Claims.Db, Ftt, time);
        Assert.Equal(new BudgetResult.AlreadyCounted(), await later.BudgetOf(a).ConsumeAsync("k", "i1", 3, Hour));
        Assert.Equal(1, await later.BudgetUsedAsync("k"));
    }

    [Fact]
    public async Task I33_an_increment_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_unknown_not_a_value()
    {
        var time = new FakeTimeProvider();
        await using var setup = await PrimitivesRig.CreateAsync(postgres, Ftt, time);
        var database = await StallAsync(setup);
        await using var rig = PrimitivesRig.Open(setup.Claims.Db, Ftt, time);

        var increment = rig.CounterOf().IncrementAsync("k");
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await increment.WaitAsync(Poll.Bound);

        Assert.Equal(new CounterResult.Unknown(), result);
        Assert.Equal(1, await rig.CounterValueAsync("k"));
    }
}
