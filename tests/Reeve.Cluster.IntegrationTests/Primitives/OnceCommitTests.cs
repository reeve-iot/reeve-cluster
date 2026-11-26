using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class OnceCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public OnceCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    private async Task<string> StallAsync(OnceRig rig)
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Claims.Db.ConnectionString).Database!;
        await rig.Claims.ExecAsync($"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
        return database;
    }

    [Fact]
    public async Task I33_an_AtMostOnce_grant_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_unknown_and_not_run()
    {
        var time = new FakeTimeProvider();
        await using var setup = await OnceRig.CreateAsync(postgres, Ftt, time);
        var a = await setup.Claims.AddReplicaAsync("a");
        var database = await StallAsync(setup);
        await using var rig = OnceRig.Open(setup.Claims.Db, Ftt, time);

        var run = rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await run.WaitAsync(Poll.Bound);

        Assert.Equal(new OnceResult.Unknown(), result);
        Assert.Empty(rig.Ran);
        Assert.NotNull(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task I33_an_AtMostOnce_grant_the_callers_token_cancelled_in_its_wait_for_the_standby_throws_and_is_not_run()
    {
        await using var setup = await OnceRig.CreateAsync(postgres, Ftt);
        var a = await setup.Claims.AddReplicaAsync("a");
        var database = await StallAsync(setup);
        await using var rig = OnceRig.Open(setup.Claims.Db, Ftt);

        using var cancel = new CancellationTokenSource();
        var run = rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute, cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Poll.Bound));
        Assert.Empty(rig.Ran);
        Assert.NotNull(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task I33_an_AtMostOnce_call_after_such_a_grant_reports_the_run_in_doubt_and_never_starts_it()
    {
        var time = new FakeTimeProvider();
        await using var setup = await OnceRig.CreateAsync(postgres, Ftt, time);
        var a = await setup.Claims.AddReplicaAsync("a");
        var b = await setup.Claims.AddReplicaAsync("b");
        var database = await StallAsync(setup);
        await using var rig = OnceRig.Open(setup.Claims.Db, Ftt, time);
        var run = rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));
        await run.WaitAsync(Poll.Bound);
        await SyncStandby.SetAsync(postgres, string.Empty);

        await rig.Claims.EndLeaseAsync("a");
        var result = await rig.OnceOf(b).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.Equal(new OnceResult.InDoubt(), result);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task I33_a_retake_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_not_run()
    {
        var time = new FakeTimeProvider();
        await using var setup = await OnceRig.CreateAsync(postgres, Ftt, time);
        var dead = await setup.Claims.AddReplicaAsync("dead");
        var sweeper = await setup.Claims.AddReplicaAsync("sweeper");
        var old = await setup.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        var database = await StallAsync(setup);
        await using var rig = OnceRig.Open(setup.Claims.Db, Ftt, time);

        var pass = rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var count = await pass.WaitAsync(Poll.Bound);

        Assert.Equal(0, count);
        Assert.Empty(rig.Ran);
        Assert.NotEqual(old, (await rig.RowAsync("k"))!.Token);
    }
}
