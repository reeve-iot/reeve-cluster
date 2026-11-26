using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class FlightCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(5);

    private readonly PostgresFixture postgres;

    public FlightCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    private async Task<string> CommitSynchronouslyAsync(PrimitivesRig rig)
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Claims.Db.ConnectionString).Database!;
        await rig.Claims.ExecAsync($"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        return database;
    }

    [Fact]
    public async Task I33_a_grant_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_unknown_and_nothing_is_computed()
    {
        var time = new FakeTimeProvider();
        await using var setup = await PrimitivesRig.CreateAsync(postgres, Ftt, time);
        var a = await setup.Claims.AddReplicaAsync("a");
        var database = await CommitSynchronouslyAsync(setup);
        await SyncStandby.StallAsync(postgres);
        await using var rig = PrimitivesRig.Open(setup.Claims.Db, Ftt, time);

        var run = rig.RunAsync(a, "k", "v1");
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await run.WaitAsync(Poll.Bound);

        Assert.Equal(new SingleFlightResult.Unknown(), result);
        Assert.Empty(rig.Computations);
        Assert.NotNull(await rig.FlightRowAsync("k"));
    }

    [Fact]
    public async Task I33_a_result_write_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_is_unknown_not_stored()
    {
        var time = new FakeTimeProvider();
        await using var setup = await PrimitivesRig.CreateAsync(postgres, Ftt, time);
        var a = await setup.Claims.AddReplicaAsync("a");
        await CommitSynchronouslyAsync(setup);
        await using var rig = PrimitivesRig.Open(setup.Claims.Db, Ftt, time);

        var stalled = new TaskCompletionSource();
        var run = rig.RunAsync(a, "k", "v1", during: async () =>
        {
            await SyncStandby.StallAsync(postgres);
            stalled.SetResult();
        });
        await stalled.Task.WaitAsync(Poll.Bound);
        while (!run.IsCompleted)
        {
            time.Advance(Ftt + TimeSpan.FromSeconds(3));
            await Task.Delay(100);
        }

        var result = await run.WaitAsync(Poll.Bound);

        Assert.Equal("v1", PrimitivesRig.Text(result));
        Assert.IsType<SingleFlightResult.ComputedUnknown>(result);
        Assert.Equal("v1", (await rig.FlightRowAsync("k"))!.Result);
    }
}
