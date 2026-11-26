using Int.Claims;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class FlightTakeoverRaceTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public FlightTakeoverRaceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_takeover_that_waited_on_the_lock_of_a_result_write_matches_nothing_and_the_next_caller_gets_the_holders_value()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var holder = await rig.Claims.AddReplicaAsync("a");
        var taker = await rig.Claims.AddReplicaAsync("b");
        var stored = FlightKeys.Store("k");
        var token = (await rig.Flights.InsertAsync(stored, holder.Id, holder.Epoch, TimeSpan.FromSeconds(2), CancellationToken.None))!.Value;
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var write = await connection.BeginTransactionAsync();
        await using (var result = new NpgsqlCommand(FlightStore.ResultSql, connection, write))
        {
            result.Parameters.AddWithValue("key", stored);
            result.Parameters.AddWithValue("token", token);
            result.Parameters.AddWithValue("timeline", holder.Timeline);
            result.Parameters.AddWithValue("result", PrimitivesRig.Bytes("held"));
            result.Parameters.AddWithValue("resultTtl", TimeSpan.FromMinutes(1));
            Assert.Equal(1, await result.ExecuteNonQueryAsync());
        }

        await rig.Claims.UntilInvalidAsync(stored, token);
        var takeover = rig.Flights.TakeoverAsync(stored, token, taker.Id, taker.Epoch, TimeSpan.FromMinutes(1), CancellationToken.None);
        await Poll.UntilAsync(
            async () => await rig.Claims.ScalarAsync<long>(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%UPDATE reeve_cluster.claim%'") > 0,
            "the takeover to wait on the row lock");
        Assert.False(takeover.IsCompleted);
        await write.CommitAsync();

        Assert.Null(await takeover.WaitAsync(Poll.Bound));
        var next = await rig.RunAsync(taker, "k", "fresh");
        Assert.IsType<SingleFlightResult.Waited>(next);
        Assert.Equal("held", PrimitivesRig.Text(next));
        Assert.Empty(rig.Computations);
        var row = await rig.FlightRowAsync("k");
        Assert.Equal((holder.Id, token, "held"), (row!.Holder, row.Token, row.Result));
    }

    [Fact]
    public async Task A_result_write_after_a_committed_takeover_is_refused_and_the_row_holds_no_result_under_the_takers_token()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var holder = await rig.Claims.AddReplicaAsync("a");
        var taker = await rig.Claims.AddReplicaAsync("b");
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        var takerStarted = new TaskCompletionSource();
        var takerFinish = new TaskCompletionSource();
        var late = rig.RunAsync(holder, "k", "late", during: async () =>
        {
            started.TrySetResult();
            await finish.Task;
        });
        await started.Task.WaitAsync(Poll.Bound);
        var first = (await rig.FlightRowAsync("k"))!;
        await rig.ExpireFlightClaimAsync("k");

        var taking = rig.RunAsync(taker, "k", "fresh", during: async () =>
        {
            takerStarted.TrySetResult();
            await takerFinish.Task;
        });
        await takerStarted.Task.WaitAsync(Poll.Bound);
        finish.SetResult();
        var refused = await late.WaitAsync(Poll.Bound);

        Assert.IsType<SingleFlightResult.ComputedRefused>(refused);
        Assert.Equal("late", PrimitivesRig.Text(refused));
        var between = (await rig.FlightRowAsync("k"))!;
        Assert.Equal(taker.Id, between.Holder);
        Assert.NotEqual(first.Token, between.Token);
        Assert.Null(between.Result);
        takerFinish.SetResult();
        Assert.IsType<SingleFlightResult.Computed>(await taking.WaitAsync(Poll.Bound));
        Assert.Equal("fresh", (await rig.FlightRowAsync("k"))!.Result);
    }
}
