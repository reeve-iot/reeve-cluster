using Int.Claims;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class FlightLateResultTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public FlightLateResultTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_late_computation_whose_holders_lease_ended_has_its_result_refused_and_nothing_is_stored()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        var result = await rig.RunAsync(a, "k", "late", during: () => rig.Claims.EndLeaseAsync("a"));

        Assert.Equal("late", PrimitivesRig.Text(result));
        Assert.IsType<SingleFlightResult.ComputedRefused>(result);
        var row = await rig.FlightRowAsync("k");
        Assert.Null(row!.Result);
        Assert.False(row.ResultLive);
    }

    [Fact]
    public async Task A_late_computation_whose_claims_time_to_live_passed_has_its_result_refused()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        var result = await rig.RunAsync(a, "k", "late", during: () => rig.ExpireFlightClaimAsync("k"));

        Assert.IsType<SingleFlightResult.ComputedRefused>(result);
        Assert.Null((await rig.FlightRowAsync("k"))!.Result);
    }

    [Fact]
    public async Task A_late_computation_after_a_takeover_is_refused_and_the_takers_result_stays()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        var late = rig.RunAsync(a, "k", "late", during: async () =>
        {
            started.TrySetResult();
            await finish.Task;
        });
        await started.Task.WaitAsync(Poll.Bound);
        await rig.ExpireFlightClaimAsync("k");

        var taker = await rig.RunAsync(b, "k", "fresh");
        finish.SetResult();
        var refused = await late.WaitAsync(Poll.Bound);

        Assert.IsType<SingleFlightResult.Computed>(taker);
        Assert.Equal("late", PrimitivesRig.Text(refused));
        Assert.IsType<SingleFlightResult.ComputedRefused>(refused);
        var row = await rig.FlightRowAsync("k");
        Assert.Equal(("b", "fresh"), (row!.Holder, row.Result));
        Assert.Equal(["k", "k"], rig.Computations);
    }

    [Fact]
    public async Task After_a_refused_result_the_next_caller_computes_again()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.RunAsync(a, "k", "late", during: () => rig.Claims.EndLeaseAsync("a"));

        var next = await rig.RunAsync(b, "k", "fresh");

        Assert.IsType<SingleFlightResult.Computed>(next);
        Assert.Equal("fresh", (await rig.FlightRowAsync("k"))!.Result);
    }

    [Fact]
    public async Task I1_a_process_paused_between_its_computation_and_the_result_write_across_fence_start_writes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.FlightBeforeResult);

        var run = rig.RunAsync(a, "k", "late");
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        hold.Release();

        Assert.IsType<SingleFlightResult.ComputedRefused>(await run);
        Assert.Null((await rig.FlightRowAsync("k"))!.Result);
    }

    [Fact]
    public async Task I32_a_result_write_on_another_timeline_is_refused_and_starts_the_fence()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        var result = await rig.RunAsync(a, "k", "late", during: () =>
        {
            a.Timeline = rig.Claims.Db.PreviousTimeline;
            return Task.CompletedTask;
        });

        Assert.IsType<SingleFlightResult.ComputedRefused>(result);
        Assert.Equal([a.Epoch], a.OtherTimelines);
        Assert.Null((await rig.FlightRowAsync("k"))!.Result);
    }
}
