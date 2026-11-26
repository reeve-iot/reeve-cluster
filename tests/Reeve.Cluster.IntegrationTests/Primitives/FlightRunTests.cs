using Int.Claims;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class FlightRunTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public FlightRunTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_call_on_a_free_key_computes_stores_the_result_with_its_time_to_live_and_ends_the_claim()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        var result = await rig.RunAsync(a, "k", "v1", resultTtl: Minute);

        Assert.Equal(new byte[] { 118, 49 }, Assert.IsType<SingleFlightResult.Computed>(result).Value);
        Assert.Equal(["k"], rig.Computations);
        var row = Assert.IsType<FlightRow>(await rig.FlightRowAsync("k"));
        Assert.Equal(("a", a.Epoch, "v1", true), (row.Holder, row.Epoch, row.Result, row.ResultLive));
        Assert.False(row.ClaimLive);
    }

    [Fact]
    public async Task A_second_call_within_the_results_time_to_live_waits_nothing_on_any_replica_and_computes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.RunAsync(a, "k", "v1");

        var second = await rig.RunAsync(b, "k", "v2");
        var third = await rig.RunAsync(a, "k", "v3");

        Assert.Equal("v1", PrimitivesRig.Text(second));
        Assert.IsType<SingleFlightResult.Waited>(second);
        Assert.IsType<SingleFlightResult.Waited>(third);
        Assert.Single(rig.Computations);
    }

    [Fact]
    public async Task A_result_stays_readable_after_its_computing_replicas_lease_ended()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.RunAsync(a, "k", "v1");
        await rig.Claims.EndLeaseAsync("a");

        var second = await rig.RunAsync(b, "k", "v2");

        Assert.Equal("v1", PrimitivesRig.Text(second));
        Assert.IsType<SingleFlightResult.Waited>(second);
        Assert.Single(rig.Computations);
    }

    [Fact]
    public async Task After_the_results_time_to_live_passed_the_next_call_computes_again_under_a_new_token()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.RunAsync(a, "k", "v1");
        var before = await rig.FlightRowAsync("k");
        await rig.ExpireResultAsync("k");

        var second = await rig.RunAsync(b, "k", "v2");

        Assert.Equal("v2", PrimitivesRig.Text(second));
        Assert.IsType<SingleFlightResult.Computed>(second);
        Assert.Equal(["k", "k"], rig.Computations);
        var after = await rig.FlightRowAsync("k");
        Assert.NotEqual(before!.Token, after!.Token);
        Assert.Equal(("b", "v2", true), (after.Holder, after.Result, after.ResultLive));
    }

    [Fact]
    public async Task I16_callers_racing_one_key_on_many_replicas_start_one_computation_and_all_get_its_value()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var replicas = new List<Replica>();
        for (var i = 0; i < 6; i++)
        {
            replicas.Add(await rig.Claims.AddReplicaAsync($"r{i}"));
        }

        var release = new TaskCompletionSource();
        var calls = replicas.Select(r => rig.RunAsync(r, "k", "v", during: () => release.Task)).ToList();
        await Poll.UntilAsync(() => rig.Computations.Count >= 1, "a computation to start");
        await Task.Delay(300);
        release.SetResult();
        var results = await Task.WhenAll(calls);

        Assert.Single(rig.Computations);
        Assert.Single(results.OfType<SingleFlightResult.Computed>());
        Assert.Equal(5, results.OfType<SingleFlightResult.Waited>().Count());
        Assert.All(results, r => Assert.Equal("v", PrimitivesRig.Text(r)));
    }

    [Fact]
    public async Task A_caller_waits_for_a_computation_in_flight_on_another_replica_and_gets_its_value()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        var first = rig.RunAsync(a, "k", "v1", during: async () =>
        {
            started.TrySetResult();
            await finish.Task;
        });
        await started.Task.WaitAsync(Poll.Bound);

        var waiter = rig.RunAsync(b, "k", "v2");
        await Task.Delay(300);
        Assert.False(waiter.IsCompleted);
        finish.SetResult();

        Assert.IsType<SingleFlightResult.Computed>(await first);
        Assert.Equal("v1", PrimitivesRig.Text(await waiter.WaitAsync(Poll.Bound)));
        Assert.Single(rig.Computations);
    }

    [Fact]
    public async Task A_waiter_takes_over_a_claim_whose_holders_lease_ended_without_a_result_and_computes()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        var first = rig.RunAsync(a, "k", "late", during: async () =>
        {
            started.TrySetResult();
            await finish.Task;
        });
        await started.Task.WaitAsync(Poll.Bound);
        var waiter = rig.RunAsync(b, "k", "fresh");

        await rig.Claims.EndLeaseAsync("a");

        var second = await waiter.WaitAsync(Poll.Bound);
        finish.SetResult();
        Assert.Equal("fresh", PrimitivesRig.Text(second));
        Assert.IsType<SingleFlightResult.Computed>(second);
        Assert.IsType<SingleFlightResult.ComputedRefused>(await first);
    }

    [Fact]
    public async Task A_computation_that_throws_releases_its_claim_so_the_next_call_computes_at_once()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            rig.FlightOf(a).RunAsync("k", _ => throw new InvalidOperationException("the computation failed"), Minute, Minute));
        Assert.Null(await rig.FlightRowAsync("k"));

        var next = await rig.RunAsync(b, "k", "v2");

        Assert.IsType<SingleFlightResult.Computed>(next);
    }

    [Fact]
    public async Task I16_a_replica_that_does_not_serve_computes_nothing_and_writes_no_row()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        a.Serving = false;

        var result = await rig.RunAsync(a, "k", "v1");

        Assert.IsType<SingleFlightResult.NotServing>(result);
        Assert.Empty(rig.Computations);
        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_computes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.FlightBeforeInsert);

        var run = rig.RunAsync(a, "k", "v1");
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        hold.Release();

        Assert.IsType<SingleFlightResult.NotServing>(await run);
        Assert.Empty(rig.Computations);
        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task A_plain_claim_and_a_single_flight_on_the_same_key_never_meet()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var plain = await b.WinAsync("k");

        Assert.IsType<SingleFlightResult.Computed>(await rig.RunAsync(a, "k", "v1"));
        Assert.Equal(2, await rig.Claims.CountAsync());

        Assert.True(await b.Claims.ReleaseAsync("k", plain));
        Assert.Equal("v1", (await rig.FlightRowAsync("k"))!.Result);
        await a.WinAsync("k");
        Assert.Equal("v1", (await rig.FlightRowAsync("k"))!.Result);
    }

    [Fact]
    public async Task A_plain_claim_on_the_single_flight_key_space_is_refused_and_writes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        await rig.RunAsync(a, "k", "v1");
        var before = await rig.FlightRowAsync("k");
        var stored = FlightKeys.Store("k");

        await Assert.ThrowsAsync<ArgumentException>(() => a.Claims.ClaimAsync(stored, Minute));
        await Assert.ThrowsAsync<ArgumentException>(() => a.Claims.ReleaseAsync(stored, before!.Token));

        Assert.Equal(before, await rig.FlightRowAsync("k"));
    }

    [Fact]
    public async Task A_call_with_a_claim_scope_open_on_the_key_is_refused_and_writes_nothing()
    {
        await using var rig = await PrimitivesRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        using var scope = rig.Scopes.Open(FlightKeys.Store("k"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.RunAsync(a, "k", "v1"));

        Assert.Equal(0, await rig.Claims.CountAsync());
        Assert.Empty(rig.Computations);
    }
}
