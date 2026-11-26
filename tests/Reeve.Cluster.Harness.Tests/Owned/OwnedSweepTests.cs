using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;

namespace Harness.Owned;

[Collection(StackCollection.Name)]
public sealed class OwnedSweepTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Keyed = "conn";
    private const string Key = "k";
    private const string Sweep = FreeRowSweep.Kind;

    private static void Pass(ClusterOptions o) => o.RetentionInterval = TimeSpan.FromMilliseconds(500);

    [Fact]
    public async Task SIMP_7_the_placed_sweep_deletes_the_free_row_and_leaves_the_keys_high_water_key()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100), Redis: true));
                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k =>
                {
                    k.Policy = AdoptionPolicy.None;
                    k.RedisFence = true;
                });
            },
            configure: o =>
            {
                Pass(o);
                o.OwnedRowRetention = TimeSpan.FromSeconds(1);
            },
            redis: true);
        var replicas = new[] { await rig.StartAsync("r1", spec), await rig.StartAsync("r2", spec) };
        await OwnedRig.UntilAsync(
            () => Task.FromResult(replicas.Any(r => r.Services.GetRequiredService<UnitRuntime>().Holds(Sweep, Sweep))),
            "the placer to place the free-row sweep");

        Assert.Equal(CreateOutcome.Created, await replicas[0].Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var start = await rig.Log.StartAsync(s => s.Kind == Keyed, "the key's unit");
        await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(start)).Contains(start.Token), "the tenure's write in Redis");
        var database = await rig.RedisAsync();
        var highWater = $"{start.RedisTag}:hw";
        Assert.Equal(start.Token, (long)await database.StringGetAsync(highWater));

        await start.Context.EndAsync();
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, Key) is null, "the sweep to delete the free row");

        await rig.ExecuteAsync(
            $"""
            WITH t AS (SELECT reeve_cluster.draw_token() AS token),
            o AS (INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token, updated_at)
                  SELECT '{Keyed}', 'later', NULL, NULL, token, now() - interval '2 days' FROM t)
            INSERT INTO reeve_cluster.release_request (kind, key, token, requested_at) SELECT '{Keyed}', 'later', token, now() FROM t
            """);
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, "later") is null, "the sweep to delete the later free row");

        Assert.Equal(0, await rig.ScalarAsync($"SELECT count(*) FROM reeve_cluster.release_request WHERE kind = '{Keyed}' AND key = 'later'"));
        Assert.Equal(start.Token, (long)await database.StringGetAsync(highWater));
    }

    [Fact]
    public async Task SIMP_2_a_fence_stops_the_sweep_at_step_5_and_the_placer_places_it_again_after_the_rejoin()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec(_ => { }, configure: Pass));
        var runtime = r1.Services.GetRequiredService<UnitRuntime>();
        await OwnedRig.UntilAsync(() => Task.FromResult(runtime.Holds(Sweep, Sweep)), "the placer to place the free-row sweep on r1");
        var gate = r1.Services.GetRequiredService<LocalPhaseGate>();
        var epoch = gate.Epoch!.Value;
        var old = (await rig.RowAsync(Sweep, Sweep))!.Value;
        Assert.Equal(("r1", epoch), (old.Owner, old.Epoch));

        using var held = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep5);
        using var afterStop = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep6);
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await held.WaitReachedAsync(timeout: OwnedRig.Limit);

        Assert.True(runtime.Holds(Sweep, Sweep));
        await rig.ExecuteAsync(
            $"INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token, updated_at) VALUES ('{Keyed}', 'stale', NULL, NULL, reeve_cluster.draw_token(), now() - interval '2 days')");

        held.Release();
        await afterStop.WaitReachedAsync(timeout: OwnedRig.Limit);
        Assert.False(runtime.Holds(Sweep, Sweep));
        Assert.NotNull(await rig.RowAsync(Keyed, "stale"));

        afterStop.Release();
        await OwnedRig.UntilAsync(() => Task.FromResult(gate.Phase == ReplicaPhase.Serving && gate.Epoch > epoch), "r1 to rejoin");

        await OwnedRig.UntilAsync(
            async () => await rig.RowAsync(Sweep, Sweep) is { Owner: "r1" } row && row.Epoch == gate.Epoch && row.Token > old.Token && runtime.Holds(Sweep, Sweep),
            "the placer to place the sweep again on the rejoined r1");
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, "stale") is null, "the new tenure to delete the row that came due");
    }
}
