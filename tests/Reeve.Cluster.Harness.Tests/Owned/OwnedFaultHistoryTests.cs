using Harness.Slice;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Owned;

[Collection("harness-failover")]
public sealed class OwnedFaultHistoryTests(ITestOutputHelper output)
{
    private const string Kind = "widget";

    [Fact]
    public async Task I12_under_every_fault_of_the_harness_no_two_owners_fenced_writes_interleave()
    {
        await using var rig = await OwnedRig.CreateWithStandbyAsync(output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(2, TimeSpan.FromMilliseconds(200), Redis: true));
                services.AddClusterUnit<OwnedWriter>(Kind, o => o.RedisFence = true);
            },
            configure: o =>
            {
                o.ReplicationCheckInterval = TimeSpan.FromSeconds(1);

                o.StabilisationWindow = TimeSpan.FromMinutes(10);
            },
            redis: true);
        foreach (var id in new[] { "r1", "r2", "r3" })
        {
            await rig.StartAsync(id, spec);
        }

        var tenure = await rig.AwaitTenureAsync(Kind, Kind, 0, 4);
        var awaited = new List<long> { tenure.Token };

        var faults = new (string Name, Func<Replica, Task> Apply, bool Restart)[]
        {
            ("a pause past fence start", o => o.PauseForAsync(FenceStart(o) + TimeSpan.FromSeconds(3)), false),
            ("a crash", o => o.CrashAsync(), true),
            ("a graceful stop", o => o.StopAsync().WaitAsync(OwnedRig.Limit), true),
            ("a cut link until the replica fences", o => CutUntilFencedAsync(o), false),
            ("a frozen database until the owner fences", o => FreezeDatabaseUntilFencedAsync(rig, o), false),
            ("a database restart of 20 s, which ends the sessions", _ => rig.Run.Stack!.Database.RestartAsync(TimeSpan.FromSeconds(20)), false),
            ("a clock step past fence start", o => { o.Clock.Shift(o.Options.LeaseTtl); return Task.CompletedTask; }, false),
            ("a primary switchover", _ => rig.Failover.Stack.SwitchOverAsync(), false),
            ("a primary failover", _ => rig.Failover.Stack.FailOverAsync(), false),
        };
        foreach (var (name, apply, restart) in faults)
        {
            var row = await rig.RowAsync(Kind, Kind);
            var owner = rig.Run.Get(row!.Value.Owner!);
            output.WriteLine($"fault: {name}; tenure {tenure.Token} on {owner.Id}");
            await apply(owner);
            tenure = await rig.AwaitTenureAsync(Kind, Kind, tenure.Token, 3);

            await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(tenure)).Contains(tenure.Token), $"tenure {tenure.Token}'s write in Redis");
            awaited.Add(tenure.Token);
            if (restart)
            {
                await rig.StartAsync(owner.Id, spec);
            }

            await rig.AllServingAsync();
            output.WriteLine($"fault: {name}: the unit runs under tenure {tenure.Token} on {tenure.Replica}");
        }

        var effects = await rig.EffectsAsync();
        var tenures = effects.Select(e => e.Tenure).ToList();
        OwnedRig.AssertNoInterleave(tenures, "the fenced effects in commit order");
        var redis = await rig.RedisLogAsync(tenure);
        OwnedRig.AssertNoInterleave(redis, "the Redis list in the order Redis took it");

        output.WriteLine($"PostgreSQL tenures: {string.Join(", ", tenures.Distinct())}; Redis tenures: {string.Join(", ", redis.Distinct())}");
        Assert.All(awaited, token => Assert.Contains(token, redis));
        Assert.True(
            tenures.Distinct().Count() >= faults.Length + 1,
            $"{tenures.Distinct().Count()} tenures wrote, fewer than the {faults.Length + 1} the faults must have made: {string.Join(", ", tenures.Distinct())}");
    }

    private static TimeSpan FenceStart(Replica replica) => replica.Options.LeaseTtl - replica.Options.SafetyMargin - replica.Options.StopBudget;

    private static async Task CutUntilFencedAsync(Replica replica)
    {
        await replica.Link!.CutAsync();
        try
        {
            await OwnedRig.UntilAsync(() => Task.FromResult(SliceRig.PhaseOf(replica) != ReplicaPhase.Serving), $"{replica.Id} to fence with its link cut");
        }
        finally
        {
            await replica.Link.RestoreAsync();
        }
    }

    private static async Task FreezeDatabaseUntilFencedAsync(OwnedRig rig, Replica owner)
    {
        var database = rig.Run.Stack!.Database;
        await database.PauseAsync();
        try
        {
            await OwnedRig.UntilAsync(() => Task.FromResult(SliceRig.PhaseOf(owner) != ReplicaPhase.Serving), $"{owner.Id} to fence with the database frozen");
        }
        finally
        {
            await database.UnpauseAsync();
        }
    }
}
