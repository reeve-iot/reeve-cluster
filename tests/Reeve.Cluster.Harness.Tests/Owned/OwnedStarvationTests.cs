using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Owned;

[Collection(StackCollection.Name)]
public sealed class OwnedStarvationTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = "k0";

    private static readonly TimeSpan Window = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task S5_1_a_unit_whose_owner_writes_steadily_is_moved_by_a_rebalance_and_the_old_tenure_wrote_before_the_new_one()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(2, TimeSpan.FromMilliseconds(300)));
                services.AddClusterUnit<OwnedWriter>(Kind);
            },
            configure: o => o.StabilisationWindow = Window);
        await rig.StartAsync("r1", spec);
        var old = await rig.AwaitTenureAsync(Kind, Kind, 0, 6);
        Assert.Equal("r1", old.Replica);

        var joined = rig.Run.Sequence.Next();
        await rig.StartAsync("r2", spec);
        var moved = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "the rebalance to move the unit to r2");
        await rig.AwaitTenureAsync(Kind, Kind, old.Token, 4);

        var effects = await rig.EffectsAsync();
        var lastOld = effects.Where(e => e.Tenure == old.Token).Max(e => e.N);
        var firstNew = effects.Where(e => e.Tenure == moved.Token).Min(e => e.N);
        Assert.True(moved.Token > old.Token);
        Assert.True(lastOld < firstNew, $"tenure {old.Token} committed n = {lastOld} after tenure {moved.Token}'s first write n = {firstNew}");
        OwnedRig.AssertNoInterleave([.. effects.Select(e => e.Tenure)], "the fenced effects in commit order");
        Assert.Equal("RC001", await rig.AssertOwnerStateAsync(Kind, Kind, old.Token, await rig.ScalarAsync("SELECT reeve_cluster.server_timeline()")));
        Assert.Equal(1, rig.Log.Starts.Count(s => s is { Kind: Kind, Replica: "r1" }));

        Assert.DoesNotContain(rig.Run.Phases.Of("r1"), c => c.Sequence > joined && c.To != ReplicaPhase.Serving);
    }
}
