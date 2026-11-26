using System.Diagnostics;
using Harness.Owned;
using Harness.Placer;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;
using OwnedWriter = Harness.Owned.OwnedWriter;

namespace Harness.Replay.Ownership;

[Collection(StackCollection.Name)]
public sealed class OwnershipPlacementReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = "k0";

    [Fact]
    public async Task Replay_M2_S4_the_placer_does_not_assign_a_live_owners_unit_to_another_member()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100)));
                services.AddClusterUnit<OwnedWriter>(Kind);
            },
            configure: o =>
            {
                o.StabilisationWindow = TimeSpan.FromMinutes(10);
                o.PlacementInterval = TimeSpan.FromMilliseconds(500);
            });
        var r1 = await rig.StartAsync("r1", spec);
        var first = await rig.AwaitTenureAsync(Kind, Kind, 0, 2);
        Assert.Equal("r1", first.Replica);
        var before = await rig.RowAsync(Kind, Kind);

        await rig.StartAsync("r2", spec);
        var passes = r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead);
        await OwnedRig.UntilAsync(() => Task.FromResult(r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead) >= passes + 3), "three placer passes with r2 serving");

        Assert.Equal(before, await rig.RowAsync(Kind, Kind));
        Assert.DoesNotContain(rig.Log.Starts, s => s.Kind == Kind && s.Replica == "r2");
        Assert.Single(rig.Log.Starts, s => s.Kind == Kind);
    }

    [Fact]
    public async Task Replay_M2_N2_placement_assigns_nothing_to_a_member_between_its_activation_and_Serving()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec());
        var first = await rig.Log.StartOfAsync("r1");

        var r2 = await rig.Run.PrepareAsync("r2", rig.Spec());
        using var serving = r2.Faults.HoldNext(MembershipFaultPoints.ActivateCommittedBeforeApplied);
        await r2.StartAsync();
        await serving.WaitReachedAsync(timeout: PlacerRig.Limit);

        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(PlacerRig.Kind, PlacerRig.Kind, CancellationToken.None));
        (string? Owner, long Token)? assigned = null;
        await PlacerRig.UntilAsync(
            async () => (assigned = await rig.RowAsync()) is { Owner: not null } row && row.Token > first.Token,
            "the freed row assigned again under a new token (to any member)");
        Assert.True(assigned!.Value.Owner == "r1", $"the handover's freed row went to {assigned.Value.Owner}, a member that has not reached Serving, not back to r1");
        var passes = r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead);
        await PlacerRig.UntilAsync(() => Task.FromResult(r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead) >= passes + 2), "two more passes");
        Assert.Equal("r1", (await rig.RowAsync())?.Owner);
        Assert.DoesNotContain(rig.Log.Starts, s => s.Replica == "r2");

        Assert.True(serving.Release());
        await r2.Started!.WaitAsync(PlacerRig.Limit);
        await PlacerRig.ServingAsync(r2);
        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(PlacerRig.Kind, PlacerRig.Kind, CancellationToken.None));
        await rig.Log.StartOfAsync("r2");
    }

    [Fact]
    public async Task Replay_M2_S5F1_the_release_request_lands_under_the_owners_overlapping_fenced_transactions()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(2, TimeSpan.FromMilliseconds(300)));
                services.AddClusterUnit<OwnedWriter>(Kind);
            },
            configure: o => o.StabilisationWindow = TimeSpan.FromMinutes(10));
        var r1 = await rig.StartAsync("r1", spec);
        var old = await rig.AwaitTenureAsync(Kind, Kind, 0, 6);
        Assert.Equal("r1", old.Replica);
        await rig.StartAsync("r2", spec);

        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None), "the release request did not store its token");

        var moved = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "the unit to start on r2");
        await rig.AwaitTenureAsync(Kind, Kind, old.Token, 2);
        var effects = await rig.EffectsAsync();
        var lastOld = effects.Where(e => e.Tenure == old.Token).Max(e => e.N);
        var firstNew = effects.Where(e => e.Tenure == moved.Token).Min(e => e.N);
        Assert.True(moved.Token > old.Token);
        Assert.True(lastOld < firstNew, $"tenure {old.Token} committed n = {lastOld} after tenure {moved.Token}'s first write n = {firstNew}");
        OwnedRig.AssertNoInterleave([.. effects.Select(e => e.Tenure)], "the fenced effects in commit order");
    }

    [Fact]
    public async Task Replay_M2_D2_the_declared_failover_bound_covers_the_new_owners_own_poll_when_PlacementInterval_exceeds_FencedTransactionTimeout()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var template = rig.Spec(hosting: true);
        var spec = new ReplicaSpec
        {
            ConfigureServices = template.ConfigureServices,
            Configure = o =>
            {
                o.PlacementInterval = TimeSpan.FromSeconds(8);
                o.FencedTransactionTimeout = TimeSpan.FromSeconds(7);
            },
        };
        var r1 = await rig.StartAsync("r1", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        await rig.Log.StartOfAsync("r1");
        var r2 = await rig.StartAsync("r2", spec);
        var r3 = await rig.StartAsync("r3", spec);
        await PlacerRig.HostWidgetsAsync(r3);

        using var never = r3.Faults.Hold(OwnedFaultPoints.PlacerAdoptBeforeRead);
        using var adopt = r2.Faults.HoldNext(OwnedFaultPoints.PlacerAdoptBeforeRead);
        using var poll = r3.Faults.HoldNext(OwnedFaultPoints.PollBeforeRead);
        await adopt.WaitReachedAsync(timeout: PlacerRig.Limit);
        await poll.WaitReachedAsync(timeout: PlacerRig.Limit);

        var options = PlacerRig.Options(r1);
        Assert.True(options.PlacementInterval > options.FencedTransactionTimeout);
        await r1.CrashAsync();
        await PlacerRig.UntilAsync(() => rig.LeaseGoneAsync("r1"), "r1's lease to run out");
        var leaseEnded = Stopwatch.GetTimestamp();

        await Task.Delay(options.PlacementInterval);
        Assert.True(adopt.Release(), "r2's adoption was not held to release");
        await PlacerRig.UntilAsync(async () => (await rig.RowAsync())?.Owner == "r3", "the assignment to r3");
        await Task.Delay(options.PlacementInterval);
        Assert.True(poll.Release(), "r3's poll was not held to release");
        var started = await rig.Log.StartOfAsync("r3");

        var elapsed = Stopwatch.GetElapsedTime(leaseEnded, started.At);
        var bound = new UnitKind(PlacerRig.Kind, typeof(WidgetUnit), new UnitKindOptions()).FailoverBound(options) - options.LeaseTtl;
        output.WriteLine($"the unit ran on r3 {elapsed} after r1's lease ended; the declared bound beyond the lease {bound}");
        Assert.True(elapsed > 2 * options.PlacementInterval, $"the unit ran on r3 {elapsed} after the lease, not after two held PlacementIntervals");
        Assert.True(elapsed <= bound, $"the unit ran on r3 {elapsed} after the lease ended, beyond the declared bound {bound} beyond the lease");
    }
}
