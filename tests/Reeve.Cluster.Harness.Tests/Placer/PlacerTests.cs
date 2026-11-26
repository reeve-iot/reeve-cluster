using System.Diagnostics;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;

namespace Harness.Placer;

[Collection(StackCollection.Name)]
public sealed class PlacerTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = PlacerRig.Kind;

    [Fact]
    public async Task A_handover_lands_under_overlapping_fenced_transactions_and_the_old_owner_is_refused_before_the_new_owners_first_write()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(new WriterPlan(2, TimeSpan.FromMilliseconds(300)));
        var r1 = await rig.StartAsync("r1", spec);
        var old = await rig.Log.StartOfAsync("r1");
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.UntilAsync(async () => (await rig.EffectsAsync()).Count(e => e.Tenure == old.Token) >= 6, "the old tenure's overlapping writes");

        var options = PlacerRig.Options(r1);
        var since = Stopwatch.GetTimestamp();
        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None));

        var moved = await rig.Log.StartOfAsync("r2");
        var movedAfter = Stopwatch.GetElapsedTime(since, moved.At);
        await PlacerRig.UntilAsync(async () => (await rig.EffectsAsync()).Count(e => e.Tenure == moved.Token) >= 2, "the new tenure's writes");

        var effects = await rig.EffectsAsync();
        var lastOld = effects.Where(e => e.Tenure == old.Token).Max(e => e.N);
        var firstNew = effects.Where(e => e.Tenure == moved.Token).Min(e => e.N);
        Assert.True(lastOld < firstNew, $"tenure {old.Token} committed n = {lastOld} after tenure {moved.Token}'s first write n = {firstNew}");
        Assert.All(effects.Where(e => e.N > lastOld), e => Assert.Equal(moved.Token, e.Tenure));
        Assert.True(moved.Token > old.Token);
        Assert.True(await rig.RefusedAsync(old.Token), $"the database still accepts tenure {old.Token}");

        var bound = options.PlacementInterval + options.StopBudget + options.FencedTransactionTimeout + TimeSpan.FromSeconds(1) + 2 * options.PlacementInterval;
        output.WriteLine($"the new tenure started {movedAfter} after the request; bound {bound}");
        Assert.True(movedAfter <= bound, $"the new tenure started {movedAfter} after the request, beyond {bound}");
    }

    [Fact]
    public async Task Nothing_is_assigned_to_a_replica_after_its_drain_step_1_even_when_the_pass_read_it_serving()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec());
        var first = await rig.Log.StartOfAsync("r1");
        var r2 = await rig.StartAsync("r2", rig.Spec());

        using var assign = r1.Faults.HoldNext(OwnedFaultPoints.PlacerBeforeAssign);
        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None));
        await assign.WaitReachedAsync(timeout: PlacerRig.Limit);

        using var drain = r2.Faults.HoldNext(MembershipFaultPoints.DrainAfterStep1);
        var stop = r2.StopAsync();
        await drain.WaitReachedAsync(timeout: PlacerRig.Limit);
        assign.Release();

        await PlacerRig.UntilAsync(async () => await rig.RowAsync() is ("r1", var token) && token > first.Token, "the row back on r1");
        Assert.DoesNotContain(rig.Log.Starts, s => s.Replica == "r2");
        drain.Release();
        await stop.WaitAsync(PlacerRig.Limit);
        Assert.DoesNotContain(rig.Log.Starts, s => s.Replica == "r2");
    }

    [Fact]
    public async Task A_delay_between_activation_and_Serving_keeps_the_placer_from_assigning_to_the_replica()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec());
        var first = await rig.Log.StartOfAsync("r1");

        var r2 = await rig.Run.PrepareAsync("r2", rig.Spec());
        using var serving = r2.Faults.HoldNext(MembershipFaultPoints.ActivateCommittedBeforeApplied);
        await r2.StartAsync();
        await serving.WaitReachedAsync(timeout: PlacerRig.Limit);

        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None));
        await PlacerRig.UntilAsync(async () => await rig.RowAsync() is ("r1", var token) && token > first.Token, "the row back on r1");
        var passes = r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead);
        await PlacerRig.UntilAsync(() => Task.FromResult(r1.Faults.Reached(OwnedFaultPoints.PlacerPassBeforeRead) >= passes + 2), "two more passes");
        Assert.DoesNotContain(rig.Log.Starts, s => s.Replica == "r2");
        Assert.Equal("r1", (await rig.RowAsync())?.Owner);

        serving.Release();
        await r2.Started!.WaitAsync(PlacerRig.Limit);
        await PlacerRig.ServingAsync(r2);
        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None));
        await rig.Log.StartOfAsync("r2");
    }

    [Fact]
    public async Task An_owner_that_stops_passing_the_filter_releases_the_unit_and_the_placer_moves_it_to_an_eligible_member()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(hosting: true);
        var r1 = await rig.StartAsync("r1", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        var old = await rig.Log.StartOfAsync("r1");
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r2);

        var options = PlacerRig.Options(r1);
        var since = Stopwatch.GetTimestamp();
        await PlacerRig.UnhostWidgetsAsync(r1);

        var moved = await rig.Log.StartOfAsync("r2");
        var movedAfter = Stopwatch.GetElapsedTime(since, moved.At);
        Assert.True(moved.Token > old.Token);
        Assert.True(await rig.RefusedAsync(old.Token), $"the database still accepts tenure {old.Token}");
        var row = await rig.RowAsync();
        Assert.Equal(("r2", moved.Token), (row?.Owner, row?.Token ?? 0));
        Assert.Equal([old.Token], rig.Log.Starts.Where(s => s.Replica == "r1").Select(s => s.Token));

        var bound = 3 * options.PlacementInterval + options.StopBudget + options.FencedTransactionTimeout + TimeSpan.FromSeconds(1);
        output.WriteLine($"the unit ran on r2 {movedAfter} after r1 withdrew the capability; bound {bound}");
        Assert.True(movedAfter <= bound, $"the unit ran on r2 {movedAfter} after r1 withdrew the capability, beyond {bound}");
    }

    [Fact]
    public async Task I31_the_failover_bound_holds_with_the_nudge_lost_and_both_polls_a_full_PlacementInterval_late()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(hosting: true);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r2);
        await rig.Log.StartOfAsync("r2");
        var r3 = await rig.StartAsync("r3", spec);
        await PlacerRig.HostWidgetsAsync(r3);

        using var pass = r1.Faults.HoldNext(OwnedFaultPoints.PlacerPassBeforeRead);
        using var poll = r3.Faults.HoldNext(OwnedFaultPoints.PollBeforeRead);
        await pass.WaitReachedAsync(timeout: PlacerRig.Limit);
        await poll.WaitReachedAsync(timeout: PlacerRig.Limit);

        var options = PlacerRig.Options(r1);
        var since = Stopwatch.GetTimestamp();
        await r2.CrashAsync();
        await LateAsync(pass, () => rig.LeaseGoneAsync("r2"), options.PlacementInterval, "r2's lease to run out");
        await LateAsync(poll, async () => (await rig.RowAsync())?.Owner == "r3", options.PlacementInterval, "the assignment to r3");
        var started = await rig.Log.StartOfAsync("r3");
        var elapsed = Stopwatch.GetElapsedTime(since, started.At);

        var bound = options.LeaseTtl + 2 * options.PlacementInterval + options.FencedTransactionTimeout;
        output.WriteLine($"the unit ran on r3 {elapsed} after the crash; bound {bound}");
        Assert.True(elapsed <= bound, $"the unit ran on r3 {elapsed} after the crash, beyond the bound {bound}");
    }

    [Fact]
    public async Task I31_a_placer_crash_adds_one_PlacementInterval_to_the_failover_bound()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(hosting: true);
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
        var since = Stopwatch.GetTimestamp();
        await r1.CrashAsync();
        await LateAsync(adopt, () => rig.LeaseGoneAsync("r1"), options.PlacementInterval, "r1's lease to run out");
        await LateAsync(poll, async () => (await rig.RowAsync())?.Owner == "r3", options.PlacementInterval, "the assignment to r3");
        var started = await rig.Log.StartOfAsync("r3");
        var elapsed = Stopwatch.GetElapsedTime(since, started.At);

        var declared = new UnitKind(Kind, typeof(WidgetUnit), new UnitKindOptions()).FailoverBound(options);
        output.WriteLine($"the unit ran on r3 {elapsed} after the crash; declared bound {declared}");
        Assert.True(elapsed <= declared, $"the unit ran on r3 {elapsed} after the crash, beyond the declared bound {declared}");
    }

    [Fact]
    public async Task A_graceful_stop_of_the_placers_replica_hands_the_placer_and_its_unit_to_the_other_replica()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec());
        var first = await rig.Log.StartOfAsync("r1");
        await rig.StartAsync("r2", rig.Spec());
        Assert.Equal("r1", await rig.PlacerOwnerAsync());

        await r1.StopAsync().WaitAsync(PlacerRig.Limit);

        var moved = await rig.Log.StartOfAsync("r2");
        Assert.True(moved.Token > first.Token);
        await PlacerRig.UntilAsync(async () => await rig.PlacerOwnerAsync() == "r2", "the placer's row on r2");
    }

    [Fact]
    public async Task A_replica_between_activation_and_Serving_does_not_take_the_placers_row()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", rig.Spec());
        await rig.Log.StartOfAsync("r1");
        Assert.Equal("r1", await rig.PlacerOwnerAsync());
        await r1.CrashAsync();
        await PlacerRig.UntilAsync(() => rig.LeaseGoneAsync("r1"), "r1's lease to run out");

        var r2 = await rig.Run.PrepareAsync("r2", rig.Spec());
        using var serving = r2.Faults.HoldNext(MembershipFaultPoints.ActivateCommittedBeforeApplied);
        await r2.StartAsync();
        await serving.WaitReachedAsync(timeout: PlacerRig.Limit);
        var attempts = r2.Faults.Reached(OwnedFaultPoints.PlacerAdoptBeforeRead);
        await PlacerRig.UntilAsync(
            () => Task.FromResult(r2.Faults.Reached(OwnedFaultPoints.PlacerAdoptBeforeRead) >= attempts + 2), "two take-over attempts of r2 before Serving");
        Assert.Equal("r1", await rig.PlacerOwnerAsync());

        Assert.True(serving.Release());
        await r2.Started!.WaitAsync(PlacerRig.Limit);
        await PlacerRig.ServingAsync(r2);
        await PlacerRig.UntilAsync(async () => await rig.PlacerOwnerAsync() == "r2", "the placer's row on r2");
        await rig.Log.StartOfAsync("r2");
    }

    private static async Task LateAsync(Reeve.Cluster.Harness.Faults.HoldRule hold, Func<Task<bool>> seen, TimeSpan interval, string what)
    {
        await PlacerRig.UntilAsync(seen, what);
        await Task.Delay(interval);
        Assert.True(hold.Release(), $"the held poll for {what} was not there to release");
    }
}
