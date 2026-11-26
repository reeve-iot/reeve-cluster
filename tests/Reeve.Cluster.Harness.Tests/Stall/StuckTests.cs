using System.Diagnostics;
using System.Globalization;
using Harness.SelfTests;
using Harness.Slice;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Stall;

[Collection(StackCollection.Name)]
public sealed class StuckTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = StallRig.Limit;

    private static readonly TimeSpan FenceTime = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(nameof(HangMode.Stuck))]
    [InlineData(nameof(HangMode.BlockThread))]
    [InlineData(nameof(HangMode.ServerSleepOwnConnection))]
    [InlineData(nameof(HangMode.ServerSleepPassConnection))]
    public async Task Stuck_in_Serving_a_reconciler_that_never_returns_ends_the_process_at_its_start_plus_the_allowance_after_the_lease_release(string mode)
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        using var hang = r1.Reconcilers.HangNext(StallRig.Widget, Enum.Parse<HangMode>(mode));
        var stalls = rig.Run.Stalls.Begin();
        var writeSequence = rig.Run.Events.Of("r1").Max(e => e.Sequence);
        try
        {
            await StallRig.UnitWriteAsync(r1, "hangs");
            await hang.WaitEnteredAsync(timeout: Limit);
            var since = Stopwatch.StartNew();

            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Exited, Limit);
            Assert.InRange(since.Elapsed, Allowance(r1) - TimeSpan.FromMilliseconds(200), Allowance(r1) + FenceTime);

            Assert.Equal([ReplicaPhase.Serving, ReplicaPhase.Fencing, ReplicaPhase.Exited], rig.Run.Phases.Path("r1").TakeLast(3));
            var events = rig.Run.Events.Of("r1");
            var step3 = events.Single(e => IsStep(e, 3));
            var exit = events.Single(e => e.Action == MembershipActions.StuckExit);
            Assert.True(step3.Sequence < exit.Sequence);
            AssertReleasedOrStalled(step3, stalls);
            if (events.Any(e => e.Action == MembershipActions.FenceStall))
            {
                Assert.DoesNotContain(events, e => e.Action == LogActions.Publish && e.Sequence > writeSequence);
                Assert.Equal(["applier-stall"], (await rig.Run.Observer.FenceHistoryAsync("r1")).Select(f => f.Trigger));
                Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the retry did not publish within the stall limit, so the applier's stall fenced beside the stuck attempt, which ended the process");
            }
            else
            {
                Assert.Contains(events, e => e.Action == MembershipActions.FenceStuck);
                Assert.Equal(["stuck-attempt"], (await rig.Run.Observer.FenceHistoryAsync("r1")).Select(f => f.Trigger));
            }

            Assert.Equal(MemberRowState.Fenced, (await rig.Run.Observer.MemberAsync("r1"))!.State);
        }
        finally
        {
            hang.Release();
        }
    }

    [Fact]
    public async Task Stuck_in_CatchingUp_an_attempt_that_never_returns_fences_with_FenceStuck_and_ends_the_process_at_its_start_plus_the_allowance()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.Run.PrepareAsync("r1", StallRig.Scripted());
        using var load = r1.Faults.HoldNext(LogFaultPoints.InsideFirstLoad);
        using var catchUp = r1.Faults.Hold(LogFaultPoints.PassBegin, skip: 1, times: 1);
        await r1.StartAsync();
        await load.WaitReachedAsync(timeout: Limit);
        await StallRig.UnitWriteAsync(r1, "behind");
        load.Release();
        await catchUp.WaitReachedAsync(timeout: Limit);
        Assert.Equal(ReplicaPhase.CatchingUp, SliceRig.PhaseOf(r1));

        using var hang = r1.Reconcilers.HangNext(StallRig.Widget, HangMode.Stuck);
        try
        {
            catchUp.Release();
            await hang.WaitEnteredAsync(timeout: Limit);
            var since = Stopwatch.StartNew();

            HarnessOperations.ReconcilerThrow(r1, StallRig.Widget, () => new InvalidOperationException("retry fault"), times: 1000);

            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Exited, Limit);
            Assert.InRange(since.Elapsed, Allowance(r1) - TimeSpan.FromMilliseconds(200), Allowance(r1) + FenceTime);
            Assert.Equal([ReplicaPhase.CatchingUp, ReplicaPhase.Fencing, ReplicaPhase.Exited], rig.Run.Phases.Path("r1").TakeLast(3));
            var actions = rig.Run.Events.Actions("r1");
            Assert.Contains(MembershipActions.FenceStuck, actions);
            Assert.Contains(MembershipActions.StuckExit, actions);
            Assert.DoesNotContain(MembershipActions.FenceStall, actions);
            Assert.Equal(["stuck-attempt"], (await rig.Run.Observer.FenceHistoryAsync("r1")).Select(f => f.Trigger));
        }
        finally
        {
            hang.Release();
        }
    }

    [Fact]
    public async Task Stuck_an_attempt_abandoned_before_its_timeout_and_outstanding_in_Fenced_ends_the_process_instead_of_a_rejoin()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted(o =>
        {
            o.ReconcileTimeout = TimeSpan.FromSeconds(2);
            o.ApplyStallLimit = TimeSpan.FromSeconds(4);
            o.RejoinBackoff = TimeSpan.FromSeconds(120);
        }));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        using var hang = r1.Reconcilers.HangNext(StallRig.Widget, HangMode.Stuck);
        try
        {
            await StallRig.UnitWriteAsync(r1, "abandoned");
            await hang.WaitEnteredAsync(timeout: Limit);
            var since = Stopwatch.StartNew();
            HarnessOperations.ReportFatalFault(r1, "a fault while an attempt hangs");
            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Fenced, Limit);

            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Exited, Limit);
            Assert.InRange(since.Elapsed, Allowance(r1) - TimeSpan.FromMilliseconds(200), r1.Options.RejoinBackoff / 2);
            Assert.Equal([ReplicaPhase.Fencing, ReplicaPhase.Fenced, ReplicaPhase.Exited], rig.Run.Phases.Path("r1").TakeLast(3));
            var actions = rig.Run.Events.Actions("r1");
            Assert.DoesNotContain(LogActions.AttemptHang, actions);
            Assert.Contains(MembershipActions.StuckExit, actions);
            Assert.DoesNotContain(MembershipActions.Rejoin, actions);
        }
        finally
        {
            hang.Release();
        }
    }

    [Theory]
    [InlineData(nameof(PoolHang.Await))]
    [InlineData(nameof(PoolHang.BlockThread))]
    [InlineData(nameof(PoolHang.ServerSleep))]
    [InlineData(nameof(PoolHang.PassConnection))]
    public async Task Stuck_repeated_a_hang_on_a_pooled_or_the_pass_connection_ends_each_process_and_the_pool_never_runs_out(string hang)
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var plan = new PoolPlan { Hang = Enum.Parse<PoolHang>(hang) };
        var spec = StallRig.Pooled(plan);
        try
        {
            var replica = await rig.StartAsync("r1", spec);
            var stallFencesBefore = 0;
            for (var cycle = 1; cycle <= 3; cycle++)
            {
                await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Serving, Limit);
                var entered = plan.Entered;
                await StallRig.UnitWriteAsync(replica, $"cycle {cycle}");
                await SliceRig.UntilAsync(() => Task.FromResult(plan.Entered > entered), $"cycle {cycle}'s attempt hangs", Limit);

                await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Exited, Limit);
                var actions = rig.Run.Events.Actions("r1");
                Assert.Equal(cycle, actions.Count(a => a == MembershipActions.StuckExit));
                Assert.DoesNotContain(MembershipActions.Rejoin, actions);
                var stallFences = actions.Count(a => a == MembershipActions.FenceStall);
                if (rig.Run.Phases.Path("r1").TakeLast(2).Contains(ReplicaPhase.Fenced))
                {
                    Assert.True(stallFences > stallFencesBefore, $"cycle {cycle} passed through Fenced without an applier-stall fence, which is the only trigger that races the stuck attempt");
                    Degraded.Report(output.WriteLine, $"cycle {cycle}: the stuck decision ran late and the applier's stall fenced first; the stuck attempt ended the process in Fenced");
                }

                stallFencesBefore = stallFences;

                await HarnessOperations.CrashReplica(replica);
                replica = await HarnessOperations.RestartReplica(rig.Run, "r1", spec);
                await replica.Started!.WaitAsync(Limit);
            }

            Assert.Equal(3, rig.Run.Events.Actions("r1").Count(a => a == MembershipActions.StuckExit));
            Assert.DoesNotContain(MembershipActions.Rejoin, rig.Run.Events.Actions("r1"));

            plan.Hang = PoolHang.None;
            await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Serving, Limit);
            var seq = await StallRig.UnitWriteAsync(replica, "served");
            Assert.IsType<AckOutcome.Acknowledged>(await StallRig.AckFromAnotherScopeAsync(replica, seq).WaitAsync(Limit));
        }
        finally
        {
            plan.Release();
            await CancelSleepsAsync(rig);
        }
    }

    [Fact]
    public async Task S8_a_server_side_hang_under_a_bounded_CommandTimeout_returns_its_slot_and_the_replica_rejoins()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var plan = new PoolPlan { CommandTimeoutSeconds = 1 };
        try
        {
            var r1 = await rig.StartAsync("r1", StallRig.Pooled(plan));
            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
            plan.Hang = PoolHang.BoundedSleep;
            var stalls = rig.Run.Stalls.Begin();
            await StallRig.UnitWriteAsync(r1, "bounded");

            await SliceRig.UntilAsync(
                () => Task.FromResult(rig.Run.Events.Actions("r1").Contains(MembershipActions.FenceStall) || SliceRig.PhaseOf(r1) == ReplicaPhase.Exited),
                "the stall fences r1",
                Limit);
            if (SliceRig.PhaseOf(r1) == ReplicaPhase.Exited)
            {
                Assert.True(stalls.Exceeds(TimeSpan.FromSeconds(1)), "r1 ended with its stuck attempt, and the runner was not stalled");
                Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the attempt became stuck before the bounded sleep returned its slot, so the replica exited instead of rejoining");
                return;
            }

            plan.Hang = PoolHang.None;
            await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Contains(MembershipActions.Rejoin)), "r1 rejoins", Limit);
            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);

            Assert.True(plan.Entered >= 1);
            var actions = rig.Run.Events.Actions("r1");
            Assert.Contains(MembershipActions.FenceStall, actions);
            Assert.DoesNotContain(MembershipActions.StuckExit, actions);
            Assert.DoesNotContain(ReplicaPhase.Exited, rig.Run.Phases.Path("r1"));
            Assert.Equal(0, await SleepsAsync(rig));

            var seq = await StallRig.UnitWriteAsync(r1, "after");
            Assert.IsType<AckOutcome.Acknowledged>(await StallRig.AckFromAnotherScopeAsync(r1, seq).WaitAsync(Limit));
        }
        finally
        {
            plan.Release();
            await CancelSleepsAsync(rig);
        }
    }

    private void AssertReleasedOrStalled(RecordedEvent step3, StallSpan stalls)
    {
        if (!Convert.ToBoolean(step3.State!["released"], CultureInfo.InvariantCulture))
        {
            Assert.True(stalls.Exceeds(TimeSpan.FromMilliseconds(500)), "step 3 did not release the lease, and the runner was not stalled");
            Degraded.Report(output.WriteLine, $"{stalls.Describe()}: step 3 did not release the lease within RenewTimeout, so the release was not checked");
        }
    }

    private static TimeSpan Allowance(Replica replica) => replica.Options.ReconcileTimeout!.Value + replica.Options.ApplyStallLimit;

    private static bool IsStep(RecordedEvent e, int k) =>
        e.Action == MembershipActions.FStep && Convert.ToInt64(e.State!["k"], CultureInfo.InvariantCulture) == k;

    private const string Sleeping = "query = 'SELECT pg_sleep(3600)' AND datname = current_database() AND pid <> pg_backend_pid()";

    private static async Task<long> SleepsAsync(StallRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND {Sleeping}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task CancelSleepsAsync(StallRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand($"SELECT count(pg_cancel_backend(pid)) FROM pg_stat_activity WHERE {Sleeping}", connection);
        await command.ExecuteScalarAsync();
    }
}
