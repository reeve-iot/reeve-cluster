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
public sealed class StallTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = StallRig.Limit;

    private static TimeSpan StallFence(Replica replica) => replica.Options.ReconcileTimeout!.Value + replica.Options.ApplyStallLimit;

    [Fact]
    public async Task CatchingUp_a_reconciler_failing_during_catch_up_keeps_the_replica_joining_with_no_fence_recorded()
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

        HarnessOperations.ReconcilerThrow(r1, StallRig.Widget, () => new InvalidOperationException("catch-up fault"), times: 1000);
        catchUp.Release();

        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Count(a => a == LogActions.AttemptFail) >= 2), "two failed attempts", Limit);
        await Task.Delay(TimeSpan.FromSeconds(4));
        Assert.Equal(ReplicaPhase.CatchingUp, SliceRig.PhaseOf(r1));
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync("r1"))!.State);
        Assert.Empty(await rig.Run.Observer.FenceHistoryAsync("r1"));
        Assert.DoesNotContain(rig.Run.Events.Actions("r1"), a => a is MembershipActions.FenceStall or MembershipActions.FenceStuck or MembershipActions.FStep);
        Assert.DoesNotContain(ReplicaPhase.Fencing, rig.Run.Phases.Path("r1"));

        HarnessOperations.FixFault(rig.Run);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
    }

    [Fact]
    public async Task Edges_a_reconciler_failing_on_one_replica_delays_writes_by_at_most_one_stall_and_the_replica_stays_joining_afterwards()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig, configureR2: LongLease);
        HarnessOperations.ReconcilerThrow(r2, StallRig.Widget, () => new InvalidOperationException("peer fault"), times: 1000);

        var stalls = rig.Run.Stalls.Begin();
        var waited = Stopwatch.StartNew();
        var seq = await StallRig.UnitWriteAsync(r1, "first");
        var outcome = await StallRig.AckFromAnotherScopeAsync(r1, seq).WaitAsync(Limit);
        var first = waited.Elapsed;

        if (!AssertAcknowledgedOrReleaseUnanswered(outcome, rig.Run.Events.Of("r2"), stalls))
        {
            HarnessOperations.FixFault(rig.Run);
            return;
        }

        Assert.True(first >= StallFence(r2) - TimeSpan.FromMilliseconds(500), $"the write was acknowledged after {first}, before the peer's stall fence at {StallFence(r2)}");
        Assert.Contains(MembershipActions.FenceStall, rig.Run.Events.Actions("r2"));
        var edgeEvents = rig.Run.Events.Snapshot();
        Assert.True(
            edgeEvents.First(e => e.Replica == "r2" && e.Action == MembershipActions.FenceStall).Sequence
                < edgeEvents.Last(e => e.Replica == "r1" && e.Action == LogActions.AckDone).Sequence,
            "the wait ended before the peer's stall fence");
        Assert.DoesNotContain("r2", rig.Run.Events.WaitSets().Last(w => w.Replica == "r1" && w.Action == LogActions.AckDone && w.AwaitedSeq == seq).Members);

        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r2").Contains(MembershipActions.Rejoin)), "r2 rejoins", Limit);
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Loading, Limit);
        await Task.Delay(TimeSpan.FromSeconds(4));
        Assert.Equal(ReplicaPhase.Loading, SliceRig.PhaseOf(r2));
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync("r2"))!.State);

        var second = await StallRig.UnitWriteAsync(r1, "second");
        Assert.IsType<AckOutcome.Acknowledged>(await StallRig.AckFromAnotherScopeAsync(r1, second).WaitAsync(Limit));
        Assert.Equal(["r1"], rig.Run.Events.WaitSets().Last(w => w.Replica == "r1" && w.Action == LogActions.AckBegin && w.AwaitedSeq == second).Members);

        HarnessOperations.FixFault(rig.Run);
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
    }

    [Fact]
    public async Task AckRecord_a_unit_write_awaited_from_another_scope_while_the_peer_fails_and_fences_is_acknowledged_within_the_stall_bound_and_raises_acked_seq_to_the_lowest_counted_cursor()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig, configureR2: LongLease);
        HarnessOperations.ReconcilerThrow(r2, StallRig.Widget, () => new InvalidOperationException("entry fault"), times: 1000);

        var stalls = rig.Run.Stalls.Begin();
        var seq = await StallRig.UnitWriteAsync(r1, "recorded");
        var outcome = await StallRig.AckFromAnotherScopeAsync(r1, seq).WaitAsync(Limit);

        var r2Events = rig.Run.Events.Of("r2");
        Assert.Contains(r2Events, e => e.Action == LogActions.AttemptFail);
        Assert.Contains(r2Events, e => e.Action == MembershipActions.FenceStall);
        Assert.Contains(r2Events, e => IsStep3(e, released: true) || IsStep3(e, released: false));
        if (!AssertAcknowledgedOrReleaseUnanswered(outcome, r2Events, stalls))
        {
            return;
        }

        Assert.Equal(seq, await AckedSeqAsync(rig));
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq == seq, "r1's cursor at the write", Limit);
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq < seq);
        Assert.Contains(rig.Run.Events.Actions("r1"), a => a == LogActions.AckRecord);
    }

    [Fact]
    public async Task Replay_M1_clockack_unanswered_a_stall_whose_step_3_is_not_answered_may_end_the_write_Unconfirmed_Timeout()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig, o => o.ApplyStallLimit = TimeSpan.FromSeconds(5), tight: true);
        Assert.Equal(TimeSpan.FromSeconds(11), r1.Options.AckTimeout);
        HarnessOperations.ReconcilerThrow(r2, StallRig.Widget, () => new InvalidOperationException("entry fault"), times: 1000);
        using var step3 = r2.Faults.HoldNext(MembershipFaultPoints.FenceBeforeStep3);

        var stalls = rig.Run.Stalls.Begin();
        var waited = Stopwatch.StartNew();
        var seq = await StallRig.UnitWriteAsync(r1, "unanswered");
        var ack = StallRig.AckFromAnotherScopeAsync(r1, seq);

        await step3.WaitReachedAsync(timeout: Limit);
        await Task.Delay(r2.Options.RenewTimeout + TimeSpan.FromMilliseconds(500));
        step3.Release();

        var outcome = await ack.WaitAsync(Limit);
        if (outcome is not AckOutcome.Unconfirmed && stalls.Exceeds(TimeSpan.FromSeconds(1)))
        {
            Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the write ended {outcome} instead of Unconfirmed(Timeout), so the unanswered step 3 was not checked");
            return;
        }

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), outcome);
        Assert.True(waited.Elapsed >= r1.Options.AckTimeout - TimeSpan.FromMilliseconds(500), $"the wait ended after {waited.Elapsed}, before AckTimeout {r1.Options.AckTimeout}");
        Assert.Contains(LogActions.AckTimeout, rig.Run.Events.Actions("r1"));

        var r2Events = rig.Run.Events.Of("r2");
        var fail = r2Events.First(e => e.Action == LogActions.AttemptFail);
        var stall = r2Events.First(e => e.Action == MembershipActions.FenceStall);
        var step = r2Events.First(e => IsStep3(e, released: false));
        Assert.True(fail.Sequence < stall.Sequence && stall.Sequence < step.Sequence);
        Assert.DoesNotContain(r2Events, e => IsStep3(e, released: true));
    }

    private static async Task<(Replica R1, Replica R2)> StartPairAsync(
        StallRig rig, Action<ClusterOptions>? configure = null, bool tight = false, Action<ClusterOptions>? configureR2 = null)
    {
        var spec = StallRig.Scripted(configure);
        var spec2 = configureR2 is null ? spec : StallRig.Scripted(o => { configure?.Invoke(o); configureR2(o); });
        if (tight)
        {
            spec = new ReplicaSpec { Profile = TestProfile.ApplyTight, Configure = spec.Configure, Reconcilers = spec.Reconcilers, Redis = spec.Redis };
        }

        var r1 = await rig.StartAsync("r1", spec);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var r2 = await rig.StartAsync("r2", spec2);
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
        return (r1, r2);
    }

    private static void LongLease(ClusterOptions options)
    {
        options.LeaseTtl = TimeSpan.FromSeconds(60);
        options.ProxyReadTimeout = TimeSpan.FromSeconds(120);
    }

    private bool AssertAcknowledgedOrReleaseUnanswered(AckOutcome outcome, IEnumerable<RecordedEvent> r2Events, StallSpan stalls)
    {
        if (r2Events.Any(e => IsStep3(e, released: true)))
        {
            Assert.IsType<AckOutcome.Acknowledged>(outcome);
            return true;
        }

        Assert.True(stalls.Exceeds(TimeSpan.FromMilliseconds(500)), $"step 3 was not answered within RenewTimeout, and the runner was not stalled; the outcome was {outcome}");
        Degraded.Report(output.WriteLine, $"{stalls.Describe()}: step 3 of the peer's fence was not answered in RenewTimeout, so its release was not checked");
        return outcome is AckOutcome.Acknowledged;
    }

    private static bool IsStep3(RecordedEvent e, bool released) =>
        e.Action == MembershipActions.FStep
        && Convert.ToInt64(e.State!["k"], CultureInfo.InvariantCulture) == 3
        && Convert.ToBoolean(e.State!["released"], CultureInfo.InvariantCulture) == released;

    private static async Task<long> AckedSeqAsync(StallRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT acked_seq FROM reeve_cluster.change_ack WHERE id", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
