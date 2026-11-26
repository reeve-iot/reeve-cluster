using Harness.SelfTests;
using Harness.Slice;
using Harness.Stall;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Skip;

[Collection(StackCollection.Name)]
public sealed class SkipTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = StallRig.Limit;
    private const string Widget = StallRig.Widget;

    [Fact]
    public async Task I15_skip_debt_a_close_converges_every_member_before_a_later_write_of_the_type_is_acknowledged()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig);
        var before = await StallRig.UnitWriteAsync(r1, "before");
        Assert.Equal(AckOutcome.Acknowledged.None, await StallRig.AckFromAnotherScopeAsync(r1, before).WaitAsync(Limit));
        var id = Opened(await OpenAsync(r1, before + 1));

        var skipped = await StallRig.UnitWriteAsync(r1, "skipped");
        Assert.Equal(new AckOutcome.Acknowledged([Widget]), await StallRig.AckFromAnotherScopeAsync(r1, skipped).WaitAsync(Limit));
        var keptOnR2 = Widget2(r2).Head;
        Assert.True(keptOnR2 < skipped, "r2 kept the state it had when it began skipping");

        using var held = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var repair = await r1.Services.GetRequiredService<IChangeSkip>().CloseAsync(id, "ops");
        await held.WaitReachedAsync(timeout: Limit);

        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= repair, "r1 applied the repair entry", Limit);
        Assert.True(Widget2(r1).Head >= repair, "r1 re-reconciled the type at the repair entry, before any later write");

        var later = await StallRig.UnitWriteAsync(r1, "later");
        var ack = StallRig.AckFromAnotherScopeAsync(r1, later);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.False(ack.IsCompleted, "the later write waits for r2 to apply the repair entry");
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq < repair);

        held.Release();
        Assert.Equal(new AckOutcome.Acknowledged([]), await ack.WaitAsync(Limit));
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq >= later);
        Assert.True(Widget2(r2).Head >= repair, "r2 re-reconciled the type at or after the repair entry");
    }

    [Fact]
    public async Task Edges_an_open_range_skip_keeps_the_fleet_joined_through_further_entries_of_its_type_and_an_open_at_or_below_acked_seq_is_refused()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig);
        Poison(r1);
        Poison(r2);

        var poison = await StallRig.UnitWriteAsync(r1, "poison");
        await SliceRig.UntilAsync(() => Task.FromResult(Failed(rig, "r1") && Failed(rig, "r2")), "both replicas fail on the entry", Limit);
        Opened(await OpenAsync(r1, poison));

        await Task.Delay(TimeSpan.FromSeconds(4));
        for (var i = 0; i < 3; i++)
        {
            var seq = await StallRig.UnitWriteAsync(r2, $"further {i}");
            Assert.Equal(new AckOutcome.Acknowledged([Widget]), await StallRig.AckFromAnotherScopeAsync(r2, seq).WaitAsync(Limit));
        }

        foreach (var replica in new[] { r1, r2 })
        {
            Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(replica));
            Assert.Equal(MemberRowState.Active, (await rig.Run.Observer.MemberAsync(replica.Id))!.State);
            Assert.DoesNotContain(rig.Run.Events.Actions(replica.Id), a => a is MembershipActions.FenceStall or MembershipActions.FStep);
        }

        Assert.Empty(await rig.Run.Observer.FenceHistoryAsync());

        var acked = await ScalarAsync(rig, "SELECT acked_seq FROM reeve_cluster.change_ack");
        Assert.Equal(new SkipOpenResult.Refused(acked), await OpenAsync(r1, acked, type: "other"));
        Assert.Equal(1, await ScalarAsync(rig, "SELECT count(*) FROM reeve_cluster.change_skip"));
    }

    [Fact]
    public async Task Edges_a_skip_opened_between_a_commit_and_the_pass_is_reported_as_skipped()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig);
        using var held = r2.Faults.HoldNext(LogFaultPoints.PassBegin);

        var seq = await StallRig.UnitWriteAsync(r1, "committed");
        await held.WaitReachedAsync(timeout: Limit);
        var ack = StallRig.AckFromAnotherScopeAsync(r1, seq);
        Opened(await OpenAsync(r1, seq));
        held.Release();

        Assert.Equal(new AckOutcome.Acknowledged([Widget]), await ack.WaitAsync(Limit));
    }

    [Fact]
    public async Task Edges_an_open_from_an_unacknowledged_entry_that_a_departed_member_applied_covers_it_and_the_stuck_replica_serves()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig);
        Poison(r2);

        var poison = await StallRig.UnitWriteAsync(r1, "applied by r1 only");
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= poison, "r1 applied the entry", Limit);
        await r1.StopAsync();
        await StuckAfterRejoinAsync(rig, r2);

        Opened(await OpenAsync(r2, poison));

        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
        Assert.True(Widget2(r2).Head < poison, "r2 serves the state it kept");
    }

    [Fact]
    public async Task Edges_an_open_stalled_before_its_commit_fences_no_replica_and_costs_acknowledgements_at_most_a_202()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await StartPairAsync(rig, o => o.FencedTransactionTimeout = TimeSpan.FromSeconds(3));
        using var held = r1.Faults.HoldNext(LogFaultPoints.OpenAfterInsert);
        var open = OpenAsync(r1, 1_000_000);
        await held.WaitReachedAsync(timeout: Limit);

        var seq = await StallRig.UnitWriteAsync(r2, "behind a stalled open");
        var outcome = await StallRig.AckFromAnotherScopeAsync(r2, seq).WaitAsync(Limit);

        Assert.True(
            outcome is AckOutcome.Acknowledged || outcome == new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout),
            $"the acknowledgement answered {outcome}");
        held.Release();
        await Assert.ThrowsAnyAsync<Exception>(() => open.WaitAsync(Limit));

        Assert.Equal(0, await ScalarAsync(rig, "SELECT count(*) FROM reeve_cluster.change_skip"));
        Assert.Empty(await rig.Run.Observer.FenceHistoryAsync());
        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r1));
        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r2));
    }

    [Fact]
    public async Task AcrossRejoin_a_replica_that_stall_fenced_on_a_poison_entry_rejoins_under_an_open_skip_keeping_the_types_state_and_serves_and_a_fresh_process_stays_Loading()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var kept = Widget2(r1);
        Poison(r1);

        var poison = await StallRig.UnitWriteAsync(r1, "poison");
        await StuckAfterRejoinAsync(rig, r1);
        Opened(await OpenAsync(r1, poison));

        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        Assert.Equal(kept, Widget2(r1));
        var reload = rig.Run.Events.Of("r1").Last(e => e.Action == LogActions.PassBegin && Equals(e.State!["reload"], true));
        Assert.Equal(new[] { Widget }, (IEnumerable<string>)reload.State!["kept"]!);

        var further = await StallRig.UnitWriteAsync(r1, "further");
        Assert.Equal(new AckOutcome.Acknowledged([Widget]), await StallRig.AckFromAnotherScopeAsync(r1, further).WaitAsync(Limit));

        await HarnessOperations.CrashReplica(r1);
        var fresh = await rig.Run.PrepareAsync("r1", StallRig.Scripted());
        Poison(fresh);
        await fresh.StartAsync();
        await SliceRig.UntilAsync(() => Task.FromResult(fresh.Reconcilers.Calls.Count(c => c.Outcome == "threw") >= 2), "the fresh load fails twice", Limit);
        Assert.Equal(ReplicaPhase.Loading, SliceRig.PhaseOf(fresh));
        Assert.False(fresh.Started!.IsCompleted);
    }

    [Fact]
    public async Task AcrossRejoin_with_the_skip_lost_in_a_failover_the_next_rejoin_reloads_the_type_and_the_replica_stays_Loading()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        Poison(r1);
        var poison = await StallRig.UnitWriteAsync(r1, "poison");
        await StuckAfterRejoinAsync(rig, r1);
        var id = Opened(await OpenAsync(r1, poison));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);

        await ExecuteAsync(rig, $"DELETE FROM reeve_cluster.change_skip WHERE id = {id}");
        var rejoins = rig.Run.Events.Actions("r1").Count(a => a == MembershipActions.Rejoin);
        HarnessOperations.ReportFatalFault(r1, "a test fence");
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Count(a => a == MembershipActions.Rejoin) > rejoins), "r1 rejoins", Limit);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Loading, Limit);
        var threw = r1.Reconcilers.Calls.Count(c => c.Outcome == "threw");
        await SliceRig.UntilAsync(() => Task.FromResult(r1.Reconcilers.Calls.Count(c => c.Outcome == "threw" && c.FullLoad) > threw), "the reload fails", Limit);

        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(ReplicaPhase.Loading, SliceRig.PhaseOf(r1));
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync("r1"))!.State);
    }

    private static async Task StuckAfterRejoinAsync(StallRig rig, Replica replica)
    {
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(replica.Id).Contains(MembershipActions.FenceStall)), $"{replica.Id} stall-fences", Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(replica.Id).Contains(MembershipActions.Rejoin)), $"{replica.Id} rejoins", Limit);
        await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Loading, Limit);
        await SliceRig.UntilAsync(
            () => Task.FromResult(replica.Reconcilers.Calls.Any(c => c.FullLoad && c.Outcome == "threw")), $"{replica.Id}'s reload fails", Limit);
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync(replica.Id))!.State);
    }

    private static void Poison(Replica replica) =>
        HarnessOperations.ReconcilerThrow(replica, Widget, () => new InvalidOperationException("a poison entry"), times: 10_000);

    private static bool Failed(StallRig rig, string replica) => rig.Run.Events.Actions(replica).Contains(LogActions.AttemptFail);

    private static ReconciledState Widget2(Replica replica) =>
        replica.Services.GetRequiredService<GenerationStore>().Current!.Get<ReconciledState>(Widget);

    private static Task<SkipOpenResult> OpenAsync(Replica replica, long from, string type = Widget) =>
        replica.Services.GetRequiredService<IChangeSkip>().OpenAsync(new SkipRequest(type, from, "poison", "ops"));

    private static long Opened(SkipOpenResult result) => Assert.IsType<SkipOpenResult.Opened>(result).SkipId;

    private static async Task<StallRigPair> StartPairAsync(StallRig rig, Action<ClusterOptions>? configure = null)
    {
        var r1 = await rig.StartAsync("r1", StallRig.Scripted(configure));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var r2 = await rig.StartAsync("r2", StallRig.Scripted(configure));
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
        return new StallRigPair(r1, r2);
    }

    private static async Task<long> ScalarAsync(StallRig rig, string sql)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task ExecuteAsync(StallRig rig, string sql)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private sealed record StallRigPair(Replica R1, Replica R2);
}
