using Harness.SelfTests;
using Harness.Slice;
using Harness.Stall;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Replay.Log;

[Collection(StackCollection.Name)]
public sealed class SkipReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = LogReplay.Limit;

    [Fact]
    public Task Replay_M1_A5_a_close_appends_a_repair_entry_that_the_pass_reconciles() => CloseBeforeThePassAsync();

    [Fact]
    public Task Replay_M1_F3_a_closed_range_does_not_cover_its_repair_entry() => CloseBeforeThePassAsync();

    private async Task CloseBeforeThePassAsync()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);

        var id = LogReplay.Opened(await LogReplay.OpenAsync(r1, 1));
        using var held = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        var seq = await StallRig.UnitWriteAsync(r1, "skipped");
        await held.WaitReachedAsync(timeout: Limit);

        var repair = await r1.Services.GetRequiredService<IChangeSkip>().CloseAsync(id, "ops");
        Assert.True(repair > seq, "the repair entry is above the entry the skip covered");
        held.Release();
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= repair, "r1's cursor reaches the repair entry", Limit);

        Assert.Equal(repair, await LogReplay.ScalarAsync(rig.Run, "SELECT until_seq FROM reeve_cluster.change_skip"));
        Assert.True(LogReplay.WidgetState(r1).Head >= repair, "the type was reconciled at the repair entry: its state is not the one it had before the skip");
        var pass = rig.Run.Events.Of("r1").Last(e => e.Action == LogActions.PassBegin && Convert.ToInt64(e.State![ObservationKeys.Head]) >= repair);
        Assert.Contains(LogReplay.Widget, (IEnumerable<string>)pass.State![ObservationKeys.Dirty]!);
    }

    [Fact]
    public async Task Replay_M1_S1r1_an_open_stalled_before_its_commit_fences_no_healthy_replica()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await LogReplay.StartPairAsync(rig);
        var op = await rig.Run.PrepareAsync("op", new ReplicaSpec { Configure = o => o.FencedTransactionTimeout = TimeSpan.FromSeconds(30) });
        using var held = op.Faults.HoldNext(LogFaultPoints.OpenAfterInsert);
        var open = LogReplay.OpenAsync(op, 1_000_000, "ledger");
        await held.WaitReachedAsync(timeout: Limit);
        var before = (await rig.Run.Observer.MembersAsync()).Members.ToDictionary(m => m.ReplicaId);

        var lastExpiry = before.Values.Max(m => m.LeaseExpiresAt);
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MembersAsync()).DatabaseNow > lastExpiry, "the old leases' expiry passes", Limit);

        foreach (var replica in new[] { r1, r2 })
        {
            var row = (await rig.Run.Observer.MemberAsync(replica.Id))!;
            Assert.Equal(MemberRowState.Active, row.State);
            Assert.True(row.LeaseExpiresAt > before[replica.Id].LeaseExpiresAt, $"{replica.Id} renewed while the open held its locks");
            Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(replica));
            Assert.DoesNotContain(rig.Run.Events.Actions(replica.Id), a => a is MembershipActions.FenceAtStart or MembershipActions.FStep);
        }

        Assert.Empty(await rig.Run.Observer.FenceHistoryAsync());
        held.Release();
        LogReplay.Opened(await open.WaitAsync(Limit));
    }

    [Fact]
    public async Task Replay_M1_S4r1_an_open_from_an_unacknowledged_entry_that_a_crashed_member_applied_is_admitted()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await LogReplay.StartPairAsync(rig);
        LogReplay.Poison(r2);

        var poison = await LogReplay.WriteAsync(r1, [new ChangeEntry(LogReplay.Widget)], optOut: true);
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= poison, "r1 applied the entry", Limit);
        await HarnessOperations.CrashReplica(r1);
        await LogReplay.StuckAfterRejoinAsync(rig, r2);

        Assert.True((await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= poison);
        Assert.True(await LogReplay.ScalarAsync(rig.Run, "SELECT acked_seq FROM reeve_cluster.change_ack") < poison);

        LogReplay.Opened(await LogReplay.OpenAsync(r2, poison));
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
        Assert.True(LogReplay.WidgetState(r2).Head < poison, "r2 serves the state it kept");
    }
}
