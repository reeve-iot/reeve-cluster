using System.Diagnostics;
using Harness.SelfTests;
using Harness.Slice;
using Harness.Stall;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Replay.Log;

[Collection(StackCollection.Name)]
public sealed class ApplierReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = LogReplay.Limit;

    [Fact]
    public async Task Replay_M1_A1_a_generation_holds_both_types_of_a_write_or_neither()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", SliceApp.Spec());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var store = r1.Services.GetRequiredService<GenerationStore>();

        using var held = r1.Faults.Hold(LogFaultPoints.BeforeAttemptAccepted, skip: 1, times: 1);
        await using var write = await HarnessOperations.StartWrite(r1);
        foreach (var type in new[] { SliceApp.Widget, SliceApp.Gadget })
        {
            await using var insert = new NpgsqlCommand("INSERT INTO slice_item (type, id, value) VALUES ($1, 'k', 'v')", write.Connection, write.Transaction)
            {
                Parameters = { new NpgsqlParameter<string> { TypedValue = type } },
            };
            await insert.ExecuteNonQueryAsync();
        }

        var seq = await write.AppendAsync([new ChangeEntry(SliceApp.Widget, "k"), new ChangeEntry(SliceApp.Gadget, "k")]);
        await write.CommitAsync();
        await held.WaitReachedAsync(timeout: Limit);

        var current = store.Current!;
        var widget = current.Get<Dictionary<string, string>>(SliceApp.Widget).ContainsKey("k");
        var gadget = current.Get<Dictionary<string, string>>(SliceApp.Gadget).ContainsKey("k");
        Assert.True(widget == gadget, $"a generation at head {current.Head} has the widget's row ({widget}) and not the gadget's ({gadget}): a request reads a state that never existed");
        Assert.True(current.Head < seq && !widget);

        held.Release();
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Publications().Any(p => p.Replica == "r1" && p.Head >= seq)), "the generation at the write's seq", Limit);
        foreach (var publication in rig.Run.Events.Publications().Where(p => p.Replica == "r1" && p.Head >= seq))
        {
            Assert.Contains(SliceApp.Widget, publication.Dirty);
            Assert.Contains(SliceApp.Gadget, publication.Dirty);
        }
    }

    [Fact]
    public async Task Replay_M1_S2_a_cursor_that_names_a_lost_entry_does_not_acknowledge_the_new_entry_at_its_seq()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var before = await StallRig.UnitWriteAsync(r1, "before");
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= before, "r1 applied the first entry", Limit);

        using var read = r1.Faults.HoldNext(LogFaultPoints.AfterSnapshotRead);
        var lost = await StallRig.UnitWriteAsync(r1, "lost");
        await read.WaitReachedAsync(timeout: Limit);
        await LogReplay.RewindLogAsync(rig.Run, before);
        using var poll = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        read.Release();
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= lost, "r1 writes the cursor at the lost entry", Limit);

        var reused = await StallRig.UnitWriteAsync(r1, "reused");
        Assert.Equal(lost, reused);
        await poll.WaitReachedAsync(timeout: Limit);
        var ack = StallRig.AckFromAnotherScopeAsync(r1, reused);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.False(ack.IsCompleted, "the wait counted a cursor that names a lost entry as having applied the new one");

        poll.Release();
        Assert.Equal(AckOutcome.Acknowledged.None, await ack.WaitAsync(Limit));
        var pass = rig.Run.Events.Of("r1").Last(e => e.Action == LogActions.PassBegin && Convert.ToInt64(e.State![ObservationKeys.Head]) == reused);
        Assert.Equal(true, pass.State!["full"]);
        var done = rig.Run.Events.WaitSets().Single(w => w.Action == LogActions.AckDone && w.AwaitedSeq == reused);
        Assert.True(pass.Sequence < done.Sequence, "the acknowledgement came after the pass that applied the new entry");
    }

    [Fact]
    public async Task Replay_M1_R2_a_wait_for_a_slow_healthy_pass_ends_at_AckTimeout_and_the_replica_goes_on_serving()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StallRig.Scripted());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var leaseAtWrite = (await rig.Run.Observer.MemberAsync("r1"))!.LeaseExpiresAt;

        using var held = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        var seq = await StallRig.UnitWriteAsync(r1, "slow");
        await held.WaitReachedAsync(timeout: Limit);

        var ackTimeout = r1.Options.AckTimeout;
        var waited = Stopwatch.StartNew();
        var outcome = await StallRig.AckFromAnotherScopeAsync(r1, seq).WaitAsync(Limit);
        waited.Stop();

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), outcome);
        Assert.InRange(waited.Elapsed, ackTimeout - TimeSpan.FromSeconds(0.5), ackTimeout + TimeSpan.FromSeconds(30));
        Assert.Contains(LogActions.AckTimeout, rig.Run.Events.Actions("r1"));
        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r1));
        Assert.DoesNotContain(rig.Run.Events.Actions("r1"), a => a is MembershipActions.FenceAtStart or MembershipActions.FStep);
        Assert.True((await rig.Run.Observer.MemberAsync("r1"))!.LeaseExpiresAt > leaseAtWrite, "the replica renewed its lease while the pass was held");
        held.Release();
    }
}
