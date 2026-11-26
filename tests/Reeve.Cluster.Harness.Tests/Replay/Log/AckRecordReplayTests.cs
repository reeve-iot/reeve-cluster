using Harness.SelfTests;
using Harness.Stall;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Log;
using Xunit.Abstractions;

namespace Harness.Replay.Log;

[Collection(StackCollection.Name)]
public sealed class AckRecordReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = LogReplay.Limit;

    [Fact]
    public async Task Replay_M1_R2S1_an_acknowledgement_awaited_from_another_scope_records_so_an_open_after_it_is_refused()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var pair = await LogReplay.StartPairAsync(rig);
        LogReplay.Poison(pair.R2);

        var seq = await StallRig.UnitWriteAsync(pair.R1, "poison for r2");
        await AcknowledgedWhileThePeerIsStuckAsync(rig, pair, awaited: seq, openFrom: seq);
    }

    [Fact]
    public async Task Replay_M1_R3S1_an_acknowledgement_awaited_at_an_Append_seq_covers_the_entries_captured_after_it()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        await LogReplay.ExecuteAsync(rig.Run, LogReplay.CreateCapturedTable);
        var pair = await LogReplay.StartPairAsync(rig, LogReplay.CapturingSpec());
        LogReplay.Poison(pair.R2);

        var (appended, captured) = await LogReplay.AppendThenCaptureAsync(pair.R1, LogReplay.Gadget);
        Assert.True(captured > appended, "the captured entry is appended at the commit, above the Append's seq");
        await AcknowledgedWhileThePeerIsStuckAsync(rig, pair, awaited: appended, openFrom: captured);
    }

    private static async Task AcknowledgedWhileThePeerIsStuckAsync(StallRig rig, ReplicaPair pair, long awaited, long openFrom)
    {
        var outcome = await StallRig.AckFromAnotherScopeAsync(pair.R1, awaited).WaitAsync(Limit);
        Assert.Equal(AckOutcome.Acknowledged.None, outcome);
        Assert.True(await LogReplay.ScalarAsync(rig.Run, "SELECT acked_seq FROM reeve_cluster.change_ack") >= openFrom, "the record raised acked_seq to the counted bound");

        await LogReplay.StuckAfterRejoinAsync(rig, pair.R2);
        var refused = Assert.IsType<SkipOpenResult.Refused>(await LogReplay.OpenAsync(pair.R1, openFrom));
        Assert.True(refused.AckedSeq >= openFrom);
        Assert.Equal(0, await LogReplay.ScalarAsync(rig.Run, "SELECT count(*) FROM reeve_cluster.change_skip"));
    }

    [Fact]
    public async Task Replay_M1_R2S1_open_an_open_before_the_write_is_named_by_an_acknowledgement_awaited_from_another_scope()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var pair = await LogReplay.StartPairAsync(rig);
        LogReplay.Opened(await LogReplay.OpenAsync(pair.R1, 1));

        var seq = await LogReplay.WriteAsync(pair.R1, [new ChangeEntry(LogReplay.Widget)]);
        Assert.Equal(new AckOutcome.Acknowledged([LogReplay.Widget]), await StallRig.AckFromAnotherScopeAsync(pair.R1, seq).WaitAsync(Limit));
        Assert.Equal(seq, (await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq);
    }

    [Fact]
    public async Task Replay_M1_R2S1_append_an_Append_of_two_types_is_named_by_every_range_below_the_bound_not_only_the_awaited_entrys_type()
    {
        await using var rig = await StallRig.CreateAsync(stack, output.WriteLine);
        var pair = await LogReplay.StartPairAsync(rig);
        LogReplay.Opened(await LogReplay.OpenAsync(pair.R1, 1));

        var seq = await LogReplay.WriteAsync(pair.R1, [new ChangeEntry(LogReplay.Widget), new ChangeEntry(LogReplay.Gadget)]);
        Assert.Equal(new AckOutcome.Acknowledged([LogReplay.Widget]), await StallRig.AckFromAnotherScopeAsync(pair.R1, seq).WaitAsync(Limit));
    }
}
