using Harness.AckRace;
using Harness.SelfTests;
using Npgsql;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Xunit.Abstractions;

namespace Harness.Replay.Log;

[Collection(StackCollection.Name)]
public sealed class TransactionReplayTests(StackFixture stack, ITestOutputHelper output)
{
    [Fact]
    public async Task Replay_M1_R4S1_a_capturing_save_in_a_transaction_handed_to_EF_is_refused_so_no_change_commits_without_its_entry()
    {
        var run = await HarnessRun.BeginAsync(output.WriteLine, stack.Stack);
        await using var _ = run;
        await run.MigrateAsync();
        await LogReplay.ExecuteAsync(run, LogReplay.CreateCapturedTable);
        var r1 = await run.PrepareAsync("r1", new ReplicaSpec { ConfigureServices = HarnessCapture.Register });

        await using var write = await HarnessOperations.StartWrite(r1);
        var refused = await HarnessOperations.SaveInForeignTransaction(write);
        var seq = await write.AppendAsync([new ChangeEntry(LogReplay.Gadget)]);
        await write.CommitAsync();

        Assert.Contains("UseTransaction", refused.Message, StringComparison.Ordinal);
        Assert.Contains(run.Events.Actions("r1"), a => a == LogActions.WSaveRefuse);
        Assert.Equal(0, await LogReplay.ScalarAsync(run, "SELECT count(*) FROM harness_captured"));
        Assert.Equal(1, await LogReplay.ScalarAsync(run, $"SELECT count(*) FROM reeve_cluster.change_log WHERE seq = {seq} AND aggregate_type = '{LogReplay.Gadget}'"));
        Assert.Equal(1, await LogReplay.ScalarAsync(run, "SELECT count(*) FROM reeve_cluster.change_log"));
    }

    [Fact]
    public async Task Replay_M1_R2m5_a_restore_keeps_the_records_so_an_open_from_an_acknowledged_entry_is_still_refused()
    {
        await using var rig = await AckRaceRig.CreateAsync(stack, output.WriteLine);
        var acknowledged = await rig.WriteAsync();
        await rig.RecordAsync(acknowledged);
        Assert.Equal(acknowledged, await LogReplay.ScalarAsync(rig.Run, "SELECT acked_seq FROM reeve_cluster.change_ack"));

        await using var backup = await HarnessOperations.TakeBackup(rig.Run);
        NpgsqlConnection.ClearAllPools();
        var later = await rig.WriteAsync();
        Assert.True(later > acknowledged);

        await HarnessOperations.RestoreDatabase(rig.Run, backup);
        NpgsqlConnection.ClearAllPools();

        Assert.Equal(acknowledged, await LogReplay.ScalarAsync(rig.Run, "SELECT max(seq) FROM reeve_cluster.change_log"));
        Assert.Equal(acknowledged, await LogReplay.ScalarAsync(rig.Run, "SELECT acked_seq FROM reeve_cluster.change_ack"));
        Assert.Equal(new SkipOpenResult.Refused(acknowledged), await rig.OpenAsync(acknowledged));
        Assert.Equal(0, await rig.SkipCountAsync());
    }
}
