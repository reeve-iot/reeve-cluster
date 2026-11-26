using Harness.Slice;
using Harness.Stall;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Harness.Replay.Log;

internal sealed record ReplicaPair(Replica R1, Replica R2);

internal static class LogReplay
{
    public static readonly TimeSpan Limit = StallRig.Limit;
    public const string Widget = StallRig.Widget;
    public const string Gadget = "gadget";

    public static async Task<ReplicaPair> StartPairAsync(StallRig rig, ReplicaSpec? spec = null)
    {
        spec ??= StallRig.Scripted();
        var r1 = await rig.StartAsync("r1", spec);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        var r2 = await rig.StartAsync("r2", spec);
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving, Limit);
        return new ReplicaPair(r1, r2);
    }

    public static void Poison(Replica replica) =>
        HarnessOperations.ReconcilerThrow(replica, Widget, () => new InvalidOperationException("a poison entry"), times: 10_000);

    public static async Task StuckAfterRejoinAsync(StallRig rig, Replica replica)
    {
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(replica.Id).Contains(MembershipActions.FenceStall)), $"{replica.Id} stall-fences", Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(replica.Id).Contains(MembershipActions.Rejoin)), $"{replica.Id} rejoins", Limit);
        await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Loading, Limit);
        await SliceRig.UntilAsync(
            () => Task.FromResult(replica.Reconcilers.Calls.Any(c => c.FullLoad && c.Outcome == "threw")), $"{replica.Id}'s reload fails", Limit);
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync(replica.Id))!.State);
    }

    public static ReconciledState WidgetState(Replica replica) =>
        replica.Services.GetRequiredService<GenerationStore>().Current!.Get<ReconciledState>(Widget);

    public static Task<SkipOpenResult> OpenAsync(Replica replica, long from, string type = Widget) =>
        replica.Services.GetRequiredService<IChangeSkip>().OpenAsync(new SkipRequest(type, from, "poison", "ops"));

    public static long Opened(SkipOpenResult result) => Assert.IsType<SkipOpenResult.Opened>(result).SkipId;

    public static async Task<long> WriteAsync(Replica replica, IReadOnlyCollection<ChangeEntry> entries, bool optOut = false)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        await using var connection = new NpgsqlConnection(replica.Options.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = new NpgsqlCommand("INSERT INTO stall_item (value) VALUES ('replay')", connection, transaction))
        {
            await command.ExecuteNonQueryAsync();
        }

        var seq = await replica.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction, entries);
        if (optOut)
        {
            replica.Services.GetRequiredService<IChangeAck>().AcknowledgeAtCommit();
        }

        await transaction.CommitAsync();
        return seq;
    }

    public static async Task RewindLogAsync(HarnessRun run, long head)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var batch = new NpgsqlBatch(connection)
        {
            BatchCommands =
            {
                new NpgsqlBatchCommand("DELETE FROM reeve_cluster.change_log WHERE seq > $1") { Parameters = { new NpgsqlParameter<long> { TypedValue = head } } },
                new NpgsqlBatchCommand("SELECT setval('reeve_cluster.change_log_seq', $1)") { Parameters = { new NpgsqlParameter<long> { TypedValue = head } } },
            },
        };
        await batch.ExecuteNonQueryAsync();
    }

    public const string CreateCapturedTable = "CREATE TABLE IF NOT EXISTS harness_captured (id int PRIMARY KEY, label text NOT NULL)";

    public static ReplicaSpec CapturingSpec() => new()
    {
        Reconcilers = [typeof(ScriptedWidget)],
        Redis = true,
        ConfigureServices = HarnessCapture.Register,
    };

    public static async Task<(long Appended, long Captured)> AppendThenCaptureAsync(Replica replica, string appendType)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        await using var connection = new NpgsqlConnection(replica.Options.ConnectionString);
        await connection.OpenAsync();
        await using var db = HarnessCapture.Context(replica, connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        var appended = await replica.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction.GetDbTransaction(), [new ChangeEntry(appendType)]);
        db.Add(new HarnessCaptured { Id = 1, Label = "captured" });
        await db.SaveChangesAsync();
        await transaction.CommitAsync();
        await using var highest = new NpgsqlCommand("SELECT max(seq) FROM reeve_cluster.change_log", connection);
        return (appended, Convert.ToInt64(await highest.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture));
    }

    public static async Task<long> ScalarAsync(HarnessRun run, string sql)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    public static async Task ExecuteAsync(HarnessRun run, string sql)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
