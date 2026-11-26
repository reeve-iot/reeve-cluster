using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;

namespace Harness.AckRace;

internal sealed class AckRaceRig : IAsyncDisposable
{
    public const string WriteType = "widget";

    public const string SkipType = "gadget";

    private AckRaceRig(HarnessRun run, Replica writer, Replica operatorProcess)
    {
        Run = run;
        Writer = writer;
        Operator = operatorProcess;
    }

    public HarnessRun Run { get; }

    public Replica Writer { get; }

    public Replica Operator { get; }

    public AckRecorder Recorder => Writer.Services.GetRequiredService<AckRecorder>();

    public IChangeSkip Skip => Operator.Services.GetRequiredService<IChangeSkip>();

    public static async Task<AckRaceRig> CreateAsync(StackFixture stack, Action<string> log)
    {
        var run = await HarnessRun.BeginAsync(log, stack.Stack);
        try
        {
            await run.MigrateAsync();
            var writer = await run.PrepareAsync("r1", Spec("r1"));
            var operatorProcess = await run.PrepareAsync("op", Spec("op"));
            return new AckRaceRig(run, writer, operatorProcess);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public async Task<long> WriteAsync()
    {
        await using var write = await HarnessOperations.StartWrite(Writer);
        var seq = await write.AppendAsync([new ChangeEntry(WriteType)]);
        await write.CommitAsync();
        return seq;
    }

    public Task<IReadOnlyList<string>> RecordAsync(long seq) =>
        Recorder.RecordAsync(seq, seq, Writer.Clock.GetTimestamp() + (30 * Writer.Clock.TimestampFrequency));

    public Task<SkipOpenResult> OpenAsync(long fromSeq) =>
        Skip.OpenAsync(new SkipRequest(SkipType, fromSeq, "poison entry", "operator"));

    public async Task SettleAsync(Task task, HoldRule? hold, Replica process)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1 AND wait_event_type = 'Lock'",
            connection)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = process.Id } },
        };

        while (!task.IsCompleted && (hold is null || hold.HeldNow == 0) && (long)(await command.ExecuteScalarAsync())! == 0)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"{process.Id} neither ended, nor reached its point, nor waited for a lock.");
            }

            await Task.WhenAny(task, Task.Delay(5));
        }
    }

    public IReadOnlyList<string> Order(params string[] actions) =>
        [.. Run.Events.Snapshot().Where(e => actions.Contains(e.Action)).OrderBy(e => e.Sequence).Select(e => e.Action)];

    public async Task<long> SkipCountAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM reeve_cluster.change_skip", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public ValueTask DisposeAsync() => Run.DisposeAsync();

    private static ReplicaSpec Spec(string id) => new()
    {
        Configure = o => o.ConnectionString = new NpgsqlConnectionStringBuilder(o.ConnectionString) { ApplicationName = id }.ConnectionString,
    };
}
