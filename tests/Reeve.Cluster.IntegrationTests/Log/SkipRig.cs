using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Log;

internal sealed class SkipRig : IAsyncDisposable
{
    private SkipRig(LogRig log, HarnessFaultPoints faults, ClusterOptions options, TimeProvider time)
    {
        Log = log;
        Faults = faults;
        Time = time;
        DataSource = new AckDataSource(Options.Create(options));
        Recorder = new AckRecorder(DataSource, time, faults, log.Events);
        Skip = new ChangeSkip(
            DataSource, Options.Create(options), log.Log, new PostCommitWakeup(NoWakeupChannel.Instance, faults, log.Events), faults, log.Events);
    }

    public LogRig Log { get; }

    public HarnessFaultPoints Faults { get; }

    public AckDataSource DataSource { get; }

    public TimeProvider Time { get; }

    public AckRecorder Recorder { get; }

    public ChangeSkip Skip { get; }

    public RecordedEvents Events => Log.Events;

    public string ConnectionString => Log.ConnectionString;

    public static async Task<SkipRig> CreateAsync(
        PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null, Func<string, string>? connectionString = null)
    {
        var faults = new HarnessFaultPoints("r1", TimeProvider.System, new ArrivalLog(new RunSequence()));
        var log = await LogRig.CreateAsync(postgres, faults);
        var options = new ClusterOptions
        {
            ConnectionString = connectionString?.Invoke(log.ConnectionString) ?? log.ConnectionString,
            ReplicaId = "r1",
            FencedTransactionTimeout = fencedTransactionTimeout ?? TimeSpan.FromSeconds(30),
        };
        return new SkipRig(log, faults, options, time ?? TimeProvider.System);
    }

    public async Task<long> WriteAsync(string type)
    {
        await using var connection = await Log.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var seq = await Log.Log.AppendAsync(transaction, [new ChangeEntry(type)]);
        await transaction.CommitAsync();
        return seq;
    }

    public Task<IReadOnlyList<string>> RecordAsync(long seq, long bound, TimeSpan? remaining = null, CancellationToken cancellationToken = default) =>
        Recorder.RecordAsync(seq, bound, Deadline(remaining ?? TimeSpan.FromSeconds(30)), cancellationToken);

    public long Deadline(TimeSpan remaining) =>
        Time.GetTimestamp() + (long)(remaining.Ticks * (decimal)Time.TimestampFrequency / TimeSpan.TicksPerSecond);

    public Task<SkipOpenResult> OpenAsync(string type, long fromSeq) =>
        Skip.OpenAsync(new SkipRequest(type, fromSeq, "test", "operator"));

    public Task<long> AckedSeqAsync() => Log.ScalarAsync<long>("SELECT acked_seq FROM reeve_cluster.change_ack");

    public Task<long> SkipCountAsync() => Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_skip");

    public Task<long> LogCountAsync() => Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log");

    public IReadOnlyList<ModelEvent> EventsOf(string action) => [.. Events.All.Where(e => e.Action == action)];

    public Task<bool> AckRowFreeAsync() => RowFreeAsync("SELECT 1 FROM reeve_cluster.change_ack FOR UPDATE NOWAIT");

    public Task<bool> SkipRowFreeAsync(long id) => RowFreeAsync($"SELECT 1 FROM reeve_cluster.change_skip WHERE id = {id} FOR UPDATE NOWAIT");

    public async Task<bool> ChangeLogLockFreeAsync()
    {
        await using var connection = await Log.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand($"SELECT pg_try_advisory_xact_lock({ChangeLogLock.Key})", connection, transaction);
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    public async Task WaitUntilASessionWaitsForALockAsync(TimeSpan? timeout = null)
    {
        var until = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", connection);
        while ((long)(await command.ExecuteScalarAsync())! == 0)
        {
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException("No session of the database waited for a lock.");
            }

            await Task.Delay(10);
        }
    }

    public async Task WaitUntilNoSessionIsIdleInATransactionAsync(TimeSpan? timeout = null)
    {
        var until = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%' AND pid <> pg_backend_pid()", connection);
        while ((long)(await command.ExecuteScalarAsync())! > 0)
        {
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException("A session of the database stayed idle in a transaction.");
            }

            await Task.Delay(10);
        }
    }

    public ValueTask DisposeAsync() => DataSource.DisposeAsync();

    private async Task<bool> RowFreeAsync(string sql)
    {
        await using var connection = await Log.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        try
        {
            await command.ExecuteScalarAsync();
            return true;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            return false;
        }
    }
}
