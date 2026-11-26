using System.Data;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Log;

internal static class TransactionTimeout
{
    public static NpgsqlBatchCommand Statement(int milliseconds)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(milliseconds, 1);
        return new NpgsqlBatchCommand("SET LOCAL transaction_timeout = " + milliseconds.ToString(CultureInfo.InvariantCulture));
    }

    public static async Task<(T Value, string Timeout)?> FirstAsync<T>(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int milliseconds, NpgsqlBatchCommand first,
        Func<NpgsqlDataReader, T> read, CancellationToken cancellationToken)
    {
        var batch = new NpgsqlBatch(connection, transaction);
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(Statement(milliseconds));
            batch.BatchCommands.Add(first);
            var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return null;
                }

                return (read(reader), reader.GetString(reader.FieldCount - 1));
            }
        }
    }

    public static async Task CommitAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var connection = transaction.Connection;
        var timeout = connection?.CommandTimeout ?? 0;
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        try
        {
            try
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (FencedTransactions.IsUnknown(e, cancellationToken, started, timeout, connection))
            {
                outcome = "unknown";
                throw;
            }

            if (FencedTransactions.IsUnknown(null, cancellationToken, started, timeout, connection))
            {
                outcome = "unknown";
                cancellationToken.ThrowIfCancellationRequested();
                throw new OperationCanceledException("The commit outcome is unknown.");
            }
            outcome = "committed";
        }
        finally
        {
            ClusterDiagnostics.RecordCommitDuration(Stopwatch.GetElapsedTime(started).TotalSeconds, "library", outcome);
        }
    }

    internal static async Task CommitAsync(Func<CancellationToken, Task> commit, CancellationToken cancellationToken, NpgsqlConnection? connection = null)
    {
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        try
        {
            await commit(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            outcome = "committed";
        }
        catch (OperationCanceledException)
        {
            outcome = "unknown";
            throw;
        }
        catch (Exception e)
        {
            outcome = FencedTransactions.IsUnknown(e, cancellationToken, started, connection?.CommandTimeout ?? 0, connection) ? "unknown" : "failed";
            throw;
        }
        finally
        {
            ClusterDiagnostics.RecordCommitDuration(Stopwatch.GetElapsedTime(started).TotalSeconds, "library", outcome);
        }
    }
}

internal sealed class AckDataSource : IAsyncDisposable
{
    public AckDataSource(Microsoft.Extensions.Options.IOptions<ClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Source = new ClusterDataSource(options.Value, static builder => builder.CommandTimeout = 0);
    }

    public ClusterDataSource Source { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed class AckRecorder
{
    private const string UpsertSql = """
        INSERT INTO reeve_cluster.change_ack (id, acked_seq) VALUES (true, $1)
        ON CONFLICT (id) DO UPDATE SET acked_seq = greatest(change_ack.acked_seq, $1)
        RETURNING acked_seq, current_setting('transaction_timeout')
        """;

    private const string SkipReadSql = """
        SELECT aggregate_type, from_seq, until_seq FROM reeve_cluster.change_skip WHERE from_seq <= $1
        """;

    private readonly AckDataSource dataSource;
    private readonly TimeProvider time;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;

    public AckRecorder(AckDataSource dataSource, TimeProvider time, IFaultPoints faultPoints, IModelEventSink events)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);

        this.dataSource = dataSource;
        this.time = time;
        this.faultPoints = faultPoints;
        this.events = events;
    }

    public async Task<IReadOnlyList<string>> RecordAsync(long seq, long bound, long deadline, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(seq, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(bound, seq);

        var connection = await dataSource.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                await faultPoints.ReachAsync(LogFaultPoints.RecordBeforeUpsert, cancellationToken).ConfigureAwait(false);

                if (RecordTimeout.FromRemaining(time.GetElapsedTime(time.GetTimestamp(), deadline)).Milliseconds is not { } milliseconds)
                {
                    throw new TimeoutException("Nothing remains of AckTimeout: the wait answers Unconfirmed(Timeout), and the record wrote nothing.");
                }

                var upsert = new NpgsqlBatchCommand(UpsertSql) { Parameters = { new NpgsqlParameter<long> { TypedValue = bound } } };
                var (ackedSeq, setting) = await TransactionTimeout.FirstAsync(
                    connection, transaction, milliseconds, upsert, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("The record's upsert returned no row.");

                events.Emit(new ModelEvent(LogActions.AckRecord, new Dictionary<string, object?>
                {
                    [ObservationKeys.AwaitedSeq] = seq,
                    [ObservationKeys.Bound] = bound,
                    [ObservationKeys.AckedSeq] = ackedSeq,
                    [ObservationKeys.TransactionTimeout] = setting,
                }));
                await faultPoints.ReachAsync(LogFaultPoints.RecordBetweenUpsertAndSkipRead, cancellationToken).ConfigureAwait(false);

                var ranges = new List<SkipRange>();
                var read = new NpgsqlCommand(SkipReadSql, connection, transaction) { Parameters = { new NpgsqlParameter<long> { TypedValue = bound } } };
                await using (read.ConfigureAwait(false))
                {
                    var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            ranges.Add(new SkipRange(reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetInt64(2)));
                        }
                    }
                }

                var skipped = SkipRead.NamedTypes(ranges, seq, bound);
                events.Emit(new ModelEvent(LogActions.AckSkipRead, new Dictionary<string, object?>
                {
                    [ObservationKeys.AwaitedSeq] = seq,
                    [ObservationKeys.Bound] = bound,
                    [ObservationKeys.Skipped] = skipped,
                }));

                await faultPoints.ReachAsync(LogFaultPoints.RecordHold, cancellationToken).ConfigureAwait(false);
                await faultPoints.ReachAsync(LogFaultPoints.RecordBeforeCommit, cancellationToken).ConfigureAwait(false);
                await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
                return skipped;
            }
        }
    }
}
