using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Log;

internal sealed class ChangeSkip : IChangeSkip
{
    private const string OpenLockSql = """
        INSERT INTO reeve_cluster.change_ack (id, acked_seq) VALUES (true, 0)
        ON CONFLICT (id) DO UPDATE SET acked_seq = change_ack.acked_seq
        RETURNING acked_seq, current_setting('transaction_timeout')
        """;

    private const string InsertSql = """
        INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, reason, opened_by)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """;

    private const string LockRowSql = """
        SELECT aggregate_type, until_seq, current_setting('transaction_timeout')
        FROM reeve_cluster.change_skip WHERE id = $1 FOR UPDATE
        """;

    private const string CloseColumnsSql = "UPDATE reeve_cluster.change_skip SET closed_by = $2, closed_at = clock_timestamp() WHERE id = $1";

    private const string UntilSql = "UPDATE reeve_cluster.change_skip SET until_seq = $2 WHERE id = $1";

    private readonly AckDataSource dataSource;
    private readonly IOptions<ClusterOptions> options;
    private readonly ChangeLog log;
    private readonly PostCommitWakeup wakeup;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;

    public ChangeSkip(
        AckDataSource dataSource, IOptions<ClusterOptions> options, ChangeLog log, PostCommitWakeup wakeup, IFaultPoints faultPoints, IModelEventSink events)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(wakeup);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);

        this.dataSource = dataSource;
        this.options = options;
        this.log = log;
        this.wakeup = wakeup;
        this.faultPoints = faultPoints;
        this.events = events;
    }

    public async Task<SkipOpenResult> OpenAsync(SkipRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.AggregateType, nameof(request));
        ArgumentNullException.ThrowIfNull(request.Reason, nameof(request));
        ArgumentNullException.ThrowIfNull(request.OpenedBy, nameof(request));

        var connection = await dataSource.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                await faultPoints.ReachAsync(LogFaultPoints.OpenBeforeUpsert, cancellationToken).ConfigureAwait(false);

                var (ackedSeq, setting) = await TransactionTimeout.FirstAsync(
                    connection, transaction, BoundMilliseconds(), new NpgsqlBatchCommand(OpenLockSql), static r => r.GetInt64(0), cancellationToken)
                    .ConfigureAwait(false) ?? throw new InvalidOperationException("The open's upsert returned no row.");

                events.Emit(new ModelEvent(LogActions.OpenLock, new Dictionary<string, object?>
                {
                    [ObservationKeys.Type] = request.AggregateType,
                    [ObservationKeys.FromSeq] = request.FromSeq,
                    [ObservationKeys.AckedSeq] = ackedSeq,
                    [ObservationKeys.TransactionTimeout] = setting,
                }));
                await faultPoints.ReachAsync(LogFaultPoints.OpenAfterUpsert, cancellationToken).ConfigureAwait(false);

                if (request.FromSeq <= ackedSeq)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    events.Emit(new ModelEvent(LogActions.OpenRefuse, new Dictionary<string, object?>
                    {
                        [ObservationKeys.Type] = request.AggregateType,
                        [ObservationKeys.FromSeq] = request.FromSeq,
                        [ObservationKeys.AckedSeq] = ackedSeq,
                    }));
                    return new SkipOpenResult.Refused(ackedSeq);
                }

                long skipId;
                var insert = new NpgsqlCommand(InsertSql, connection, transaction)
                {
                    Parameters =
                    {
                        new NpgsqlParameter<string> { TypedValue = request.AggregateType },
                        new NpgsqlParameter<long> { TypedValue = request.FromSeq },
                        new NpgsqlParameter<string> { TypedValue = request.Reason },
                        new NpgsqlParameter<string> { TypedValue = request.OpenedBy },
                    },
                };
                await using (insert.ConfigureAwait(false))
                {
                    skipId = (long)(await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
                }

                await faultPoints.ReachAsync(LogFaultPoints.OpenAfterInsert, cancellationToken).ConfigureAwait(false);

                await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
                events.Emit(new ModelEvent(LogActions.OpenCommit, new Dictionary<string, object?>
                {
                    [ObservationKeys.SkipId] = skipId,
                    [ObservationKeys.Type] = request.AggregateType,
                    [ObservationKeys.FromSeq] = request.FromSeq,
                    [ObservationKeys.AckedSeq] = ackedSeq,
                }));
                return new SkipOpenResult.Opened(skipId);
            }
        }
    }

    public async Task<long> CloseAsync(long skipId, string closedBy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(closedBy);

        var connection = await dataSource.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var (type, untilSeq) = await LockRowAsync(connection, transaction, skipId, cancellationToken).ConfigureAwait(false);
                if (untilSeq is { } closedAt)
                {
                    await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
                    return closedAt;
                }

                await ExecuteAsync(connection, transaction, CloseColumnsSql, skipId, closedBy, cancellationToken).ConfigureAwait(false);

                await faultPoints.ReachAsync(LogFaultPoints.CloseBeforeRepairAppend, cancellationToken).ConfigureAwait(false);
                var repair = await AppendRepairAsync(transaction, type, cancellationToken).ConfigureAwait(false);
                var repairSeq = repair.HighestSeq;
                await faultPoints.ReachAsync(LogFaultPoints.CloseAfterRepairAppend, cancellationToken).ConfigureAwait(false);

                await ExecuteAsync(connection, transaction, UntilSql, skipId, repairSeq, cancellationToken).ConfigureAwait(false);

                await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
                events.Emit(new ModelEvent(LogActions.SkipClose, new Dictionary<string, object?>
                {
                    [ObservationKeys.SkipId] = skipId,
                    [ObservationKeys.Type] = type,
                    [ObservationKeys.RepairSeq] = repairSeq,
                }));
                await wakeup.AfterCommitAsync(repair.HighestSeq, repair.Xid, CancellationToken.None).ConfigureAwait(false);
                return repairSeq;
            }
        }
    }

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");

    private async Task<(string Type, long? UntilSeq)> LockRowAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, long skipId, CancellationToken cancellationToken)
    {
        var lockRow = new NpgsqlBatchCommand(LockRowSql) { Parameters = { new NpgsqlParameter<long> { TypedValue = skipId } } };
        var found = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), lockRow,
            static r => (r.GetString(0), r.IsDBNull(1) ? (long?)null : r.GetInt64(1)), cancellationToken).ConfigureAwait(false);
        return found?.Value ?? throw new KeyNotFoundException($"No skip has the id {skipId}.");
    }

    private Task<AppendResult> AppendRepairAsync(NpgsqlTransaction transaction, string type, CancellationToken cancellationToken) =>
        log.AppendInCurrentScopeAsync(transaction, [new ChangeEntry(type)], cancellationToken);

    private static async Task ExecuteAsync<T>(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, long skipId, T value, CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand(sql, connection, transaction)
        {
            Parameters = { new NpgsqlParameter<long> { TypedValue = skipId }, new NpgsqlParameter<T> { TypedValue = value } },
        };
        await using (command.ConfigureAwait(false))
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
