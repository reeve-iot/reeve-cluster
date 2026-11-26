using System.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Log;

internal readonly record struct RetentionRun(long? Minimum, long Deleted);

internal sealed partial class Retention(
    ClusterDataSource dataSource, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints faultPoints, IModelEventSink events,
    ILogger<Retention> logger)
    : BackgroundService
{
    internal const string MinimumSql = """
        SELECT min(least(applied_seq, (SELECT max(seq) FROM reeve_cluster.change_log))), current_setting('transaction_timeout')
        FROM reeve_cluster.member
        WHERE state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()
        """;

    internal const string DeleteSql = "DELETE FROM reeve_cluster.change_log WHERE seq < $1";

    internal const string HorizonSql = "UPDATE reeve_cluster.cluster SET horizon_seq = greatest(horizon_seq, $1)";

    private readonly TimeSpan interval = options.Value.RetentionInterval;

    internal async Task<RetentionRun> RunAsync(CancellationToken cancellationToken = default)
    {
        var connection = await dataSource.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                var milliseconds = (int)Math.Ceiling(Math.Min(interval.TotalMilliseconds, int.MaxValue));
                var (minimum, _) = await TransactionTimeout.FirstAsync(
                    connection, transaction, milliseconds, new NpgsqlBatchCommand(MinimumSql),
                    static r => r.IsDBNull(0) ? (long?)null : r.GetInt64(0), cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("The minimum's aggregate returned no row.");
                if (minimum is not { } bound)
                {
                    return new RetentionRun(null, 0);
                }

                await faultPoints.ReachAsync(LogFaultPoints.RetentionBetweenMinimumAndDelete, cancellationToken).ConfigureAwait(false);
                long deleted;
                var delete = new NpgsqlCommand(DeleteSql, connection, transaction) { Parameters = { new NpgsqlParameter<long> { TypedValue = bound } } };
                await using (delete.ConfigureAwait(false))
                {
                    deleted = await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                if (deleted == 0)
                {
                    return new RetentionRun(bound, 0);
                }

                await faultPoints.ReachAsync(LogFaultPoints.RetentionBeforeHorizonUpdate, cancellationToken).ConfigureAwait(false);
                var horizon = new NpgsqlCommand(HorizonSql, connection, transaction) { Parameters = { new NpgsqlParameter<long> { TypedValue = bound } } };
                await using (horizon.ConfigureAwait(false))
                {
                    await horizon.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                events.Emit(new ModelEvent(LogActions.Retention, new Dictionary<string, object?>
                {
                    [ObservationKeys.Horizon] = bound,
                    [ObservationKeys.Deleted] = deleted,
                }));
                LogDeleted(logger, deleted, bound);
                return new RetentionRun(bound, deleted);
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            using var timer = new PeriodicTimer(interval, time);
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await RunAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
                {
                    LogFailed(logger, ex);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Retention deleted {Deleted} change-log entries below seq {Horizon}, the new horizon.")]
    private static partial void LogDeleted(ILogger logger, long deleted, long horizon);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Retention failed; the next run starts afresh.")]
    private static partial void LogFailed(ILogger logger, Exception exception);
}
