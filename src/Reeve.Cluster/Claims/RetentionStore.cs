using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal readonly record struct BudgetRetained(int Windows, int Ids);

internal interface IRetentionRows
{
    Task<int> DeleteClaimsAsync(int limit, CancellationToken cancellationToken);

    Task<int> DeleteOnceAsync(int limit, CancellationToken cancellationToken);

    Task<int> DeleteFlightsAsync(int limit, CancellationToken cancellationToken);

    Task<BudgetRetained> DeleteBudgetsAsync(int limit, CancellationToken cancellationToken);

    Task<int> DeleteCountersAsync(int limit, CancellationToken cancellationToken);

    Task<int> DeleteSlotsAsync(int limit, CancellationToken cancellationToken);
}

internal sealed class RetentionStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IRetentionRows
{
    internal const string PlainClaim = "c.once_mode IS NULL AND c.key NOT LIKE 'reeve.flight:%'";

    internal const string OnceRecord = "c.once_mode IS NOT NULL";

    internal const string FlightRow = "c.once_mode IS NULL AND c.key LIKE 'reeve.flight:%'";

    internal const string ClaimsSql = $"""
        DELETE FROM reeve_cluster.claim c
        WHERE {PlainClaim} AND c.expires_at < now() - @retention
          AND c.key IN (
            SELECT c.key FROM reeve_cluster.claim c
            WHERE {PlainClaim} AND c.expires_at < now() - @retention
            ORDER BY c.expires_at
            LIMIT @limit)
        RETURNING c.key, current_setting('transaction_timeout')
        """;

    internal const string OnceSql = $"""
        DELETE FROM reeve_cluster.claim c
        WHERE {OnceRecord} AND c.expires_at < now() - @retention
          AND NOT (c.once_mode = 'at_least_once' AND c.completed_at IS NULL)
          AND c.key IN (
            SELECT c.key FROM reeve_cluster.claim c
            WHERE {OnceRecord} AND c.expires_at < now() - @retention
              AND NOT (c.once_mode = 'at_least_once' AND c.completed_at IS NULL)
            ORDER BY c.expires_at
            LIMIT @limit)
        RETURNING c.key, current_setting('transaction_timeout')
        """;

    internal const string FlightsSql = $"""
        DELETE FROM reeve_cluster.claim c
        WHERE {FlightRow} AND c.expires_at < now() - @retention AND (c.result_expires_at IS NULL OR c.result_expires_at < now() - @retention)
          AND c.key IN (
            SELECT c.key FROM reeve_cluster.claim c
            WHERE {FlightRow} AND c.expires_at < now() - @retention AND (c.result_expires_at IS NULL OR c.result_expires_at < now() - @retention)
            ORDER BY c.expires_at
            LIMIT @limit)
        RETURNING c.key, current_setting('transaction_timeout')
        """;

    internal const string BudgetIdsSql = """
        DELETE FROM reeve_cluster.budget_id i
        USING (
            SELECT d.key, d.window_start, d.id
            FROM reeve_cluster.budget_id d
            WHERE (d.key, d.window_start) IN (
                SELECT b.key, b.window_start FROM reeve_cluster.budget b
                WHERE b.window_end < now() - @retention
                ORDER BY b.window_end
                LIMIT @limit)
            LIMIT @idLimit) x
        WHERE i.key = x.key AND i.window_start = x.window_start AND i.id = x.id
        RETURNING i.id
        """;

    internal const string BudgetWindowsSql = """
        DELETE FROM reeve_cluster.budget b
        WHERE b.window_end < now() - @retention
          AND (b.key, b.window_start) IN (
            SELECT b2.key, b2.window_start FROM reeve_cluster.budget b2
            WHERE b2.window_end < now() - @retention
            ORDER BY b2.window_end
            LIMIT @limit)
          AND NOT EXISTS (SELECT 1 FROM reeve_cluster.budget_id i WHERE i.key = b.key AND i.window_start = b.window_start)
        RETURNING b.key
        """;

    internal const string CountersSql = """
        DELETE FROM reeve_cluster.counter t
        WHERE t.updated_at < now() - @retention
          AND t.key IN (
            SELECT t2.key FROM reeve_cluster.counter t2
            WHERE t2.updated_at < now() - @retention
            ORDER BY t2.updated_at
            LIMIT @limit)
        RETURNING t.key
        """;

    internal const string SlotsSql = """
        DELETE FROM reeve_cluster.slot s
        WHERE s.expires_at < now() - @retention
          AND (s.key, s.id) IN (
            SELECT s2.key, s2.id FROM reeve_cluster.slot s2
            WHERE s2.expires_at < now() - @retention
            ORDER BY s2.expires_at
            LIMIT @limit)
        RETURNING s.key
        """;

    internal const int IdsPerWindow = 10;

    public async Task<int> DeleteClaimsAsync(int limit, CancellationToken cancellationToken) =>
        (await RunAsync([Statement(ClaimsSql, options.Value.OwnedRowRetention, limit)], cancellationToken).ConfigureAwait(false))[0];

    public async Task<int> DeleteOnceAsync(int limit, CancellationToken cancellationToken) =>
        options.Value.OnceRetention is not { } retention
            ? 0
            : (await RunAsync([Statement(OnceSql, retention, limit)], cancellationToken).ConfigureAwait(false))[0];

    public async Task<int> DeleteFlightsAsync(int limit, CancellationToken cancellationToken) =>
        (await RunAsync([Statement(FlightsSql, options.Value.OwnedRowRetention, limit)], cancellationToken).ConfigureAwait(false))[0];

    public async Task<BudgetRetained> DeleteBudgetsAsync(int limit, CancellationToken cancellationToken)
    {
        var ids = Statement(BudgetIdsSql, options.Value.OwnedRowRetention, limit);
        ids.Parameters.Add(new NpgsqlParameter("idLimit", limit * IdsPerWindow));
        var counts = await RunAsync([ids, Statement(BudgetWindowsSql, options.Value.OwnedRowRetention, limit)], cancellationToken).ConfigureAwait(false);
        return new BudgetRetained(counts[1], counts[0]);
    }

    public async Task<int> DeleteCountersAsync(int limit, CancellationToken cancellationToken) =>
        options.Value.CounterRetention is not { } retention
            ? 0
            : (await RunAsync([Statement(CountersSql, retention, limit)], cancellationToken).ConfigureAwait(false))[0];

    public async Task<int> DeleteSlotsAsync(int limit, CancellationToken cancellationToken) =>
        (await RunAsync([Statement(SlotsSql, options.Value.OwnedRowRetention, limit)], cancellationToken).ConfigureAwait(false))[0];

    private static NpgsqlBatchCommand Statement(string sql, TimeSpan retention, int limit) =>
        new(sql) { Parameters = { new NpgsqlParameter("retention", retention), new NpgsqlParameter("limit", limit) } };

    private async Task<int[]> RunAsync(NpgsqlBatchCommand[] statements, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var counts = new int[statements.Length];
        var batch = new NpgsqlBatch(connection, transaction);
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(TransactionTimeout.Statement(BoundMilliseconds()));
            foreach (var statement in statements)
            {
                batch.BatchCommands.Add(statement);
            }

            var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                var result = 0;
                do
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        counts[result]++;
                    }

                    result++;
                }
                while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return counts;
    }

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
