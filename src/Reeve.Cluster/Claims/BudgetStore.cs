using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal readonly record struct BudgetStatement(bool Inserted, int? Used);

internal interface IBudgetRows
{
    Task<BudgetStatement> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken);
}

internal sealed class BudgetStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IBudgetRows
{
    internal const string WindowEndSql = "to_timestamp((extract(epoch FROM fresh.window_start) + extract(epoch FROM @period))::double precision)";

    internal const string ConsumeSql = $"""
        WITH w AS (
            SELECT to_timestamp(
                floor(extract(epoch FROM clock_timestamp()) / extract(epoch FROM @period))::double precision * extract(epoch FROM @period)::double precision) AS start),
        fresh AS (
            INSERT INTO reeve_cluster.budget_id (key, window_start, id)
            SELECT @key, w.start, @id FROM w
            ON CONFLICT (key, window_start, id) DO NOTHING
            RETURNING window_start),
        counted AS (
            INSERT INTO reeve_cluster.budget AS b (key, window_start, used, window_end)
            SELECT @key, fresh.window_start, 1, {WindowEndSql} FROM fresh
            ON CONFLICT (key, window_start) DO UPDATE SET used = b.used + 1,
                window_end = GREATEST(b.window_end, excluded.window_end)
                WHERE b.used < @limit
            RETURNING b.used)
        SELECT EXISTS (SELECT 1 FROM fresh), (SELECT used FROM counted), current_setting('transaction_timeout')
        """;

    public async Task<BudgetStatement> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var consume = new NpgsqlBatchCommand(ConsumeSql)
        {
            Parameters =
            {
                new NpgsqlParameter("key", key), new NpgsqlParameter("id", id), new NpgsqlParameter("limit", limit), new NpgsqlParameter("period", period),
            },
        };
        var statement = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), consume,
            static r => new BudgetStatement(r.GetBoolean(0), r.IsDBNull(1) ? null : r.GetInt32(1)), cancellationToken).ConfigureAwait(false);
        var read = statement?.Value ?? throw new InvalidOperationException("The consume statement returned no row.");
        if (read is { Inserted: true, Used: not null })
        {
            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        }

        return read;
    }

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
