using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal interface ICounterRows
{
    Task<long> IncrementAsync(string key, CancellationToken cancellationToken);
}

internal sealed class CounterStore(OwnedDataSource data, IOptions<ClusterOptions> options) : ICounterRows
{
    internal const string IncrementSql = """
        INSERT INTO reeve_cluster.counter AS t (key, value, updated_at) VALUES (@key, 1, clock_timestamp())
        ON CONFLICT (key) DO UPDATE SET value = t.value + 1, updated_at = clock_timestamp()
        RETURNING t.value, current_setting('transaction_timeout')
        """;

    public async Task<long> IncrementAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var increment = new NpgsqlBatchCommand(IncrementSql) { Parameters = { new NpgsqlParameter("key", key) } };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), increment, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false);
        var value = written?.Value ?? throw new InvalidOperationException("The increment returned no row.");
        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return value;
    }

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
