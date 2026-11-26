using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal interface IFlightRows
{
    Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken);

    Task<FlightRead?> ReadAsync(string key, CancellationToken cancellationToken);

    Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken);

    Task<bool> WriteResultAsync(string key, Guid token, long timeline, byte[] result, TimeSpan resultTtl, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken);
}

internal sealed class FlightStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IFlightRows
{
    internal const string InsertSql = """
        INSERT INTO reeve_cluster.claim AS c (key, holder_replica, holder_epoch, token, expires_at)
        VALUES (@key, @replica, @epoch, gen_random_uuid(), clock_timestamp() + @ttl)
        ON CONFLICT (key) DO NOTHING
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ReadSql = $"""
        SELECT c.holder_replica, c.holder_epoch, c.token, {ClaimStore.ValidRow},
               CASE WHEN c.result_expires_at > clock_timestamp() THEN c.result END
        FROM reeve_cluster.claim c
        WHERE c.key = @key
        """;

    internal const string TakeoverSql = $"""
        UPDATE reeve_cluster.claim c
        SET holder_replica = @replica, holder_epoch = @epoch, token = gen_random_uuid(), expires_at = clock_timestamp() + @ttl,
            result = NULL, result_expires_at = NULL
        WHERE c.key = @key AND c.token = @expected AND NOT ({ClaimStore.ValidRow})
          AND (c.result_expires_at IS NULL OR c.result_expires_at <= clock_timestamp())
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ResultSql = """
        UPDATE reeve_cluster.claim c
        SET result = @result, result_expires_at = clock_timestamp() + @resultTtl, expires_at = clock_timestamp()
        WHERE c.key = @key AND c.token = @token AND reeve_cluster.claim_is(@key, @token, @timeline)
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ReleaseSql = """
        DELETE FROM reeve_cluster.claim
        WHERE key = @key AND token = @token AND result IS NULL
        RETURNING token, current_setting('transaction_timeout')
        """;

    public async Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var insert = new NpgsqlBatchCommand(InsertSql)
        {
            Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("replica", replica), new NpgsqlParameter("epoch", epoch), new NpgsqlParameter("ttl", ttl) },
        };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), insert, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (written is not { } row)
        {
            return null;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return row.Value;
    }

    public async Task<FlightRead?> ReadAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new FlightRead(reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.GetBoolean(3), reader.IsDBNull(4) ? null : (byte[])reader.GetValue(4))
            : null;
    }

    public async Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var takeover = new NpgsqlBatchCommand(TakeoverSql)
        {
            Parameters =
            {
                new NpgsqlParameter("key", key), new NpgsqlParameter("expected", expected), new NpgsqlParameter("replica", replica),
                new NpgsqlParameter("epoch", epoch), new NpgsqlParameter("ttl", ttl),
            },
        };
        var taken = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), takeover, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (taken is not { } row)
        {
            return null;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return row.Value;
    }

    public async Task<bool> WriteResultAsync(string key, Guid token, long timeline, byte[] result, TimeSpan resultTtl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var write = new NpgsqlBatchCommand(ResultSql)
        {
            Parameters =
            {
                new NpgsqlParameter("key", key), new NpgsqlParameter("token", token), new NpgsqlParameter("timeline", timeline),
                new NpgsqlParameter("result", result), new NpgsqlParameter("resultTtl", resultTtl),
            },
        };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), write, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (written is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var release = new NpgsqlBatchCommand(ReleaseSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("token", token) } };
        var deleted = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), release, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (deleted is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
