using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Log;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal interface IClaimRows
{
    Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken);

    Task<ClaimRead?> ReadAsync(string key, CancellationToken cancellationToken);

    Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken);
}

internal sealed class ClaimStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IClaimRows
{
    internal const string ValidRow = """
        c.expires_at > clock_timestamp()
          AND EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = c.holder_replica AND m.epoch = c.holder_epoch AND m.state = 'active'
              AND m.lease_expires_at > clock_timestamp())
        """;

    internal const string InsertSql = """
        INSERT INTO reeve_cluster.claim AS c (key, holder_replica, holder_epoch, token, expires_at)
        VALUES (@key, @replica, @epoch, gen_random_uuid(), clock_timestamp() + @ttl)
        ON CONFLICT (key) DO NOTHING
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ReadSql = $"""
        SELECT c.holder_replica, c.holder_epoch, c.token, {ValidRow}
        FROM reeve_cluster.claim c
        WHERE c.key = @key
        """;

    internal const string LockSql = """
        SELECT current_setting('transaction_timeout') FROM reeve_cluster.claim WHERE key = @key AND token = @expected FOR NO KEY UPDATE
        """;

    internal const string TakeoverSql = $"""
        UPDATE reeve_cluster.claim c
        SET holder_replica = @replica, holder_epoch = @epoch, token = gen_random_uuid(), expires_at = clock_timestamp() + @ttl
        WHERE c.key = @key AND c.token = @expected AND NOT ({ValidRow})
        RETURNING c.token
        """;

    internal const string ReleaseSql = """
        DELETE FROM reeve_cluster.claim
        WHERE key = @key AND token = @token
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

    public async Task<ClaimRead?> ReadAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new ClaimRead(reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.GetBoolean(3))
            : null;
    }

    public async Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var lockRow = new NpgsqlBatchCommand(LockSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("expected", expected) } };
        var locked = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), lockRow, static _ => true, cancellationToken).ConfigureAwait(false);
        if (locked is null)
        {
            return null;
        }

        var takeover = new NpgsqlCommand(TakeoverSql, connection, transaction);
        await using (takeover.ConfigureAwait(false))
        {
            takeover.Parameters.AddWithValue("key", key);
            takeover.Parameters.AddWithValue("expected", expected);
            takeover.Parameters.AddWithValue("replica", replica);
            takeover.Parameters.AddWithValue("epoch", epoch);
            takeover.Parameters.AddWithValue("ttl", ttl);
            if (await takeover.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not Guid token)
            {
                return null;
            }

            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
            return token;
        }
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
