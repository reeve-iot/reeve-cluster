using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Slots;

internal interface ISlotRows
{
    Task<Guid?> AcquireAsync(
        string key, string id, string replica, long epoch, long timeline, int limit, TimeSpan ttl, Func<CancellationToken, ValueTask> afterDelete,
        CancellationToken cancellationToken);

    Task<bool> RenewAsync(string key, string id, Guid token, long timeline, TimeSpan ttl, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken);

    Task<IReadOnlyList<SlotHolder>> ReadHoldersAsync(string key, CancellationToken cancellationToken);
}

internal sealed class SlotStore(OwnedDataSource data, IOptions<ClusterOptions> options) : ISlotRows
{
    internal const string ValidRow = """
        s.expires_at > clock_timestamp()
          AND EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = s.holder_replica AND m.epoch = s.holder_epoch AND m.state IN ('active', 'draining'))
        """;

    internal const string LockSql = "SELECT reeve_cluster.slot_lock(@key, @timeline), current_setting('transaction_timeout')";

    internal const string ReacquireSql = $"""
        UPDATE reeve_cluster.slot s
        SET expires_at = clock_timestamp() + @ttl
        WHERE s.key = @key AND s.id = @id AND ({ValidRow})
        RETURNING s.token
        """;

    internal const string DeleteInvalidSql = $"""
        DELETE FROM reeve_cluster.slot s
        WHERE s.key = @key AND NOT ({ValidRow})
        """;

    internal const string InsertSql = """
        INSERT INTO reeve_cluster.slot (key, id, holder_replica, holder_epoch, token, expires_at)
        SELECT @key, @id, @replica, @epoch, gen_random_uuid(), clock_timestamp() + @ttl
        WHERE (SELECT count(*) FROM reeve_cluster.slot s WHERE s.key = @key) < @limit
        ON CONFLICT (key, id) DO NOTHING
        RETURNING token
        """;

    internal const string RenewSql = $"""
        UPDATE reeve_cluster.slot s
        SET expires_at = clock_timestamp() + @ttl
        WHERE s.key = @key AND s.id = @id AND reeve_cluster.slot_is(@key, @id, @token, @timeline)
          AND s.token = @token AND ({ValidRow})
        RETURNING s.token, current_setting('transaction_timeout')
        """;

    internal const string ReleaseSql = """
        DELETE FROM reeve_cluster.slot
        WHERE key = @key AND id = @id AND token = @token
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string ReleaseAnySql = """
        DELETE FROM reeve_cluster.slot
        WHERE key = @key AND id = @id
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string HoldersSql = $"""
        SELECT s.id, s.holder_replica, s.holder_epoch
        FROM reeve_cluster.slot s
        WHERE s.key = @key AND ({ValidRow})
        ORDER BY s.id COLLATE "C"
        """;

    public async Task<Guid?> AcquireAsync(
        string key, string id, string replica, long epoch, long timeline, int limit, TimeSpan ttl, Func<CancellationToken, ValueTask> afterDelete,
        CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);

        var lockKey = new NpgsqlBatchCommand(LockSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("timeline", timeline) } };
        await TransactionTimeout.FirstAsync(connection, transaction, BoundMilliseconds(), lockKey, static r => r.GetBoolean(0), cancellationToken).ConfigureAwait(false);

        var reacquire = new NpgsqlCommand(ReacquireSql, connection, transaction);
        await using (reacquire.ConfigureAwait(false))
        {
            reacquire.Parameters.AddWithValue("key", key);
            reacquire.Parameters.AddWithValue("id", id);
            reacquire.Parameters.AddWithValue("ttl", ttl);
            if (await reacquire.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is Guid existing)
            {
                await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
                return existing;
            }
        }

        var delete = new NpgsqlCommand(DeleteInvalidSql, connection, transaction);
        await using (delete.ConfigureAwait(false))
        {
            delete.Parameters.AddWithValue("key", key);
            await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await afterDelete(cancellationToken).ConfigureAwait(false);

        var insert = new NpgsqlCommand(InsertSql, connection, transaction);
        await using (insert.ConfigureAwait(false))
        {
            insert.Parameters.AddWithValue("key", key);
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("replica", replica);
            insert.Parameters.AddWithValue("epoch", epoch);
            insert.Parameters.AddWithValue("ttl", ttl);
            insert.Parameters.AddWithValue("limit", limit);
            var granted = await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as Guid?;
            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
            return granted;
        }
    }

    public async Task<bool> RenewAsync(string key, string id, Guid token, long timeline, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var renew = new NpgsqlBatchCommand(RenewSql)
        {
            Parameters =
            {
                new NpgsqlParameter("key", key), new NpgsqlParameter("id", id), new NpgsqlParameter("token", token),
                new NpgsqlParameter("timeline", timeline), new NpgsqlParameter("ttl", ttl),
            },
        };
        var updated = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), renew, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (updated is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var release = new NpgsqlBatchCommand(token is null ? ReleaseAnySql : ReleaseSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("id", id) } };
        if (token is { } named)
        {
            release.Parameters.Add(new NpgsqlParameter("token", named));
        }

        var deleted = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), release, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (deleted is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<IReadOnlyList<SlotHolder>> ReadHoldersAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(HoldersSql, connection) { CommandTimeout = BoundSeconds() };
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var holders = new List<SlotHolder>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            holders.Add(new SlotHolder { Id = reader.GetString(0), ReplicaId = reader.GetString(1), Epoch = reader.GetInt64(2) });
        }

        return holders;
    }

    private int BoundSeconds() => (int)Math.Ceiling((options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).TotalSeconds);

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
