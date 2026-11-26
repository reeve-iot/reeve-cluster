using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;

#pragma warning disable CA2007

namespace Reeve.Cluster.Owned;

internal sealed record OwnedRead(OwnedRow? Row, OwnerMember? Owner, long? RequestToken)
{
    public OwnedCondition Condition => OwnedRowCondition.Of(Row, Owner, RequestToken);
}

internal readonly record struct HeldRow(string Kind, string Key, long Token, bool ReleaseRequested, TimeSpan HeldFor = default);

internal readonly record struct OrphanedRow(string Kind, string Key, OwnedRow Row);

internal interface IUnitRows
{
    Task<IReadOnlyList<HeldRow>> ReadHeldAsync(string owner, long epoch, CancellationToken cancellationToken);

    Task<IReadOnlyList<OrphanedRow>> ReadOrphanedAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken);

    Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(string kind, string key, string owner, long epoch, long token, CancellationToken cancellationToken);
}

internal sealed class OwnedDataSource : IAsyncDisposable
{
    public OwnedDataSource(IOptions<ClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Source = new ClusterDataSource(options.Value, static builder => builder.CommandTimeout = 0);
    }

    public ClusterDataSource Source { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed partial class OwnedStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IUnitRows, ICreatorRows
{
    internal const string ReadHeldSql = """
        SELECT o.kind, o.key, o.token, r.token IS NOT NULL, EXTRACT(EPOCH FROM clock_timestamp() - o.updated_at)::float8
        FROM reeve_cluster.owned o
        LEFT JOIN reeve_cluster.release_request r ON r.kind = o.kind AND r.key = o.key AND r.token = o.token
        WHERE o.owner_replica = @owner AND o.owner_epoch = @epoch
        """;

    internal const string ReadOrphanedSql = """
        SELECT o.kind, o.key, o.owner_replica, o.owner_epoch, o.token
        FROM reeve_cluster.owned o
        WHERE o.kind = ANY(@kinds) AND o.owner_replica IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch AND m.state = 'active'
              AND m.lease_expires_at > clock_timestamp())
        """;

    internal const string ReadSweepableSql = """
        SELECT kind, key, token FROM reeve_cluster.owned
        WHERE owner_replica IS NULL AND updated_at < clock_timestamp() - @retention
        ORDER BY updated_at
        LIMIT @limit
        """;

    internal const string ReadSql = """
        SELECT o.owner_replica, o.owner_epoch, o.token, m.epoch, m.state, m.lease_expires_at > clock_timestamp(), r.token
        FROM reeve_cluster.owned o
        LEFT JOIN reeve_cluster.member m ON m.replica_id = o.owner_replica
        LEFT JOIN reeve_cluster.release_request r ON r.kind = o.kind AND r.key = o.key
        WHERE o.kind = @kind AND o.key = @key
        """;

    internal const string InsertSql = """
        INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token)
        VALUES (@kind, @key, @owner, @epoch, reeve_cluster.draw_token())
        ON CONFLICT (kind, key) DO NOTHING
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string LockSql = """
        SELECT current_setting('transaction_timeout') FROM reeve_cluster.owned WHERE kind = @kind AND key = @key FOR NO KEY UPDATE
        """;

    internal const string TakeSql = """
        UPDATE reeve_cluster.owned o
        SET owner_replica = @owner, owner_epoch = @epoch, token = reeve_cluster.draw_token()
        WHERE o.kind = @kind AND o.key = @key AND o.token = @token
          AND o.owner_replica IS NOT DISTINCT FROM @expected_owner AND o.owner_epoch IS NOT DISTINCT FROM @expected_epoch
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch AND m.state = 'active'
              AND m.lease_expires_at > clock_timestamp())
        RETURNING o.token
        """;

    internal const string RequestSql = """
        INSERT INTO reeve_cluster.release_request AS r (kind, key, token, requested_at)
        VALUES (@kind, @key, @token, clock_timestamp())
        ON CONFLICT (kind, key) DO UPDATE SET token = excluded.token, requested_at = excluded.requested_at
        WHERE r.token < excluded.token
        RETURNING r.token, current_setting('transaction_timeout')
        """;

    internal const string ReleaseSql = """
        UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL
        WHERE kind = @kind AND key = @key AND owner_replica = @owner AND owner_epoch = @epoch AND token = @token
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string DeleteRequestSql = """
        DELETE FROM reeve_cluster.release_request WHERE kind = @kind AND key = @key AND token = @token
        """;

    internal const string ClearSql = """
        UPDATE reeve_cluster.owned o SET owner_replica = NULL, owner_epoch = NULL
        WHERE o.kind = @kind AND o.key = @key AND o.owner_replica = @expected_owner AND o.owner_epoch = @expected_epoch
          AND o.token = @token
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch AND m.state = 'active'
              AND m.lease_expires_at > clock_timestamp())
        RETURNING o.token, current_setting('transaction_timeout')
        """;

    internal const string SweepSql = """
        DELETE FROM reeve_cluster.owned
        WHERE kind = @kind AND key = @key AND owner_replica IS NULL AND token = @token
          AND updated_at < clock_timestamp() - @retention
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string SweepRequestsSql = """
        DELETE FROM reeve_cluster.release_request WHERE kind = @kind AND key = @key AND token <= @token
        """;

    public async Task<OwnedRead> ReadAsync(string kind, string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new OwnedRead(null, null, null);
        }

        var row = new OwnedRow(
            reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2));
        OwnerMember? owner = reader.IsDBNull(3)
            ? null
            : new OwnerMember(reader.GetInt64(3), Enum.Parse<MemberRowState>(reader.GetString(4), ignoreCase: true), reader.GetBoolean(5));
        return new OwnedRead(row, owner, reader.IsDBNull(6) ? null : reader.GetInt64(6));
    }

    public async Task<IReadOnlyList<HeldRow>> ReadHeldAsync(string owner, long epoch, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadHeldSql, connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("epoch", epoch);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var rows = new List<HeldRow>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new HeldRow(reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetBoolean(3), TimeSpan.FromSeconds(Math.Max(0, reader.GetDouble(4)))));
        }

        return rows;
    }

    public async Task<IReadOnlyList<OrphanedRow>> ReadOrphanedAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadOrphanedSql, connection);
        command.Parameters.Add(new NpgsqlParameter("kinds", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = kinds.ToArray() });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var rows = new List<OrphanedRow>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new OrphanedRow(reader.GetString(0), reader.GetString(1), new OwnedRow(reader.GetString(2), reader.GetInt64(3), reader.GetInt64(4))));
        }

        return rows;
    }

    public async Task<IReadOnlyList<(string Kind, string Key, long Token)>> ReadSweepableAsync(int limit, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSweepableSql, connection);
        command.Parameters.AddWithValue("retention", options.Value.OwnedRowRetention);
        command.Parameters.AddWithValue("limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var rows = new List<(string, string, long)>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2)));
        }

        return rows;
    }

    public Task<long?> InsertAsync(string kind, string key, string owner, long epoch, CancellationToken cancellationToken) =>
        RunAsync(
            Command(InsertSql, kind, key, ("owner", owner), ("epoch", epoch)),
            null,
            cancellationToken);

    public async Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var locked = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), Command(LockSql, kind, key), static _ => true, cancellationToken).ConfigureAwait(false);
        if (locked is null)
        {
            return null;
        }

        var take = new NpgsqlCommand(TakeSql, connection, transaction);
        await using (take.ConfigureAwait(false))
        {
            take.Parameters.AddWithValue("kind", kind);
            take.Parameters.AddWithValue("key", key);
            take.Parameters.AddWithValue("token", expected.Token);
            take.Parameters.Add(new NpgsqlParameter("expected_owner", NpgsqlDbType.Text) { Value = (object?)expected.OwnerReplica ?? DBNull.Value });
            take.Parameters.Add(new NpgsqlParameter("expected_epoch", NpgsqlDbType.Bigint) { Value = (object?)expected.OwnerEpoch ?? DBNull.Value });
            take.Parameters.AddWithValue("owner", owner);
            take.Parameters.AddWithValue("epoch", epoch);
            if (await take.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not long token)
            {
                return null;
            }

            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
            return token;
        }
    }

    public async Task<bool> RequestReleaseAsync(string kind, string key, long token, CancellationToken cancellationToken) =>
        await RunAsync(Command(RequestSql, kind, key, ("token", token)), null, cancellationToken).ConfigureAwait(false) is not null;

    public async Task<bool> ReleaseAsync(string kind, string key, string owner, long epoch, long token, CancellationToken cancellationToken) =>
        await RunAsync(
            Command(ReleaseSql, kind, key, ("owner", owner), ("epoch", epoch), ("token", token)),
            Command(DeleteRequestSql, kind, key, ("token", token)),
            cancellationToken).ConfigureAwait(false) is not null;

    public async Task<bool> ClearAsync(string kind, string key, OwnedRow expected, CancellationToken cancellationToken)
    {
        if (expected.OwnerReplica is not { } expectedOwner || expected.OwnerEpoch is not { } expectedEpoch)
        {
            return false;
        }

        return await RunAsync(
            Command(ClearSql, kind, key, ("expected_owner", expectedOwner), ("expected_epoch", expectedEpoch), ("token", expected.Token)),
            null,
            cancellationToken).ConfigureAwait(false) is not null;
    }

    public async Task<bool> SweepAsync(string kind, string key, long token, CancellationToken cancellationToken) =>
        await RunAsync(
            Command(SweepSql, kind, key, ("token", token), ("retention", options.Value.OwnedRowRetention)),
            Command(SweepRequestsSql, kind, key, ("token", token)),
            cancellationToken).ConfigureAwait(false) is not null;

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");

    private static NpgsqlBatchCommand Command(string sql, string kind, string key, params (string Name, object Value)[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql)
        {
            Parameters = { new NpgsqlParameter("kind", kind), new NpgsqlParameter("key", key) },
        };
        foreach (var (name, value) in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter(name, value));
        }

        return command;
    }

    private async Task<long?> RunAsync(NpgsqlBatchCommand first, NpgsqlBatchCommand? then, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), first, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false);
        if (written is not { } row)
        {
            return null;
        }

        if (then is not null)
        {
            var batch = new NpgsqlBatch(connection, transaction) { BatchCommands = { then } };
            await using (batch.ConfigureAwait(false))
            {
                await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return row.Value;
    }
}
