using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Claims;

internal interface IOnceRows
{
    Task<Guid?> InsertAsync(string key, OnceMode mode, string effect, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken);

    Task<OnceRead?> ReadAsync(string key, CancellationToken cancellationToken);

    Task<bool> CompleteAsync(string key, Guid token, CancellationToken cancellationToken);

    Task<IReadOnlyList<OnceCandidate>> ListRetakeableAsync(IReadOnlyCollection<string> effects, int limit, CancellationToken cancellationToken);

    Task<Guid?> RetakeAsync(string key, Guid expected, string replica, long epoch, CancellationToken cancellationToken);
}

internal sealed class OnceStore(OwnedDataSource data, IOptions<ClusterOptions> options) : IOnceRows
{
    internal const string InsertSql = """
        INSERT INTO reeve_cluster.claim AS c (key, holder_replica, holder_epoch, token, expires_at, once_mode, once_effect, once_ttl)
        VALUES (@key, @replica, @epoch, gen_random_uuid(), clock_timestamp() + @ttl, @mode, @effect, @ttl)
        ON CONFLICT (key) DO NOTHING
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ReadSql = $"""
        SELECT c.holder_replica, c.holder_epoch, c.token, {ClaimStore.ValidRow}, c.completed_at IS NOT NULL
        FROM reeve_cluster.claim c
        WHERE c.key = @key
        """;

    internal const string CompleteSql = $"""
        UPDATE reeve_cluster.claim c
        SET completed_at = clock_timestamp()
        WHERE c.key = @key AND c.token = @token AND c.once_mode IS NOT NULL AND c.completed_at IS NULL AND ({ClaimStore.ValidRow})
        RETURNING c.token, current_setting('transaction_timeout')
        """;

    internal const string ListSql = $"""
        SELECT c.key, c.token, c.once_effect
        FROM reeve_cluster.claim c
        WHERE c.once_mode = 'at_least_once' AND c.completed_at IS NULL AND c.once_effect = ANY(@effects) AND NOT ({ClaimStore.ValidRow})
        ORDER BY c.expires_at
        LIMIT @limit
        """;

    internal const string RetakeLockSql = """
        SELECT current_setting('transaction_timeout') FROM reeve_cluster.claim
        WHERE key = @key AND token = @expected AND once_mode = 'at_least_once' FOR NO KEY UPDATE
        """;

    internal const string RetakeSql = $"""
        UPDATE reeve_cluster.claim c
        SET holder_replica = @replica, holder_epoch = @epoch, token = gen_random_uuid(), expires_at = clock_timestamp() + c.once_ttl
        WHERE c.key = @key AND c.token = @expected AND c.once_mode = 'at_least_once' AND c.completed_at IS NULL AND NOT ({ClaimStore.ValidRow})
        RETURNING c.token
        """;

    public async Task<Guid?> InsertAsync(string key, OnceMode mode, string effect, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var insert = new NpgsqlBatchCommand(InsertSql)
        {
            Parameters =
            {
                new NpgsqlParameter("key", key), new NpgsqlParameter("replica", replica), new NpgsqlParameter("epoch", epoch),
                new NpgsqlParameter("ttl", ttl), new NpgsqlParameter("mode", ModeName(mode)), new NpgsqlParameter("effect", effect),
            },
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

    public async Task<OnceRead?> ReadAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new OnceRead(reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.GetBoolean(3), reader.GetBoolean(4))
            : null;
    }

    public async Task<bool> CompleteAsync(string key, Guid token, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var complete = new NpgsqlBatchCommand(CompleteSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("token", token) } };
        var marked = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), complete, static r => r.GetGuid(0), cancellationToken).ConfigureAwait(false);
        if (marked is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<IReadOnlyList<OnceCandidate>> ListRetakeableAsync(IReadOnlyCollection<string> effects, int limit, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ListSql, connection);
        command.Parameters.AddWithValue("effects", effects.ToArray());
        command.Parameters.AddWithValue("limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var candidates = new List<OnceCandidate>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            candidates.Add(new OnceCandidate(reader.GetString(0), reader.GetGuid(1), reader.GetString(2)));
        }

        return candidates;
    }

    public async Task<Guid?> RetakeAsync(string key, Guid expected, string replica, long epoch, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var lockRow = new NpgsqlBatchCommand(RetakeLockSql) { Parameters = { new NpgsqlParameter("key", key), new NpgsqlParameter("expected", expected) } };
        var locked = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), lockRow, static _ => true, cancellationToken).ConfigureAwait(false);
        if (locked is null)
        {
            return null;
        }

        var retake = new NpgsqlCommand(RetakeSql, connection, transaction);
        await using (retake.ConfigureAwait(false))
        {
            retake.Parameters.AddWithValue("key", key);
            retake.Parameters.AddWithValue("expected", expected);
            retake.Parameters.AddWithValue("replica", replica);
            retake.Parameters.AddWithValue("epoch", epoch);
            if (await retake.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not Guid token)
            {
                return null;
            }

            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
            return token;
        }
    }

    internal static string ModeName(OnceMode mode) => mode switch
    {
        OnceMode.AtMostOnce => "at_most_once",
        OnceMode.AtLeastOnce => "at_least_once",
        _ => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
