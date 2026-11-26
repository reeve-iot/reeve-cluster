using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Log;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

#pragma warning disable CA2007

namespace Reeve.Cluster.Commands;

internal sealed record ActiveCommandRow(long Id, CommandState State, long? TakenToken, string IdempotencyKey, byte[] Payload)
{
    public ActiveCommand Active => new(Id, State, TakenToken);
}

internal sealed record TakeResult(TakeAction Action, ActiveCommandRow? Row);

internal interface ICommandRows
{
    Task<SentCommand?> InsertAsync(string kind, string key, string idempotencyKey, byte[] payload, CancellationToken cancellationToken);

    Task<SentCommand?> ReadAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken);

    Task<bool> ResendAsync(long id, CancellationToken cancellationToken);

    Task<IReadOnlyList<(string Kind, string Key)>> ListActiveKeysAsync(IReadOnlyCollection<string> kinds, CancellationToken cancellationToken);

    Task<IReadOnlyList<ActiveCommandRow>> ReadActiveAsync(string kind, string key, CancellationToken cancellationToken);

    Task<TakeResult> TakeAsync(IFencedOwner owner, long id, CancellationToken cancellationToken);

    Task<CompleteAction> CompleteAsync(IFencedOwner owner, long id, CommandOutcome outcome, CancellationToken cancellationToken);

    Task<TakeResult> RetakeAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken);

    Task<MarkAction> MarkInDoubtAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken);

    Task<IReadOnlyList<ExpirableCommand>> ListExpirableAsync(int limit, CancellationToken cancellationToken);

    Task<bool> ExpireAsync(long id, CancellationToken cancellationToken);

    Task<int> DeleteRetainedAsync(int limit, CancellationToken cancellationToken);

    Task<CommandStatus?> ReadStatusAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken);
}

internal sealed class CommandStore(OwnedDataSource data, IOptions<ClusterOptions> options) : ICommandRows
{
    internal const string InsertSql = """
        INSERT INTO reeve_cluster.command AS c (kind, key, idempotency_key, payload, state)
        VALUES (@kind, @key, @idempotency, @payload, 'pending')
        ON CONFLICT (kind, key, idempotency_key) DO NOTHING
        RETURNING c.id, current_setting('transaction_timeout')
        """;

    internal const string ReadSql = """
        SELECT c.id, c.state, c.payload
        FROM reeve_cluster.command c
        WHERE c.kind = @kind AND c.key = @key AND c.idempotency_key = @idempotency
        """;

    internal const string ResendSql = """
        UPDATE reeve_cluster.command c
        SET state = 'pending', created_at = now(), completed_at = NULL
        WHERE c.id = @id AND c.state = 'expired'
        RETURNING c.id, current_setting('transaction_timeout')
        """;

    internal const string ActiveKeysSql = """
        SELECT DISTINCT c.kind, c.key
        FROM reeve_cluster.command c
        WHERE c.kind = ANY(@kinds) AND c.state IN ('pending', 'taken')
        """;

    internal const string ActiveSql = """
        (SELECT c.id, c.state, c.taken_token, c.idempotency_key, c.payload, c.created_at
           FROM reeve_cluster.command c
          WHERE c.kind = @kind AND c.key = @key AND c.state = 'taken'
          ORDER BY c.created_at, c.id)
        UNION ALL
        (SELECT c.id, c.state, c.taken_token, c.idempotency_key, c.payload, c.created_at
           FROM reeve_cluster.command c
          WHERE c.kind = @kind AND c.key = @key AND c.state = 'pending'
          ORDER BY c.created_at, c.id
          LIMIT 1)
        ORDER BY created_at, id
        """;

    internal const string TakeSql = """
        UPDATE reeve_cluster.command c
        SET state = 'taken', taken_token = @token, taken_at = now()
        WHERE c.id = @id AND c.kind = @kind AND c.key = @key AND c.state = 'pending'
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.command t
            WHERE t.kind = c.kind AND t.key = c.key AND t.state = 'taken')
        RETURNING c.id, c.state, c.taken_token, c.idempotency_key, c.payload
        """;

    internal const string CompleteSql = """
        UPDATE reeve_cluster.command c
        SET state = 'done', result = @result, error = @error, completed_at = now()
        WHERE c.id = @id AND c.kind = @kind AND c.key = @key AND c.state = 'taken' AND c.taken_token = @token
        RETURNING c.id
        """;

    internal const string RetakeSql = """
        UPDATE reeve_cluster.command c
        SET taken_token = @token, taken_at = now()
        WHERE c.id = @id AND c.kind = @kind AND c.key = @key AND c.state = 'taken' AND c.taken_token = @old AND c.taken_token < @token
        RETURNING c.id, c.state, c.taken_token, c.idempotency_key, c.payload
        """;

    internal const string InDoubtSql = """
        UPDATE reeve_cluster.command c
        SET state = 'in_doubt', completed_at = now()
        WHERE c.id = @id AND c.kind = @kind AND c.key = @key AND c.state = 'taken' AND c.taken_token = @old AND c.taken_token < @token
        RETURNING c.id
        """;

    internal const string ExpirableSql = """
        SELECT c.id, c.kind, c.key
        FROM reeve_cluster.command c
        WHERE c.state = 'pending' AND c.created_at < clock_timestamp() - @ttl
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.owned o
            JOIN reeve_cluster.member m ON m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch
            WHERE o.kind = c.kind AND o.key = c.key AND m.state = 'active' AND m.lease_expires_at > clock_timestamp())
        ORDER BY c.created_at, c.id
        LIMIT @limit
        """;

    internal const string ExpireSql = """
        UPDATE reeve_cluster.command c
        SET state = 'expired', completed_at = now()
        WHERE c.id = @id AND c.state = 'pending' AND c.created_at < clock_timestamp() - @ttl
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.owned o
            JOIN reeve_cluster.member m ON m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch
            WHERE o.kind = c.kind AND o.key = c.key AND m.state = 'active' AND m.lease_expires_at > clock_timestamp())
        RETURNING c.id, current_setting('transaction_timeout')
        """;

    internal const string RetentionSql = """
        DELETE FROM reeve_cluster.command
        WHERE state IN ('done', 'in_doubt', 'expired') AND completed_at < now() - @retention
          AND id IN (
            SELECT id FROM reeve_cluster.command
            WHERE state IN ('done', 'in_doubt', 'expired') AND completed_at < now() - @retention
            ORDER BY completed_at
            LIMIT @limit)
        RETURNING id, current_setting('transaction_timeout')
        """;

    internal const string StatusSql = """
        SELECT c.id, c.state, c.result, c.error
        FROM reeve_cluster.command c
        WHERE c.kind = @kind AND c.key = @key AND c.idempotency_key = @idempotency
        """;

    public async Task<SentCommand?> InsertAsync(string kind, string key, string idempotencyKey, byte[] payload, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var insert = new NpgsqlBatchCommand(InsertSql)
        {
            Parameters =
            {
                new NpgsqlParameter("kind", kind),
                new NpgsqlParameter("key", key),
                new NpgsqlParameter("idempotency", idempotencyKey),
                new NpgsqlParameter("payload", payload),
            },
        };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), insert, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false);
        if (written is not { } row)
        {
            return null;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return new SentCommand(row.Value, CommandState.Pending, payload);
    }

    public async Task<SentCommand?> ReadAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("idempotency", idempotencyKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new SentCommand(reader.GetInt64(0), CommandStates.Parse(reader.GetString(1)), reader.GetFieldValue<byte[]>(2))
            : null;
    }

    public async Task<bool> ResendAsync(long id, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var resend = new NpgsqlBatchCommand(ResendSql) { Parameters = { new NpgsqlParameter("id", id) } };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), resend, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false);
        if (written is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<IReadOnlyList<(string Kind, string Key)>> ListActiveKeysAsync(IReadOnlyCollection<string> kinds, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ActiveKeysSql, connection);
        command.Parameters.AddWithValue("kinds", kinds.ToArray());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var keys = new List<(string, string)>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            keys.Add((reader.GetString(0), reader.GetString(1)));
        }

        return keys;
    }

    public async Task<IReadOnlyList<ActiveCommandRow>> ReadActiveAsync(string kind, string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ActiveSql, connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var rows = new List<ActiveCommandRow>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(Row(reader));
        }

        return rows;
    }

    public Task<TakeResult> TakeAsync(IFencedOwner owner, long id, CancellationToken cancellationToken) =>
        TakeCoreAsync(owner, TakeSql, id, null, cancellationToken);

    public Task<TakeResult> RetakeAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken) =>
        TakeCoreAsync(owner, RetakeSql, id, oldToken, cancellationToken);

    private async Task<TakeResult> TakeCoreAsync(IFencedOwner owner, string sql, long id, long? oldToken, CancellationToken cancellationToken)
    {
        var start = owner.Start();
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        await FencedTransactions.AssertAsync(owner, start, connection, transaction, cancellationToken).ConfigureAwait(false);

        ActiveCommandRow? taken;
        var update = new NpgsqlCommand(sql, connection, transaction);
        await using (update.ConfigureAwait(false))
        {
            update.Parameters.AddWithValue("id", id);
            update.Parameters.AddWithValue("kind", start.Kind);
            update.Parameters.AddWithValue("key", start.Key);
            update.Parameters.AddWithValue("token", start.Token);
            if (oldToken is { } old)
            {
                update.Parameters.AddWithValue("old", old);
            }

            var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                taken = await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? Row(reader) : null;
            }
        }

        if (taken is null)
        {
            return new TakeResult(TakeDecisions.AfterTake(updated: false, cancellationRequestedBeforeReturn: false), null);
        }

        try
        {
            await FencedTransactions.CommitAsync(transaction.CommitAsync, connection, cancellationToken).ConfigureAwait(false);
        }
        catch (CommitOutcomeUnknownException)
        {
            return new TakeResult(TakeDecisions.AfterTake(updated: true, cancellationRequestedBeforeReturn: true), taken);
        }

        return new TakeResult(TakeDecisions.AfterTake(updated: true, cancellationRequestedBeforeReturn: false), taken);
    }

    public async Task<CompleteAction> CompleteAsync(IFencedOwner owner, long id, CommandOutcome outcome, CancellationToken cancellationToken)
    {
        var (updated, unknown) = await WriteAsync(
            owner, CompleteSql, id, null, cancellationToken,
            command =>
            {
                command.Parameters.Add(new NpgsqlParameter("result", NpgsqlDbType.Bytea) { Value = outcome.Error is null ? outcome.Result.ToArray() : DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter("error", NpgsqlDbType.Text) { Value = (object?)outcome.Error ?? DBNull.Value });
            }).ConfigureAwait(false);
        return CompleteDecisions.AfterComplete(updated, unknown);
    }

    public async Task<MarkAction> MarkInDoubtAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken)
    {
        var (updated, unknown) = await WriteAsync(owner, InDoubtSql, id, oldToken, cancellationToken, null).ConfigureAwait(false);
        return MarkDecisions.AfterMark(updated, unknown);
    }

    private async Task<(bool Updated, bool Unknown)> WriteAsync(
        IFencedOwner owner, string sql, long id, long? oldToken, CancellationToken cancellationToken, Action<NpgsqlCommand>? parameters)
    {
        var start = owner.Start();
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        await FencedTransactions.AssertAsync(owner, start, connection, transaction, cancellationToken).ConfigureAwait(false);

        bool updated;
        var update = new NpgsqlCommand(sql, connection, transaction);
        await using (update.ConfigureAwait(false))
        {
            update.Parameters.AddWithValue("id", id);
            update.Parameters.AddWithValue("kind", start.Kind);
            update.Parameters.AddWithValue("key", start.Key);
            update.Parameters.AddWithValue("token", start.Token);
            if (oldToken is { } old)
            {
                update.Parameters.AddWithValue("old", old);
            }

            parameters?.Invoke(update);
            updated = await update.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null;
        }

        if (!updated)
        {
            return (false, false);
        }

        try
        {
            await FencedTransactions.CommitAsync(transaction.CommitAsync, connection, cancellationToken).ConfigureAwait(false);
        }
        catch (CommitOutcomeUnknownException)
        {
            return (true, true);
        }

        return (true, false);
    }

    public async Task<IReadOnlyList<ExpirableCommand>> ListExpirableAsync(int limit, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ExpirableSql, connection);
        command.Parameters.AddWithValue("ttl", options.Value.CommandPendingTtl);
        command.Parameters.AddWithValue("limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var found = new List<ExpirableCommand>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            found.Add(new ExpirableCommand(reader.GetInt64(0), reader.GetString(1), reader.GetString(2)));
        }

        return found;
    }

    public async Task<bool> ExpireAsync(long id, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var expire = new NpgsqlBatchCommand(ExpireSql)
        {
            Parameters = { new NpgsqlParameter("id", id), new NpgsqlParameter("ttl", options.Value.CommandPendingTtl) },
        };
        var written = await TransactionTimeout.FirstAsync(
            connection, transaction, BoundMilliseconds(), expire, static r => r.GetInt64(0), cancellationToken).ConfigureAwait(false);
        if (written is null)
        {
            return false;
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<int> DeleteRetainedAsync(int limit, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var deleted = 0;
        var batch = new NpgsqlBatch(connection, transaction);
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(TransactionTimeout.Statement(BoundMilliseconds()));
            batch.BatchCommands.Add(new NpgsqlBatchCommand(RetentionSql)
            {
                Parameters = { new NpgsqlParameter("retention", options.Value.CommandRetention), new NpgsqlParameter("limit", limit) },
            });
            var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                do
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        deleted++;
                    }
                }
                while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    public async Task<CommandStatus?> ReadStatusAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(StatusSql, connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("idempotency", idempotencyKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new CommandStatus(
                reader.GetInt64(0),
                CommandStates.Parse(reader.GetString(1)),
                reader.IsDBNull(2) ? null : reader.GetFieldValue<byte[]>(2),
                reader.IsDBNull(3) ? null : reader.GetString(3))
            : null;
    }

    private static ActiveCommandRow Row(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        CommandStates.Parse(reader.GetString(1)),
        reader.IsDBNull(2) ? null : reader.GetInt64(2),
        reader.GetString(3),
        reader.GetFieldValue<byte[]>(4));

    private int BoundMilliseconds() =>
        RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(1)).Milliseconds
        ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
}
