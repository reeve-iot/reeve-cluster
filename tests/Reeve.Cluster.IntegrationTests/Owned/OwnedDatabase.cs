using Int.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Migrations;

namespace Int.Owned;

internal sealed class OwnedDatabase
{
    public const long TimelineUnit = 1L << 40;

    private OwnedDatabase(string connectionString, long timeline)
    {
        ConnectionString = connectionString;
        Timeline = timeline;
    }

    public string ConnectionString { get; }

    public long Timeline { get; }

    public long PreviousTimeline => Timeline - 1;

    public long LaterTimeline => Timeline + 1;

    public static async Task<OwnedDatabase> CreateAsync(PostgresFixture postgres)
    {
        var cs = await postgres.CreateDatabaseAsync();
        return await MigrateAsync(cs);
    }

    public static async Task<OwnedDatabase> MigrateAsync(string connectionString)
    {
        var runner = ClusterRunners.For(connectionString);
        await runner.MigrateAsync();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        var timeline = await ScalarAsync<long>(connection, "SELECT reeve_cluster.server_timeline()");
        return new OwnedDatabase(connectionString, timeline);
    }

    public async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task<long> AddMemberAsync(string replica, string state = "active", string lease = "1 minute")
    {
        await using var connection = await OpenAsync();
        return await ScalarAsync<long>(
            connection,
            $"""
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, version, protocol, process_id)
            VALUES (@replica, nextval('reeve_cluster.member_epoch'), @state, clock_timestamp() + interval '{lease}', '0.2.0', 1, gen_random_uuid())
            RETURNING epoch
            """,
            ("replica", replica),
            ("state", state));
    }

    public async Task<long> CreateOwnedAsync(string kind, string key, string replica)
    {
        await using var connection = await OpenAsync();
        return await ScalarAsync<long>(
            connection,
            """
            INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token)
            SELECT @kind, @key, replica_id, epoch, reeve_cluster.draw_token() FROM reeve_cluster.member WHERE replica_id = @replica
            RETURNING token
            """,
            ("kind", kind),
            ("key", key),
            ("replica", replica));
    }

    public static async Task<long?> AdoptAsync(NpgsqlConnection connection, string kind, string key, long expectedToken, string replica)
    {
        await ExecAsync(connection, "SELECT 1 FROM reeve_cluster.owned WHERE kind = @kind AND key = @key FOR NO KEY UPDATE", ("kind", kind), ("key", key));
        await using var command = Command(
            connection,
            """
            UPDATE reeve_cluster.owned o
               SET owner_replica = m.replica_id, owner_epoch = m.epoch, token = reeve_cluster.draw_token()
              FROM reeve_cluster.member m
             WHERE o.kind = @kind AND o.key = @key AND o.token = @token AND m.replica_id = @replica
            RETURNING o.token
            """,
            ("kind", kind),
            ("key", key),
            ("token", expectedToken),
            ("replica", replica));
        var result = await command.ExecuteScalarAsync();
        return result is null ? null : (long)result;
    }

    public static async Task<NpgsqlTransaction> BeginFencedAsync(NpgsqlConnection connection, string kind, string key, long token, long timeline, TimeSpan timeout)
    {
        var transaction = await connection.BeginTransactionAsync();
        await using var batch = new NpgsqlBatch(connection, transaction)
        {
            BatchCommands =
            {
                new NpgsqlBatchCommand($"SET LOCAL transaction_timeout = '{(long)timeout.TotalMilliseconds}ms'"),
                new NpgsqlBatchCommand("SELECT reeve_cluster.assert_owner(@kind, @key, @token, @timeline)")
                {
                    Parameters =
                    {
                        new NpgsqlParameter("kind", kind),
                        new NpgsqlParameter("key", key),
                        new NpgsqlParameter("token", token),
                        new NpgsqlParameter("timeline", timeline),
                    },
                },
            },
        };
        await batch.ExecuteNonQueryAsync();
        return transaction;
    }

    public async Task<bool> CanLockAsync(string table, string where, string mode, params (string Name, object Value)[] parameters)
    {
        await using var connection = await OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        try
        {
            await ExecAsync(connection, $"SELECT 1 FROM reeve_cluster.{table} WHERE {where} {mode} NOWAIT", parameters);
            return true;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            return false;
        }
    }

    public Task<bool> CanLockOwnedAsync(string kind, string key, string mode) =>
        CanLockAsync("owned", "kind = @kind AND key = @key", mode, ("kind", kind), ("key", key));

    public Task<bool> CanLockClusterAsync() => CanLockAsync("cluster", "id", "FOR UPDATE");

    public async Task<long> LastTokenAsync()
    {
        await using var connection = await OpenAsync();
        return await ScalarAsync<long>(connection, "SELECT last_value FROM reeve_cluster.owned_token");
    }

    public async Task LoseSuffixAsync(long sequenceBack)
    {
        await using var connection = await OpenAsync();
        await ExecAsync(connection, "SELECT setval('reeve_cluster.owned_token', @v)", ("v", sequenceBack));
    }

    public static long TimelineOf(long token) => token / TimelineUnit;

    public static long SequenceValueOf(long token) => token % TimelineUnit;

    public static NpgsqlCommand Command(NpgsqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return command;
    }

    public static async Task<int> ExecAsync(NpgsqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = Command(connection, sql, parameters);
        return await command.ExecuteNonQueryAsync();
    }

    public static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = Command(connection, sql, parameters);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    public static async Task<string> SqlStateOfAsync(Func<Task> call)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(call);
        return ex.SqlState;
    }

    private static TimeSpan Safety(TimeSpan bound) => bound < Poll.Bound ? Poll.Bound : bound;

    public async Task WaitUntilBlockedAsync(int pid, TimeSpan bound)
    {
        await using var connection = await OpenAsync();
        bound = Safety(bound);
        var deadline = DateTime.UtcNow + bound;
        while (true)
        {
            var waiting = await ScalarAsync<bool>(connection, "SELECT coalesce((SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = @pid), false)", ("pid", pid));
            if (waiting)
            {
                return;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Backend {pid} did not block on a lock within {bound}.");
            await Task.Delay(20);
        }
    }

    public async Task WaitForALockWaiterAsync()
    {
        await using var connection = await OpenAsync();
        var deadline = DateTime.UtcNow + Poll.Bound;
        while (!await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock')"))
        {
            Assert.True(DateTime.UtcNow < deadline, $"No statement blocked on a lock within {Poll.Bound}.");
            await Task.Delay(20);
        }
    }

    public async Task WaitUntilGoneAsync(int pid, TimeSpan bound)
    {
        await using var connection = await OpenAsync();
        bound = Safety(bound);
        var deadline = DateTime.UtcNow + bound;
        while (await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = @pid)", ("pid", pid)))
        {
            Assert.True(DateTime.UtcNow < deadline, $"Backend {pid} was still there after {bound}.");
            await Task.Delay(20);
        }
    }
}
