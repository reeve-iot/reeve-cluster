using System.Reflection;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Schema;
using Reeve.Cluster.Migrations;

namespace Int.Migrations;

public sealed class MigrationRunnerTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public MigrationRunnerTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private static ClusterMigrationRunner Runner(string connectionString) => ClusterRunners.For(connectionString);

    private static List<string> Shipped() => typeof(ClusterMigrationsDbContext).Assembly.GetTypes()
        .Select(t => t.GetCustomAttribute<MigrationAttribute>()?.Id)
        .Where(id => id is not null)
        .Order(StringComparer.Ordinal)
        .ToList()!;

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<List<(string Id, string ProductVersion)>> HistoryAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand("SELECT \"MigrationId\", \"ProductVersion\" FROM reeve_cluster.\"__ClusterMigrationsHistory\" ORDER BY \"MigrationId\"", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(string, string)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }

    [Fact]
    public async Task A_first_run_applies_the_whole_chain_and_records_it()
    {
        var cs = await postgres.CreateDatabaseAsync();
        var shipped = Shipped();

        var result = await Runner(cs).MigrateAsync();

        Assert.Equal(shipped, result.AppliedMigrations);
        Assert.Equal(shipped[^1], result.LastMigration);
        await using var connection = await OpenAsync(cs);
        Assert.Equal(shipped, (await HistoryAsync(connection)).Select(h => h.Id));
    }

    [Fact]
    public async Task Two_runners_started_together_apply_the_chain_once()
    {
        var cs = await postgres.CreateDatabaseAsync();
        var shipped = Shipped();
        const int runners = 6;
        using var start = new Barrier(runners);

        var results = await Task.WhenAll(Enumerable.Range(0, runners).Select(_ => Task.Run(async () =>
        {
            var runner = Runner(cs);
            start.SignalAndWait();
            return await runner.MigrateAsync();
        })));

        Assert.Single(results, r => r.AppliedMigrations.Count > 0);
        Assert.Equal(shipped, results.Single(r => r.AppliedMigrations.Count > 0).AppliedMigrations);
        Assert.All(results, r => Assert.Equal(shipped[^1], r.LastMigration));
        await using var connection = await OpenAsync(cs);
        Assert.Equal(shipped.Count, (await HistoryAsync(connection)).Count);
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.cluster"));
    }

    [Fact]
    public async Task A_rerun_changes_nothing()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        await using var connection = await OpenAsync(cs);
        await using (var edit = new NpgsqlCommand("UPDATE reeve_cluster.cluster SET horizon_seq = 7", connection))
        {
            await edit.ExecuteNonQueryAsync();
        }

        var before = await HistoryAsync(connection);
        var result = await Runner(cs).MigrateAsync();
        var after = await HistoryAsync(connection);

        Assert.Empty(result.AppliedMigrations);
        Assert.Equal(Shipped()[^1], result.LastMigration);
        Assert.Equal(before, after);
        Assert.Equal(7L, await ScalarAsync<long>(connection, "SELECT horizon_seq FROM reeve_cluster.cluster"));
    }

    [Fact]
    public async Task A_runtime_role_without_DDL_rights_starts_when_nothing_is_pending()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        var runtime = await RuntimeRoleAsync(cs);
        var result = await Runner(runtime).MigrateAsync();

        Assert.Empty(result.AppliedMigrations);
        Assert.Equal(Shipped()[^1], result.LastMigration);
    }

    [Fact]
    public async Task A_runtime_role_without_DDL_rights_fails_when_a_migration_is_pending()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        var latest = Shipped()[^1];
        await using (var admin = await OpenAsync(cs))
        await using (var remove = new NpgsqlCommand("DELETE FROM reeve_cluster.\"__ClusterMigrationsHistory\" WHERE \"MigrationId\" = @id", admin))
        {
            remove.Parameters.AddWithValue("id", latest);
            await remove.ExecuteNonQueryAsync();
        }

        var runtime = await RuntimeRoleAsync(cs);
        var failure = await Assert.ThrowsAsync<PostgresException>(() => Runner(runtime).MigrateAsync());
        Assert.Equal("42501", failure.SqlState);
    }

    private static async Task<string> RuntimeRoleAsync(string connectionString)
    {
        var role = "runtime_" + Guid.NewGuid().ToString("N")[..12];
        await using var admin = await OpenAsync(connectionString);
        await using var grant = new NpgsqlCommand($"""
            CREATE ROLE {role} LOGIN PASSWORD '{role}';
            GRANT USAGE ON SCHEMA reeve_cluster TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA reeve_cluster TO {role};
            GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA reeve_cluster TO {role};
            """, admin);
        await grant.ExecuteNonQueryAsync();
        return new NpgsqlConnectionStringBuilder(connectionString) { Username = role, Password = role }.ConnectionString;
    }

    [Fact]
    public async Task A_history_row_of_a_newer_build_is_left_alone()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        await using var connection = await OpenAsync(cs);
        await using (var newer = new NpgsqlCommand("INSERT INTO reeve_cluster.\"__ClusterMigrationsHistory\" (\"MigrationId\", \"ProductVersion\") VALUES ('99991231000000_FromANewerBuild', '10.0.0')", connection))
        {
            await newer.ExecuteNonQueryAsync();
        }

        var result = await Runner(cs).MigrateAsync();

        Assert.Empty(result.AppliedMigrations);
        Assert.Equal("99991231000000_FromANewerBuild", result.LastMigration);
    }

    [Fact]
    public async Task A_failing_migration_leaves_nothing_of_itself_behind()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await using (var setup = await OpenAsync(cs))
        await using (var blocker = new NpgsqlCommand("CREATE SCHEMA reeve_cluster; CREATE FUNCTION reeve_cluster.server_timeline() RETURNS int LANGUAGE sql AS 'SELECT 1'", setup))
        {
            await blocker.ExecuteNonQueryAsync();
        }

        var failure = await Assert.ThrowsAnyAsync<PostgresException>(() => Runner(cs).MigrateAsync());
        Assert.Equal(PostgresErrorCodes.DuplicateFunction, failure.SqlState);

        await using var connection = await OpenAsync(cs);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'reeve_cluster' AND c.relname = 'member'"));
        Assert.Empty(await HistoryAsync(connection));
    }

    [Fact]
    public async Task The_runner_waits_on_an_advisory_lock_of_its_own_and_applies_nothing_before_it_has_it()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await using var holder = await OpenAsync(cs);
        await using var transaction = await holder.BeginTransactionAsync();
        await using (var take = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({ClusterMigrationRunner.AdvisoryLockKey})", holder, transaction))
        {
            await take.ExecuteNonQueryAsync();
        }

        var running = Runner(cs).MigrateAsync();
        await using var observer = await OpenAsync(cs);
        var waiting = $"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND ((classid::bigint << 32) | objid::bigint) = {ClusterMigrationRunner.AdvisoryLockKey}";
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (await ScalarAsync<long>(observer, waiting) == 0)
        {
            Assert.False(running.IsCompleted, "The runner finished while another transaction held its advisory lock.");
            await Task.Delay(50, deadline.Token);
        }

        Assert.False(running.IsCompleted);
        Assert.Equal(0L, await ScalarAsync<long>(observer, "SELECT count(*) FROM pg_namespace WHERE nspname = 'reeve_cluster'"));

        await transaction.CommitAsync();
        var result = await running;

        Assert.NotEmpty(result.AppliedMigrations);
    }

    [Fact]
    public async Task The_runner_holds_no_session_advisory_lock_once_it_has_returned()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();

        await using var connection = await OpenAsync(cs);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'"));
    }

    [Fact]
    public async Task The_runner_is_usable_on_a_database_where_the_schema_already_exists_empty()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await using (var connection = await OpenAsync(cs))
        await using (var create = new NpgsqlCommand("CREATE SCHEMA reeve_cluster", connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var result = await Runner(cs).MigrateAsync();

        Assert.NotEmpty(result.AppliedMigrations);
    }

    [Fact]
    public async Task I25_every_sequence_of_the_schema_is_CACHE_1()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();

        await using var connection = await OpenAsync(cs);
        await SchemaAssertions.EverySequenceIsCacheOneAsync(connection);
    }

    [Fact]
    public async Task The_I25_helper_fails_on_a_sequence_that_is_not_CACHE_1_and_on_a_schema_without_one()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        await using var connection = await OpenAsync(cs);
        await using (var cache = new NpgsqlCommand("CREATE SEQUENCE reeve_cluster.cached_probe CACHE 20", connection))
        {
            await cache.ExecuteNonQueryAsync();
        }

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => SchemaAssertions.EverySequenceIsCacheOneAsync(connection));
        Assert.Contains("cached_probe (CACHE 20)", ex.Message, StringComparison.Ordinal);

        await Assert.ThrowsAnyAsync<Exception>(() => SchemaAssertions.EverySequenceIsCacheOneAsync(connection, "no_such_schema"));
    }

    [Fact]
    public async Task The_epoch_sequence_hands_out_consecutive_values()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();

        await using var connection = await OpenAsync(cs);
        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT nextval('reeve_cluster.member_epoch')"));
        Assert.Equal(2L, await ScalarAsync<long>(connection, "SELECT nextval('reeve_cluster.member_epoch')"));
    }

    [Fact]
    public async Task The_cluster_table_holds_one_row_with_the_floors_unset_and_horizon_zero_and_can_hold_no_other()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();

        await using var connection = await OpenAsync(cs);
        await using (var read = new NpgsqlCommand("SELECT protocol_floor, version_floor, horizon_seq FROM reeve_cluster.cluster", connection))
        await using (var reader = await read.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0, reader.GetInt32(0));
            Assert.True(reader.IsDBNull(1));
            Assert.Equal(0L, reader.GetInt64(2));
            Assert.False(await reader.ReadAsync());
        }

        await using var second = new NpgsqlCommand("INSERT INTO reeve_cluster.cluster (id) VALUES (false)", connection);
        await Assert.ThrowsAsync<PostgresException>(() => second.ExecuteNonQueryAsync());
        await using var duplicate = new NpgsqlCommand("INSERT INTO reeve_cluster.cluster (id) VALUES (true)", connection);
        await Assert.ThrowsAsync<PostgresException>(() => duplicate.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task The_member_table_takes_a_joining_row_and_refuses_an_unknown_state_and_a_second_row_of_one_replica()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        await using var connection = await OpenAsync(cs);

        const string insert = """
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, version, protocol, process_id)
            VALUES (@id, nextval('reeve_cluster.member_epoch'), @state, clock_timestamp() + interval '30 seconds', '0.1.0', 1, gen_random_uuid())
            """;

        async Task InsertAsync(string id, string state)
        {
            await using var command = new NpgsqlCommand(insert, connection);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("state", state);
            await command.ExecuteNonQueryAsync();
        }

        await InsertAsync("r1", "joining");
        await Assert.ThrowsAsync<PostgresException>(() => InsertAsync("r2", "serving"));
        await Assert.ThrowsAsync<PostgresException>(() => InsertAsync("r1", "joining"));

        await using var read = new NpgsqlCommand("SELECT applied_seq, applied_token, serving_since, activated_at_seq, capabilities FROM reeve_cluster.member WHERE replica_id = 'r1'", connection);
        await using var reader = await read.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.True(reader.IsDBNull(1));
        Assert.True(reader.IsDBNull(2));
        Assert.True(reader.IsDBNull(3));
        Assert.Empty(reader.GetFieldValue<string[]>(4));
    }

    [Fact]
    public async Task The_fence_history_keeps_one_row_per_replica_and_epoch_and_reads_the_latest_span_by_replica()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await Runner(cs).MigrateAsync();
        await using var connection = await OpenAsync(cs);

        async Task FenceAsync(string id, long epoch, string trigger, string span)
        {
            await using var command = new NpgsqlCommand("INSERT INTO reeve_cluster.fence_history (replica_id, epoch, trigger, span) VALUES (@id, @epoch, @trigger, @span::interval)", connection);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("epoch", epoch);
            command.Parameters.AddWithValue("trigger", trigger);
            command.Parameters.AddWithValue("span", span);
            await command.ExecuteNonQueryAsync();
        }

        await FenceAsync("r1", 1, "renewal_timeout", "5 seconds");
        await FenceAsync("r1", 2, "zero_row_renewal", "90 seconds");
        await FenceAsync("r2", 1, "renewal_timeout", "1 second");
        await Assert.ThrowsAsync<PostgresException>(() => FenceAsync("r1", 2, "renewal_timeout", "1 second"));

        Assert.Equal(TimeSpan.FromSeconds(90), await ScalarAsync<TimeSpan>(connection, "SELECT span FROM reeve_cluster.fence_history WHERE replica_id = 'r1' ORDER BY epoch DESC LIMIT 1"));
        Assert.Equal(3L, await ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.fence_history"));
    }
}
