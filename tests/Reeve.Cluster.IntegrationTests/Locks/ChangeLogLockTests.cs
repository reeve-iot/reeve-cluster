using Int.Migrations;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;

namespace Int.Locks;

public sealed class ChangeLogLockTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public ChangeLogLockTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_holder_of_the_change_log_key_blocks_an_append_whose_trigger_spells_the_key_as_a_literal()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await ClusterRunners.For(cs).MigrateAsync();
        await using var holder = await OpenAsync(cs);
        await using var holding = await holder.BeginTransactionAsync();
        await using (var take = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({ChangeLogLock.Key})", holder, holding))
        {
            await take.ExecuteNonQueryAsync();
        }

        await using var writer = await OpenAsync(cs);
        await using var appending = await writer.BeginTransactionAsync();
        await using (var bound = new NpgsqlCommand("SET LOCAL lock_timeout = '300ms'", writer, appending))
        {
            await bound.ExecuteNonQueryAsync();
        }

        await using var append = new NpgsqlCommand(
            "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k')", writer, appending);
        var error = await Assert.ThrowsAsync<PostgresException>(() => append.ExecuteNonQueryAsync());

        Assert.Equal(PostgresErrorCodes.LockNotAvailable, error.SqlState);
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}
