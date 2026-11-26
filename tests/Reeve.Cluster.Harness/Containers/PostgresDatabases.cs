using Npgsql;

namespace Reeve.Cluster.Harness.Containers;

public static class PostgresDatabases
{
    public static async Task<string> CreateDatabaseAsync(this PostgresFixture fixture, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        return await CreateDatabaseAsync(fixture.ConnectionString, cancellationToken);
    }

    public static async Task<string> CreateDatabaseAsync(string serverConnectionString, CancellationToken cancellationToken = default)
    {
        var name = "t_" + Guid.NewGuid().ToString("N");

        await using (var admin = new NpgsqlConnection(serverConnectionString))
        {
            await admin.OpenAsync(cancellationToken);
            await using var create = new NpgsqlCommand($"CREATE DATABASE {name}", admin);
            await create.ExecuteNonQueryAsync(cancellationToken);
        }

        return new NpgsqlConnectionStringBuilder(serverConnectionString) { Database = name, Pooling = false }.ConnectionString;
    }
}
