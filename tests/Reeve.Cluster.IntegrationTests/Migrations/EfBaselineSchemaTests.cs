using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Schema;
using Reeve.Cluster.Migrations;

namespace Int.Migrations;

public sealed class EfBaselineSchemaTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public EfBaselineSchemaTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I25_every_sequence_of_the_EF_built_schema_is_CACHE_1()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await MigrateWithEfAsync(cs);

        await using var connection = await OpenAsync(cs);
        await SchemaAssertions.EverySequenceIsCacheOneAsync(connection);
    }

    [Fact]
    public async Task The_baseline_is_recorded_in_the_clusters_own_history_table_and_not_in_EFs_default()
    {
        var cs = await postgres.CreateDatabaseAsync();
        await MigrateWithEfAsync(cs);

        await using var connection = await OpenAsync(cs);
        var ids = await RowsAsync(connection, "SELECT \"MigrationId\" FROM reeve_cluster.\"__ClusterMigrationsHistory\" ORDER BY \"MigrationId\"");
        var inTheAssembly = typeof(ClusterMigrationsDbContext).Assembly.GetTypes()
            .Select(t => t.GetCustomAttribute<Microsoft.EntityFrameworkCore.Migrations.MigrationAttribute>()?.Id)
            .OfType<string>()
            .Order(StringComparer.Ordinal)
            .ToList();

        Assert.Equal(inTheAssembly, ids);
        Assert.EndsWith("_ClusterInitialBaseline", ids[0], StringComparison.Ordinal);
        Assert.Equal("1", (await RowsAsync(connection, "SELECT count(*) FROM pg_class WHERE relname = '__ClusterMigrationsHistory'")).Single());
        Assert.Empty(await RowsAsync(connection, "SELECT 1 FROM pg_class WHERE relname = '__EFMigrationsHistory'"));
    }

    private static async Task MigrateWithEfAsync(string connectionString)
    {
        await using var context = new ClusterMigrationsDbContext(ClusterMigrationsDesignTimeFactory.Options(connectionString));
        await context.Database.MigrateAsync();
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string query)
    {
        await using var command = new NpgsqlCommand(query, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            rows.Add(reader.IsDBNull(0) ? "<null>" : Convert.ToString(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture)!);
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
