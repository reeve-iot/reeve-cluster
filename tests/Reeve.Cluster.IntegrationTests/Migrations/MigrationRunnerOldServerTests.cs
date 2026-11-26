using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Migrations;

public sealed class MigrationRunnerOldServerTests : IClassFixture<Postgres17Fixture>
{
    private readonly Postgres17Fixture postgres;

    public MigrationRunnerOldServerTests(Postgres17Fixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task PostgreSQL_17_is_refused_with_its_named_reason_and_nothing_is_written()
    {
        var cs = await PostgresDatabases.CreateDatabaseAsync(postgres.ConnectionString);
        var runner = ClusterRunners.For(cs);

        var ex = await Assert.ThrowsAsync<ClusterConfigurationException>(() => runner.MigrateAsync());

        Assert.Equal(ClusterConfigurationReason.PostgreSqlTooOld, ex.Reason);
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_namespace WHERE nspname = 'reeve_cluster'", connection);
        Assert.Equal(0L, (long)(await command.ExecuteScalarAsync())!);
    }
}
