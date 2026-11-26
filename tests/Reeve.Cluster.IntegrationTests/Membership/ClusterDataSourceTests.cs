using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;

namespace Int.Membership;

public sealed class ClusterDataSourceTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public ClusterDataSourceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_connection_with_transaction_timeout_zero_on_the_primary_opens()
    {
        await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = postgres.ConnectionString });

        await using var connection = await source.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand("select 1", connection);

        Assert.Equal(1, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task A_non_zero_transaction_timeout_on_the_database_fails_the_first_connection_with_its_named_reason()
    {
        var database = new NpgsqlConnectionStringBuilder(postgres.ConnectionString).Database!;
        await using (var admin = await postgres.OpenConnectionAsync())
        await using (var alter = new NpgsqlCommand($"ALTER DATABASE \"{database}\" SET transaction_timeout = '30s'", admin))
        {
            await alter.ExecuteNonQueryAsync();
        }

        try
        {
            await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = postgres.ConnectionString });

            var ex = await Assert.ThrowsAsync<ClusterConfigurationException>(async () =>
            {
                await using var connection = await source.DataSource.OpenConnectionAsync();
            });

            Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, ex.Reason);
            Assert.Contains("30s", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var admin = await postgres.OpenConnectionAsync();
            await using var reset = new NpgsqlCommand($"ALTER DATABASE \"{database}\" RESET transaction_timeout", admin);
            await reset.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task A_synchronous_open_runs_the_same_check()
    {
        var connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Options = "-c transaction_timeout=5000" }.ConnectionString;
        await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = connectionString });

        var ex = Assert.Throws<ClusterConfigurationException>(() => source.DataSource.OpenConnection());

        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, ex.Reason);
    }

    [Fact]
    public async Task A_non_zero_transaction_timeout_in_the_connection_string_fails_the_same_way()
    {
        var connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Options = "-c transaction_timeout=5000" }.ConnectionString;
        await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = connectionString });

        var ex = await Assert.ThrowsAsync<ClusterConfigurationException>(async () =>
        {
            await using var connection = await source.DataSource.OpenConnectionAsync();
        });

        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, ex.Reason);
    }
}
