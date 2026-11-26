using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Testcontainers.PostgreSql;

namespace Int.Membership;

public sealed class ClusterDataSourceStandbyTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder(PostgresFixture.Image).Build();

    public Task InitializeAsync() => container.StartAsync();

    public Task DisposeAsync() => container.DisposeAsync().AsTask();

    [Fact]
    public async Task I24_a_connection_to_a_standby_fails_with_NotPrimary()
    {
        var signal = await container.ExecAsync(["bash", "-c", "touch \"$PGDATA/standby.signal\""]);
        Assert.True(signal.ExitCode == 0, signal.Stderr);
        await container.StopAsync();
        await container.StartAsync();

        var connectionString = new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Pooling = false }.ConnectionString;
        await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = connectionString });
        var fault = await Assert.ThrowsAsync<ClusterConfigurationException>(async () =>
        {
            await using var connection = await source.DataSource.OpenConnectionAsync();
        });

        Assert.Equal(ClusterConfigurationReason.NotPrimary, fault.Reason);
    }
}
