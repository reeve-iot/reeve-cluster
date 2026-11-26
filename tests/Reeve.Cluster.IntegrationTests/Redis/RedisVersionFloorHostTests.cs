using Int.Owned;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Redis;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Int.Redis;

public sealed class RedisVersionFloorHostTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;

    public RedisVersionFloorHostTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task Redis_6_2_4_refuses_the_start_of_a_host_with_only_a_Redis_claim_and_of_a_host_with_only_a_Redis_single_flight_before_the_join()
    {
        await using var server = new RedisBuilder("redis:6.2.4-alpine").Build();
        await server.StartAsync();
        await using var application = await ConnectionMultiplexer.ConnectAsync(server.GetConnectionString());
        Assert.Equal(new Version(6, 2, 4), application.GetServers().Single().Version);

        foreach (var primitive in new[] { "claim", "single-flight" })
        {
            var db = await OwnedDatabase.CreateAsync(postgres);
            using var host = BuildHost(db, application, primitive);

            var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => host.StartAsync().WaitAsync(Wait));

            Assert.Equal(ClusterConfigurationReason.RedisTooOld, refusal.Reason);
            Assert.Contains("6.2.4", refusal.Message, StringComparison.Ordinal);
            Assert.Equal(0, await MemberRowsAsync(db));
        }
    }

    [Fact]
    public async Task Redis_6_2_5_starts_a_host_with_only_a_Redis_claim_and_a_host_with_only_a_Redis_single_flight_and_they_join()
    {
        await using var server = new RedisBuilder("redis:6.2.5-alpine").Build();
        await server.StartAsync();
        await using var application = await ConnectionMultiplexer.ConnectAsync(server.GetConnectionString());
        Assert.Equal(new Version(6, 2, 5), application.GetServers().Single().Version);

        foreach (var primitive in new[] { "claim", "single-flight" })
        {
            var db = await OwnedDatabase.CreateAsync(postgres);
            using var host = BuildHost(db, application, primitive);
            try
            {
                await host.StartAsync().WaitAsync(Wait);

                Assert.Equal(1, await MemberRowsAsync(db));
            }
            finally
            {
                using var bound = new CancellationTokenSource(Wait);
                await host.StopAsync(bound.Token);
            }
        }
    }

    private static async Task<long> MemberRowsAsync(OwnedDatabase db)
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM reeve_cluster.member", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static IHost BuildHost(OwnedDatabase db, IConnectionMultiplexer application, string primitive)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(application);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = $"s{Guid.NewGuid():N}:");
        if (primitive == "claim")
        {
            builder.Services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);
        }
        else
        {
            builder.Services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse);
        }

        return builder.Build();
    }
}
