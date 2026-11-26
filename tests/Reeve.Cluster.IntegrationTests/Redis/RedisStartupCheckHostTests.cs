using System.Net;
using System.Net.Sockets;
using Int.Owned;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class RedisStartupCheckHostTests : IClassFixture<PostgresFixture>, IClassFixture<RedisFixture>
{
    private const string Kind = "consumer";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly RedisFixture redis;

    public RedisStartupCheckHostTests(PostgresFixture postgres, RedisFixture redis)
    {
        this.postgres = postgres;
        this.redis = redis;
    }

    [Fact]
    public async Task A_start_refused_for_an_eviction_policy_leaves_no_member_row_and_no_lease()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var application = await redis.ConnectAsync();
        await using var admin = await ConnectAdminAsync();
        var server = admin.GetServers().Single();
        try
        {
            await server.ConfigSetAsync("maxmemory-policy", "allkeys-lru");
            using var host = BuildHost(db, application);

            var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => host.StartAsync().WaitAsync(Wait));

            Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
            await AssertNothingLeftBehindAsync(db, host);
        }
        finally
        {
            await server.ConfigSetAsync("maxmemory-policy", "noeviction");
        }
    }

    [Fact]
    public async Task A_start_refused_because_a_listed_server_is_not_reached_leaves_no_member_row_and_no_lease()
    {
        using var silent = new TcpListener(IPAddress.Loopback, 0);
        silent.Start();
        var options = ConfigurationOptions.Parse(redis.ConnectionString);
        options.EndPoints.Add(new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)silent.LocalEndpoint).Port));
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 4000;
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var application = await ConnectionMultiplexer.ConnectAsync(options);
        using var host = BuildHost(db, application);

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => host.StartAsync().WaitAsync(Wait));

        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
        await AssertNothingLeftBehindAsync(db, host);
    }

    [Fact]
    public async Task The_same_host_with_a_safe_policy_joins_and_its_member_row_is_seen_by_the_probe()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var application = await redis.ConnectAsync();
        using var host = BuildHost(db, application);
        try
        {
            await host.StartAsync().WaitAsync(Wait);

            Assert.Equal(1, await MemberRowsAsync(db, live: false));
            Assert.Equal(1, await MemberRowsAsync(db, live: true));
        }
        finally
        {
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
        }
    }

    [Fact]
    public async Task A_refused_start_never_calls_StartAsync_of_a_hosted_service_registered_after_the_library()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var application = await redis.ConnectAsync();
        await using var admin = await ConnectAdminAsync();
        var server = admin.GetServers().Single();
        var probe = new StartProbe();
        try
        {
            await server.ConfigSetAsync("maxmemory-policy", "allkeys-lru");
            using var host = BuildHost(db, application, services => services.AddSingleton<IHostedService>(probe));

            await Assert.ThrowsAsync<ClusterConfigurationException>(() => host.StartAsync().WaitAsync(Wait));

            Assert.False(probe.Started);
        }
        finally
        {
            await server.ConfigSetAsync("maxmemory-policy", "noeviction");
        }
    }

    private static async Task AssertNothingLeftBehindAsync(OwnedDatabase db, IHost host)
    {
        Assert.Equal(0, await MemberRowsAsync(db, live: false));
        Assert.Equal(0, await MemberRowsAsync(db, live: true));
        Assert.NotEqual(ReplicaPhase.Serving, host.Services.GetRequiredService<LocalPhaseGate>().Phase);
    }

    private static async Task<long> MemberRowsAsync(OwnedDatabase db, bool live)
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = live
            ? "SELECT count(*) FROM reeve_cluster.member WHERE replica_id = 'r1' AND lease_expires_at > clock_timestamp()"
            : "SELECT count(*) FROM reeve_cluster.member";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static IHost BuildHost(OwnedDatabase db, IConnectionMultiplexer application, Action<IServiceCollection>? extra = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(new FenceRedisHostTests.FencedUnit { Redis = application });
        builder.Services.AddSingleton(application);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = $"s{Guid.NewGuid():N}:");
        builder.Services.AddClusterKeyedResource<FenceRedisHostTests.FencedUnit>(Kind, o =>
        {
            o.Policy = AdoptionPolicy.None;
            o.RedisFence = true;
        });
        extra?.Invoke(builder.Services);
        return builder.Build();
    }

    private sealed class StartProbe : IHostedService
    {
        public volatile bool Started;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Started = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private async Task<ConnectionMultiplexer> ConnectAdminAsync()
    {
        var options = ConfigurationOptions.Parse(redis.ConnectionString);
        options.AllowAdmin = true;
        return await ConnectionMultiplexer.ConnectAsync(options);
    }
}
