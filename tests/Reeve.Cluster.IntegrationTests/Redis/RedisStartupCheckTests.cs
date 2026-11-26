using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class RedisStartupCheckTests : IClassFixture<RedisFixture>
{
    private static readonly EndPoint At = new DnsEndPoint("redis", 6379);
    private static readonly UnitKind[] Fenced = [new("consumer", typeof(object), new UnitKindOptions { RedisFence = true })];

    private readonly RedisFixture redis;

    public RedisStartupCheckTests(RedisFixture redis) => this.redis = redis;

    [Theory]
    [InlineData("6.2.5", null)]
    [InlineData("8.10.2", null)]
    [InlineData("6.2.4", ClusterConfigurationReason.RedisTooOld)]
    [InlineData("6.0.20", ClusterConfigurationReason.RedisTooOld)]
    public void A_primary_below_6_2_5_is_refused(string version, ClusterConfigurationReason? reason) =>
        Assert.Equal(reason, RedisStartupCheck.EvaluateVersion(At, Version.Parse(version))?.Reason);

    [Theory]
    [InlineData("noeviction", true)]
    [InlineData("volatile-lru", false)]
    [InlineData("volatile-ttl", false)]
    [InlineData("allkeys-lru", false)]
    [InlineData("allkeys-random", false)]
    [InlineData(null, false)]
    public void Only_noeviction_passes(string? policy, bool passes) =>
        Assert.Equal(passes, RedisStartupCheck.EvaluatePolicy(At, policy) is null);

    [Fact]
    public async Task An_allkeys_or_volatile_policy_fails_the_start_of_a_RedisFence_application_whose_connection_has_no_AllowAdmin()
    {
        await using var admin = await ConnectAdminAsync();
        var server = admin.GetServers().Single();
        await using var application = await redis.ConnectAsync();
        try
        {
            await server.ConfigSetAsync("maxmemory-policy", "allkeys-lru");
            var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Check(application).StartingAsync(CancellationToken.None));
            Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
            Assert.Contains(nameof(ClusterRedisOptions.CheckEvictionPolicy), refusal.Message, StringComparison.Ordinal);

            await Check(application, kinds: []).StartingAsync(CancellationToken.None);
            await Check(application, checkEvictionPolicy: false).StartingAsync(CancellationToken.None);

            await server.ConfigSetAsync("maxmemory-policy", "volatile-lfu");
            var volatileRefusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Check(application).StartingAsync(CancellationToken.None));
            Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, volatileRefusal.Reason);

            var claimRefusal = await Assert.ThrowsAsync<ClusterConfigurationException>(
                () => Check(application, kinds: [], requirements: [new RedisRequirement("claim")]).StartingAsync(CancellationToken.None));
            Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, claimRefusal.Reason);
        }
        finally
        {
            await server.ConfigSetAsync("maxmemory-policy", "noeviction");
        }

        await Check(application).StartingAsync(CancellationToken.None);
    }

    [Fact]
    public async Task A_policy_the_check_cannot_read_fails_the_start()
    {
        await using var admin = await ConnectAdminAsync();
        var server = admin.GetServers().Single();
        await server.ExecuteAsync("ACL", "SETUSER", "reader", "on", ">secret", "~*", "&*", "+@all");
        await server.ExecuteAsync("ACL", "SETUSER", "noinfo", "on", ">secret", "~*", "&*", "+@all", "-info");
        try
        {
            await using var reader = await ConnectAsUserAsync("reader");
            await Check(reader).StartingAsync(CancellationToken.None);

            await using var noinfo = await ConnectAsUserAsync("noinfo");
            var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Check(noinfo).StartingAsync(CancellationToken.None));
            Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
            Assert.Contains(nameof(ClusterRedisOptions.CheckEvictionPolicy), refusal.Message, StringComparison.Ordinal);
        }
        finally
        {
            await server.ExecuteAsync("ACL", "DELUSER", "reader", "noinfo");
        }
    }

    [Fact]
    public async Task A_redis_that_is_not_connected_at_start_refuses_a_RedisFence_start_when_the_policy_check_is_on()
    {
        var options = ConfigurationOptions.Parse("127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 100;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(options);

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Check(connection).StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_redis_that_is_not_connected_at_start_is_only_a_warning_without_a_RedisFence_kind_or_with_the_check_off(bool withoutFenceKind)
    {
        var options = ConfigurationOptions.Parse("127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 100;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(options);

        if (withoutFenceKind)
        {
            await Check(connection, kinds: []).StartingAsync(CancellationToken.None);
        }
        else
        {
            await Check(connection, checkEvictionPolicy: false).StartingAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task A_listed_server_that_is_not_reached_refuses_a_RedisFence_start_and_names_it()
    {
        var options = ConfigurationOptions.Parse(redis.ConnectionString);
        options.EndPoints.Add("127.0.0.1", 1);
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 1000;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(options);

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Check(connection).StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
        Assert.Contains("127.0.0.1:1", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("not connected", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_check_is_the_first_hosted_service_also_when_the_package_is_added_after_the_library()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHostedService, Placeholder>();
        services.AddReeveClusterRedis();
        services.AddReeveClusterRedis();

        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).ToList();
        Assert.Equal(typeof(RedisStartupCheck), hosted[0].ImplementationType);
        Assert.Single(hosted, d => d.ImplementationType == typeof(RedisStartupCheck));
    }

    private static RedisStartupCheck Check(IConnectionMultiplexer connection, UnitKind[]? kinds = null, bool checkEvictionPolicy = true, RedisRequirement[]? requirements = null) =>
        new(
            connection,
            kinds ?? Fenced,
            requirements ?? [],
            Options.Create(new ClusterRedisOptions { CheckEvictionPolicy = checkEvictionPolicy }),
            NullLogger<RedisStartupCheck>.Instance);

    private async Task<ConnectionMultiplexer> ConnectAdminAsync()
    {
        var options = ConfigurationOptions.Parse(redis.ConnectionString);
        options.AllowAdmin = true;
        return await ConnectionMultiplexer.ConnectAsync(options);
    }

    private async Task<ConnectionMultiplexer> ConnectAsUserAsync(string user)
    {
        var options = ConfigurationOptions.Parse(redis.ConnectionString);
        options.User = user;
        options.Password = "secret";
        return await ConnectionMultiplexer.ConnectAsync(options);
    }

    private sealed class Placeholder : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
