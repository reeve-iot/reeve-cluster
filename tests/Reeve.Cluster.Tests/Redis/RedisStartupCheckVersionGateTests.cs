using System.Net;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Unit.Redis;

public sealed class RedisStartupCheckVersionGateTests
{
    private static readonly UnitKind[] Fenced = [new("consumer", typeof(object), new UnitKindOptions { RedisFence = true })];
    private static readonly UnitKind[] Unfenced = [new("consumer", typeof(object), new UnitKindOptions())];

    [Theory]
    [InlineData("6.2.4")]
    [InlineData("6.0.20")]
    public async Task A_redis_below_6_2_5_starts_without_a_RedisFence_kind(string version)
    {
        await Check(version, Unfenced).StartingAsync(CancellationToken.None);
        await Check(version, []).StartingAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData("6.2.4", true)]
    [InlineData("6.2.4", false)]
    [InlineData("6.0.20", true)]
    public async Task The_same_redis_refuses_the_start_of_a_RedisFence_kind(string version, bool checkEvictionPolicy)
    {
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(
            () => Check(version, Fenced, checkEvictionPolicy).StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisTooOld, refusal.Reason);
        Assert.Contains("RedisFence needs 6.2.5", refusal.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("claim")]
    [InlineData("single-flight")]
    public async Task A_redis_below_6_2_5_refuses_the_start_where_a_Redis_claim_or_single_flight_is_registered(string primitive)
    {
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(
            () => Check("6.2.4", Unfenced, floors: [Requirement(primitive)]).StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisTooOld, refusal.Reason);
        Assert.Contains("6.2.5", refusal.Message, StringComparison.Ordinal);
        Assert.Contains(primitive, refusal.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("claim")]
    [InlineData("single-flight")]
    [InlineData("budget")]
    [InlineData("counter")]
    [InlineData("slot")]
    public async Task A_Redis_primitive_without_a_RedisFence_kind_reads_the_eviction_policy_and_the_opt_out_starts_without_reading_it(string primitive)
    {
        var floors = new[] { Requirement(primitive) };
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(
            () => Check("6.2.5", Unfenced, checkEvictionPolicy: true, floors: floors).StartingAsync(CancellationToken.None));
        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);

        await Check("6.2.5", Unfenced, checkEvictionPolicy: false, floors: floors).StartingAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData("budget")]
    [InlineData("counter")]
    public async Task A_budget_or_counter_registration_puts_the_policy_check_on_the_start_but_not_the_6_2_5_floor(string primitive)
    {
        var floors = new[] { Requirement(primitive) };

        await Check("6.2.4", Unfenced, checkEvictionPolicy: false, floors: floors).StartingAsync(CancellationToken.None);

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(
            () => Check("6.2.4", Unfenced, checkEvictionPolicy: true, floors: floors).StartingAsync(CancellationToken.None));
        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
    }

    [Theory]
    [InlineData("noeviction", true)]
    [InlineData("volatile-lru", false)]
    [InlineData("volatile-ttl", false)]
    [InlineData("allkeys-lru", false)]
    [InlineData("allkeys-lfu", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void Only_noeviction_passes_the_policy(string? policy, bool passes)
    {
        var refusal = RedisStartupCheck.EvaluatePolicy(new DnsEndPoint("redis", 6379), policy);

        if (passes)
        {
            Assert.Null(refusal);
            return;
        }

        Assert.NotNull(refusal);
        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
        Assert.Contains("noeviction", refusal.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("claims")]
    [InlineData("single-flight")]
    [InlineData("budget")]
    [InlineData("counter")]
    [InlineData("slots")]
    public async Task Each_Redis_primitive_registration_puts_the_policy_check_on_the_start(string primitive)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Fake<IConnectionMultiplexer>(member => member == nameof(IConnectionMultiplexer.GetServers)
            ? new[] { Fake<IServer>(serverMember => serverMember switch
            {
                nameof(IServer.IsConnected) => true,
                nameof(IServer.IsReplica) => false,
                nameof(IServer.Version) => Version.Parse("7.2.0"),
                nameof(IServer.EndPoint) => new DnsEndPoint("redis", 6379),
                _ => null,
            }) }
            : null));
        switch (primitive)
        {
            case "claims": services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse); break;
            case "single-flight": services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse); break;
            case "budget": services.AddReeveClusterRedisBudget(OnRedisUnavailable.Refuse); break;
            case "counter": services.AddReeveClusterRedisCounter(); break;
            default: services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse); break;
        }

        using var provider = services.BuildServiceProvider();
        var check = Assert.Single(provider.GetServices<IHostedService>()) as IHostedLifecycleService;
        Assert.NotNull(check);

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => check.StartingAsync(CancellationToken.None));
        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
    }

    [Theory]
    [InlineData("slot", true)]
    [InlineData("claim", false)]
    [InlineData("single-flight", false)]
    [InlineData("budget", false)]
    [InlineData("counter", false)]
    public async Task With_no_connected_primary_the_start_is_refused_where_a_slot_is_registered_and_only_warns_for_the_other_primitives(string primitive, bool refused)
    {
        var check = Check("6.2.5", Unfenced, connected: false, floors: [Requirement(primitive)]);

        if (!refused)
        {
            await check.StartingAsync(CancellationToken.None);
            return;
        }

        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => check.StartingAsync(CancellationToken.None));
        Assert.Equal(ClusterConfigurationReason.RedisEvictionPolicy, refusal.Reason);
        Assert.Contains("No connected Redis primary", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_claims_only_registration_with_no_connected_primary_warns_and_the_start_continues()
    {
        var logger = new CapturingLogger();

        await Check("6.2.5", Unfenced, connected: false, floors: [new RedisRequirement("claim")], logger: logger).StartingAsync(CancellationToken.None);

        Assert.Contains(logger.Warnings, m => m.Contains("Redis is not connected at start", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_claim_and_a_slot_registered_together_resolve_one_startup_check_and_a_redis_below_6_2_5_is_refused_naming_both()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Fake<IConnectionMultiplexer>(member => member == nameof(IConnectionMultiplexer.GetServers)
            ? new[] { Fake<IServer>(serverMember => serverMember switch
            {
                nameof(IServer.IsConnected) => true,
                nameof(IServer.IsReplica) => false,
                nameof(IServer.Version) => Version.Parse("6.0.20"),
                nameof(IServer.EndPoint) => new DnsEndPoint("redis", 6379),
                _ => null,
            }) }
            : null));
        services.AddReeveClusterRedisClaims(OnRedisUnavailable.Grant);
        services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);

        using var provider = services.BuildServiceProvider();
        var check = Assert.Single(provider.GetServices<IHostedService>()) as IHostedLifecycleService;
        Assert.NotNull(check);
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => check.StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisTooOld, refusal.Reason);
        Assert.Contains("the Redis claim", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("the Redis slot", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_claim_only_registration_resolves_the_startup_check_and_refuses_6_2_4()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Fake<IConnectionMultiplexer>(member => member == nameof(IConnectionMultiplexer.GetServers)
            ? new[] { Fake<IServer>(serverMember => serverMember switch
            {
                nameof(IServer.IsConnected) => true,
                nameof(IServer.IsReplica) => false,
                nameof(IServer.Version) => Version.Parse("6.2.4"),
                nameof(IServer.EndPoint) => new DnsEndPoint("redis", 6379),
                _ => null,
            }) }
            : null));
        services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);

        using var provider = services.BuildServiceProvider();
        var check = Assert.Single(provider.GetServices<IHostedService>()) as IHostedLifecycleService;
        Assert.NotNull(check);
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => check.StartingAsync(CancellationToken.None));

        Assert.Equal(ClusterConfigurationReason.RedisTooOld, refusal.Reason);
        Assert.Contains("6.2.4", refusal.Message, StringComparison.Ordinal);
        Assert.Contains("claim", refusal.Message, StringComparison.Ordinal);
    }

    private static RedisRequirement Requirement(string primitive) =>
        new(primitive, NeedsVersionFloor: primitive is not ("budget" or "counter"), RefusesUnreachable: primitive == "slot");

    private static RedisStartupCheck Check(string version, UnitKind[] kinds, bool checkEvictionPolicy = true, RedisRequirement[]? floors = null, bool connected = true, ILogger<RedisStartupCheck>? logger = null)
    {
        var primary = Fake<IServer>(member => member switch
        {
            nameof(IServer.IsConnected) => connected,
            nameof(IServer.IsReplica) => false,
            nameof(IServer.Version) => Version.Parse(version),
            nameof(IServer.EndPoint) => new DnsEndPoint("redis", 6379),
            _ => null,
        });
        var connection = Fake<IConnectionMultiplexer>(member => member == nameof(IConnectionMultiplexer.GetServers) ? new[] { primary } : null);
        return new RedisStartupCheck(
            connection,
            kinds,
            floors ?? [],
            Options.Create(new ClusterRedisOptions { CheckEvictionPolicy = checkEvictionPolicy }),
            logger ?? NullLogger<RedisStartupCheck>.Instance);
    }

    private sealed class CapturingLogger : ILogger<RedisStartupCheck>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }

    private static T Fake<T>(Func<string, object?> answer)
        where T : class
    {
        var proxy = DispatchProxy.Create<T, FakeProxy>();
        ((FakeProxy)(object)proxy).Answer = answer;
        return proxy;
    }

    public class FakeProxy : DispatchProxy
    {
        public Func<string, object?> Answer { get; set; } = _ => null;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var name = targetMethod!.Name;
            return Answer(name.StartsWith("get_", StringComparison.Ordinal) ? name[4..] : name);
        }
    }
}
