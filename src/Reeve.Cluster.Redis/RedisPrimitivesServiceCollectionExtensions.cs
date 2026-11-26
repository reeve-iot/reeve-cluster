using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using Reeve.Cluster.Slots;
using StackExchange.Redis;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the Redis primitives. They are separate from the PostgreSQL ones, which stay the
/// default: the caller picks the store by the interface it asks for, and a Redis primitive never falls back to PostgreSQL. The policy is a
/// required argument; a value that is not a policy, <c>default</c> included, is refused with a <see cref="ClusterConfigurationException"/>
/// (<see cref="ClusterConfigurationReason.RedisPolicyRequired"/>) and nothing is registered. These need the application's
/// <c>IConnectionMultiplexer</c> and <c>AddReeveCluster</c> with <c>ClusterAreas.Primitives</c>, whose serving check every one but <see cref="IRedisCounter"/> uses; the start is refused without that area; the key prefix is <see cref="ClusterRedisOptions.KeyPrefix"/>.
/// Once-runs have no Redis implementation.
/// </summary>
public static class RedisPrimitivesServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IRedisClaims"/>; where it is registered, the start refuses a Redis below 6.2.5 and, unless
    /// <see cref="ClusterRedisOptions.CheckEvictionPolicy"/> is off, a <c>maxmemory-policy</c> other than <c>noeviction</c>, since the
    /// claim key's TTL is the grant and an evicted key ends it early.
    /// </summary>
    /// <exception cref="ClusterConfigurationException">The value is not a policy.</exception>
    public static IServiceCollection AddReeveClusterRedisClaims(this IServiceCollection services, OnRedisUnavailable onRedisUnavailable)
    {
        ArgumentNullException.ThrowIfNull(services);
        var policy = RedisDecisions.RequirePolicy(onRedisUnavailable);
        AddOnce<IRedisClaims>(services, sp => new RedisClaims(sp.GetRequiredService<RedisClaimStore>(), sp.GetRequiredService<IClaimGate>(), policy));
        return AddRequirement(AddClaimStore(services), "claim");
    }

    /// <summary>
    /// Registers <see cref="IRedisSingleFlight"/>; where it is registered, the start refuses a Redis below 6.2.5 and, unless
    /// <see cref="ClusterRedisOptions.CheckEvictionPolicy"/> is off, a <c>maxmemory-policy</c> other than <c>noeviction</c>, since an
    /// evicted flight or result key ends the flight or loses its result.
    /// </summary>
    /// <exception cref="ClusterConfigurationException">The value is not a policy.</exception>
    public static IServiceCollection AddReeveClusterRedisSingleFlight(this IServiceCollection services, OnRedisUnavailable onRedisUnavailable)
    {
        ArgumentNullException.ThrowIfNull(services);
        var policy = RedisDecisions.RequirePolicy(onRedisUnavailable);
        AddOnce<IRedisSingleFlight>(
            services,
            sp => new RedisSingleFlight(sp.GetRequiredService<RedisClaimStore>(), sp.GetRequiredService<IClaimGate>(), policy, sp.GetRequiredService<TimeProvider>()));
        return AddRequirement(AddClaimStore(services), "single-flight");
    }

    /// <summary>
    /// Registers <see cref="IRedisSlots"/>, which needs <c>AddReeveCluster</c> for the serving check and the slot holder check; where it
    /// is registered, the start refuses a Redis below 6.2.5 and, unless <see cref="ClusterRedisOptions.CheckEvictionPolicy"/> is off, a
    /// <c>maxmemory-policy</c> other than <c>noeviction</c>, since an evicted slot set restarts the count and lifts the limit.
    /// </summary>
    /// <exception cref="ClusterConfigurationException">The value is not a policy.</exception>
    public static IServiceCollection AddReeveClusterRedisSlots(this IServiceCollection services, OnRedisUnavailable onRedisUnavailable)
    {
        ArgumentNullException.ThrowIfNull(services);
        var policy = RedisDecisions.RequirePolicy(onRedisUnavailable);
        services.AddOptions<ClusterRedisOptions>();
        AddOnce<IRedisSlots>(
            services,
            sp =>
            {
                var time = sp.GetRequiredService<TimeProvider>();
                var registry = new SlotRegistry(time);
                ClusterDiagnostics.TrackSlots(registry, SlotStores.Redis);
                return new RedisSlots(
                    new RedisSlotStore(sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase(), sp.GetRequiredService<IOptions<ClusterRedisOptions>>().Value.KeyPrefix),
                    sp.GetRequiredService<ISlotGate>(),
                    registry,
                    policy,
                    sp.GetRequiredService<IOptions<ClusterOptions>>(),
                    time,
                    sp.GetService<ILogger<RedisSlots>>() ?? NullLogger<RedisSlots>.Instance,
                    new SlotMetricsObserver(SlotStores.Redis));
            });
        return AddRequirement(services, "slot", refusesUnreachable: true);
    }

    /// <summary>
    /// Registers <see cref="IRedisBudget"/>; where it is registered, the start refuses, unless
    /// <see cref="ClusterRedisOptions.CheckEvictionPolicy"/> is off, a <c>maxmemory-policy</c> other than <c>noeviction</c>, since an
    /// evicted window restarts its count past the limit. It puts no version floor on the start.
    /// </summary>
    /// <exception cref="ClusterConfigurationException">The value is not a policy.</exception>
    public static IServiceCollection AddReeveClusterRedisBudget(this IServiceCollection services, OnRedisUnavailable onRedisUnavailable)
    {
        ArgumentNullException.ThrowIfNull(services);
        var policy = RedisDecisions.RequirePolicy(onRedisUnavailable);
        services.AddOptions<ClusterRedisOptions>();
        AddOnce<IRedisBudget>(
            services,
            sp => new RedisBudget(
                new RedisBudgetStore(sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase(), sp.GetRequiredService<IOptions<ClusterRedisOptions>>().Value.KeyPrefix),
                sp.GetRequiredService<IClaimGate>(),
                policy,
                sp.GetService<ILogger<RedisBudget>>() ?? NullLogger<RedisBudget>.Instance));
        return AddRequirement(services, "budget", needsVersionFloor: false);
    }

    /// <summary>
    /// Registers <see cref="IRedisCounter"/>, which takes no policy: it grants nothing, and an unanswered increment is <see cref="CounterResult.Unknown"/>.
    /// Where it is registered, the start refuses, unless <see cref="ClusterRedisOptions.CheckEvictionPolicy"/> is off, a
    /// <c>maxmemory-policy</c> other than <c>noeviction</c>, since an evicted counter restarts at zero. It puts no version floor on the start.
    /// </summary>
    public static IServiceCollection AddReeveClusterRedisCounter(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddOptions<ClusterRedisOptions>();
        AddOnce<IRedisCounter>(
            services,
            sp => new RedisCounter(
                new RedisCounterStore(
                    sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase(),
                    sp.GetRequiredService<IOptions<ClusterRedisOptions>>().Value.KeyPrefix,
                    sp.GetService<IOptions<ClusterOptions>>()?.Value.CounterRetention)));
        return AddRequirement(services, "counter", needsVersionFloor: false);
    }

    private static void AddOnce<T>(IServiceCollection services, Func<IServiceProvider, T> factory)
        where T : class
    {
        if (services.Any(d => d.ServiceType == typeof(T)))
        {
            throw new InvalidOperationException($"{typeof(T).Name} is registered already.");
        }

        services.AddSingleton(factory);

        services.AddSingleton(new PrimitivesRequested(typeof(T).Name));
    }

    private static IServiceCollection AddClaimStore(IServiceCollection services)
    {
        services.AddOptions<ClusterRedisOptions>();
        services.TryAddSingleton(sp => new RedisClaimStore(
            sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase(), sp.GetRequiredService<IOptions<ClusterRedisOptions>>().Value.KeyPrefix));
        return services;
    }

    private static IServiceCollection AddRequirement(IServiceCollection services, string primitive, bool needsVersionFloor = true, bool refusesUnreachable = false)
    {
        services.AddSingleton(new RedisRequirement(primitive, needsVersionFloor, refusesUnreachable));
        services.AddOptions<ClusterRedisOptions>();
        return services.AddRedisStartupCheck();
    }
}
