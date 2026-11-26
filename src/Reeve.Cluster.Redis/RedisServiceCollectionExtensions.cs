using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Fence;
using Reeve.Cluster.Wakeup;
using StackExchange.Redis;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the Redis package.</summary>
public static class RedisServiceCollectionExtensions
{
    /// <summary>
    /// Replaces the library's in-process wake-up channel with the Redis one, so a write's wake-up and an applied notice reach the
    /// other replicas at once instead of at their next poll. The application registers the <see cref="IConnectionMultiplexer"/>
    /// itself; it may be called before or after <c>AddReeveCluster</c>. The kinds that declare <c>RedisFence</c> get their
    /// <c>FenceRedis</c> raised and refreshed through it. When a kind declares <c>RedisFence</c>, the start fails with a
    /// <see cref="Reeve.Cluster.ClusterConfigurationException"/> if a connected Redis primary runs a version below 6.2.5, which
    /// <c>RedisFence</c>, Redis claims and Redis single-flight require. When a kind declares <c>RedisFence</c>, or a Redis claim, single-flight, budget, counter or slot is registered, it also refuses a
    /// <c>maxmemory-policy</c> other than <c>noeviction</c> or one it cannot read (<see cref="ClusterRedisOptions.CheckEvictionPolicy"/>).
    /// A claim, single-flight or slot registration adds the same version floor; without a fence kind or one of these registrations, nothing
    /// is checked. It also makes the <c>Ask</c> relay the transport of requests to other processes: the host builds it at start and
    /// subscribes two Redis channels of this process (<see cref="ClusterRedisOptions.KeyPrefix"/> + <c>ask:</c>), kept until the process exits.
    /// Where <c>AddReeveCluster</c> leaves <c>ClusterAreas.Ask</c> off, in either call order, there is no relay and no <c>Ask</c> relay subscription.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets the options, for example the key prefix.</param>
    public static IServiceCollection AddReeveClusterRedis(this IServiceCollection services, Action<ClusterRedisOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<ClusterRedisOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.Replace(ServiceDescriptor.Singleton<IWakeupChannel, RedisWakeupChannel>());

        services.Replace(ServiceDescriptor.Singleton<ITenureFence, RedisTenureFences>());

        services.AddAskRelay();

        services.AddRedisStartupCheck();

        return services;
    }

    internal static IServiceCollection AddRedisStartupCheck(this IServiceCollection services)
    {
        if (!services.Any(d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(RedisStartupCheck)))
        {
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService, RedisStartupCheck>());
        }

        return services;
    }
}
