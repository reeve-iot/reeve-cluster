using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Relay;
using Reeve.Cluster.Status;
using StackExchange.Redis;

namespace Microsoft.Extensions.DependencyInjection;

internal static class AskRelayRegistration
{
    public static IServiceCollection AddAskRelay(this IServiceCollection services)
    {
        if (services.Any(d => !d.IsKeyedService && d.ServiceType == typeof(AskRelay))
            || services.Any(d => !d.IsKeyedService && d.ImplementationInstance is ClusterAreaSelection selection && !selection.Has(ClusterAreas.Ask)))
        {
            return services;
        }

        var before = services.ToHashSet();
        services.TryAddSingleton<IAskWire>(sp => new RedisAskWire(sp.GetRequiredService<IConnectionMultiplexer>(), sp.GetRequiredService<IOptions<ClusterRedisOptions>>()));
        services.TryAddSingleton(sp => new AskRelay(
            sp.GetRequiredService<IAskWire>(),
            sp.GetRequiredService<IAskInbound>(),
            sp.GetRequiredService<MemberIdentity>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            AskRelayTimings.Default,
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<AskRelay>>()));
        services.Replace(ServiceDescriptor.Singleton<IAskTransport>(sp => sp.GetRequiredService<AskRelay>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, AskRelayStatusSection>());
        services.Insert(0, ServiceDescriptor.Singleton<IHostedService, AskRelayStartup>());

        if (!services.Any(d => !d.IsKeyedService && d.ImplementationInstance is ClusterAreaSelection))
        {
            services.AddSingleton(new AskAreaDescriptors([.. services.Where(d => !before.Contains(d))]));
        }

        return services;
    }
}
