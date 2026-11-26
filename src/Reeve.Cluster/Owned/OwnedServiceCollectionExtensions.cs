using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Owned;

internal static class OwnedServiceCollectionExtensions
{
    public static IServiceCollection AddClusterOwned(this IServiceCollection services)
    {
        services.TryAddSingleton<OwnedDataSource>();
        services.TryAddSingleton<OwnedStore>();
        services.TryAddSingleton<IUnitRows>(sp => sp.GetRequiredService<OwnedStore>());

        services.TryAddSingleton<UnitRuntime>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, UnitRuntime>(sp => sp.GetRequiredService<UnitRuntime>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IFenceParticipant, UnitFenceParticipant>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, OwnedStatusSection>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDrainParticipant, UnitDrainParticipant>());

        services.TryAddSingleton<ITenureFence, NoTenureFence>();

        services.TryAddSingleton<ICreatorRows>(sp => sp.GetRequiredService<OwnedStore>());
        services.TryAddSingleton<IKeyedResources>(sp => new KeyedResources(
            sp.GetServices<UnitKind>(),
            sp.GetRequiredService<ICreatorRows>(),
            sp.GetRequiredService<LocalPhaseGate>(),
            sp.GetRequiredService<UnitRuntime>().Nudge,
            sp.GetRequiredService<UnitRuntime>().Holds,
            () => sp.GetRequiredService<ClusterCapabilitiesRegistry>().Current,
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<KeyedResources>>()));
        if (!services.Any(d => d.ImplementationInstance is UnitKind { Kind: FreeRowSweep.Kind }))
        {
            services.AddClusterUnit<FreeRowSweep>(FreeRowSweep.Kind, o => o.Convergent = true);
        }

        services.TryAddSingleton<ReleaseOverdueReport>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, ReleaseOverdueReport>(sp => sp.GetRequiredService<ReleaseOverdueReport>()));
        services.TryAddSingleton<PlacerUnit>();
        services.TryAddSingleton<PlacerAdopter>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PlacerAdopter>(sp => sp.GetRequiredService<PlacerAdopter>()));
        return services;
    }
}
