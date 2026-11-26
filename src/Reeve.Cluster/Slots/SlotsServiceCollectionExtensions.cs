using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Slots;

internal static class SlotsServiceCollectionExtensions
{
    public static IServiceCollection AddClusterSlots(this IServiceCollection services)
    {
        services.TryAddSingleton<SlotStore>(sp => new SlotStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<ISlotRows>(sp => sp.GetRequiredService<SlotStore>());
        services.TryAddSingleton<ISlotGate>(sp => new LocalSlotGate(sp.GetRequiredService<LocalPhaseGate>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<SlotScopes>();
        services.TryAddSingleton<SlotRegistry>(sp =>
        {
            var registry = new SlotRegistry(sp.GetRequiredService<TimeProvider>());
            ClusterDiagnostics.TrackSlots(registry, SlotStores.PostgreSql);
            return registry;
        });
        services.TryAddSingleton<ClusterSlots>(sp => new ClusterSlots(
            sp.GetRequiredService<ISlotRows>(),
            sp.GetRequiredService<ISlotGate>(),
            sp.GetRequiredService<SlotRegistry>(),
            sp.GetRequiredService<SlotScopes>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<ClusterSlots>>(),
            sp.GetService<ISlotObserver>() ?? new SlotMetricsObserver(SlotStores.PostgreSql)));
        services.TryAddSingleton<IClusterSlots>(sp => sp.GetRequiredService<ClusterSlots>());

        return services;
    }
}
