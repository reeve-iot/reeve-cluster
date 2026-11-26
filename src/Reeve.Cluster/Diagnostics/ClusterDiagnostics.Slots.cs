using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Reeve.Cluster.Slots;

namespace Reeve.Cluster;

public static partial class ClusterDiagnostics
{
    internal const string SlotAcquiresName = "reeve_cluster.slot.acquires";

    internal const string SlotRenewalsName = "reeve_cluster.slot.renewals";

    internal const string SlotHeldName = "reeve_cluster.slot.held";

    private static readonly ConditionalWeakTable<SlotRegistry, string> SlotRegistries = new();

    internal static void TrackSlots(SlotRegistry registry, string store) => SlotRegistries.AddOrUpdate(registry, store);

    internal static void UntrackSlots(SlotRegistry registry) => SlotRegistries.Remove(registry);

    internal static readonly Counter<long> SlotAcquires = Meter.CreateCounter<long>(SlotAcquiresName, "{acquire}", "Answered slot acquires.");

    internal static readonly Counter<long> SlotRenewals = Meter.CreateCounter<long>(SlotRenewalsName, "{renewal}", "Answered slot renewals.");

    internal static readonly ObservableGauge<int> SlotHeld = Meter.CreateObservableGauge<int>(SlotHeldName, ObserveSlotHeld, "{slot}", "Slots this process holds.");

    private static IEnumerable<Measurement<int>> ObserveSlotHeld()
    {
        var byStore = new SortedDictionary<string, int>(StringComparer.Ordinal);
        foreach (var pair in SlotRegistries)
        {
            byStore[pair.Value] = byStore.GetValueOrDefault(pair.Value) + pair.Key.Live();
        }

        foreach (var (store, held) in byStore)
        {
            yield return new Measurement<int>(held, new KeyValuePair<string, object?>("store", store));
        }
    }
}
