using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster;

/// <summary>The instruments of owned work.</summary>
public static partial class ClusterDiagnostics
{
    private static readonly ConditionalWeakTable<UnitRuntime, object?> Runtimes = new();

    internal static void TrackRuntime(UnitRuntime runtime) => Runtimes.Add(runtime, null);
    internal static void UntrackRuntime(UnitRuntime runtime) => Runtimes.Remove(runtime);

    internal const string OwnedRunningName = "reeve_cluster.owned.running";

    private static IEnumerable<Measurement<int>> ObserveOwnedRunning()
    {
        foreach (var pair in Runtimes)
        {
            foreach (var (kind, running) in pair.Key.RunningByKind())
            {
                yield return new Measurement<int>(running, new KeyValuePair<string, object?>("kind", kind));
            }
        }
    }

    internal static readonly ObservableGauge<int> OwnedRunning = Meter.CreateObservableGauge(
        OwnedRunningName, ObserveOwnedRunning, "{tenure}", "Tenures this replica runs, by kind.");

    internal const string TakeoverDurationName = "reeve_cluster.owned.takeover.duration";
    internal static readonly Histogram<double> TakeoverDuration = Meter.CreateHistogram<double>(TakeoverDurationName, "s", "Ownership takeover duration.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60, 120] });
    internal const string OverdueReleasesName = "reeve_cluster.owned.release_overdue";

    internal static readonly UpDownCounter<long> OverdueReleases = Meter.CreateUpDownCounter<long>(
        OverdueReleasesName,
        unit: "{request}",
        description: "Release requests in force past ReleaseGrace, as the placer last read them.");
}
