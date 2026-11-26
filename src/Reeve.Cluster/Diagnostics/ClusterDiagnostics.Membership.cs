using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster;

/// <summary>The instruments of leased membership.</summary>
public static partial class ClusterDiagnostics
{
    private static readonly ConditionalWeakTable<LocalPhaseGate, object?> Gates = new();
    private static readonly KeyValuePair<string, object?>[][] PhaseTags =
    [
        [new("phase", "starting")], [new("phase", "loading")], [new("phase", "catching_up")],
        [new("phase", "activated")], [new("phase", "serving")], [new("phase", "fencing")],
        [new("phase", "fenced")], [new("phase", "draining")], [new("phase", "exited")],
    ];

    internal static void TrackPhase(LocalPhaseGate gate) => Gates.Add(gate, null);
    internal static void UntrackPhase(LocalPhaseGate gate) => Gates.Remove(gate);

    internal const string ReplicaPhasesName = "reeve_cluster.replica.phase";
    internal const string FencesName = "reeve_cluster.fences";
    internal const string RenewalDurationName = "reeve_cluster.lease.renewal.duration";
    internal const string FullLoadDurationName = "reeve_cluster.applier.full_load.duration";
    internal const string RejoinsName = "reeve_cluster.rejoins";
    internal const string RejoinDurationName = "reeve_cluster.rejoin.duration";
    internal const string DrainDurationName = "reeve_cluster.drain.duration";

    internal const string DecisionRejoin = "rejoin";

    internal const string DecisionHigherEpoch = "higher_epoch";

    internal const string DrainDrained = "drained";

    internal const string DrainUnanswered = "unanswered";

    internal const string DrainTakenOver = "taken_over";

    private static IEnumerable<Measurement<int>> ObservePhases()
    {
        var counts = new int[9];
        foreach (var pair in Gates)
            counts[(int)pair.Key.Phase]++;
        for (var i = 0; i < counts.Length; i++)
            yield return new Measurement<int>(counts[i], PhaseTags[i]);
    }

    internal static readonly ObservableGauge<int> ReplicaPhases = Meter.CreateObservableGauge<int>(ReplicaPhasesName, ObservePhases, "{replica}", "Replicas by local phase.");

    internal static readonly Counter<long> Fences = Meter.CreateCounter<long>(FencesName, "{fence}", "Fences started at step 0.");

    internal static readonly Histogram<double> RenewalDuration = Meter.CreateHistogram<double>(RenewalDurationName, "s", "Lease renewal duration.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5] });

    internal static readonly Histogram<double> FullLoadDuration = Meter.CreateHistogram<double>(FullLoadDurationName, "s", "Full load duration.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600] });

    internal static readonly Counter<long> Rejoins = Meter.CreateCounter<long>(RejoinsName, "{rejoin}", "Rejoin decisions after a fence.");

    internal static readonly Histogram<double> RejoinDuration = Meter.CreateHistogram<double>(RejoinDurationName, "s", "Time out of service that a rejoin ended.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [1, 2.5, 5, 10, 30, 60, 90, 120, 180, 300, 600] });

    internal static readonly Histogram<double> DrainDuration = Meter.CreateHistogram<double>(DrainDurationName, "s", "Graceful stop duration.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 90, 120, 180] });

    internal const string FenceStepOverrunsName = "reeve_cluster.fence.step_overruns";

    internal static readonly Counter<long> FenceStepOverruns = Meter.CreateCounter<long>(
        FenceStepOverrunsName,
        unit: "{step}",
        description: "Fence steps not awaited past StopBudget.");
}
