using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Reeve.Cluster.Log;

namespace Reeve.Cluster;

/// <summary>The instruments of the change log, the generations, the pins and the applier's attempts.</summary>
public static partial class ClusterDiagnostics
{
    private static readonly ConditionalWeakTable<Applier, object?> Appliers = new();
    private static readonly ConditionalWeakTable<GenerationStore, object?> Stores = new();
    private static readonly KeyValuePair<string, object?>[] CurrentPins = [new("generation", GenerationCurrent)];
    private static readonly KeyValuePair<string, object?>[] SupersededPins = [new("generation", GenerationSuperseded)];

    internal static void TrackApplier(Applier applier) => Appliers.Add(applier, null);
    internal static void UntrackApplier(Applier applier) => Appliers.Remove(applier);
    internal static void TrackStore(GenerationStore store) => Stores.Add(store, null);

    internal const string ChangeLogHeadName = "reeve_cluster.change_log.head";
    internal const string ChangeLogHorizonName = "reeve_cluster.change_log.horizon";
    internal const string ChangeLogRetainedName = "reeve_cluster.change_log.retained";
    internal const string GenerationHeadName = "reeve_cluster.generation.head";
    internal const string GenerationPinsName = "reeve_cluster.generation.pins";
    internal const string AttemptDurationName = "reeve_cluster.applier.attempt.duration";
    internal const string DeferredRunningName = "reeve_cluster.applier.deferred.running";
    internal const string DeferredOldestAgeName = "reeve_cluster.applier.deferred.oldest_age";
    internal const string InvalidCursorReloadsName = "reeve_cluster.applier.invalid_cursor_reloads";

    internal const string GenerationCurrent = "current";

    internal const string GenerationSuperseded = "superseded";

    internal const string OutcomeAccepted = "accepted";

    internal const string OutcomeFailed = "failed";

    internal const string OutcomeTimedOut = "timed_out";

    internal const string PassFull = "full";

    internal const string PassIncremental = "incremental";

    private static IEnumerable<Measurement<long>> ObserveChangeLogHead()
    {
        foreach (var pair in Appliers)
        {
            var head = pair.Key.LastHead;
            if (head != Applier.Unread)
            {
                yield return new Measurement<long>(head);
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveChangeLogHorizon()
    {
        foreach (var pair in Appliers)
        {
            var horizon = pair.Key.LastHorizon;
            if (horizon != Applier.Unread)
            {
                yield return new Measurement<long>(horizon);
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveChangeLogRetained()
    {
        foreach (var pair in Appliers)
        {
            var head = pair.Key.LastHead;
            var horizon = pair.Key.LastHorizon;
            if (head != Applier.Unread && horizon != Applier.Unread)
            {
                yield return new Measurement<long>(horizon == 0 ? Math.Max(head, 0) : Math.Max(head - horizon + 1, 0));
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveGenerationHead()
    {
        foreach (var pair in Stores)
        {
            if (pair.Key.Current is { } current)
            {
                yield return new Measurement<long>(current.Head);
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveGenerationPins()
    {
        foreach (var pair in Stores)
        {
            if (pair.Key.Current is Generation current)
            {
                var onCurrent = current.Pins;
                yield return new Measurement<long>(onCurrent, CurrentPins);
                yield return new Measurement<long>(Math.Max(0, pair.Key.Pins - onCurrent), SupersededPins);
            }
        }
    }

    private static IEnumerable<Measurement<int>> ObserveDeferredRunning()
    {
        foreach (var pair in Stores)
        {
            var store = pair.Key;
            var running = new int[store.AggregateTypes.Count];
            store.ReadDeferred(running, new long[running.Length]);
            for (var i = 0; i < running.Length; i++)
            {
                yield return new Measurement<int>(running[i], store.TypeTags[i]);
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveDeferredOldestAge()
    {
        foreach (var pair in Stores)
        {
            var store = pair.Key;
            var running = new int[store.AggregateTypes.Count];
            var oldest = new long[running.Length];
            store.ReadDeferred(running, oldest);
            for (var i = 0; i < running.Length; i++)
            {
                yield return new Measurement<double>(running[i] == 0 ? 0 : store.Time.GetElapsedTime(oldest[i]).TotalSeconds, store.TypeTags[i]);
            }
        }
    }

    internal static readonly ObservableGauge<long> ChangeLogHead = Meter.CreateObservableGauge(
        ChangeLogHeadName, ObserveChangeLogHead, "{seq}", "Change-log head as the applier last read it.");

    internal static readonly ObservableGauge<long> ChangeLogHorizon = Meter.CreateObservableGauge(
        ChangeLogHorizonName, ObserveChangeLogHorizon, "{seq}", "Change-log horizon as the applier last read it.");

    internal static readonly ObservableGauge<long> ChangeLogRetained = Meter.CreateObservableGauge(
        ChangeLogRetainedName, ObserveChangeLogRetained, "{entry}", "Change-log entries between the horizon and the head, as the applier last read them.");

    internal static readonly ObservableGauge<long> GenerationHead = Meter.CreateObservableGauge(
        GenerationHeadName, ObserveGenerationHead, "{seq}", "Head of the current generation.");

    internal static readonly ObservableUpDownCounter<long> GenerationPins = Meter.CreateObservableUpDownCounter(
        GenerationPinsName, ObserveGenerationPins, "{pin}", "Scopes holding a pin on the current generation or on a superseded one.");

    internal static readonly Histogram<double> AttemptDuration = Meter.CreateHistogram<double>(
        AttemptDurationName,
        "s",
        "Reconciler attempt duration.",
        advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300] });

    internal static readonly ObservableUpDownCounter<int> DeferredRunning = Meter.CreateObservableUpDownCounter(
        DeferredRunningName, ObserveDeferredRunning, "{work}", "Deferred work running.");

    internal static readonly ObservableGauge<double> DeferredOldestAge = Meter.CreateObservableGauge(
        DeferredOldestAgeName, ObserveDeferredOldestAge, "s", "Age of the oldest running deferred work.");

    internal static readonly Counter<long> InvalidCursorReloads = Meter.CreateCounter<long>(
        InvalidCursorReloadsName, "{pass}", "Applier passes that found the local cursor invalid and reloaded in full.");

    internal static void RecordAttemptDuration(string aggregateType, string outcome, string pass, TimeSpan elapsed) =>
        AttemptDuration.Record(
            elapsed.TotalSeconds,
            new TagList { { "aggregate_type", aggregateType }, { "outcome", outcome }, { "pass", pass } });
}
