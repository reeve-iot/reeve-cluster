using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Reeve.Cluster;

namespace Reeve.Cluster.AspNetCore;

internal static class HttpDiagnostics
{
    internal const string AdmissionRefusalsName = "reeve_cluster.admission.refusals";
    internal const string WriteRefusedName = "reeve_cluster.write_refused";
    internal const string WriteUnconfirmedName = "reeve_cluster.write_unconfirmed";
    internal const string DrainAwaitedRequestsName = "reeve_cluster.drain.awaited_requests";

    private static readonly ConditionalWeakTable<RequestTracker, object?> Trackers = new();

    internal static void TrackRequests(RequestTracker tracker) => Trackers.Add(tracker, null);

    private static IEnumerable<Measurement<int>> ObserveAwaitedRequests()
    {
        foreach (var pair in Trackers)
        {
            var awaited = pair.Key.AwaitedAtDrain;
            if (awaited >= 0)
            {
                yield return new Measurement<int>(awaited);
            }
        }
    }

    internal static readonly ObservableGauge<int> DrainAwaitedRequests = ClusterDiagnostics.Meter.CreateObservableGauge<int>(DrainAwaitedRequestsName, ObserveAwaitedRequests, "{request}", "Requests drain step 4 still waits for.");

    internal static readonly Counter<long> AdmissionRefusals = ClusterDiagnostics.Meter.CreateCounter<long>(AdmissionRefusalsName, "{request}", "Requests refused at admission.");

    internal static readonly Counter<long> WriteRefused = ClusterDiagnostics.Meter.CreateCounter<long>(WriteRefusedName, "{request}", "Requests and invocations answered write_refused.");

    internal static readonly Counter<long> WriteUnconfirmed = ClusterDiagnostics.Meter.CreateCounter<long>(WriteUnconfirmedName, "{request}", "Requests and invocations answered write_unconfirmed.");
}
