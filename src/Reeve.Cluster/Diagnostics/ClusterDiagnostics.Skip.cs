using System.Diagnostics.Metrics;

namespace Reeve.Cluster;

/// <summary>The instruments of skip.</summary>
public static partial class ClusterDiagnostics
{
    internal const string OpenSkipsName = "reeve_cluster.skip.open";

    internal static readonly UpDownCounter<long> OpenSkips = Meter.CreateUpDownCounter<long>(
        OpenSkipsName,
        unit: "{skip}",
        description: "Open skips, as each replica last read them.");
}
