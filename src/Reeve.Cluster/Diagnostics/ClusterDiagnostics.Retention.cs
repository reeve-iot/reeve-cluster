using System.Diagnostics.Metrics;

namespace Reeve.Cluster;

/// <summary>The instruments of primitive retention.</summary>
public static partial class ClusterDiagnostics
{
    internal const string RetentionDeletedName = "reeve_cluster.retention.deleted";

    internal static readonly Counter<long> RetentionDeleted = Meter.CreateCounter<long>(RetentionDeletedName, "{row}", "Rows deleted by primitive retention.");
}
