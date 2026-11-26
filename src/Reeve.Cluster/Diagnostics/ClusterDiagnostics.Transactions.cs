using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Reeve.Cluster;

/// <summary>The instruments for transaction durations.</summary>
public static partial class ClusterDiagnostics
{
    internal const string CommitDurationName = "reeve_cluster.commit.duration";
    internal static readonly Histogram<double> CommitDuration = Meter.CreateHistogram<double>(CommitDurationName, "s", "Commit round trip duration.", advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] });

    internal static void RecordCommitDuration(double seconds, string transaction, string outcome)
    {
        try
        {
            CommitDuration.Record(seconds, new TagList { { "transaction", transaction }, { "outcome", outcome } });
        }
        catch (Exception)
        {
        }
    }
}
