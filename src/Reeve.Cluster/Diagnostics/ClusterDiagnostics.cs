using System.Diagnostics.Metrics;

namespace Reeve.Cluster;

/// <summary>
/// The one <see cref="Meter"/> of the library, named <see cref="Name"/>. The library emits metrics and logs, not
/// traces. Each area declares its instruments in its own part of this class. No replica id and no key is ever a
/// metric tag.
/// </summary>
public static partial class ClusterDiagnostics
{
    /// <summary>The name of the meter, for <c>AddMeter</c>.</summary>
    public const string Name = "Reeve.Cluster";

    internal static Meter Meter => Holder.Meter;

    private static class Holder
    {
        internal static readonly Meter Meter = new(Name);
    }
}
