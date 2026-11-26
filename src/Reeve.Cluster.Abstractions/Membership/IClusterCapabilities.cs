namespace Reeve.Cluster;

/// <summary>
/// The capabilities this replica advertises on its member row: the application's own, plus one
/// <c>unit:&lt;kind&gt;</c> per registered unit kind, which the library adds. Capabilities are not readiness.
/// </summary>
public interface IClusterCapabilities
{
    /// <summary>Everything advertised now, sorted: the application's capabilities and the unit kinds'.</summary>
    IReadOnlyList<string> Current { get; }

    /// <summary>
    /// Replaces the application's capabilities. The join carries them; on a joined replica whose row is still its own it is
    /// updated before this returns, so the placer's next read sees the change. A row that is draining, fenced or past its lease
    /// is left as it is, and the next join writes the set. Throws when the write does not complete; <see cref="Current"/> is
    /// then unchanged, and the row may or may not hold the new set until the next join or a retry. Once <see cref="Current"/>
    /// holds the new set, the replica's next own-row poll stops and releases each placed unit whose kind's eligibility filter it
    /// no longer passes; after a call that threw, the replica keeps its units until a retry succeeds. A call that threw after a write that made the replica
    /// eligible leaves the row saying eligible while <see cref="Current"/> says not: the placer may assign a unit, which the replica
    /// releases at its next poll without starting it, until a retry succeeds.
    /// </summary>
    Task SetAsync(IEnumerable<string> capabilities, CancellationToken cancellationToken = default);
}

/// <summary>The naming of capabilities.</summary>
public static class ClusterCapabilities
{
    /// <summary>The prefix of the capability the library adds for a registered unit kind; the application sets none that starts with it.</summary>
    public const string UnitPrefix = "unit:";

    /// <summary>The capability a replica advertises for the unit kind <paramref name="kind"/>.</summary>
    public static string ForUnit(string kind)
    {
        Validate(kind);
        return UnitPrefix + kind;
    }

    /// <summary>Throws <see cref="ArgumentException"/> unless <paramref name="capability"/> is a non-empty name with no leading or trailing whitespace.</summary>
    public static void Validate(string capability)
    {
        if (string.IsNullOrWhiteSpace(capability) || capability != capability.Trim())
        {
            throw new ArgumentException("A capability is a non-empty name with no leading or trailing whitespace.", nameof(capability));
        }
    }
}
