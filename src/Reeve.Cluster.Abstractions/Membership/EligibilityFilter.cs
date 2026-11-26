namespace Reeve.Cluster;

/// <summary>
/// A predicate over a member's capabilities: the member must advertise every required
/// capability and none of the forbidden ones. Placement, adoption and routing take one, so "a replica in degraded mode must
/// not own the scheduler" is <c>EligibilityFilter.Any.Forbid("degraded-mode")</c> in a unit's declaration: an owner of the
/// unit that enters degraded mode releases it at its next poll. A keyed resource is filtered when it is created and adopted
/// only; its owner keeps a key it no longer passes the filter for.
/// </summary>
public sealed class EligibilityFilter
{
    /// <summary>The filter every member passes.</summary>
    public static EligibilityFilter Any { get; } = new([], []);

    internal EligibilityFilter(IReadOnlyList<string> required, IReadOnlyList<string> forbidden)
    {
        Required = required;
        Forbidden = forbidden;
    }

    /// <summary>The capabilities a member must advertise.</summary>
    public IReadOnlyList<string> Required { get; }

    /// <summary>The capabilities a member must not advertise.</summary>
    public IReadOnlyList<string> Forbidden { get; }

    /// <summary>A filter that also requires <paramref name="capabilities"/>.</summary>
    public EligibilityFilter Require(params string[] capabilities) => new(Merge(Required, capabilities), Forbidden);

    /// <summary>A filter that also forbids <paramref name="capabilities"/>.</summary>
    public EligibilityFilter Forbid(params string[] capabilities) => new(Required, Merge(Forbidden, capabilities));

    /// <summary>Whether a member advertising <paramref name="capabilities"/> passes.</summary>
    public bool IsEligible(IEnumerable<string> capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        var set = capabilities.ToHashSet(StringComparer.Ordinal);
        return Required.All(set.Contains) && !Forbidden.Any(set.Contains);
    }

    private static string[] Merge(IReadOnlyList<string> existing, string[] added)
    {
        ArgumentNullException.ThrowIfNull(added);
        foreach (var capability in added)
        {
            ClusterCapabilities.Validate(capability);
        }

        return [.. existing.Concat(added).Distinct(StringComparer.Ordinal)];
    }
}
