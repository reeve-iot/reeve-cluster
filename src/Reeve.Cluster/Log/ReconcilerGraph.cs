using System.Collections.Frozen;

namespace Reeve.Cluster.Log;

internal sealed class ReconcilerGraph
{
    private readonly string[] types;
    private readonly FrozenDictionary<string, string[]> dependencies;

    private ReconcilerGraph(string[] types, FrozenDictionary<string, string[]> dependencies)
    {
        this.types = types;
        this.dependencies = dependencies;
    }

    public IReadOnlyList<string> Types => types;

    public static ReconcilerGraph Independent(IEnumerable<string> aggregateTypes)
    {
        ArgumentNullException.ThrowIfNull(aggregateTypes);
        return new ReconcilerGraph([.. aggregateTypes], FrozenDictionary<string, string[]>.Empty);
    }

    public static ReconcilerGraph Build(IReadOnlyList<IReconciler> reconcilers)
    {
        ArgumentNullException.ThrowIfNull(reconcilers);
        var names = reconcilers.Select(r => r.AggregateType).ToArray();

        var positions = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < names.Length; i++)
        {
            positions.TryAdd(names[i], i);
        }

        var declared = new Dictionary<string, string[]>(StringComparer.Ordinal);
        for (var i = 0; i < reconcilers.Count; i++)
        {
            var type = names[i];
            var list = reconcilers[i].DependsOn
                ?? throw new ClusterConfigurationException(
                    ClusterConfigurationReason.ReconcilerDependencyUnknown, $"The DependsOn of the reconciler of '{type}' is null.");
            var edges = new SortedSet<int>();
            foreach (var dependency in list)
            {
                if (string.IsNullOrEmpty(dependency))
                {
                    throw new ClusterConfigurationException(
                        ClusterConfigurationReason.ReconcilerDependencyUnknown, $"The DependsOn of the reconciler of '{type}' holds a null or empty name.");
                }

                if (!positions.TryGetValue(dependency, out var position))
                {
                    throw new ClusterConfigurationException(
                        ClusterConfigurationReason.ReconcilerDependencyUnknown,
                        $"The reconciler of '{type}' depends on '{dependency}', which no registered reconciler owns.");
                }

                if (position >= i)
                {
                    throw new ClusterConfigurationException(
                        ClusterConfigurationReason.ReconcilerDependencyNotEarlier,
                        position == i
                            ? $"The reconciler of '{type}' (position {i}) depends on itself."
                            : $"The reconciler of '{type}' (position {i}) depends on '{dependency}' (position {position}), which is not registered before it; a dependency is declared earlier, which also rules out a cycle.");
                }

                edges.Add(position);
            }

            if (edges.Count > 0)
            {
                declared.TryAdd(type, [.. edges.Select(p => names[p])]);
            }
        }

        return new ReconcilerGraph(names, declared.ToFrozenDictionary(StringComparer.Ordinal));
    }

    public IReadOnlyList<string> DependenciesOf(string type) => dependencies.TryGetValue(type, out var list) ? list : [];

    public HashSet<string> Close(IEnumerable<string> given)
    {
        var closed = new HashSet<string>(given, StringComparer.Ordinal);
        if (dependencies.Count == 0)
        {
            return closed;
        }

        foreach (var type in types)
        {
            if (!closed.Contains(type) && DependenciesOf(type).Any(closed.Contains))
            {
                closed.Add(type);
            }
        }

        return closed;
    }

    public IReadOnlyList<string> ClosureOf(string type)
    {
        var needed = new HashSet<string>(StringComparer.Ordinal) { type };
        var pending = new Stack<string>([type]);
        while (pending.Count > 0)
        {
            foreach (var dependency in DependenciesOf(pending.Pop()))
            {
                if (needed.Add(dependency))
                {
                    pending.Push(dependency);
                }
            }
        }

        return [.. types.Where(needed.Contains)];
    }
}
