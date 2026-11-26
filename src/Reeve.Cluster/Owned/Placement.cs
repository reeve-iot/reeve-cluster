namespace Reeve.Cluster.Owned;

internal sealed record PlacedKind(string Kind, EligibilityFilter Eligibility);

internal sealed record PlacementMember(string ReplicaId, long Epoch, IReadOnlyList<string> Capabilities);

internal readonly record struct PlacedRow(string Kind, string Key, OwnedRow Row, bool HoldValid);

internal readonly record struct Handover(string Kind, string Key, long Token, string From);

internal readonly record struct Assignment(string Kind, string Key, OwnedRow? Expected, PlacementMember Target);

internal readonly record struct Barred(string Kind, string Key, string ReplicaId);

internal static class Placement
{
    public const string PlacerKind = "placer";

    public static string SingletonKey(string kind) => kind;

    public static IReadOnlyList<PlacedKind> PlacedKinds(IEnumerable<UnitKind> registered) =>
        [.. registered
            .Where(k => k.Options.Policy == AdoptionPolicy.Placer && !k.Keyed && k.Kind != PlacerKind)
            .Select(k => new PlacedKind(k.Kind, k.Options.Eligibility))];

    public static IReadOnlyList<Assignment> Plan(
        IReadOnlyList<PlacedKind> kinds,
        IReadOnlyList<PlacedRow> rows,
        IReadOnlyList<PlacementMember> members,
        IReadOnlyDictionary<(string Kind, string Key), Handover> handovers,
        IReadOnlySet<Barred>? barred = null)
    {
        ArgumentNullException.ThrowIfNull(kinds);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(handovers);

        var placed = kinds.ToDictionary(k => k.Kind, StringComparer.Ordinal);
        var load = Load(placed, rows, members);

        var open = new List<(string Kind, string Key, OwnedRow? Expected)>();
        foreach (var row in rows)
        {
            if (placed.ContainsKey(row.Kind) && !row.HoldValid)
            {
                open.Add((row.Kind, row.Key, row.Row));
            }
        }

        foreach (var kind in kinds)
        {
            var key = SingletonKey(kind.Kind);
            if (!rows.Any(r => r.Kind == kind.Kind && r.Key == key))
            {
                open.Add((kind.Kind, key, null));
            }
        }

        open.Sort((x, y) => (string.CompareOrdinal(x.Kind, y.Kind), string.CompareOrdinal(x.Key, y.Key)) switch
        {
            (0, var byKey) => byKey,
            (var byKind, _) => byKind,
        });

        var plan = new List<Assignment>();
        foreach (var (kind, key, expected) in open)
        {
            var eligible = Eligible(placed[kind], key, members, barred).ToList();
            if (handovers.TryGetValue((kind, key), out var handover) && eligible.Any(m => m.ReplicaId != handover.From))
            {
                eligible.RemoveAll(m => m.ReplicaId == handover.From);
            }

            var target = eligible
                .OrderBy(m => load[m.ReplicaId])
                .ThenBy(m => m.ReplicaId, StringComparer.Ordinal)
                .FirstOrDefault();
            if (target is null)
            {
                continue;
            }

            load[target.ReplicaId]++;
            plan.Add(new Assignment(kind, key, expected, target));
        }

        return plan;
    }

    public static Handover? Rebalance(
        IReadOnlyList<PlacedKind> kinds,
        IReadOnlyList<PlacedRow> rows,
        IReadOnlyList<PlacementMember> members,
        IReadOnlySet<Barred>? barred = null)
    {
        ArgumentNullException.ThrowIfNull(kinds);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(members);

        var placed = kinds.ToDictionary(k => k.Kind, StringComparer.Ordinal);
        var load = Load(placed, rows, members);

        var held = rows
            .Where(r => r.HoldValid && placed.ContainsKey(r.Kind) && r.Row.OwnerReplica is { } owner && load.ContainsKey(owner))
            .OrderByDescending(r => load[r.Row.OwnerReplica!])
            .ThenBy(r => r.Row.OwnerReplica, StringComparer.Ordinal)
            .ThenBy(r => r.Kind, StringComparer.Ordinal)
            .ThenBy(r => r.Key, StringComparer.Ordinal);
        foreach (var row in held)
        {
            var owner = row.Row.OwnerReplica!;
            if (Eligible(placed[row.Kind], row.Key, members, barred).Any(m => load[owner] - load[m.ReplicaId] >= 2))
            {
                return new Handover(row.Kind, row.Key, row.Row.Token, owner);
            }
        }

        return null;
    }

    private static Dictionary<string, int> Load(Dictionary<string, PlacedKind> placed, IReadOnlyList<PlacedRow> rows, IReadOnlyList<PlacementMember> members)
    {
        var load = members.ToDictionary(m => m.ReplicaId, _ => 0, StringComparer.Ordinal);
        foreach (var row in rows)
        {
            if (row.HoldValid && placed.ContainsKey(row.Kind) && row.Row.OwnerReplica is { } owner && load.ContainsKey(owner))
            {
                load[owner]++;
            }
        }

        return load;
    }

    private static IEnumerable<PlacementMember> Eligible(PlacedKind kind, string key, IReadOnlyList<PlacementMember> members, IReadOnlySet<Barred>? barred)
    {
        var capability = ClusterCapabilities.ForUnit(kind.Kind);
        return members.Where(m =>
            m.Capabilities.Contains(capability, StringComparer.Ordinal)
            && kind.Eligibility.IsEligible(m.Capabilities)
            && barred?.Contains(new Barred(kind.Kind, key, m.ReplicaId)) != true);
    }
}
