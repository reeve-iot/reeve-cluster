using System.Collections.Concurrent;

namespace Reeve.Cluster.Owned;

internal sealed class PlacerMemory(TimeSpan window, TimeProvider time)
{
    private readonly ConcurrentDictionary<(string Kind, string Key), Handover> handovers = new();
    private readonly Dictionary<(string Kind, string Key), (long Token, string Target)> assigned = [];
    private readonly Dictionary<Barred, long> barredAt = [];
    private HashSet<(string ReplicaId, long Epoch)>? members;
    private long changedAt = time.GetTimestamp();

    public IReadOnlyDictionary<(string Kind, string Key), Handover> Handovers => handovers;

    public IReadOnlySet<Barred> Barred => barredAt.Keys.ToHashSet();

    public bool MayRebalance() => time.GetElapsedTime(changedAt) >= window && handovers.IsEmpty;

    public void Observe(PlacementRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        var now = time.GetTimestamp();
        var serving = read.Members.Select(m => (m.ReplicaId, m.Epoch)).ToHashSet();
        if (members is not null && !members.SetEquals(serving))
        {
            changedAt = now;
        }

        members = serving;
        var rows = read.Rows.ToDictionary(r => (r.Kind, r.Key));
        foreach (var entry in handovers)
        {
            if (!rows.TryGetValue(entry.Key, out var row) || row.Row.Token != entry.Value.Token)
            {
                handovers.TryRemove(entry);
            }
        }

        foreach (var (key, (token, target)) in assigned.ToList())
        {
            if (!rows.TryGetValue(key, out var row) || row.Row.Token != token)
            {
                assigned.Remove(key);
            }
            else if (row.Row.OwnerReplica is null)
            {
                assigned.Remove(key);
                if (!handovers.ContainsKey(key) && serving.Any(m => m.ReplicaId == target))
                {
                    barredAt[new Barred(key.Kind, key.Key, target)] = now;
                }
            }
        }

        foreach (var (entry, at) in barredAt.ToList())
        {
            if (time.GetElapsedTime(at, now) >= window)
            {
                barredAt.Remove(entry);
            }
        }
    }

    public void Requested(Handover handover) => handovers[(handover.Kind, handover.Key)] = handover;

    public void Forget(Handover handover) => handovers.TryRemove(new KeyValuePair<(string Kind, string Key), Handover>((handover.Kind, handover.Key), handover));

    public void Assigned((string Kind, string Key) key, long token, string target)
    {
        handovers.TryRemove(key, out _);
        assigned[key] = (token, target);
    }
}
