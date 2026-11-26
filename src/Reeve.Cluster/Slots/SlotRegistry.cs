namespace Reeve.Cluster.Slots;

internal sealed record SlotEntry(string Key, string Id, Guid Token, long Epoch, long StartedAt, TimeSpan Ttl);

internal sealed class SlotRegistry(TimeProvider time)
{
    private readonly Dictionary<(string Key, string Id), SlotEntry> entries = [];

    public SlotEntry? TryGet(string key, string id, Guid token)
    {
        lock (entries)
        {
            if (!entries.TryGetValue((key, id), out var entry))
            {
                return null;
            }

            if (Passed(entry))
            {
                entries.Remove((key, id));
                return null;
            }

            return entry.Token == token ? entry : null;
        }
    }

    public void Record(string key, string id, Guid token, long epoch, long startedAt, TimeSpan ttl)
    {
        lock (entries)
        {
            entries[(key, id)] = new SlotEntry(key, id, token, epoch, startedAt, ttl);
        }
    }

    public bool Extend(string key, string id, Guid token, long startedAt, TimeSpan ttl)
    {
        lock (entries)
        {
            if (!entries.TryGetValue((key, id), out var entry) || entry.Token != token)
            {
                return false;
            }

            entries[(key, id)] = entry with { StartedAt = startedAt, Ttl = ttl };
            return true;
        }
    }

    public void Remove(string key, string id, Guid? token)
    {
        lock (entries)
        {
            if (entries.TryGetValue((key, id), out var entry) && (token is null || entry.Token == token))
            {
                entries.Remove((key, id));
            }
        }
    }

    public int Live()
    {
        lock (entries)
        {
            return entries.Values.Count(entry => !Passed(entry));
        }
    }

    private bool Passed(SlotEntry entry) => time.GetElapsedTime(entry.StartedAt) >= entry.Ttl;
}
