namespace Reeve.Cluster.Harness.Seeds;

internal sealed class SeededRandom
{
    private readonly object gate = new();
    private readonly List<string> decisions = [];
    private ulong state;

    public SeededRandom(string stream, long seed)
    {
        Stream = stream;
        state = unchecked((ulong)seed);
    }

    public string Stream { get; }

    public IReadOnlyList<string> Decisions
    {
        get
        {
            lock (gate)
            {
                return [.. decisions];
            }
        }
    }

    public double NextDouble(string label) => Record(label, (Next() >> 11) * (1.0 / (1UL << 53)));

    public int NextInt(string label, int min, int max)
    {
        if (max <= min)
        {
            throw new ArgumentOutOfRangeException(nameof(max), "max must exceed min.");
        }

        return Record(label, min + (int)(Next() % (ulong)((long)max - min)));
    }

    public TimeSpan NextDuration(string label, TimeSpan min, TimeSpan max)
    {
        var ms = NextInt(label, (int)min.TotalMilliseconds, (int)max.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(ms);
    }

    public T Pick<T>(string label, IReadOnlyList<T> items)
    {
        if (items.Count == 0)
        {
            throw new ArgumentException("Nothing to pick from.", nameof(items));
        }

        var index = (int)(Next() % (ulong)items.Count);
        Record(label, index);
        return items[index];
    }

    public int PickIndex(string label, int count) => Pick(label, Enumerable.Range(0, count).ToList());

    private T Record<T>(string label, T value)
    {
        lock (gate)
        {
            decisions.Add($"{Stream}/{label}={value}");
        }

        return value;
    }

    private ulong Next()
    {
        lock (gate)
        {
            state = unchecked(state + 0x9E3779B97F4A7C15UL);
            var z = state;
            z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
            z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
            return z ^ (z >> 31);
        }
    }
}
