namespace Reeve.Cluster.Harness.Faults;

internal sealed record Arrival(long Sequence, string Replica, string Point, long Timestamp, DateTimeOffset At);

internal sealed class RunSequence
{
    private long value;

    public long Next() => Interlocked.Increment(ref value);
}

internal sealed class ArrivalLog(RunSequence sequence)
{
    private readonly object gate = new();
    private readonly List<Arrival> arrivals = [];

    public Arrival Record(string replica, string point, TimeProvider clock)
    {
        lock (gate)
        {
            var arrival = new Arrival(sequence.Next(), replica, point, clock.GetTimestamp(), clock.GetUtcNow());
            arrivals.Add(arrival);
            return arrival;
        }
    }

    public IReadOnlyList<Arrival> Snapshot()
    {
        lock (gate)
        {
            return [.. arrivals];
        }
    }

    public int Count(string replica, FaultPoint point)
    {
        lock (gate)
        {
            return arrivals.Count(a => a.Replica == replica && a.Point == point.Name);
        }
    }
}
