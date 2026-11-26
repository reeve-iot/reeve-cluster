namespace Reeve.Cluster.Harness.Faults;

internal sealed record RecordedEvent(
    long Sequence,
    string Replica,
    string Action,
    IReadOnlyDictionary<string, object?>? State,
    long Timestamp,
    DateTimeOffset At);

internal sealed class ModelEventLog(RunSequence sequence)
{
    private readonly object gate = new();
    private readonly List<RecordedEvent> events = [];

    public void Record(string replica, in ModelEvent modelEvent, TimeProvider clock)
    {
        var state = modelEvent.State is null ? null : modelEvent.State.ToDictionary(kv => kv.Key, kv => Snapshot(kv.Value));
        lock (gate)
        {
            events.Add(new RecordedEvent(sequence.Next(), replica, modelEvent.Action, state, clock.GetTimestamp(), clock.GetUtcNow()));
        }
    }

    private static object? Snapshot(object? value) => value switch
    {
        IReadOnlyDictionary<string, int> map => new Dictionary<string, int>(map),
        IReadOnlyDictionary<string, object?> map => new Dictionary<string, object?>(map),
        string => value,
        IEnumerable<string> items => items.ToList(),
        _ => value,
    };

    public IReadOnlyList<RecordedEvent> Snapshot()
    {
        lock (gate)
        {
            return [.. events];
        }
    }
}

internal sealed class ReplicaModelEvents(string replica, TimeProvider clock, ModelEventLog log, Func<bool> isAlive) : IModelEventSink
{
    public void Emit(in ModelEvent modelEvent)
    {
        if (isAlive())
        {
            log.Record(replica, modelEvent, clock);
        }
    }
}
