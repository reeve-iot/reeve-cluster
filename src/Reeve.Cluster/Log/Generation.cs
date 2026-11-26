using System.Collections.Frozen;

namespace Reeve.Cluster.Log;

internal sealed class TypeState
{
    private readonly CancellationTokenSource superseded = new();

    public TypeState(string aggregateType, object state, long generation)
    {
        AggregateType = aggregateType;
        State = state;
        Generation = generation;
    }

    public string AggregateType { get; }

    public object State { get; }

    public long Generation { get; }

    public CancellationToken Superseded => superseded.Token;

    public Task CancelAsync() => superseded.CancelAsync();
}

internal sealed class Generation : IGeneration
{
    private readonly GenerationStore store;
    private readonly FrozenDictionary<string, TypeState> states;
    private int pins;

    public Generation(GenerationStore store, long id, long head, Guid? headToken, FrozenDictionary<string, TypeState> states)
    {
        this.store = store;
        this.states = states;
        Id = id;
        Head = head;
        HeadToken = headToken;
    }

    public long Id { get; }

    public long Head { get; }

    public Guid? HeadToken { get; }

    public object Get(string aggregateType) => StateOf(aggregateType).State;

    public CancellationToken Superseded(string aggregateType) => StateOf(aggregateType).Superseded;

    public bool TryActivate(string aggregateType, Action activate)
    {
        var state = StateOf(aggregateType);
        ArgumentNullException.ThrowIfNull(activate);
        return store.TryActivate(state, activate);
    }

    internal int Pins => Volatile.Read(ref pins);

    internal void AddPin()
    {
        Interlocked.Increment(ref pins);
        store.AddPin();
    }

    internal void RemovePin()
    {
        Interlocked.Decrement(ref pins);
        store.RemovePin();
    }

    public TypeState StateOf(string aggregateType)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        return states.TryGetValue(aggregateType, out var state)
            ? state
            : throw new KeyNotFoundException($"No reconciler of aggregate type '{aggregateType}' is registered.");
    }

    public FrozenDictionary<string, TypeState> States => states;
}
