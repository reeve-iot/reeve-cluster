using System.Collections.Frozen;
using Microsoft.Extensions.Logging;

namespace Reeve.Cluster.Log;

internal interface IGenerationSource
{
    IGeneration? Current { get; }

    void EnsureNotInsideTheSwap();
}

internal sealed class GenerationStore : IGenerationSource
{
    private readonly object gate = new();
    private readonly ReconcilerGraph graph;
    private readonly string[] types;
    private readonly FrozenSet<string> typeSet;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly List<UnreturnedAttempt> unreturned = [];
    private Action[] unreturnedHandlers = [];
    private Generation? current;
    private ApplierPass? openPass;
    private long passes;
    private long pins;
    private readonly object deferredGate = new();
    private readonly List<DeferredRun> deferredRuns = [];
    private readonly KeyValuePair<string, object?>[][] typeTags;

    public GenerationStore(
        IEnumerable<string> aggregateTypes, IFaultPoints faultPoints, IModelEventSink events, TimeProvider time, ILogger logger)
        : this(ReconcilerGraph.Independent(aggregateTypes), faultPoints, events, time, logger)
    {
    }

    public GenerationStore(ReconcilerGraph graph, IFaultPoints faultPoints, IModelEventSink events, TimeProvider time, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);

        this.graph = graph;
        types = [.. graph.Types];
        typeSet = types.ToFrozenSet(StringComparer.Ordinal);
        if (typeSet.Count != types.Length)
        {
            throw new ArgumentException("Aggregate types must be unique.", nameof(graph));
        }

        FaultPoints = faultPoints;
        this.events = events;
        Time = time;
        this.logger = logger;
        typeTags = [.. types.Select(t => new[] { new KeyValuePair<string, object?>("aggregate_type", t) })];
        ClusterDiagnostics.TrackStore(this);
    }

    public IGeneration? Current => Volatile.Read(ref current);

    public IReadOnlyList<string> AggregateTypes => types;

    public ReconcilerGraph Graph => graph;

    public IReadOnlyList<UnreturnedAttempt> Unreturned
    {
        get
        {
            lock (gate)
            {
                return [.. unreturned];
            }
        }
    }

    internal object Gate => gate;

    internal KeyValuePair<string, object?>[][] TypeTags => typeTags;

    internal long Pins => Interlocked.Read(ref pins);

    internal void AddPin() => Interlocked.Increment(ref pins);

    internal void RemovePin() => Interlocked.Decrement(ref pins);

    internal void ReadDeferred(int[] running, long[] oldestStarted)
    {
        lock (deferredGate)
        {
            foreach (var run in deferredRuns)
            {
                if (running[run.TypeIndex]++ == 0 || run.Started - oldestStarted[run.TypeIndex] < 0)
                {
                    oldestStarted[run.TypeIndex] = run.Started;
                }
            }
        }
    }

    internal TimeProvider Time { get; }

    internal IFaultPoints FaultPoints { get; }

    public ApplierPass BeginPass(long head, Guid? headToken, IEnumerable<string> dirtyTypes, bool full = false)
    {
        ArgumentNullException.ThrowIfNull(dirtyTypes);
        EnsureNotInsideTheSwap();
        var given = dirtyTypes.ToHashSet(StringComparer.Ordinal);
        foreach (var type in given)
        {
            if (!typeSet.Contains(type))
            {
                throw new ArgumentException($"No reconciler of aggregate type '{type}' is registered.", nameof(dirtyTypes));
            }
        }

        var dirty = graph.Close(given);

        IReadOnlyList<ApplierAttempt> orphans = [];
        ApplierPass? previous;
        ApplierPass pass;
        lock (gate)
        {
            if (current is null && dirty.Count != types.Length)
            {
                throw new InvalidOperationException("Before the first publication every type is dirty: the first pass loads them all.");
            }

            previous = openPass;
            if (previous is not null)
            {
                orphans = previous.EndLocked(AttemptOutcomeKind.Abandoned);
            }

            pass = new ApplierPass(this, ++passes, head, headToken, [.. types.Where(dirty.Contains)], current, full);
            openPass = pass;
        }

        previous?.AfterEnd(orphans);
        return pass;
    }

    public IGeneration? Publish(ApplierPass pass)
    {
        ArgumentNullException.ThrowIfNull(pass);
        if (!ReferenceEquals(pass.Store, this))
        {
            throw new ArgumentException("The pass belongs to another store.", nameof(pass));
        }

        EnsureNotInsideTheSwap();
        Generation generation;
        List<(string Type, Task Callbacks)> supersessions = [];
        IReadOnlyDictionary<string, ReconcileResult> results;
        lock (gate)
        {
            if (pass.TakeForPublicationLocked() is not { } taken)
            {
                return null;
            }

            results = taken;
            var previous = current;
            var id = (previous?.Id ?? 0) + 1;
            List<TypeState> replaced = [];
            var states = new Dictionary<string, TypeState>(types.Length, StringComparer.Ordinal);
            foreach (var type in types)
            {
                if (results.TryGetValue(type, out var result))
                {
                    states[type] = new TypeState(type, result.State, id);
                    if (previous is not null)
                    {
                        replaced.Add(previous.States[type]);
                    }
                }
                else
                {
                    states[type] = previous!.States[type];
                }
            }

            generation = new Generation(this, id, pass.Head, pass.HeadToken, states.ToFrozenDictionary(StringComparer.Ordinal));
            events.Emit(new ModelEvent(LogActions.Publish, new Dictionary<string, object?>
            {
                ["generation"] = id,
                ["head"] = pass.Head,
                ["headToken"] = pass.HeadToken,
                ["dirty"] = pass.DirtyTypes,
            }));
            Volatile.Write(ref current, generation);
            foreach (var state in replaced)
            {
                supersessions.Add((state.AggregateType, state.CancelAsync()));
            }
        }

        foreach (var (type, callbacks) in supersessions)
        {
            Observe(callbacks, type);
        }

        using (ExecutionContext.SuppressFlow())
        {
            foreach (var type in pass.DirtyTypes)
            {
                if (results[type].Deferred is { } deferred)
                {
                    var run = new DeferredRun(Array.IndexOf(types, type), Time.GetTimestamp());
                    lock (deferredGate)
                    {
                        deferredRuns.Add(run);
                    }

                    _ = RunDeferredAsync(deferred, generation, generation.States[type], run);
                }
            }
        }

        return generation;
    }

    internal bool TryActivate(TypeState state, Action activate)
    {
        EnsureNotInsideTheSwap();
        lock (gate)
        {
            if (!ReferenceEquals(current!.States[state.AggregateType], state))
            {
                EmitTryActivate(state, activated: false);
                return false;
            }

            activate();
            EmitTryActivate(state, activated: true);
            return true;
        }
    }

    public void EnsureNotInsideTheSwap()
    {
        if (Monitor.IsEntered(gate))
        {
            throw new InvalidOperationException(
                "The generation store was called from inside an activation; TryActivate's activation must not call back into the applier or TryActivate.");
        }
    }

    internal void Observe(Task callbacks, string aggregateType)
    {
        if (callbacks.IsCompletedSuccessfully)
        {
            return;
        }

        _ = callbacks.ContinueWith(
            (t, type) => logger.LogError(t.Exception, "A cancellation callback of aggregate type {AggregateType} threw.", type),
            aggregateType,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    internal void EmitAttempt(string action, ApplierAttempt attempt) =>
        events.Emit(new ModelEvent(action, new Dictionary<string, object?>
        {
            ["pass"] = attempt.PassNumber,
            ["attempt"] = attempt.Number,
            ["type"] = attempt.AggregateType,
        }));

    internal void AddUnreturnedLocked(UnreturnedAttempt attempt) => unreturned.Add(attempt);

    internal void RemoveUnreturnedLocked(ApplierAttempt attempt) => unreturned.RemoveAll(u => ReferenceEquals(u.Attempt, attempt));

    internal IDisposable OnUnreturned(Action handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Change(handlers => [.. handlers, handler]);
        return new Unsubscriber(() => Change(handlers => [.. handlers.Where(h => !ReferenceEquals(h, handler))]));

        void Change(Func<Action[], Action[]> change)
        {
            Action[] seen, next;
            do
            {
                seen = Volatile.Read(ref unreturnedHandlers);
                next = change(seen);
            }
            while (!ReferenceEquals(Interlocked.CompareExchange(ref unreturnedHandlers, next, seen), seen));
        }
    }

    internal void NotifyUnreturned()
    {
        foreach (var handler in Volatile.Read(ref unreturnedHandlers))
        {
            handler();
        }
    }

    private sealed class Unsubscriber(Action unsubscribe) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                unsubscribe();
            }
        }
    }

    private void EmitTryActivate(TypeState state, bool activated) =>
        events.Emit(new ModelEvent(LogActions.TryActivate, new Dictionary<string, object?>
        {
            ["type"] = state.AggregateType,
            ["stateGeneration"] = state.Generation,
            ["activated"] = activated,
        }));

    private sealed class DeferredRun(int typeIndex, long started)
    {
        public int TypeIndex { get; } = typeIndex;

        public long Started { get; } = started;
    }

    private async Task RunDeferredAsync(Func<IGeneration, CancellationToken, Task> deferred, Generation generation, TypeState state, DeferredRun run)
    {
        var token = state.Superseded;
        try
        {
            await Task.Run(() => deferred(generation, token), CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            logger.LogError(e, "Deferred work of aggregate type {AggregateType} threw.", state.AggregateType);
        }
        finally
        {
            lock (deferredGate)
            {
                deferredRuns.Remove(run);
            }
        }
    }
}

internal static class GenerationPinning
{
    public static IGeneration PinCurrent(this IClusterScopeWriter scope, IGenerationSource source)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(source);
        var generation = source.Current
            ?? throw new InvalidOperationException("No generation is published yet: the first pass has not completed.");
        scope.Pin(generation);
        return generation;
    }
}
