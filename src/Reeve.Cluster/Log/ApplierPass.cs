namespace Reeve.Cluster.Log;

internal enum AttemptOutcomeKind
{
    Accepted,

    Failed,

    TimedOut,

    Abandoned,
}

internal sealed record AttemptOutcome(AttemptOutcomeKind Kind, ApplierAttempt Attempt, Exception? Error);

internal sealed record UnreturnedAttempt(ApplierAttempt Attempt, AttemptOutcomeKind Why, long Since);

internal sealed class ApplierAttempt
{
    private readonly CancellationTokenSource cancellation = new();
    private readonly TaskCompletionSource returned = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ApplierAttempt(long passNumber, int number, string aggregateType, long started, long? deadline)
    {
        PassNumber = passNumber;
        Number = number;
        AggregateType = aggregateType;
        Started = started;
        Deadline = deadline;
    }

    public long PassNumber { get; }

    public int Number { get; }

    public string AggregateType { get; }

    public long Started { get; }

    public long? Deadline { get; }

    public CancellationToken Token => cancellation.Token;

    public Task Returned => returned.Task;

    internal Task? Reconciling { get; set; }

    internal bool HasReturned { get; set; }

    internal AttemptOutcomeKind? DiscardedAs { get; set; }

    internal Task CancelAsync() => cancellation.CancelAsync();

    internal void CompleteReturned() => returned.TrySetResult();
}

internal sealed class ApplierPass
{
    private readonly HashSet<string> dirty;
    private readonly Dictionary<string, ReconcileResult> accepted = new(StringComparer.Ordinal);
    private readonly List<ApplierAttempt> acceptedAttempts = [];
    private readonly TaskCompletionSource abandoned = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private ApplierAttempt? awaited;
    private int attempts;
    private bool isAbandoned;
    private bool isPublished;

    internal ApplierPass(
        GenerationStore store, long number, long head, Guid? headToken, IReadOnlyList<string> dirtyTypes, Generation? baseGeneration, bool isFull = false)
    {
        Store = store;
        IsFull = isFull;
        Number = number;
        Head = head;
        HeadToken = headToken;
        DirtyTypes = dirtyTypes;
        Base = baseGeneration;
        dirty = dirtyTypes.ToHashSet(StringComparer.Ordinal);
    }

    public long Number { get; }

    public long Head { get; }

    public Guid? HeadToken { get; }

    public bool IsFull { get; }

    public IReadOnlyList<string> DirtyTypes { get; }

    public Generation? Base { get; }

    public bool IsAbandoned
    {
        get
        {
            lock (Store.Gate)
            {
                return isAbandoned;
            }
        }
    }

    internal GenerationStore Store { get; }

    public bool HasDirtyDependency(string aggregateType) => Store.Graph.DependenciesOf(aggregateType).Any(dirty.Contains);

    public IReadOnlyDictionary<string, object> DependencyStates(string aggregateType)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        var declared = Store.Graph.DependenciesOf(aggregateType);
        var states = new Dictionary<string, object>(declared.Count, StringComparer.Ordinal);
        if (declared.Count == 0)
        {
            return states;
        }

        lock (Store.Gate)
        {
            if (isAbandoned)
            {
                throw new InvalidOperationException($"Pass {Number} ended; '{aggregateType}' has no dependency states.");
            }

            foreach (var dependency in declared)
            {
                if (dirty.Contains(dependency))
                {
                    states[dependency] = accepted.TryGetValue(dependency, out var result)
                        ? result.State
                        : throw new InvalidOperationException(
                            $"Pass {Number} reconciles '{dependency}' but has no accepted result of it for '{aggregateType}' to read.");
                }
                else
                {
                    states[dependency] = (Base ?? throw new InvalidOperationException($"Pass {Number} has no base generation."))
                        .Get(dependency);
                }
            }
        }

        return states;
    }

    public Task<AttemptOutcome> RunAttemptAsync(string aggregateType, Func<ApplierAttempt, Task<ReconcileResult>> run, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(run);

        var timer = new CancellationTokenSource();
        Task delay;
        (ApplierAttempt Attempt, bool AbandonedBefore) begun;
        try
        {
            delay = Task.Delay(timeout, Store.Time, timer.Token);
            begun = Begin(aggregateType, timeout == Timeout.InfiniteTimeSpan ? null : timeout);
        }
        catch
        {
            timer.Cancel();
            timer.Dispose();
            throw;
        }

        if (begun.AbandonedBefore)
        {
            timer.Cancel();
            timer.Dispose();
            return Task.FromResult(new AttemptOutcome(AttemptOutcomeKind.Abandoned, begun.Attempt, null));
        }

        return RunTimedAsync(begun.Attempt, run, delay, timer);
    }

    public Task<AttemptOutcome> RunAttemptAsync(string aggregateType, Func<ApplierAttempt, Task<ReconcileResult>> run, Task timeout)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(timeout);
        var (attempt, abandonedBefore) = Begin(aggregateType, null);
        return abandonedBefore ? Task.FromResult(new AttemptOutcome(AttemptOutcomeKind.Abandoned, attempt, null)) : AwaitAsync(attempt, run, timeout);
    }

    public void Abandon()
    {
        Store.EnsureNotInsideTheSwap();
        IReadOnlyList<ApplierAttempt> discarded;
        lock (Store.Gate)
        {
            discarded = EndLocked(AttemptOutcomeKind.Abandoned);
        }

        AfterEnd(discarded);
    }

    internal void AfterEnd(IReadOnlyList<ApplierAttempt> discarded)
    {
        abandoned.TrySetResult();
        foreach (var attempt in discarded)
        {
            Store.Observe(attempt.CancelAsync(), attempt.AggregateType);
        }

        if (discarded.Count > 0)
        {
            Store.NotifyUnreturned();
        }
    }

    internal IReadOnlyList<ApplierAttempt> EndLocked(AttemptOutcomeKind awaitedAs)
    {
        if (isAbandoned || isPublished)
        {
            return [];
        }

        isAbandoned = true;
        List<ApplierAttempt> discarded = [.. acceptedAttempts];
        accepted.Clear();
        acceptedAttempts.Clear();
        if (awaited is { } orphan)
        {
            awaited = null;
            DiscardLocked(orphan, awaitedAs);
            discarded.Add(orphan);
        }

        return discarded;
    }

    internal IReadOnlyDictionary<string, ReconcileResult>? TakeForPublicationLocked()
    {
        if (isPublished)
        {
            throw new InvalidOperationException($"Pass {Number} was published already.");
        }

        if (isAbandoned)
        {
            return null;
        }

        if (awaited is not null || accepted.Count != dirty.Count)
        {
            throw new InvalidOperationException($"Pass {Number} has dirty types without an accepted result.");
        }

        isPublished = true;
        return accepted;
    }

    private (ApplierAttempt Attempt, bool AbandonedBefore) Begin(string aggregateType, TimeSpan? timeout)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        if (!dirty.Contains(aggregateType))
        {
            throw new ArgumentException($"Aggregate type '{aggregateType}' is not dirty in pass {Number}.", nameof(aggregateType));
        }

        Store.EnsureNotInsideTheSwap();
        lock (Store.Gate)
        {
            if (isPublished)
            {
                throw new InvalidOperationException($"Pass {Number} is published.");
            }

            var started = Store.Time.GetTimestamp();
            long? deadline = timeout is { } t ? started + (long)(t.TotalSeconds * Store.Time.TimestampFrequency) : null;
            if (isAbandoned)
            {
                var never = new ApplierAttempt(Number, ++attempts, aggregateType, started, deadline)
                {
                    DiscardedAs = AttemptOutcomeKind.Abandoned,
                    HasReturned = true,
                };
                never.CompleteReturned();
                Store.Observe(never.CancelAsync(), aggregateType);
                return (never, true);
            }

            if (awaited is not null)
            {
                throw new InvalidOperationException($"Pass {Number} awaits attempt {awaited.Number} already.");
            }

            if (accepted.ContainsKey(aggregateType))
            {
                throw new InvalidOperationException($"Pass {Number} accepted a result of '{aggregateType}' already.");
            }

            awaited = new ApplierAttempt(Number, ++attempts, aggregateType, started, deadline);
            return (awaited, false);
        }
    }

    private async Task<AttemptOutcome> RunTimedAsync(ApplierAttempt attempt, Func<ApplierAttempt, Task<ReconcileResult>> run, Task delay, CancellationTokenSource timer)
    {
        using (timer)
        {
            try
            {
                return await AwaitAsync(attempt, run, delay).ConfigureAwait(false);
            }
            finally
            {
                timer.Cancel();
            }
        }
    }

    private async Task<AttemptOutcome> AwaitAsync(ApplierAttempt attempt, Func<ApplierAttempt, Task<ReconcileResult>> run, Task timeout)
    {
        var outcome = await AwaitCoreAsync(attempt, run, timeout).ConfigureAwait(false);
        if (outcome.Kind != AttemptOutcomeKind.Abandoned)
        {
            ClusterDiagnostics.RecordAttemptDuration(
                attempt.AggregateType,
                outcome.Kind switch
                {
                    AttemptOutcomeKind.Accepted => ClusterDiagnostics.OutcomeAccepted,
                    AttemptOutcomeKind.TimedOut => ClusterDiagnostics.OutcomeTimedOut,
                    _ => ClusterDiagnostics.OutcomeFailed,
                },
                IsFull ? ClusterDiagnostics.PassFull : ClusterDiagnostics.PassIncremental,
                Store.Time.GetElapsedTime(attempt.Started));
        }

        return outcome;
    }

    private async Task<AttemptOutcome> AwaitCoreAsync(ApplierAttempt attempt, Func<ApplierAttempt, Task<ReconcileResult>> run, Task timeout)
    {
        Task<ReconcileResult> reconciling;
        lock (Store.Gate)
        {
            if (attempt.DiscardedAs is not null)
            {
                attempt.HasReturned = true;
                Store.RemoveUnreturnedLocked(attempt);
                attempt.CompleteReturned();
                return new AttemptOutcome(AttemptOutcomeKind.Abandoned, attempt, null);
            }

            reconciling = Task.Run(
                () => run(attempt) ?? throw new InvalidOperationException($"The reconciler of '{attempt.AggregateType}' returned no task."),
                CancellationToken.None);
            attempt.Reconciling = reconciling;
        }

        _ = ObserveReturnAsync(attempt, reconciling);

        var first = await Task.WhenAny(reconciling, timeout, abandoned.Task).ConfigureAwait(false);
        if (first != reconciling)
        {
            return Discard(attempt, first == timeout ? AttemptOutcomeKind.TimedOut : AttemptOutcomeKind.Abandoned, null);
        }

        ReconcileResult result;
        try
        {
            result = await reconciling.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            return Discard(attempt, AttemptOutcomeKind.Failed, e);
        }

        if (result?.State is null)
        {
            return Discard(attempt, AttemptOutcomeKind.Failed, new InvalidOperationException(
                $"The reconciler of '{attempt.AggregateType}' returned no state."));
        }

        try
        {
            await Store.FaultPoints.ReachAsync(LogFaultPoints.BeforeAttemptAccepted).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            return Discard(attempt, AttemptOutcomeKind.Failed, e);
        }

        lock (Store.Gate)
        {
            if (ReferenceEquals(awaited, attempt))
            {
                awaited = null;
                accepted[attempt.AggregateType] = result;
                acceptedAttempts.Add(attempt);
                Store.EmitAttempt(LogActions.AttemptOK, attempt);
                return new AttemptOutcome(AttemptOutcomeKind.Accepted, attempt, null);
            }
        }

        return new AttemptOutcome(AttemptOutcomeKind.Abandoned, attempt, null);
    }

    private AttemptOutcome Discard(ApplierAttempt attempt, AttemptOutcomeKind why, Exception? error)
    {
        IReadOnlyList<ApplierAttempt> discarded;
        lock (Store.Gate)
        {
            if (ReferenceEquals(awaited, attempt))
            {
                discarded = EndLocked(why);
            }
            else
            {
                why = AttemptOutcomeKind.Abandoned;
                error = null;
                discarded = [];
            }
        }

        AfterEnd(discarded);

        return new AttemptOutcome(why, attempt, error);
    }

    private void DiscardLocked(ApplierAttempt attempt, AttemptOutcomeKind why)
    {
        attempt.DiscardedAs = why;
        if (why == AttemptOutcomeKind.Failed)
        {
            Store.EmitAttempt(LogActions.AttemptFail, attempt);
        }
        else if (why == AttemptOutcomeKind.TimedOut)
        {
            Store.EmitAttempt(LogActions.AttemptHang, attempt);
            if (attempt.HasReturned)
            {
                Store.EmitAttempt(LogActions.LateReturn, attempt);
            }
        }

        if (!attempt.HasReturned && attempt.Reconciling is not { IsCompleted: true })
        {
            Store.AddUnreturnedLocked(new UnreturnedAttempt(attempt, why, Store.Time.GetTimestamp()));
        }
    }

    private async Task ObserveReturnAsync(ApplierAttempt attempt, Task<ReconcileResult> reconciling)
    {
        try
        {
            await reconciling.ConfigureAwait(false);
        }
        catch
        {
        }

        lock (Store.Gate)
        {
            attempt.HasReturned = true;
            Store.RemoveUnreturnedLocked(attempt);
            if (attempt.DiscardedAs == AttemptOutcomeKind.TimedOut)
            {
                Store.EmitAttempt(LogActions.LateReturn, attempt);
            }
        }

        attempt.CompleteReturned();
    }
}
