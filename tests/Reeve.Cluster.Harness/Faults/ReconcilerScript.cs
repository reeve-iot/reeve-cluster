namespace Reeve.Cluster.Harness.Faults;

internal enum HangMode
{
    Cooperative,

    Stuck,

    BlockThread,

    ServerSleepOwnConnection,

    ServerSleepPassConnection,
}

internal sealed record ReconcileCall(string AggregateType, long Head, long PassNumber, int Attempt, bool FullLoad, DateTimeOffset StartedAt)
{
    public string? Outcome { get; internal set; }
}

internal sealed record ReconciledState(string AggregateType, long Head);

internal sealed class ReconcilerHang : IDisposable
{
    private readonly object gate = new();
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<(int Count, TaskCompletionSource Done)> waiters = [];
    private readonly ReconcilerScript owner;
    private readonly string aggregateType;
    private int remaining;
    private int entered;

    internal ReconcilerHang(ReconcilerScript owner, string aggregateType, HangMode mode, int times)
    {
        this.owner = owner;
        this.aggregateType = aggregateType;
        Mode = mode;
        remaining = times;
    }

    public HangMode Mode { get; }

    public int Entered
    {
        get
        {
            lock (gate)
            {
                return entered;
            }
        }
    }

    public async Task WaitEnteredAsync(int count = 1, TimeSpan? timeout = null)
    {
        Task done;
        lock (gate)
        {
            if (entered >= count)
            {
                return;
            }

            var waiter = (count, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            waiters.Add(waiter);
            done = waiter.Item2.Task;
        }

        try
        {
            await done.WaitAsync(timeout ?? TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"No reconciler attempt of '{aggregateType}' entered the hang {count} time(s): {Entered} did.");
        }
    }

    internal void StopHoldingLaterAttempts()
    {
        lock (gate)
        {
            remaining = 0;
        }
    }

    public void Release()
    {
        lock (gate)
        {
            remaining = 0;
        }

        released.TrySetResult();
    }

    public void Dispose()
    {
        Release();
        owner.Remove(aggregateType, this);
    }

    internal bool TryClaim()
    {
        lock (gate)
        {
            if (remaining <= 0)
            {
                return false;
            }

            remaining--;
            return true;
        }
    }

    internal async Task HangAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        List<TaskCompletionSource> ready = [];
        lock (gate)
        {
            entered++;
            foreach (var waiter in waiters.Where(w => entered >= w.Count).ToList())
            {
                ready.Add(waiter.Done);
                waiters.Remove(waiter);
            }
        }

        foreach (var waiter in ready)
        {
            waiter.TrySetResult();
        }

        switch (Mode)
        {
            case HangMode.Stuck:
                await released.Task.ConfigureAwait(false);
                break;
            case HangMode.BlockThread:
                released.Task.GetAwaiter().GetResult();
                break;
            case HangMode.ServerSleepOwnConnection:
                {
                    await using var session = await context.OpenSnapshotSessionAsync(CancellationToken.None).ConfigureAwait(false);
                    await SleepAsync((Npgsql.NpgsqlConnection)session.Connection).ConfigureAwait(false);
                    break;
                }

            case HangMode.ServerSleepPassConnection:
                await SleepAsync((Npgsql.NpgsqlConnection)context.Connection).ConfigureAwait(false);
                break;
            default:
                await released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private static bool IsQueryCancelled(Exception exception) =>
        exception is Npgsql.PostgresException { SqlState: Npgsql.PostgresErrorCodes.QueryCanceled }
        || (exception is OperationCanceledException && exception.InnerException is Npgsql.PostgresException { SqlState: Npgsql.PostgresErrorCodes.QueryCanceled });

    private async Task SleepAsync(Npgsql.NpgsqlConnection connection)
    {
        await using var command = new Npgsql.NpgsqlCommand("SELECT pg_sleep(86400)", connection) { CommandTimeout = 0 };
        var running = command.ExecuteNonQueryAsync(CancellationToken.None);
        await Task.WhenAny(running, released.Task).ConfigureAwait(false);

        while (!running.IsCompleted)
        {
            try
            {
                command.Cancel();
            }
            catch (Exception exception) when (exception is NullReferenceException or InvalidOperationException)
            {
            }

            await Task.WhenAny(running, Task.Delay(100)).ConfigureAwait(false);
        }

        try
        {
            await running.ConfigureAwait(false);
        }
        catch (Exception exception) when (IsQueryCancelled(exception))
        {
        }
    }
}

internal sealed class ReconcilerScript(TimeProvider clock)
{
    private readonly object gate = new();
    private readonly Dictionary<string, List<Behaviour>> behaviours = new(StringComparer.Ordinal);
    private readonly List<ReconcileCall> calls = [];

    public IReadOnlyList<ReconcileCall> Calls
    {
        get
        {
            lock (gate)
            {
                return [.. calls];
            }
        }
    }

    public ReconcilerHang HangNext(string aggregateType, HangMode mode = HangMode.Cooperative, int times = 1)
    {
        var hang = new ReconcilerHang(this, aggregateType, mode, times);
        Enqueue(aggregateType, new Behaviour(hang, null, null, null));
        return hang;
    }

    public void ThrowNext(string aggregateType, Func<Exception> fault, int times = 1)
    {
        for (var i = 0; i < times; i++)
        {
            Enqueue(aggregateType, new Behaviour(null, fault, null, null));
        }
    }

    public void DelayNext(string aggregateType, TimeSpan delay) =>
        Enqueue(aggregateType, new Behaviour(null, null, delay, null));

    public void Returns(string aggregateType, Func<ReconcileContext, object> state) =>
        Enqueue(aggregateType, new Behaviour(null, null, null, state), standing: true);

    public void ClearFaults()
    {
        lock (gate)
        {
            foreach (var list in behaviours.Values)
            {
                foreach (var hang in list.Select(b => b.Hang).OfType<ReconcilerHang>().ToList())
                {
                    hang.StopHoldingLaterAttempts();
                }

                list.RemoveAll(b => b.Standing is null);
            }
        }
    }

    internal void Remove(string aggregateType, ReconcilerHang hang)
    {
        lock (gate)
        {
            if (behaviours.TryGetValue(aggregateType, out var list))
            {
                list.RemoveAll(b => ReferenceEquals(b.Hang, hang));
            }
        }
    }

    internal async Task<ReconcileResult> RunAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var call = new ReconcileCall(context.AggregateType, context.Head, context.PassNumber, context.Attempt, context.FullLoad, clock.GetUtcNow());
        lock (gate)
        {
            calls.Add(call);
        }

        var behaviour = Take(context.AggregateType);
        try
        {
            if (behaviour?.Fault is { } fault)
            {
                throw fault();
            }

            if (behaviour?.Hang is { } hang)
            {
                await hang.HangAsync(context, cancellationToken).ConfigureAwait(false);
            }

            if (behaviour?.Delay is { } delay)
            {
                await Task.Delay(delay, clock, cancellationToken).ConfigureAwait(false);
            }

            var state = StandingResult(context.AggregateType)?.Invoke(context) ?? new ReconciledState(context.AggregateType, context.Head);
            Finish(call, "returned");
            return new ReconcileResult(state);
        }
        catch (OperationCanceledException)
        {
            Finish(call, "cancelled");
            throw;
        }
        catch
        {
            Finish(call, "threw");
            throw;
        }
    }

    private void Finish(ReconcileCall call, string outcome)
    {
        lock (gate)
        {
            call.Outcome = outcome;
        }
    }

    private void Enqueue(string aggregateType, Behaviour behaviour, bool standing = false)
    {
        lock (gate)
        {
            if (!behaviours.TryGetValue(aggregateType, out var list))
            {
                behaviours[aggregateType] = list = [];
            }

            if (standing)
            {
                list.RemoveAll(b => b.Standing is not null);
            }

            list.Add(behaviour);
        }
    }

    private Behaviour? Take(string aggregateType)
    {
        lock (gate)
        {
            if (!behaviours.TryGetValue(aggregateType, out var list))
            {
                return null;
            }

            for (var i = 0; i < list.Count; i++)
            {
                var candidate = list[i];
                if (candidate.Standing is not null)
                {
                    continue;
                }

                if (candidate.Hang is { } hang)
                {
                    if (hang.TryClaim())
                    {
                        return candidate;
                    }

                    list.RemoveAt(i--);
                    continue;
                }

                list.RemoveAt(i);
                return candidate;
            }

            return null;
        }
    }

    private Func<ReconcileContext, object>? StandingResult(string aggregateType)
    {
        lock (gate)
        {
            return behaviours.TryGetValue(aggregateType, out var list)
                ? list.FirstOrDefault(b => b.Standing is not null)?.Standing
                : null;
        }
    }

    private sealed record Behaviour(
        ReconcilerHang? Hang,
        Func<Exception>? Fault,
        TimeSpan? Delay,
        Func<ReconcileContext, object>? Standing);
}

internal abstract class FaultyReconciler(string aggregateType, ReconcilerScript script) : IReconciler
{
    public string AggregateType { get; } = aggregateType;

    public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
        script.RunAsync(context, cancellationToken);
}
