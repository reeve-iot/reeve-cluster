namespace Reeve.Cluster.Harness.Faults;

internal abstract class PointRule : IDisposable
{
    private readonly object gate = new();
    private int skip;
    private int remaining;
    private bool disposed;
    private HarnessFaultPoints? owner;

    protected PointRule(FaultPoint point, int skip, int times)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfLessThan(times, 1);
        Point = point;
        this.skip = skip;
        remaining = times;
    }

    public FaultPoint Point { get; }

    public int Arrivals { get; private set; }

    protected bool IsDisposed
    {
        get
        {
            lock (gate)
            {
                return disposed;
            }
        }
    }

    protected object Gate => gate;

    internal void Attach(HarnessFaultPoints faults) => owner = faults;

    internal bool Claim()
    {
        lock (gate)
        {
            if (disposed)
            {
                return false;
            }

            Arrivals++;
            if (skip > 0)
            {
                skip--;
                return false;
            }

            if (remaining <= 0)
            {
                return false;
            }

            remaining--;
            return true;
        }
    }

    internal abstract ValueTask ApplyAsync(Arrival arrival, TimeProvider clock, CancellationToken cancellationToken);

    public void Dispose()
    {
        object? state;
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            state = OnDisposing();
            disposed = true;
        }

        owner?.Remove(this);
        OnDisposed(state);
    }

    protected virtual object? OnDisposing() => null;

    protected virtual void OnDisposed(object? state)
    {
    }
}

internal sealed class DelayRule(FaultPoint point, TimeSpan delay, int skip, int times) : PointRule(point, skip, times)
{
    internal override async ValueTask ApplyAsync(Arrival arrival, TimeProvider clock, CancellationToken cancellationToken) =>
        await Task.Delay(delay, clock, cancellationToken).ConfigureAwait(false);
}

internal sealed class ThrowRule(FaultPoint point, Func<Exception> factory, int skip, int times) : PointRule(point, skip, times)
{
    internal override ValueTask ApplyAsync(Arrival arrival, TimeProvider clock, CancellationToken cancellationToken) =>
        throw factory();
}

internal sealed class ObserveRule(FaultPoint point, Func<Arrival, ValueTask> callback) : PointRule(point, 0, int.MaxValue)
{
    internal override ValueTask ApplyAsync(Arrival arrival, TimeProvider clock, CancellationToken cancellationToken) =>
        callback(arrival);
}

internal sealed class HoldRule : PointRule
{
    private readonly List<HeldArrival> queue = [];
    private readonly List<(int Count, TaskCompletionSource Done)> waiters = [];
    private int heldTotal;
    private int releasedTotal;
    private int cancelledTotal;

    public HoldRule(FaultPoint point, int skip, int times)
        : base(point, skip, times)
    {
    }

    public int HeldNow
    {
        get
        {
            lock (Gate)
            {
                return queue.Count;
            }
        }
    }

    public int HeldTotal
    {
        get
        {
            lock (Gate)
            {
                return heldTotal;
            }
        }
    }

    public int ReleasedTotal
    {
        get
        {
            lock (Gate)
            {
                return releasedTotal;
            }
        }
    }

    public int CancelledTotal
    {
        get
        {
            lock (Gate)
            {
                return cancelledTotal;
            }
        }
    }

    public IReadOnlyList<Arrival> Held
    {
        get
        {
            lock (Gate)
            {
                return [.. queue.Select(h => h.Arrival)];
            }
        }
    }

    public async Task WaitReachedAsync(int count = 1, TimeSpan? timeout = null)
    {
        Task done;
        lock (Gate)
        {
            if (heldTotal >= count)
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
            throw new TimeoutException(
                $"Fault point '{Point.Name}' was not held {count} time(s) in time: {HeldTotal} held, {Arrivals} arrival(s) matched the rule.");
        }
    }

    public bool Release() => ReleaseAt(0, null);

    public bool Fail(Exception fault)
    {
        ArgumentNullException.ThrowIfNull(fault);
        return ReleaseAt(0, fault);
    }

    public bool ReleaseAt(int index) => ReleaseAt(index, null);

    public int ReleaseAll()
    {
        HeldArrival[] all;
        lock (Gate)
        {
            all = DrainLocked();
        }

        foreach (var held in all)
        {
            held.Gate.TrySetResult();
        }

        return all.Length;
    }

    protected override object? OnDisposing() => DrainLocked();

    protected override void OnDisposed(object? state)
    {
        foreach (var held in (HeldArrival[])state!)
        {
            held.Gate.TrySetResult();
        }
    }

    internal override async ValueTask ApplyAsync(Arrival arrival, TimeProvider clock, CancellationToken cancellationToken)
    {
        var held = new HeldArrival(arrival, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        List<TaskCompletionSource> ready = [];
        lock (Gate)
        {
            if (IsDisposed)
            {
                return;
            }

            queue.Add(held);
            heldTotal++;
            foreach (var waiter in waiters.Where(w => heldTotal >= w.Count).ToList())
            {
                ready.Add(waiter.Done);
                waiters.Remove(waiter);
            }
        }

        foreach (var waiter in ready)
        {
            waiter.TrySetResult();
        }

        try
        {
            await held.Gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            lock (Gate)
            {
                if (queue.Remove(held))
                {
                    cancelledTotal++;
                    throw;
                }
            }
        }

        if (held.Fault is { } fault)
        {
            throw fault;
        }
    }

    private bool ReleaseAt(int index, Exception? fault)
    {
        HeldArrival chosen;
        lock (Gate)
        {
            if (index < 0 || index >= queue.Count)
            {
                return false;
            }

            chosen = queue[index];
            queue.RemoveAt(index);
            releasedTotal++;
            chosen.Fault = fault;
        }

        chosen.Gate.TrySetResult();
        return true;
    }

    private HeldArrival[] DrainLocked()
    {
        var all = queue.ToArray();
        queue.Clear();
        releasedTotal += all.Length;
        return all;
    }

    private sealed class HeldArrival(Arrival arrival, TaskCompletionSource gate)
    {
        public Arrival Arrival { get; } = arrival;

        public TaskCompletionSource Gate { get; } = gate;

        public Exception? Fault { get; set; }
    }
}
