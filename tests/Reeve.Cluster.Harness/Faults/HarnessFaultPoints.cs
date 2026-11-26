using System.Runtime.ExceptionServices;

namespace Reeve.Cluster.Harness.Faults;

internal sealed class HarnessFaultPoints : IFaultPoints
{
    private readonly object gate = new();
    private readonly Dictionary<string, List<PointRule>> rules = new(StringComparer.Ordinal);
    private readonly List<ParkedArrival> parked = [];
    private readonly string replica;
    private readonly TimeProvider clock;
    private readonly ArrivalLog log;
    private TaskCompletionSource? pauseGate;
    private bool abandoned;

    public HarnessFaultPoints(string replica, TimeProvider clock, ArrivalLog log)
    {
        this.replica = replica;
        this.clock = clock;
        this.log = log;
    }

    public bool IsPaused
    {
        get
        {
            lock (gate)
            {
                return pauseGate is not null;
            }
        }
    }

    public bool IsAbandoned
    {
        get
        {
            lock (gate)
            {
                return abandoned;
            }
        }
    }

    public int Reached(FaultPoint point) => log.Count(replica, point);

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        var entry = FaultPointCatalogue.Find(point) ?? throw new UnknownFaultPointException(point);

        Arrival arrival;
        PointRule[] claimed;
        ParkedArrival? waiting = null;
        lock (gate)
        {
            if (abandoned)
            {
                return new ValueTask(new TaskCompletionSource().Task);
            }

            arrival = log.Record(replica, entry.Name, clock);
            if (pauseGate is not null)
            {
                waiting = new ParkedArrival(arrival);
                parked.Add(waiting);
                claimed = [];
            }
            else
            {
                claimed = ClaimLocked(entry.Name);
                if (claimed.Length == 0)
                {
                    return ValueTask.CompletedTask;
                }
            }
        }

        return waiting is null
            ? ApplyAsync(arrival, claimed, cancellationToken)
            : ParkAsync(waiting, cancellationToken);
    }

    public HoldRule Hold(FaultPoint point, int skip = 0, int times = int.MaxValue) => Add(new HoldRule(point, skip, times));

    public HoldRule HoldNext(FaultPoint point) => Hold(point, 0, 1);

    public DelayRule Delay(FaultPoint point, TimeSpan delay, int skip = 0, int times = int.MaxValue) =>
        Add(new DelayRule(point, delay, skip, times));

    public ThrowRule Throw(FaultPoint point, Func<Exception> fault, int skip = 0, int times = 1) =>
        Add(new ThrowRule(point, fault, skip, times));

    public ObserveRule Observe(FaultPoint point, Func<Arrival, ValueTask> callback) => Add(new ObserveRule(point, callback));

    public void Pause()
    {
        lock (gate)
        {
            pauseGate ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    public void Resume()
    {
        TaskCompletionSource? current;
        ParkedArrival[] woken;
        lock (gate)
        {
            current = pauseGate;
            if (current is null)
            {
                return;
            }

            pauseGate = null;
            woken = [.. parked.OrderBy(p => p.Arrival.Sequence)];
            parked.Clear();
            foreach (var arrival in woken)
            {
                arrival.Claimed = ClaimLocked(arrival.Arrival.Point);
            }
        }

        current.TrySetResult();
        foreach (var arrival in woken)
        {
            arrival.Woken.TrySetResult();
        }
    }

    public void Abandon()
    {
        lock (gate)
        {
            abandoned = true;
        }
    }

    internal void Remove(PointRule rule)
    {
        lock (gate)
        {
            if (rules.TryGetValue(rule.Point.Name, out var list) && list.Remove(rule) && list.Count == 0)
            {
                rules.Remove(rule.Point.Name);
            }
        }
    }

    private T Add<T>(T rule)
        where T : PointRule
    {
        rule.Attach(this);
        lock (gate)
        {
            if (!rules.TryGetValue(rule.Point.Name, out var list))
            {
                rules[rule.Point.Name] = list = [];
            }

            list.Add(rule);
        }

        return rule;
    }

    private PointRule[] ClaimLocked(string point) =>
        rules.TryGetValue(point, out var list) ? [.. list.Where(rule => rule.Claim())] : [];

    private async ValueTask ParkAsync(ParkedArrival waiting, CancellationToken cancellationToken)
    {
        await waiting.Woken.Task.ConfigureAwait(false);
        await ApplyAsync(waiting.Arrival, waiting.Claimed, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ApplyAsync(Arrival arrival, PointRule[] claimed, CancellationToken cancellationToken)
    {
        Exception? fault = null;
        try
        {
            foreach (var rule in claimed)
            {
                await rule.ApplyAsync(arrival, clock, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            fault = exception;
        }

        await MayGoOnAsync().ConfigureAwait(false);

        if (fault is not null)
        {
            ExceptionDispatchInfo.Capture(fault).Throw();
        }
    }

    private async ValueTask MayGoOnAsync()
    {
        while (true)
        {
            Task waiting;
            lock (gate)
            {
                if (abandoned)
                {
                    waiting = new TaskCompletionSource().Task;
                }
                else if (pauseGate is { } paused)
                {
                    waiting = paused.Task;
                }
                else
                {
                    return;
                }
            }

            await waiting.ConfigureAwait(false);
        }
    }

    private sealed class ParkedArrival(Arrival arrival)
    {
        public Arrival Arrival { get; } = arrival;

        public TaskCompletionSource Woken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PointRule[] Claimed { get; set; } = [];
    }
}
