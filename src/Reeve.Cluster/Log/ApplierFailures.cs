using Microsoft.Extensions.Logging;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Log;

internal enum StallVerdict
{
    None,

    Within,

    KeepRetrying,

    Fence,
}

internal static class ApplierFailureRules
{
    public static TimeSpan RetryDelay(int failures, TimeSpan first, TimeSpan cap)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(failures, 1);
        var ceiling = cap < first ? first : cap;
        var delay = first;
        for (var i = 1; i < failures && delay < ceiling; i++)
        {
            delay += delay;
        }

        return delay < ceiling ? delay : ceiling;
    }

    public static TimeSpan BeforeStallDeadline(TimeSpan delay, TimeSpan? untilLastRetry) =>
        untilLastRetry is { } until && until > TimeSpan.Zero && until < delay ? until : delay;

    public static StallVerdict StallLimit(ReplicaPhase phase, TimeSpan? stalledFor, TimeSpan limit)
    {
        if (stalledFor is not { } stalled)
        {
            return StallVerdict.None;
        }

        return phase switch
        {
            ReplicaPhase.Activated or ReplicaPhase.Serving or ReplicaPhase.Draining => stalled >= limit ? StallVerdict.Fence : StallVerdict.Within,
            ReplicaPhase.Loading or ReplicaPhase.CatchingUp => stalled >= limit ? StallVerdict.KeepRetrying : StallVerdict.Within,
            _ => StallVerdict.None,
        };
    }

    public static long StuckAt(UnreturnedAttempt attempt, TimeSpan allowance, long frequency)
    {
        ArgumentNullException.ThrowIfNull(attempt);
        return attempt.Attempt.Started + (long)(allowance.TotalSeconds * frequency);
    }

    public static UnreturnedAttempt? FirstStuck(IEnumerable<UnreturnedAttempt> unreturned, long now, TimeSpan allowance, long frequency) =>
        unreturned
            .Where(u => StuckAt(u, allowance, frequency) <= now)
            .OrderBy(u => u.Attempt.Started)
            .FirstOrDefault();

    public static long? NextStuckAt(IEnumerable<UnreturnedAttempt> unreturned, TimeSpan allowance, long frequency) =>
        unreturned.Select(u => (long?)StuckAt(u, allowance, frequency)).Min();
}

internal sealed class ApplierStallClock : IDisposable
{
    private readonly object sync = new();
    private readonly LocalPhaseGate gate;
    private readonly TimeSpan limit;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly ILogger logger;
    private readonly ITimer timer;

    private long? since;
    private long epoch;
    private bool disposed;
    private Task deciding = Task.CompletedTask;

    public ApplierStallClock(LocalPhaseGate gate, TimeSpan limit, TimeProvider time, IFaultPoints points, ILogger logger)
    {
        this.gate = gate;
        this.limit = limit;
        this.time = time;
        this.points = points;
        this.logger = logger;
        timer = time.CreateTimer(_ => Decide(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public bool IsStalled
    {
        get
        {
            lock (sync)
            {
                return since is not null;
            }
        }
    }

    public TimeSpan Limit => limit;

    public TimeSpan? Left
    {
        get
        {
            lock (sync)
            {
                return since is { } start ? limit - time.GetElapsedTime(start) : null;
            }
        }
    }

    internal Task Deciding
    {
        get
        {
            lock (sync)
            {
                return deciding;
            }
        }
    }

    public void Stalled(long passEpoch, long passStarted)
    {
        bool due;
        lock (sync)
        {
            if (disposed)
            {
                return;
            }

            since ??= passStarted;
            epoch = passEpoch;
            var left = limit - time.GetElapsedTime(since.Value);
            due = left <= TimeSpan.Zero;
            if (!due)
            {
                timer.Change(left, Timeout.InfiniteTimeSpan);
            }
        }

        if (due)
        {
            Decide();
        }
    }

    public void Recovered()
    {
        lock (sync)
        {
            since = null;
            if (!disposed)
            {
                timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }
    }

    public void Dispose()
    {
        lock (sync)
        {
            disposed = true;
        }

        timer.Dispose();
    }

    private void Decide()
    {
        var task = DecideAsync();
        lock (sync)
        {
            deciding = task;
        }
    }

    private async Task DecideAsync()
    {
        long started;
        long stallEpoch;
        lock (sync)
        {
            if (disposed || since is not { } s)
            {
                return;
            }

            started = s;
            stallEpoch = epoch;
        }

        try
        {
            await points.ReachAsync(LogFaultPoints.StallLimitDecision).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "The stall-limit decision's fault point threw; the decision runs anyway.");
        }

        TimeSpan stalledFor;
        lock (sync)
        {
            if (disposed || since != started)
            {
                return;
            }

            stalledFor = time.GetElapsedTime(started);
        }

        var phase = gate.Phase;
        switch (ApplierFailureRules.StallLimit(phase, stalledFor, limit))
        {
            case StallVerdict.Fence:
                logger.LogWarning(
                    "The applier has stalled for {StalledFor} in {Phase}, past ReconcileTimeout + ApplyStallLimit ({Limit}): the replica fences.",
                    stalledFor, phase, limit);
                gate.RequestFence(
                    new FenceRequest(FenceTrigger.ApplierStall, $"the applier stalled for {stalledFor}, past ReconcileTimeout + ApplyStallLimit ({limit})"),
                    stallEpoch);
                break;
            case StallVerdict.KeepRetrying:
                logger.LogWarning(
                    "The applier has stalled for {StalledFor} in {Phase}, past ReconcileTimeout + ApplyStallLimit ({Limit}); not a fence trigger here, it keeps retrying.",
                    stalledFor, phase, limit);
                break;
            case StallVerdict.Within:
                lock (sync)
                {
                    if (!disposed && since == started)
                    {
                        timer.Change(limit - stalledFor, Timeout.InfiniteTimeSpan);
                    }
                }

                break;
        }
    }
}

internal sealed class StuckAttemptWatch : IDisposable
{
    internal static readonly TimeSpan OversleepSlack = TimeSpan.FromMilliseconds(250);

    private readonly object sync = new();
    private readonly GenerationStore store;
    private readonly IPhaseGate gate;
    private readonly TimeSpan allowance;
    private readonly TimeSpan grace;
    private readonly IFaultPoints points;
    private readonly ILogger logger;
    private readonly ITimer timer;
    private readonly IDisposable subscription;

    private readonly Dictionary<ApplierAttempt, long> graces = new(ReferenceEqualityComparer.Instance);
    private bool raised;
    private bool disposed;
    private Task deciding = Task.CompletedTask;

    public StuckAttemptWatch(GenerationStore store, IPhaseGate gate, TimeSpan allowance, TimeSpan grace, IFaultPoints points, ILogger logger)
    {
        this.grace = grace;
        this.store = store;
        this.gate = gate;
        this.allowance = allowance;
        this.points = points;
        this.logger = logger;
        timer = store.Time.CreateTimer(_ => Decide(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        subscription = store.OnUnreturned(Arm);
        Arm();
    }

    internal Task Deciding
    {
        get
        {
            lock (sync)
            {
                return deciding;
            }
        }
    }

    public void Dispose()
    {
        lock (sync)
        {
            disposed = true;
        }

        subscription.Dispose();
        timer.Dispose();
    }

    private void Arm()
    {
        bool now;
        lock (sync)
        {
            if (raised || disposed)
            {
                return;
            }

            var time = store.Time;
            var unreturned = store.Unreturned;
            foreach (var attempt in graces.Keys.Where(a => !unreturned.Any(u => ReferenceEquals(u.Attempt, a))).ToList())
            {
                graces.Remove(attempt);
            }

            if (unreturned.Select(u => (long?)DeadlineLocked(u)).Min() is not { } at)
            {
                timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                return;
            }

            var due = time.GetElapsedTime(time.GetTimestamp(), at);
            now = due <= TimeSpan.Zero;
            timer.Change(now ? Timeout.InfiniteTimeSpan : due, Timeout.InfiniteTimeSpan);
        }

        if (now)
        {
            Decide();
        }
    }

    private long DeadlineLocked(UnreturnedAttempt unreturned)
    {
        var stuckAt = ApplierFailureRules.StuckAt(unreturned, allowance, store.Time.TimestampFrequency);
        return graces.TryGetValue(unreturned.Attempt, out var graceEnds) && graceEnds > stuckAt ? graceEnds : stuckAt;
    }

    private void Decide()
    {
        var task = DecideAsync();
        lock (sync)
        {
            deciding = task;
        }
    }

    private async Task DecideAsync()
    {
        lock (sync)
        {
            if (raised || disposed)
            {
                return;
            }
        }

        try
        {
            await points.ReachAsync(LogFaultPoints.StuckAttemptDecision).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "The stuck-attempt decision's fault point threw; the decision runs anyway.");
        }

        var time = store.Time;
        var now = time.GetTimestamp();
        UnreturnedAttempt? stuck;
        TimeSpan? overslept = null;
        lock (sync)
        {
            if (raised || disposed)
            {
                return;
            }

            stuck = store.Unreturned.Where(u => DeadlineLocked(u) <= now).OrderBy(u => u.Attempt.Started).FirstOrDefault();
            if (stuck is not null && !graces.ContainsKey(stuck.Attempt))
            {
                var late = time.GetElapsedTime(ApplierFailureRules.StuckAt(stuck, allowance, time.TimestampFrequency), now);
                if (late > OversleepSlack)
                {
                    graces[stuck.Attempt] = now + (long)(grace.TotalSeconds * time.TimestampFrequency);
                    overslept = late;
                }
            }

            if (stuck is not null && overslept is null)
            {
                raised = true;
                timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }

        if (stuck is null || overslept is not null)
        {
            if (overslept is { } by)
            {
                logger.LogWarning(
                    "The stuck-attempt decision for attempt {Attempt} of pass {Pass} ({AggregateType}) ran {Late} late, so the process overslept; it is judged again after {Grace}.",
                    stuck!.Attempt.Number, stuck.Attempt.PassNumber, stuck.Attempt.AggregateType, by, grace);
            }

            Arm();
            return;
        }

        var attempt = stuck.Attempt;
        var detail = $"attempt {attempt.Number} of pass {attempt.PassNumber} ({attempt.AggregateType}, {stuck.Why}) has not returned "
            + $"ReconcileTimeout + ApplyStallLimit ({allowance}) after its start";
        logger.LogCritical(
            "A reconciler attempt is stuck: attempt {Attempt} of pass {Pass} ({AggregateType}, {Why}) has not returned {Allowance} after its start; the process ends.",
            attempt.Number, attempt.PassNumber, attempt.AggregateType, stuck.Why, allowance);
        gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt, detail));
    }
}
