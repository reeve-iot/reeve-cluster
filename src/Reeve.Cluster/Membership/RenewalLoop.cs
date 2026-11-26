using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Membership;

internal sealed class RenewalLoop : IAsyncDisposable
{
    private readonly LocalPhaseGate gate;
    private readonly IMemberStore store;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly TimeSpan leaseTtl;
    private readonly TimeSpan renewTimeout;
    private readonly TimeSpan renewInterval;
    private readonly ITimer timer;
    private readonly CancellationTokenSource stopping = new();
    private readonly HashSet<Task> inFlight = [];
    private int disposed;

    public RenewalLoop(
        LocalPhaseGate gate,
        IMemberStore store,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        IModelEventSink events,
        ILogger<RenewalLoop> logger)
    {
        this.gate = gate;
        this.store = store;
        this.time = time;
        this.points = points;
        this.events = events;
        this.logger = logger;
        var o = options.Value;
        replicaId = o.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
        leaseTtl = o.LeaseTtl;
        renewTimeout = o.RenewTimeout;
        renewInterval = o.RenewInterval;
        timer = time.CreateTimer(_ => Tick(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    internal int InFlight
    {
        get
        {
            lock (inFlight)
            {
                return inFlight.Count;
            }
        }
    }

    internal Task WhenIdle()
    {
        lock (inFlight)
        {
            return Task.WhenAll([.. inFlight]);
        }
    }

    public void Restart()
    {
        if (!stopping.IsCancellationRequested)
        {
            timer.Change(renewInterval, renewInterval);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        await timer.DisposeAsync().ConfigureAwait(false);
        await stopping.CancelAsync().ConfigureAwait(false);
        Task[] running;
        lock (inFlight)
        {
            running = [.. inFlight];
        }

        await Task.WhenAll(running).ConfigureAwait(false);
    }

    private void Tick()
    {
        if (stopping.IsCancellationRequested || !gate.TryGetRenewal(out var target))
        {
            return;
        }

        var attempt = AttemptAsync(target);
        lock (inFlight)
        {
            if (!attempt.IsCompleted)
            {
                inFlight.Add(attempt);
                _ = attempt.ContinueWith(
                    t =>
                    {
                        lock (inFlight)
                        {
                            inFlight.Remove(t);
                        }
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
    }

    internal async Task AttemptAsync(RenewalTarget target)
    {
        try
        {
            await points.ReachAsync(MembershipFaultPoints.RenewSend, stopping.Token).ConfigureAwait(false);
            if (!gate.TryGetRenewal(out var current) || current != target)
            {
                return;
            }

            var t0 = gate.Now;
            events.Emit(new ModelEvent(MembershipActions.RenewSend, new Dictionary<string, object?> { ["epoch"] = target.Epoch }));
            using var clock = new AttemptClock(time, renewTimeout, stopping.Token);

            RenewalAnswer? answer = null;
            try
            {
                await points.ReachAsync(MembershipFaultPoints.RenewExec, clock.Token).ConfigureAwait(false);
                var returned = await store.RenewAsync(replicaId, target.Epoch, leaseTtl, clock.Token).ConfigureAwait(false);
                events.Emit(new ModelEvent(MembershipActions.RenewExec, new Dictionary<string, object?>
                {
                    ["epoch"] = target.Epoch,
                    ["rows"] = returned.RowsAffected,
                    ["timeline"] = returned.Timeline,
                }));

                await points.ReachAsync(MembershipFaultPoints.RenewAnswer, clock.Token).ConfigureAwait(false);
                answer = returned;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                if (!stopping.IsCancellationRequested)
                {
                    logger.LogDebug(exception, "Renewal attempt of epoch {Epoch} got no answer.", target.Epoch);
                }
            }

            var cancelRequested = !clock.TryAnswer();
            var elapsed = gate.Now - t0;
            var outcome = RenewalRules.Classify(answer, cancelRequested, elapsed, renewTimeout, gate.TimelineCheckDisabled && answer?.Timeline is { } answered ? answered : target.Timeline);
            if (cancelRequested && answer is { } late)
            {
                logger.LogInformation(
                    "Renewal attempt of epoch {Epoch} returned {Rows} row(s) after its cancel at RenewTimeout; it is not counted.",
                    target.Epoch,
                    late.RowsAffected);
            }

            if (!stopping.IsCancellationRequested)
            {
                ClusterDiagnostics.RenewalDuration.Record(elapsed.TotalSeconds, new KeyValuePair<string, object?>("outcome", outcome switch
                {
                    RenewalOutcome.Renewed => "renewed",
                    RenewalOutcome.OtherTimeline => "other_timeline",
                    RenewalOutcome.NotMine => "not_mine",
                    RenewalOutcome.Unknown => "unknown",
                    _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
                }));
                gate.ApplyRenewal(new RenewalAttempt(target.Epoch, t0), outcome);
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
        }
    }

    private sealed class AttemptClock : IDisposable
    {
        private const int Pending = 0;
        private const int Answered = 1;
        private const int TimedOut = 2;

        private readonly CancellationTokenSource cancel;
        private readonly ITimer timer;
        private int state;

        public AttemptClock(TimeProvider time, TimeSpan timeout, CancellationToken stopping)
        {
            cancel = CancellationTokenSource.CreateLinkedTokenSource(stopping);
            timer = time.CreateTimer(_ => Expire(), null, timeout, Timeout.InfiniteTimeSpan);
        }

        public CancellationToken Token => cancel.Token;

        public bool TryAnswer() => Interlocked.CompareExchange(ref state, Answered, Pending) == Pending && !cancel.IsCancellationRequested;

        public void Dispose()
        {
            timer.Dispose();
            cancel.Dispose();
        }

        private void Expire()
        {
            if (Interlocked.CompareExchange(ref state, TimedOut, Pending) == Pending)
            {
                try
                {
                    _ = cancel.CancelAsync();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }
}
