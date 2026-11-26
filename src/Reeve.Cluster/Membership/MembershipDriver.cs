using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Membership;

internal enum LoadOutcome
{
    Loaded,

    Failed,
}

internal sealed record PublishedPass(Core.Cursor Head, long Entries, bool Full);

internal interface IMemberLoad
{
    Core.Cursor Cursor { get; }

    PublishedPass? LastPublished { get; }

    long LastPassEntries { get; }

    Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken);

    void StartApplying(long epoch);

    void StopForGood();
}

internal sealed class NoMemberLoad : IMemberLoad
{
    public Core.Cursor Cursor => Core.Cursor.Empty;

    public PublishedPass? LastPublished => new(Core.Cursor.Empty, 0, Full: true);

    public long LastPassEntries => 0;

    public Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken) => Task.FromResult(LoadOutcome.Loaded);

    public void StartApplying(long epoch)
    {
    }

    public void StopForGood()
    {
    }
}

internal sealed class MembershipDriver : BackgroundService, IHostedLifecycleService
{
    public const string JoinRefused = "joining.refused";

    public const string JoinUnanswered = "joining.unanswered";

    private readonly LocalPhaseGate gate;
    private readonly IMemberStore store;
    private readonly RenewalLoop renewal;
    private readonly IMemberLoad load;
    private readonly IMemberActivation activation;
    private readonly DrainCoordinator drain;
    private readonly MemberIdentity identity;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly IHostApplicationLifetime lifetime;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly ILogger logger;
    private readonly ClusterOptions options;
    private readonly string replicaId;
    private readonly TaskCompletionSource firstLoad = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object stopGate = new();
    private IDisposable? exitSubscription;
    private Task? stopping;
    private Task? graceful;
    private int exitRequested;
    private int loadFailures;
    private long loadFailuresEpoch;
    private long highestEpoch;

    public MembershipDriver(
        LocalPhaseGate gate,
        IMemberStore store,
        RenewalLoop renewal,
        IMemberLoad load,
        IMemberActivation activation,
        DrainCoordinator drain,
        MemberIdentity identity,
        ClusterCapabilitiesRegistry capabilities,
        IHostApplicationLifetime lifetime,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        ILogger<MembershipDriver> logger)
    {
        this.gate = gate;
        this.store = store;
        this.renewal = renewal;
        this.load = load;
        this.activation = activation;
        this.drain = drain;
        this.identity = identity;
        this.capabilities = capabilities;
        this.lifetime = lifetime;
        this.time = time;
        this.points = points;
        this.logger = logger;
        this.options = options.Value;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    }

    internal Task FirstLoad => firstLoad.Task;

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
        var loop = ExecuteTask ?? Task.CompletedTask;
        try
        {
            await Task.WhenAny(firstLoad.Task, exited.Task, loop).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            using var bound = new CancellationTokenSource(options.LeaseTtl, time);
            await StopAsync(bound.Token).ConfigureAwait(false);
            throw;
        }
    }

    public override Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => StopGracefullyAsync(cancellationToken);

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        lock (stopGate)
        {
            if (stopping is { } first)
            {
                return first.WaitAsync(cancellationToken);
            }

            stopping = StopOnceAsync(cancellationToken);
            return stopping;
        }
    }

    private async Task StopOnceAsync(CancellationToken cancellationToken)
    {
        await StopGracefullyAsync(cancellationToken).ConfigureAwait(false);
        load.StopForGood();
        await renewal.DisposeAsync().ConfigureAwait(false);
    }

    private Task StopGracefullyAsync(CancellationToken cancellationToken)
    {
        lock (stopGate)
        {
            return graceful ??= RunGracefulStopAsync(cancellationToken);
        }
    }

    private async Task RunGracefulStopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        exitSubscription?.Dispose();
        await drain.StopAsync().ConfigureAwait(false);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => RunAsync(stoppingToken);

    internal async Task RunAsync(CancellationToken stoppingToken)
    {
        exitSubscription ??= gate.Subscribe(change =>
        {
            if (change.To == ReplicaPhase.Exited)
            {
                RequestExit(change.From);
            }
        });

        if (gate.Phase == ReplicaPhase.Exited)
        {
            RequestExit(null);
            return;
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var changed = gate.NextPhaseChange;
                var phase = gate.Phase;
                switch (phase)
                {
                    case ReplicaPhase.Starting:
                        await JoinAsync(stoppingToken).ConfigureAwait(false);
                        break;
                    case ReplicaPhase.Loading:
                        await LoadAsync(changed, stoppingToken).ConfigureAwait(false);
                        break;
                    case ReplicaPhase.CatchingUp or ReplicaPhase.Activated when gate.Epoch is { } epoch:
                        await activation.RunAsync(phase, epoch, changed, stoppingToken).ConfigureAwait(false);
                        break;
                    case ReplicaPhase.Exited:
                        RequestExit(null);
                        return;
                    default:
                        await changed.WaitAsync(stoppingToken).ConfigureAwait(false);
                        break;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && stoppingToken.IsCancellationRequested)
        {
            if (exception is not OperationCanceledException)
            {
                logger.LogInformation(exception, "The membership loop of {ReplicaId} stopped; a call that failed after the stop began is not retried.", replicaId);
            }
        }
    }

    private void RequestExit(ReplicaPhase? from)
    {
        exited.TrySetResult();
        if (Interlocked.Exchange(ref exitRequested, 1) == 0)
        {
            logger.LogCritical("Replica {ReplicaId} exits from {Phase}.", replicaId, from?.ToString() ?? "Starting");
            _ = Task.Run(lifetime.StopApplication);
        }
    }

    private async Task JoinAsync(CancellationToken cancellationToken)
    {
        while (gate.Phase == ReplicaPhase.Starting)
        {
            await points.ReachAsync(MembershipFaultPoints.JoinBefore, cancellationToken).ConfigureAwait(false);
            using var joinLock = await capabilities.EnterJoinAsync(cancellationToken).ConfigureAwait(false);
            var cursor = load.Cursor;
            var request = new JoinRequest(replicaId, options.LeaseTtl, identity with { Capabilities = capabilities.Current }, cursor.Seq, cursor.Token);
            var t0 = gate.Now;
            JoinAnswer? answer = null;
            Exception? failure = null;
            using (var bound = new CancellationTokenSource(options.RenewTimeout, time))
            using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, bound.Token))
            {
                try
                {
                    answer = await store.JoinAsync(request, linked.Token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
                {
                    failure = exception;
                }
            }

            if (answer is { } joined)
            {
                await points.ReachAsync(MembershipFaultPoints.JoinAfter, cancellationToken).ConfigureAwait(false);

                if (joined.Epoch <= highestEpoch)
                {
                    logger.LogCritical(
                        "Replica {ReplicaId} was given epoch {Epoch}, at or below epoch {Highest} that this process held: the epoch sequence went back (a restore under a running replica, or a failover that lost the join). The process exits.",
                        replicaId, joined.Epoch, highestEpoch);
                    gate.ReportFatalFault($"epoch {joined.Epoch} at or below the held epoch {highestEpoch}");
                    return;
                }

                highestEpoch = joined.Epoch;
                if (gate.Joined(joined.Epoch, joined.Timeline, t0))
                {
                    gate.Readiness.Clear(JoinRefused);
                    gate.Readiness.Clear(JoinUnanswered);
                    renewal.Restart();
                    logger.LogInformation("Replica {ReplicaId} joined with epoch {Epoch} on timeline {Timeline}.", replicaId, joined.Epoch, joined.Timeline);
                }

                return;
            }

            joinLock.Dispose();
            if (failure is null)
            {
                gate.Apply(MemberTrigger.JoinRefused);
                gate.Readiness.Clear(JoinUnanswered);
                gate.Readiness.Raise(new ReadinessReason(
                    JoinRefused,
                    $"The member row of '{replicaId}' is held by a valid lease (another process with this replica id, or a restored row); the join retries every RenewInterval until it runs out."));
            }
            else
            {
                logger.LogWarning(failure, "The join of {ReplicaId} got no answer; retrying.", replicaId);
                gate.Readiness.Raise(new ReadinessReason(JoinUnanswered, $"The join of '{replicaId}' got no answer from the database ({failure.GetType().Name}); it retries every RenewInterval."));
            }

            await Task.Delay(options.RenewInterval, time, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task LoadAsync(Task phaseChanged, CancellationToken cancellationToken)
    {
        if (gate.Epoch is not { } epoch || gate.JoinedTimeline is not { } timeline)
        {
            return;
        }

        if (epoch != loadFailuresEpoch)
        {
            loadFailures = 0;
            loadFailuresEpoch = epoch;
        }

        LoadOutcome outcome;
        var started = time.GetTimestamp();
        try
        {
            outcome = await load.LoadAsync(epoch, timeline, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The load of epoch {Epoch} failed; retrying.", epoch);
            outcome = LoadOutcome.Failed;
        }

        if (gate.Epoch != epoch)
        {
            return;
        }

        var cell = gate.Apply(outcome == LoadOutcome.Loaded ? MemberTrigger.StateLoaded : MemberTrigger.LoadFailed);
        if (!cell.IsIgnored)
        {
            ClusterDiagnostics.FullLoadDuration.Record(time.GetElapsedTime(started).TotalSeconds, new KeyValuePair<string, object?>("outcome", outcome == LoadOutcome.Loaded ? "loaded" : "failed"));
        }
        if (outcome == LoadOutcome.Loaded && !cell.IsIgnored)
        {
            loadFailures = 0;
            load.StartApplying(epoch);
            firstLoad.TrySetResult();
        }

        if (cell.Effects.HasFlag(MemberEffects.RetryWithBackoff))
        {
            var cap = options.ApplyStallLimit > options.RenewInterval ? options.ApplyStallLimit : options.RenewInterval;
            var delay = Log.ApplierFailureRules.RetryDelay(++loadFailures, options.RenewInterval, cap);
            using var backoff = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            await Task.WhenAny(Task.Delay(delay, time, backoff.Token), phaseChanged).ConfigureAwait(false);
            await backoff.CancelAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}
