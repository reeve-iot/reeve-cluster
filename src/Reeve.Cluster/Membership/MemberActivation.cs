using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Membership;

internal interface IMemberActivation
{
    Task RunAsync(ReplicaPhase phase, long epoch, Task phaseChanged, CancellationToken cancellationToken);
}

internal sealed class NoMemberActivation : IMemberActivation
{
    public Task RunAsync(ReplicaPhase phase, long epoch, Task phaseChanged, CancellationToken cancellationToken) =>
        phaseChanged.WaitAsync(cancellationToken);
}

internal sealed record FloorsRead(int ProtocolFloor, string? VersionFloor, DateTimeOffset ReadAt, FloorMove? Moved = null);

internal sealed record ActivationRecord(long Epoch, long Head, DateTimeOffset? ServingSince);

internal sealed class MemberActivation : IMemberActivation, IDisposable
{
    internal const string RefusalsName = "reeve_cluster.activation.refusals";

    private static readonly Counter<long> Refusals = ClusterDiagnostics.Meter.CreateCounter<long>(
        RefusalsName,
        unit: "{refusal}",
        description: "Activation attempts a floor refused, tagged with reason. A refused member retries every RenewInterval, so this counts attempts, not members.");

    private readonly LocalPhaseGate gate;
    private readonly IActivationStore store;
    private readonly IMemberLoad load;
    private readonly MemberIdentity identity;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IUpgradePathGuard guard;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly TimeSpan pollInterval;
    private readonly TimeSpan retryInterval;
    private readonly TimeSpan statementBound;
    private readonly IDisposable applied;
    private readonly IDisposable phases;
    private readonly object sync = new();

    private TaskCompletionSource appliedSignal = NewSignal();

    private ActivationRecord? record;
    private FloorsRead? floors;
    private string? raisedReason;

    public MemberActivation(
        LocalPhaseGate gate,
        IActivationStore store,
        IMemberLoad load,
        MemberIdentity identity,
        IWakeupChannel channel,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        ILogger<MemberActivation> logger,
        IUpgradePathGuard guard)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);
        this.gate = gate;
        this.store = store;
        this.load = load;
        this.identity = identity;
        this.time = time;
        this.points = points;
        this.logger = logger;
        this.guard = guard;
        var o = options.Value;
        replicaId = o.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
        pollInterval = o.ApplyPollInterval;
        retryInterval = o.RenewInterval;
        statementBound = o.RenewTimeout;

        applied = channel.Subscribe(WakeupKind.Applied, _ => Volatile.Read(ref appliedSignal).TrySetResult());

        phases = gate.Subscribe(change =>
        {
            if (change.Kind == PhaseEventKind.Transition)
            {
                gate.Readiness.Clear(ActivationRules.FailingReason);
                gate.Readiness.Clear(UpgradePathRules.Reason);
            }
        });
    }

    public ActivationRecord? Current
    {
        get
        {
            lock (sync)
            {
                return record is { } r && r.Epoch == gate.Epoch ? r : null;
            }
        }
    }

    public FloorsRead? Floors
    {
        get
        {
            lock (sync)
            {
                return floors;
            }
        }
    }

    public Task RunAsync(ReplicaPhase phase, long epoch, Task phaseChanged, CancellationToken cancellationToken) => phase switch
    {
        ReplicaPhase.CatchingUp => CatchUpRoundAsync(epoch, phaseChanged, cancellationToken),
        ReplicaPhase.Activated => ServingRoundAsync(epoch, phaseChanged, cancellationToken),
        _ => phaseChanged.WaitAsync(cancellationToken),
    };

    public void Dispose()
    {
        phases.Dispose();
        applied.Dispose();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task CatchUpRoundAsync(long epoch, Task phaseChanged, CancellationToken cancellationToken)
    {
        var notice = NextApplied();
        if (load.LastPublished is not { } last)
        {
            await WaitAsync(phaseChanged, notice, pollInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        var threshold = load.LastPassEntries;
        Backlog backlog;
        try
        {
            using var timer = new CancellationTokenSource(statementBound, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            backlog = await store.ReadBacklogAsync(last.Head, threshold, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The caught-up check of epoch {Epoch} got no answer; retrying.", epoch);
            await WaitAsync(phaseChanged, notice, pollInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!ActivationRules.IsCaughtUp(backlog.CursorValid, backlog.EntriesAfter, threshold))
        {
            await WaitAsync(phaseChanged, notice, pollInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (await HeldByUpgradePathAsync(guard, epoch, cancellationToken).ConfigureAwait(false))
        {
            await WaitAsync(phaseChanged, notice: null, retryInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (await ActivateOnceAsync(epoch, cancellationToken).ConfigureAwait(false) is not { } answer)
        {
            await WaitAsync(phaseChanged, notice: null, retryInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        gate.Readiness.Clear(ActivationRules.FailingReason);

        if (answer.Outcome == ActivationOutcome.Unanswered)
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.UnansweredActivation, $"the activation of epoch {epoch} got no answer"), epoch);
            return;
        }

        if (answer.FloorsRead)
        {
            lock (sync)
            {
                floors = new FloorsRead(answer.ProtocolFloor ?? 0, answer.VersionFloor, time.GetUtcNow(), answer.Moved);
            }
        }

        if (ActivationRules.TriggerFor(answer.Outcome) is { } trigger)
        {
            var payload = new Dictionary<string, object?>
            {
                ["head"] = answer.Head,
                ["cursor"] = last.Head.Seq,
                ["entriesAfter"] = backlog.EntriesAfter,
                ["threshold"] = threshold,
                ["protocolFloor"] = answer.ProtocolFloor,
                ["versionFloor"] = answer.VersionFloor,
                ["protocol"] = identity.Protocol,
            };
            var cell = gate.Apply(trigger, epoch, payload);
            if (trigger == MemberTrigger.ActivationCommitted && !cell.IsIgnored)
            {
                lock (sync)
                {
                    record = new ActivationRecord(epoch, answer.Head, null);
                }

                ClearReason();
                logger.LogInformation(
                    "Replica {ReplicaId} activated epoch {Epoch} at head {Head}; it serves once its applier reaches it.", replicaId, epoch, answer.Head);
                return;
            }

            if (trigger == MemberTrigger.ActivationRefusedByFloor && !cell.IsIgnored)
            {
                RaiseReason(answer);
            }
        }

        if (answer.Outcome is ActivationOutcome.Committed or ActivationOutcome.LeftCatchingUp)
        {
            return;
        }

        await WaitAsync(phaseChanged, notice: null, retryInterval, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> HeldByUpgradePathAsync(IUpgradePathGuard upgradePath, long epoch, CancellationToken cancellationToken)
    {
        OlderActiveMember? blocker;
        try
        {
            using var timer = new CancellationTokenSource(statementBound, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            blocker = await upgradePath.FindBlockerAsync(replicaId, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The upgrade-path guard's read of epoch {Epoch} got no answer; the replica stays joining and asks again.", epoch);
            return true;
        }

        if (blocker is null)
        {
            gate.Readiness.Clear(UpgradePathRules.Reason);
            return false;
        }

        var first = gate.Readiness.Current.All(r => r.Code != UpgradePathRules.Reason);
        gate.Readiness.Raise(UpgradePathRules.ReasonFor(blocker, upgradePath.MinimumPeerProtocol));
        if (first)
        {
            logger.LogWarning(
                "Replica {ReplicaId} stays joining: member {Blocker} is active and speaks protocol {Protocol}, below the {Required} this release needs of every other member.",
                replicaId, blocker.ReplicaId, blocker.Protocol, upgradePath.MinimumPeerProtocol);
        }

        if (gate.Phase != ReplicaPhase.CatchingUp || gate.Epoch != epoch)
        {
            gate.Readiness.Clear(UpgradePathRules.Reason);
        }

        return true;
    }

    private async Task<ActivationAnswer?> ActivateOnceAsync(long epoch, CancellationToken cancellationToken)
    {
        var request = new ActivationRequest(replicaId, epoch, identity.Version, identity.Protocol);
        var committing = false;
        bool MayCommit()
        {
            committing = gate.Phase == ReplicaPhase.CatchingUp && gate.Epoch == epoch;
            return committing;
        }

        try
        {
            using var timer = new CancellationTokenSource(statementBound, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            return await store.ActivateAsync(request, MayCommit, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            if (!committing)
            {
                ReportFailedRound(epoch, exception);
                return null;
            }

            logger.LogWarning(exception, "The commit of epoch {Epoch}'s activation got no answer; the replica fences.", epoch);
            return new ActivationAnswer(ActivationOutcome.Unanswered, 0, null, null, FloorsRead: false);
        }
    }

    private async Task ServingRoundAsync(long epoch, Task phaseChanged, CancellationToken cancellationToken)
    {
        var notice = NextApplied();
        await points.ReachAsync(MembershipFaultPoints.ActivateCommittedBeforeApplied, cancellationToken).ConfigureAwait(false);
        ActivationRecord? current;
        lock (sync)
        {
            current = record is { } r && r.Epoch == epoch ? r : null;
        }

        if (current is null)
        {
            await phaseChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var cursor = load.Cursor;
        if (!ActivationRules.HasReachedHead(cursor, current.Head))
        {
            await WaitAsync(phaseChanged, notice, pollInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        bool written;
        try
        {
            using var timer = new CancellationTokenSource(statementBound, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            written = await store.SetServingSinceAsync(replicaId, epoch, cursor, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The serving_since write of epoch {Epoch} got no answer; retrying.", epoch);
            written = false;
        }

        if (!written)
        {
            await WaitAsync(phaseChanged, notice, pollInterval, cancellationToken).ConfigureAwait(false);
            return;
        }

        var cell = gate.Apply(
            MemberTrigger.AppliedToActivationHead,
            epoch,
            new Dictionary<string, object?> { ["head"] = cursor.Seq, ["act"] = current.Head });
        if (!cell.IsIgnored && cell.To == ReplicaPhase.Serving)
        {
            lock (sync)
            {
                if (record is { } r && r.Epoch == epoch)
                {
                    record = r with { ServingSince = time.GetUtcNow() };
                }
            }

            logger.LogInformation("Replica {ReplicaId} serves epoch {Epoch} from cursor {Cursor}.", replicaId, epoch, cursor.Seq);
        }
    }

    private Task NextApplied()
    {
        var signal = Volatile.Read(ref appliedSignal);
        if (signal.Task.IsCompleted)
        {
            Interlocked.CompareExchange(ref appliedSignal, NewSignal(), signal);
            signal = Volatile.Read(ref appliedSignal);
        }

        return signal.Task;
    }

    private async Task WaitAsync(Task phaseChanged, Task? notice, TimeSpan delay, CancellationToken cancellationToken)
    {
        using var tick = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = Task.Delay(delay, time, tick.Token);
        await (notice is null ? Task.WhenAny(phaseChanged, wait) : Task.WhenAny(phaseChanged, notice, wait)).ConfigureAwait(false);
        await tick.CancelAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }

    private void ReportFailedRound(long epoch, Exception exception)
    {
        logger.LogError(
            exception,
            "The activation of epoch {Epoch} failed before its commit was sent; nothing was committed, the replica stays CatchingUp, and it is retried after RenewInterval {RenewInterval}.",
            epoch,
            retryInterval);
        gate.Readiness.Raise(new ReadinessReason(
            ActivationRules.FailingReason,
            $"The activation of epoch {epoch} failed before its commit was sent ({exception.GetType().Name}: {exception.Message}); nothing was committed, the replica stays CatchingUp, and it is retried after RenewInterval {retryInterval}."));

        if (gate.Phase != ReplicaPhase.CatchingUp || gate.Epoch != epoch)
        {
            gate.Readiness.Clear(ActivationRules.FailingReason);
        }
    }

    private void RaiseReason(ActivationAnswer answer)
    {
        var code = ActivationRules.ReasonCode(answer.Outcome)!;
        var description = answer.Outcome == ActivationOutcome.RefusedByProtocolFloor
            ? $"The protocol floor is {answer.ProtocolFloor}; this replica speaks protocol {identity.Protocol}. It stays joining and never serves (a rollback, or a skipped release)."
            : $"The version floor is '{answer.VersionFloor}'; this replica runs '{identity.Version}', below it by SemVer precedence or not SemVer. It stays joining and never serves (a rollback, or a skipped release).";
        gate.Readiness.Clear(code == ActivationRules.ProtocolFloorReason ? ActivationRules.VersionFloorReason : ActivationRules.ProtocolFloorReason);
        gate.Readiness.Raise(new ReadinessReason(code, description));
        Refusals.Add(1, new KeyValuePair<string, object?>("reason", code));
        bool first;
        lock (sync)
        {
            first = raisedReason != code;
            raisedReason = code;
        }

        if (first)
        {
            logger.LogWarning("Replica {ReplicaId} was refused activation ({Reason}): {Description}", replicaId, code, description);
        }
    }

    private void ClearReason()
    {
        gate.Readiness.Clear(ActivationRules.ProtocolFloorReason);
        gate.Readiness.Clear(ActivationRules.VersionFloorReason);
        lock (sync)
        {
            raisedReason = null;
        }
    }
}
