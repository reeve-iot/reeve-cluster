using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Slots;

namespace Reeve.Cluster.Membership;

internal readonly record struct RenewalTarget(long Epoch, long Timeline);

internal sealed class LocalPhaseGate : IPhaseGate, IDisposable
{
    private const long NoFenceStart = long.MinValue;
    private const long NoServingEpoch = long.MinValue;

    private readonly object sync = new();
    private readonly LeaseTiming timing;
    private readonly TimeProvider time;
    private readonly IModelEventSink events;
    private readonly IFaultPoints points;
    private readonly ILogger logger;
    private readonly long origin;
    private readonly ITimer fenceTimer;
    private readonly ReadinessReasons readiness = new();

    private Action<FenceRequest>[] stepZeroHooks = [];
    private Action<FenceRequest>[] fenceHandlers = [];
    private Action<FenceRequest>[] exitHandlers = [];
    private Action<PhaseEvent>[] phaseHandlers = [];
    private Action<long>[] renewedHandlers = [];

    private int phase = (int)ReplicaPhase.Starting;
    private long fenceStartTicks = NoFenceStart;

    private long servingEpoch = NoServingEpoch;
    private TaskCompletionSource phaseChanged = NewSignal();

    private long? epoch;
    private long? joinedTimeline;
    private TimeSpan? deadline;
    private bool drainRowWriteStarted;
    private bool drainStep4Passed;
    private bool exitAfterFence;
    private ExitCauses exitCauses;
    private TimeSpan? joinedAt;
    private FenceRun? currentFence;
    private TimeSpan? outOfServiceSince;
    private bool fenceStartReached;
    private bool disposed;

    public LocalPhaseGate(IOptions<ClusterOptions> options, TimeProvider time, IModelEventSink events, ILogger<LocalPhaseGate> logger)
        : this(LeaseTiming.From(options.Value), time, events, NoFaultPoints.Instance, logger)
    {
    }

    public LocalPhaseGate(IOptions<ClusterOptions> options, TimeProvider time, IModelEventSink events, IFaultPoints points, ILogger<LocalPhaseGate> logger)
        : this(LeaseTiming.From(options.Value), time, events, points, logger)
    {
    }

    internal LocalPhaseGate(LeaseTiming timing, TimeProvider time, IModelEventSink events, ILogger<LocalPhaseGate> logger)
        : this(timing, time, events, NoFaultPoints.Instance, logger)
    {
    }

    internal LocalPhaseGate(LeaseTiming timing, TimeProvider time, IModelEventSink events, IFaultPoints points, ILogger<LocalPhaseGate> logger)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(points);
        ArgumentNullException.ThrowIfNull(logger);

        this.timing = timing;
        this.time = time;
        this.events = events;
        this.points = points;
        this.logger = logger;
        origin = time.GetTimestamp();
        fenceTimer = time.CreateTimer(_ => OnFenceTimer(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        ClusterDiagnostics.TrackPhase(this);
    }

    public ReplicaPhase Phase => (ReplicaPhase)Volatile.Read(ref phase);

    public TimeSpan Now => time.GetElapsedTime(origin);

    public long? FenceStartTimestamp
    {
        get
        {
            var ticks = Interlocked.Read(ref fenceStartTicks);
            return ticks == NoFenceStart
                ? null
                : origin + (long)Math.Round(ticks * (double)time.TimestampFrequency / TimeSpan.TicksPerSecond);
        }
    }

    public bool ExitAfterFence
    {
        get
        {
            lock (sync)
            {
                return exitAfterFence;
            }
        }
    }

    public IReadinessReasons Readiness => readiness;

    public bool DrainStep4Passed
    {
        get
        {
            lock (sync)
            {
                return drainStep4Passed;
            }
        }
    }

    public FenceRun? CurrentFence
    {
        get
        {
            lock (sync)
            {
                return currentFence;
            }
        }
    }

    public bool CheckTimeline(long statementEpoch, long timeline)
    {
        lock (sync)
        {
            if (epoch != statementEpoch)
            {
                return false;
            }

            if (joinedTimeline == timeline || TimelineCheckDisabled)
            {
                return true;
            }

            RequestFenceLocked(
                new FenceRequest(FenceTrigger.Timeline, $"a statement found timeline {timeline}; the join answered on {joinedTimeline}"),
                triggerEventEmitted: false);
            return false;
        }
    }

    public void OnOtherTimeline(long statementEpoch)
    {
        lock (sync)
        {
            if (epoch == statementEpoch && !TimelineCheckDisabled)
            {
                RequestFenceLocked(
                    new FenceRequest(FenceTrigger.Timeline, $"a statement was refused on another timeline than the one the join answered on ({joinedTimeline})"),
                    triggerEventEmitted: false);
            }
        }
    }

    public bool TryGetTimeline(long statementEpoch, out long timeline)
    {
        lock (sync)
        {
            if (epoch == statementEpoch && joinedTimeline is { } t)
            {
                timeline = t;
                return true;
            }

            timeline = default;
            return false;
        }
    }

    internal volatile bool TimelineCheckDisabled;

    internal long? ServingEpoch
    {
        get
        {
            var stamp = Volatile.Read(ref servingEpoch);
            return stamp == NoServingEpoch ? null : stamp;
        }
    }

    public long? Epoch
    {
        get
        {
            lock (sync)
            {
                return epoch;
            }
        }
    }

    public long? JoinedTimeline
    {
        get
        {
            lock (sync)
            {
                return joinedTimeline;
            }
        }
    }

    public TimeSpan? Deadline
    {
        get
        {
            lock (sync)
            {
                return deadline;
            }
        }
    }

    public Task NextPhaseChange => Volatile.Read(ref phaseChanged).Task;

    public bool WouldServe(ReplicaPhase seen) => ServingCheck.Evaluate(seen, Now, FenceStart()) == ServingCheckOutcome.Serving;

    public ServingCheckResult CheckServing()
    {
        var seen = Phase;
        if (ServingCheck.Evaluate(seen, Now, FenceStart()) == ServingCheckOutcome.Serving)
        {
            return Passed(seen);
        }

        if (seen != ReplicaPhase.Serving)
        {
            return new ServingCheckResult(ServingCheckOutcome.NotServing, seen);
        }

        lock (sync)
        {
            var current = Phase;
            switch (ServingCheck.Evaluate(current, Now, FenceStart()))
            {
                case ServingCheckOutcome.Serving:
                    return Passed(current);
                case ServingCheckOutcome.NotServing:
                    return new ServingCheckResult(ServingCheckOutcome.NotServing, current);
                default:
                    ReachFenceStartLocked();
                    RequestFenceLocked(new FenceRequest(FenceTrigger.FenceStart, "a serving check found fence start passed"));
                    return new ServingCheckResult(ServingCheckOutcome.FenceStartPassed, current);
            }
        }
    }

    internal SlotHolderOutcome CheckSlotHolder(long grantedEpoch)
    {
        lock (sync)
        {
            var outcome = SlotDecisions.HolderCheck(new SlotHolderState(
                Phase, Now, FenceStart(), drainStep4Passed, epoch, grantedEpoch, currentFence is { } fence && fence.Epoch == grantedEpoch));
            if (outcome == SlotHolderOutcome.FenceStartPassed)
            {
                ReachFenceStartLocked();
                RequestFenceLocked(new FenceRequest(FenceTrigger.FenceStart, "a slot holder check found fence start passed"));
            }

            return outcome;
        }
    }

    private ServingCheckResult Passed(ReplicaPhase seen) => new(ServingCheckOutcome.Serving, seen) { Epoch = ServingEpoch };

    public FenceDecision RequestFence(FenceRequest request)
    {
        lock (sync)
        {
            return RequestFenceLocked(request);
        }
    }

    public FenceDecision RequestFence(FenceRequest request, long forEpoch)
    {
        lock (sync)
        {
            return epoch == forEpoch ? RequestFenceLocked(request) : FenceDecision.Ignored;
        }
    }

    public FenceDecision ReportFatalFault(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        logger.LogCritical("A module reported a fatal local fault in phase {Phase}: {Reason}", Phase, reason);
        return RequestFence(new FenceRequest(FenceTrigger.FatalFault, reason));
    }

    public IDisposable OnStepZero(Action<FenceRequest> hook) => Add(ref stepZeroHooks, hook);

    public IDisposable OnFenceRequested(Action<FenceRequest> handler) => Add(ref fenceHandlers, handler);

    public IDisposable OnExitRequested(Action<FenceRequest> handler) => Add(ref exitHandlers, handler);

    public IDisposable Subscribe(Action<PhaseEvent> handler) => Add(ref phaseHandlers, handler);

    public IDisposable OnRenewed(Action<long> handler) => Add(ref renewedHandlers, handler);

    public bool Joined(long joinedEpoch, long timeline, TimeSpan t0)
    {
        lock (sync)
        {
            var cell = MemberStateMachine.Decide(Phase, MemberTrigger.JoinSucceeded);
            if (cell.IsIgnored)
            {
                return false;
            }

            epoch = joinedEpoch;
            joinedTimeline = timeline;
            drainRowWriteStarted = false;
            drainStep4Passed = false;
            exitAfterFence = false;
            exitCauses = ExitCauses.None;
            joinedAt = t0;
            currentFence = null;
            fenceStartReached = false;
            SetDeadlineLocked(LeaseClock.AdvanceDeadline(null, t0, timing));
            events.Emit(new ModelEvent(MembershipActions.Join, new Dictionary<string, object?> { ["epoch"] = joinedEpoch, ["timeline"] = timeline }));
            SetPhaseLocked(cell.To);
            ArmFenceTimerLocked();
            return true;
        }
    }

    public MemberTransition Apply(MemberTrigger trigger) => Apply(trigger, forEpoch: null, payload: null);

    public MemberTransition Apply(MemberTrigger trigger, long forEpoch, IReadOnlyDictionary<string, object?>? payload = null) =>
        Apply(trigger, (long?)forEpoch, payload);

    private MemberTransition Apply(MemberTrigger trigger, long? forEpoch, IReadOnlyDictionary<string, object?>? payload)
    {
        if (trigger is MemberTrigger.FenceStart or MemberTrigger.ApplierStall or MemberTrigger.StuckAttempt or MemberTrigger.FatalFault
            or MemberTrigger.ZeroRowRenewal or MemberTrigger.TimelineChanged or MemberTrigger.ActivationUnanswered or MemberTrigger.JoinSucceeded
            or MemberTrigger.FenceCompleted or MemberTrigger.FenceCompletedToExit or MemberTrigger.DrainCompleted)
        {
            throw new ArgumentException($"{trigger} has its own entry point on the gate.", nameof(trigger));
        }

        lock (sync)
        {
            var current = Phase;
            var cell = MemberStateMachine.Decide(current, trigger, drainRowWriteStarted);
            if (cell.IsIgnored)
            {
                return cell;
            }

            if (forEpoch is { } wanted && epoch != wanted)
            {
                return new MemberTransition(null, null, current, trigger, RowKind.Any, current, RowKind.Unchanged, MemberEffects.None);
            }

            if (trigger == MemberTrigger.AppliedToActivationHead && !ServingCheck.MayEnterServing(Now, FenceStart()))
            {
                ReachFenceStartLocked();
                RequestFenceLocked(new FenceRequest(FenceTrigger.FenceStart, "fence start passed before Serving"));
                return MemberStateMachine.Decide(ReplicaPhase.Fencing, trigger);
            }

            if (cell.ModelAction == MembershipActions.Drain1 && !ServingCheck.MayEnterServing(Now, FenceStart()))
            {
                ReachFenceStartLocked();
                RequestFenceLocked(new FenceRequest(FenceTrigger.FenceStart, "fence start passed before the drain"));
                cell = MemberStateMachine.Decide(ReplicaPhase.Fencing, trigger);
            }

            if (cell.Effects.HasFlag(MemberEffects.CompleteFenceFirst))
            {
                exitAfterFence = true;
                exitCauses |= ExitCauses.Stop;
                return cell;
            }

            if (cell.ModelAction == MembershipActions.Rejoin)
            {
                events.Emit(new ModelEvent(MembershipActions.Rejoin, new Dictionary<string, object?> { ["epoch"] = epoch, ["exit"] = cell.To == ReplicaPhase.Exited }));
                var exits = cell.To == ReplicaPhase.Exited;
                ClusterDiagnostics.Rejoins.Add(1, new KeyValuePair<string, object?>("decision", exits ? ClusterDiagnostics.DecisionHigherEpoch : ClusterDiagnostics.DecisionRejoin));
                if (!exits)
                {
                    outOfServiceSince ??= currentFence?.Step0At;
                }
            }

            if (cell.ModelAction is MembershipActions.Activate or MembershipActions.EnterServing)
            {
                var data = new Dictionary<string, object?>(payload ?? new Dictionary<string, object?>()) { ["epoch"] = epoch };
                events.Emit(new ModelEvent(cell.ModelAction, data));
            }

            if (cell.ModelAction == MembershipActions.Drain1)
            {
                events.Emit(new ModelEvent(MembershipActions.Drain1, new Dictionary<string, object?> { ["epoch"] = epoch }));
            }

            if (cell.ModelAction == LogActions.LoadDone)
            {
                events.Emit(new ModelEvent(LogActions.LoadDone, new Dictionary<string, object?> { ["epoch"] = epoch }));
            }

            if (cell.To == ReplicaPhase.Starting)
            {
                epoch = null;
                joinedTimeline = null;
                SetDeadlineLocked(null);
            }

            SetPhaseLocked(cell.To);
            return cell;
        }
    }

    public ReplicaPhase CompleteFence()
    {
        lock (sync)
        {
            var current = Phase;
            if (current != ReplicaPhase.Fencing)
            {
                return current;
            }

            var cell = MemberStateMachine.Decide(current, exitAfterFence ? MemberTrigger.FenceCompletedToExit : MemberTrigger.FenceCompleted);

            events.Emit(new ModelEvent(MembershipActions.FStep, new Dictionary<string, object?> { ["k"] = 6, ["epoch"] = epoch }));
            if (cell.To == ReplicaPhase.Exited)
            {
                if (FenceTriggerPolicy.ExitAction(exitCauses) is { } exit)
                {
                    events.Emit(new ModelEvent(exit, new Dictionary<string, object?> { ["epoch"] = epoch }));
                }
            }

            SetPhaseLocked(cell.To);
            return cell.To;
        }
    }

    public bool BeginDrainStep4()
    {
        lock (sync)
        {
            var cell = MemberStateMachine.Decide(Phase, MemberTrigger.DrainCompleted, drainRowWriteStarted);
            if (cell.IsIgnored)
            {
                return false;
            }

            drainStep4Passed = true;
            drainRowWriteStarted = true;
            events.Emit(new ModelEvent(cell.ModelAction!, new Dictionary<string, object?> { ["epoch"] = epoch }));

            Interlocked.Exchange(ref fenceStartTicks, NoFenceStart);
            DisarmFenceTimerLocked();
            var step4 = new PhaseEvent(PhaseEventKind.DrainStep4, ReplicaPhase.Draining, ReplicaPhase.Draining);
            foreach (var handler in Volatile.Read(ref phaseHandlers))
            {
                try
                {
                    handler(step4);
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "A phase-event subscriber threw on drain step 4.");
                }
            }

            return true;
        }
    }

    public void CompleteDrain()
    {
        lock (sync)
        {
            if (Phase == ReplicaPhase.Draining && drainRowWriteStarted)
            {
                SetPhaseLocked(ReplicaPhase.Exited);
            }
        }
    }

    public bool TryGetRenewal(out RenewalTarget target)
    {
        lock (sync)
        {
            if (epoch is { } e && joinedTimeline is { } t && RenewalRules.IsActive(Phase, drainRowWriteStarted))
            {
                target = new RenewalTarget(e, t);
                return true;
            }

            target = default;
            return false;
        }
    }

    public RenewalResolution ApplyRenewal(RenewalAttempt attempt, RenewalOutcome outcome)
    {
        var resolution = ApplyRenewalLocked(attempt, outcome);
        if (resolution.Verdict == RenewalVerdict.DeadlineAdvanced)
        {
            foreach (var handler in Volatile.Read(ref renewedHandlers))
            {
                try
                {
                    handler(attempt.Epoch);
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "A renewal subscriber threw on epoch {Epoch}.", attempt.Epoch);
                }
            }
        }

        return resolution;
    }

    private RenewalResolution ApplyRenewalLocked(RenewalAttempt attempt, RenewalOutcome outcome)
    {
        lock (sync)
        {
            var context = new RenewalContext(Phase, drainRowWriteStarted, epoch ?? long.MinValue, deadline);
            var resolution = RenewalRules.Resolve(context, attempt, outcome, timing);
            switch (resolution.Verdict)
            {
                case RenewalVerdict.DeadlineAdvanced:
                    SetDeadlineLocked(resolution.Deadline);
                    events.Emit(new ModelEvent(MembershipActions.RenewAnswer, new Dictionary<string, object?> { ["epoch"] = attempt.Epoch, ["res"] = "one" }));
                    ArmFenceTimerLocked();
                    break;
                case RenewalVerdict.FenceZeroRow:
                    events.Emit(new ModelEvent(MembershipActions.RenewZeroL, new Dictionary<string, object?> { ["epoch"] = attempt.Epoch, ["res"] = "zero" }));
                    RequestFenceLocked(new FenceRequest(FenceTrigger.ZeroRowRenewal, "a renewal answered with zero rows"), triggerEventEmitted: true);
                    break;
                case RenewalVerdict.FenceOtherTimeline:
                    events.Emit(new ModelEvent(MembershipActions.RenewAnswer, new Dictionary<string, object?> { ["epoch"] = attempt.Epoch, ["res"] = "other" }));
                    RequestFenceLocked(new FenceRequest(FenceTrigger.Timeline, "a renewal answered on another timeline than the join's"), triggerEventEmitted: true);
                    break;
            }

            return resolution;
        }
    }

    public void Dispose()
    {
        lock (sync)
        {
            disposed = true;
        }

        fenceTimer.Dispose();
        ClusterDiagnostics.UntrackPhase(this);
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private TimeSpan? FenceStart()
    {
        var ticks = Interlocked.Read(ref fenceStartTicks);
        return ticks == NoFenceStart ? null : TimeSpan.FromTicks(ticks);
    }

    private static MemberTrigger ToMemberTrigger(FenceTrigger trigger) => trigger switch
    {
        FenceTrigger.FenceStart => MemberTrigger.FenceStart,
        FenceTrigger.ApplierStall => MemberTrigger.ApplierStall,
        FenceTrigger.StuckAttempt => MemberTrigger.StuckAttempt,
        FenceTrigger.FatalFault => MemberTrigger.FatalFault,
        FenceTrigger.ZeroRowRenewal => MemberTrigger.ZeroRowRenewal,
        FenceTrigger.Timeline => MemberTrigger.TimelineChanged,
        FenceTrigger.UnansweredActivation => MemberTrigger.ActivationUnanswered,
        _ => throw new ArgumentOutOfRangeException(nameof(trigger), trigger, null),
    };

    private FenceDecision RequestFenceLocked(FenceRequest request) => RequestFenceLocked(request, triggerEventEmitted: false);

    private FenceDecision RequestFenceLocked(FenceRequest request, bool triggerEventEmitted)
    {
        var current = Phase;
        var (decision, cause) = FenceTriggerPolicy.Judge(current, request.Trigger, drainRowWriteStarted);
        var cell = MemberStateMachine.Decide(current, ToMemberTrigger(request.Trigger), drainRowWriteStarted);
        switch (decision)
        {
            case FenceDecision.Fence:
            case FenceDecision.FenceThenExit:
                exitAfterFence |= decision == FenceDecision.FenceThenExit;
                exitCauses |= cause;
                logger.LogWarning("Fence from {Phase}: {Trigger} ({Detail}).", current, request.Trigger, request.Detail);

                var now = Now;
                currentFence = new FenceRun(epoch ?? 0, request, current, cell.Effects, now, joinedAt is { } joined ? now - joined : TimeSpan.Zero);
                if (!triggerEventEmitted && TriggerAction(request.Trigger, cell) is { } action)
                {
                    events.Emit(new ModelEvent(action, new Dictionary<string, object?> { ["epoch"] = epoch, ["from"] = current.ToString() }));
                }

                Interlocked.Exchange(ref fenceStartTicks, NoFenceStart);
                DisarmFenceTimerLocked();
                SetPhaseLocked(ReplicaPhase.Fencing);
                ClusterDiagnostics.Fences.Add(1, new KeyValuePair<string, object?>("trigger", FenceCode(request.Trigger)));
                Invoke(stepZeroHooks, request);
                Invoke(fenceHandlers, request);
                break;

            case FenceDecision.ExitAfterFence:
                exitAfterFence = true;
                exitCauses |= cause;
                EmitFaultFenced(request, current);
                logger.LogWarning("{Trigger} during the fence ({Detail}): the process exits at step 6.", request.Trigger, request.Detail);
                break;

            case FenceDecision.ExitNow:
                EmitFaultFenced(request, current);

                if (cell.ModelAction is { } exitAction)
                {
                    events.Emit(new ModelEvent(exitAction, new Dictionary<string, object?> { ["epoch"] = epoch, ["from"] = current.ToString() }));
                }

                logger.LogCritical("{Trigger} in {Phase} ({Detail}): the process exits.", request.Trigger, current, request.Detail);
                Interlocked.Exchange(ref fenceStartTicks, NoFenceStart);
                DisarmFenceTimerLocked();
                SetPhaseLocked(ReplicaPhase.Exited);
                Invoke(exitHandlers, request);
                break;
        }

        return decision;
    }

    private static string FenceCode(FenceTrigger trigger) => trigger switch
    {
        FenceTrigger.FenceStart => "fence_start",
        FenceTrigger.ApplierStall => "applier_stall",
        FenceTrigger.StuckAttempt => "stuck_attempt",
        FenceTrigger.FatalFault => "fatal_fault",
        FenceTrigger.ZeroRowRenewal => "zero_row_renewal",
        FenceTrigger.Timeline => "timeline",
        FenceTrigger.UnansweredActivation => "unanswered_activation",
        _ => throw new ArgumentOutOfRangeException(nameof(trigger), trigger, null),
    };

    private void SetDeadlineLocked(TimeSpan? value)
    {
        deadline = value;
        if (value is { } d)
        {
            Interlocked.Exchange(ref fenceStartTicks, LeaseClock.FenceStart(d, timing).Ticks);
        }
        else
        {
            Interlocked.Exchange(ref fenceStartTicks, NoFenceStart);
            DisarmFenceTimerLocked();
        }
    }

    private void DisarmFenceTimerLocked()
    {
        if (!disposed)
        {
            fenceTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }
    }

    private void ArmFenceTimerLocked()
    {
        if (disposed || FenceStart() is not { } start)
        {
            return;
        }

        var due = start - Now;
        fenceTimer.Change(due > TimeSpan.Zero ? due : TimeSpan.Zero, Timeout.InfiniteTimeSpan);
    }

    private void OnFenceTimer()
    {
        if (!FenceStartDue())
        {
            return;
        }

        var reached = points.ReachAsync(MembershipFaultPoints.FenceBeforeStep0);
        if (reached.IsCompletedSuccessfully)
        {
            reached.GetAwaiter().GetResult();
            FenceAtTimer();
        }
        else
        {
            _ = FenceAfterPointAsync(reached);
        }
    }

    private async Task FenceAfterPointAsync(ValueTask reached)
    {
        try
        {
            await reached.ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "The fence timer's fault point threw; the timer fences anyway.");
        }

        FenceAtTimer();
    }

    private bool FenceStartDue()
    {
        lock (sync)
        {
            if (disposed || FenceStart() is not { } start)
            {
                return false;
            }

            if (Now < start)
            {
                ArmFenceTimerLocked();
                return false;
            }

            return true;
        }
    }

    private void FenceAtTimer()
    {
        lock (sync)
        {
            if (disposed || FenceStart() is not { } start)
            {
                return;
            }

            if (Now < start)
            {
                ArmFenceTimerLocked();
                return;
            }

            ReachFenceStartLocked();
            RequestFenceLocked(new FenceRequest(FenceTrigger.FenceStart, "the fence timer reached fence start"));
        }
    }

    private static string? TriggerAction(FenceTrigger trigger, MemberTransition cell) => cell.ModelAction switch
    {
        MembershipActions.RenewAnswer or MembershipActions.RenewZeroL => trigger == FenceTrigger.Timeline ? MembershipActions.TimelineFence : null,
        var action => action,
    };

    private void EmitFaultFenced(FenceRequest request, ReplicaPhase current)
    {
        if (request.Trigger == FenceTrigger.FatalFault && current is ReplicaPhase.Fencing or ReplicaPhase.Fenced)
        {
            events.Emit(new ModelEvent(MembershipActions.FaultFenced, new Dictionary<string, object?> { ["phase"] = current.ToString() }));
        }
    }

    private void ReachFenceStartLocked()
    {
        if (!fenceStartReached && epoch is { } e)
        {
            fenceStartReached = true;
            events.Emit(new ModelEvent(MembershipActions.ReachFS, new Dictionary<string, object?> { ["epoch"] = e }));
        }
    }

    private void SetPhaseLocked(ReplicaPhase to)
    {
        var from = Phase;
        if (from == to)
        {
            return;
        }

        if (to == ReplicaPhase.Exited)
        {
            Interlocked.Exchange(ref fenceStartTicks, NoFenceStart);
            DisarmFenceTimerLocked();
        }

        if (to == ReplicaPhase.Serving)
        {
            Volatile.Write(ref servingEpoch, epoch ?? NoServingEpoch);
            if (outOfServiceSince is { } since)
            {
                outOfServiceSince = null;
                ClusterDiagnostics.RejoinDuration.Record((Now - since).TotalSeconds);
            }
        }

        Volatile.Write(ref phase, (int)to);
        if (from == ReplicaPhase.Serving)
        {
            Volatile.Write(ref servingEpoch, NoServingEpoch);
        }

        Interlocked.Exchange(ref phaseChanged, NewSignal()).TrySetResult();
        var change = new PhaseEvent(PhaseEventKind.Transition, from, to);
        foreach (var handler in Volatile.Read(ref phaseHandlers))
        {
            try
            {
                handler(change);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "A phase-event subscriber threw on {From} → {To}.", from, to);
            }
        }
    }

    private void Invoke(Action<FenceRequest>[] handlers, FenceRequest request)
    {
        foreach (var handler in handlers)
        {
            try
            {
                handler(request);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "A fence or exit subscriber threw on {Trigger}.", request.Trigger);
            }
        }
    }

    private Subscription Add<T>(ref T[] handlers, T handler)
        where T : Delegate
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (sync)
        {
            handlers = [.. handlers, handler];
        }

        return new Subscription(this, handler);
    }

    private void Remove(Delegate handler)
    {
        lock (sync)
        {
            stepZeroHooks = [.. stepZeroHooks.Where(h => !ReferenceEquals(h, handler))];
            fenceHandlers = [.. fenceHandlers.Where(h => !ReferenceEquals(h, handler))];
            exitHandlers = [.. exitHandlers.Where(h => !ReferenceEquals(h, handler))];
            phaseHandlers = [.. phaseHandlers.Where(h => !ReferenceEquals(h, handler))];
            renewedHandlers = [.. renewedHandlers.Where(h => !ReferenceEquals(h, handler))];
        }
    }

    private sealed class Subscription(LocalPhaseGate gate, Delegate handler) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                gate.Remove(handler);
            }
        }
    }
}

internal sealed record FenceRun(long Epoch, FenceRequest Request, ReplicaPhase From, MemberEffects Effects, TimeSpan Step0At, TimeSpan Span)
{
    public bool SkipLeaseRelease => Effects.HasFlag(MemberEffects.SkipLeaseRelease);
}

internal sealed class ReadinessReasons : IReadinessReasons
{
    private readonly object sync = new();
    private ReadinessReason[] current = [];

    public IReadOnlyList<ReadinessReason> Current => Volatile.Read(ref current);

    public void Raise(ReadinessReason reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        lock (sync)
        {
            var index = Array.FindIndex(current, r => r.Code == reason.Code);
            current = index < 0 ? [.. current, reason] : [.. current[..index], reason, .. current[(index + 1)..]];
        }
    }

    public void Clear(string code)
    {
        lock (sync)
        {
            current = [.. current.Where(r => r.Code != code)];
        }
    }
}
