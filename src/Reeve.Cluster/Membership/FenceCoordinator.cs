using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Membership;

internal sealed class FenceCoordinator : IHostedService, IDisposable
{
    private readonly LocalPhaseGate gate;
    private readonly ILookup<FenceStep, IFenceParticipant> participants;
    private readonly IFenceStore store;
    private readonly FencePassConnection pass;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly ClusterOptions options;
    private readonly string replicaId;
    private readonly CancellationTokenSource stopping = new();
    private readonly IDisposable subscription;
    private Task? running;
    private int disposed;

    public FenceCoordinator(
        LocalPhaseGate gate,
        IEnumerable<IFenceParticipant> participants,
        IFenceStore store,
        FencePassConnection pass,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        IModelEventSink events,
        ILogger<FenceCoordinator> logger)
    {
        ArgumentNullException.ThrowIfNull(participants);
        this.gate = gate;
        this.participants = participants.ToLookup(p => p.Step);
        this.store = store;
        this.pass = pass;
        this.time = time;
        this.points = points;
        this.events = events;
        this.logger = logger;
        this.options = options.Value;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));

        subscription = gate.OnFenceRequested(OnFence);
    }

    internal Task? Running => Volatile.Read(ref running);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);
        if (Running is { } run)
        {
            await run.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        subscription.Dispose();
        stopping.Cancel();
    }

    private void OnFence(FenceRequest request)
    {
        if (gate.CurrentFence is not { } fence)
        {
            return;
        }

        if (ExecutionContext.IsFlowSuppressed())
        {
            Volatile.Write(ref running, Task.Run(() => RunAsync(fence)));
            return;
        }

        using (ExecutionContext.SuppressFlow())
        {
            Volatile.Write(ref running, Task.Run(() => RunAsync(fence)));
        }
    }

    private async Task RunAsync(FenceRun fence)
    {
        try
        {
            await RunStepsAsync(fence).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogError(exception, "Fence of epoch {Epoch} failed in its steps; it ends at step 6.", fence.Epoch);
        }

        var phase = gate.CompleteFence();
        logger.LogWarning("Fence of epoch {Epoch} ({Trigger}) ended in {Phase}.", fence.Epoch, fence.Request.Trigger, phase);
        if (phase == ReplicaPhase.Fenced)
        {
            try
            {
                await AwaitRejoinAsync(fence).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException && !stopping.IsCancellationRequested)
            {
                logger.LogError(exception, "Rejoin wait for fenced epoch {Epoch} failed; exiting.", fence.Epoch);
                gate.ReportFatalFault($"rejoin for epoch {fence.Epoch} failed: {exception.Message}");
            }
        }
    }

    private async Task RunStepsAsync(FenceRun fence)
    {
        var context = new FenceContext(fence.Request);

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep1).ConfigureAwait(false);
        await RunAwaitedAsync(FenceStep.CloseTraffic, context).ConfigureAwait(false);
        EmitStep(fence, 1);

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep2).ConfigureAwait(false);
        await RunAwaitedAsync(FenceStep.RefuseCommits, context).ConfigureAwait(false);
        EmitStep(fence, 2);

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep3).ConfigureAwait(false);
        var released = false;
        if (!fence.SkipLeaseRelease)
        {
            released = await ReleaseLeaseAsync(fence, context).ConfigureAwait(false);
        }

        EmitStep(fence, 3, ("skip3", fence.SkipLeaseRelease), ("released", released));

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep4).ConfigureAwait(false);
        await RunBoundedAsync(FenceStep.AbortConnections, context, []).ConfigureAwait(false);
        EmitStep(fence, 4);

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep5).ConfigureAwait(false);
        var answered = false;
        var inTime = await RunBoundedAsync(FenceStep.StopUnits, context, [async ct => answered = await RecordAsync(fence, ct).ConfigureAwait(false)]).ConfigureAwait(false);
        var recorded = inTime[^1] && answered;
        EmitStep(fence, 5, ("recorded", recorded));

        await ReachAsync(MembershipFaultPoints.FenceBeforeStep6).ConfigureAwait(false);
        await RunAwaitedAsync(FenceStep.StopApplier, context).ConfigureAwait(false);
        await DisposePassConnectionAsync().ConfigureAwait(false);
    }

    private async Task<bool> ReleaseLeaseAsync(FenceRun fence, FenceContext context)
    {
        var answered = false;
        Func<CancellationToken, Task>[] works =
        [
            async ct => answered = await ReleaseStatementAsync(fence, ct).ConfigureAwait(false),
            .. Participants(FenceStep.ReleaseLease, context),
        ];
        var inTime = await BoundedWork.RunAsync(time, fence.Step0At + options.RenewTimeout - gate.Now, works).ConfigureAwait(false);

        return inTime[0] && answered;
    }

    private async Task<bool> ReleaseStatementAsync(FenceRun fence, CancellationToken cancellationToken)
    {
        try
        {
            await ReachAsync(MembershipFaultPoints.FenceBeforeLeaseRelease, cancellationToken).ConfigureAwait(false);
            var released = await store.ReleaseLeaseAsync(replicaId, fence.Epoch, cancellationToken).ConfigureAwait(false);
            await ReachAsync(MembershipFaultPoints.FenceAfterLeaseRelease, cancellationToken).ConfigureAwait(false);
            return released;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "Fence step 3 of epoch {Epoch} got no answer; the lease runs out on its own.", fence.Epoch);
            return false;
        }
    }

    private async Task<bool> RecordAsync(FenceRun fence, CancellationToken cancellationToken)
    {
        try
        {
            await ReachAsync(MembershipFaultPoints.FenceBeforeHistoryRecord, cancellationToken).ConfigureAwait(false);
            await store.RecordFenceAsync(Record(fence), gate.Now - fence.Step0At, cancellationToken).ConfigureAwait(false);
            await ReachAsync(MembershipFaultPoints.FenceAfterHistoryRecord, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "The fence_history record of epoch {Epoch} got no answer in step 5; it is not made again.", fence.Epoch);
            return false;
        }
    }

    private FenceRecord Record(FenceRun fence) => new(replicaId, fence.Epoch, FenceTriggerNames.Of(fence.Request.Trigger), fence.Span);

    private async Task RunAwaitedAsync(FenceStep step, FenceContext context) =>
        await Task.WhenAll(participants[step].Select(p => RunParticipantAsync(p, context, CancellationToken.None))).ConfigureAwait(false);

    private async Task<bool[]> RunBoundedAsync(FenceStep step, FenceContext context, Func<CancellationToken, Task>[] own)
    {
        var inTime = await BoundedWork.RunAsync(time, options.StopBudget, [.. Participants(step, context), .. own]).ConfigureAwait(false);
        if (inTime.Contains(false))
        {
            CountOverrun((int)step);
            logger.LogWarning("Fence step {Step} did not finish within StopBudget; the fence goes on without it.", (int)step);
        }

        return inTime;
    }

    private IEnumerable<Func<CancellationToken, Task>> Participants(FenceStep step, FenceContext context) =>
        participants[step].Select(p => (Func<CancellationToken, Task>)(ct => RunParticipantAsync(p, context, ct)));

    private async Task RunParticipantAsync(IFenceParticipant participant, FenceContext context, CancellationToken cancellationToken)
    {
        try
        {
            await participant.RunAsync(context, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                logger.LogError(exception, "Fence participant {Participant} of step {Step} failed.", participant.GetType().Name, (int)participant.Step);
            }
        }
    }

    private async Task DisposePassConnectionAsync()
    {
        if (pass.Take() is not { } connection)
        {
            return;
        }

        var inTime = await BoundedWork.RunAsync(time, options.StopBudget, [_ => connection.DisposeAsync().AsTask()]).ConfigureAwait(false);
        if (!inTime[0])
        {
            CountOverrun(6);
            logger.LogWarning("The pass connection's dispose did not return within StopBudget; the fence goes on without it.");
        }
    }

    private async Task AwaitRejoinAsync(FenceRun fence)
    {
        while (!stopping.IsCancellationRequested)
        {
            var changed = gate.NextPhaseChange;
            if (gate.Phase != ReplicaPhase.Fenced)
            {
                return;
            }

            var row = await ReadRejoinStateAsync().ConfigureAwait(false);
            var decision = RejoinDecision.Decide(
                row, fence.Epoch, fence.Span, gate.Now - fence.Step0At, options.LeaseTtl, options.RejoinBackoff);
            if (stopping.IsCancellationRequested)
            {
                return;
            }

            if (decision.Trigger is { } trigger)
            {
                gate.Apply(trigger);
                return;
            }

            var wait = decision.Action == RejoinAction.AwaitBackoff ? decision.Wait : options.RenewInterval;
            if (decision.Action == RejoinAction.AwaitBackoff)
            {
                logger.LogInformation("Epoch {Epoch} lasted {Span}; the rejoin waits {Wait}.", fence.Epoch, fence.Span, wait);
            }

            await Task.WhenAny(Task.Delay(wait, time, stopping.Token), changed).ConfigureAwait(false);
        }
    }

    private async Task<OwnRowObservation?> ReadRejoinStateAsync()
    {
        try
        {
            using var bound = new CancellationTokenSource(options.RenewTimeout, time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(stopping.Token, bound.Token);
            return await store.ReadRejoinStateAsync(replicaId, linked.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!stopping.IsCancellationRequested)
            {
                logger.LogDebug(exception, "The rejoin read of {ReplicaId} got no answer; retrying.", replicaId);
            }

            return null;
        }
    }

    private async ValueTask ReachAsync(FaultPoint point, CancellationToken cancellationToken = default)
    {
        try
        {
            await points.ReachAsync(point, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(exception, "Fault point {Point} threw; the fence goes on.", point.Name);
        }
    }

    private void EmitStep(FenceRun fence, int k, params (string Key, object? Value)[] state)
    {
        var payload = new Dictionary<string, object?> { ["k"] = k, ["epoch"] = fence.Epoch };
        foreach (var (key, value) in state)
        {
            payload[key] = value;
        }

        events.Emit(new ModelEvent(MembershipActions.FStep, payload));
    }

    private static void CountOverrun(int step) =>
        ClusterDiagnostics.FenceStepOverruns.Add(1, new TagList { { "step", step } });
}

internal sealed class FencePassConnection
{
    private IAsyncDisposable? current;

    public IDisposable Use(IAsyncDisposable connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        Volatile.Write(ref current, connection);
        return new Handle(this, connection);
    }

    public IAsyncDisposable? Take() => Interlocked.Exchange(ref current, null);

    private sealed class Handle(FencePassConnection owner, IAsyncDisposable connection) : IDisposable
    {
        public void Dispose() => Interlocked.CompareExchange(ref owner.current, null, connection);
    }
}

internal sealed class FenceSinkParticipant(IFenceSink sink) : IFenceParticipant
{
    public FenceStep Step => FenceStep.AbortConnections;

    public ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken) => sink.AbortConnectionsAsync(cancellationToken);
}
