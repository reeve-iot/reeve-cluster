using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Membership;

internal sealed class DrainCoordinator
{
    private readonly LocalPhaseGate gate;
    private readonly ILookup<DrainStep, IDrainParticipant> participants;
    private readonly IMemberStore store;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly ClusterOptions options;
    private readonly string replicaId;

    public DrainCoordinator(
        LocalPhaseGate gate,
        IEnumerable<IDrainParticipant> participants,
        IMemberStore store,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        IModelEventSink events,
        ILogger<DrainCoordinator> logger)
    {
        ArgumentNullException.ThrowIfNull(participants);
        this.gate = gate;
        this.participants = participants.ToLookup(p => p.Step);
        this.store = store;
        this.time = time;
        this.points = points;
        this.events = events;
        this.logger = logger;
        this.options = options.Value;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    }

    public async Task StopAsync()
    {
        var cell = gate.Apply(MemberTrigger.GracefulStop);
        if (cell.IsIgnored || gate.Epoch is not { } epoch)
        {
            return;
        }

        var started = time.GetTimestamp();
        var outcome = await StopCoreAsync(cell, epoch).ConfigureAwait(false);
        if (outcome is not null)
        {
            ClusterDiagnostics.DrainDuration.Record(time.GetElapsedTime(started).TotalSeconds, new KeyValuePair<string, object?>("outcome", outcome));
        }
    }

    private async Task<string?> StopCoreAsync(MemberTransition cell, long epoch)
    {
        var context = new DrainContext();
        var draining = cell.To == ReplicaPhase.Draining;
        if (cell.Effects.HasFlag(MemberEffects.RunDrainSteps1To4))
        {
            await RunAwaitedAsync(DrainStep.CloseTraffic, context).ConfigureAwait(false);
            await WriteRowAsync("serving_since", epoch, store.ClearServingSinceAsync).ConfigureAwait(false);
            await ReachAsync(MembershipFaultPoints.DrainAfterStep1).ConfigureAwait(false);
        }

        if (cell.Effects.HasFlag(MemberEffects.RunDrainSteps1To4) || cell.Effects.HasFlag(MemberEffects.RunDrainSteps2To4))
        {
            if (draining && TakenOver())
            {
                return ClusterDiagnostics.DrainTakenOver;
            }

            await ReleaseUnitsAsync(context).ConfigureAwait(false);
            if (draining && TakenOver())
            {
                return ClusterDiagnostics.DrainTakenOver;
            }

            EmitStep(draining, 2);

            EmitStep(draining, 3);
            await ReachAsync(MembershipFaultPoints.DrainBeforeStep4).ConfigureAwait(false);
            if (draining && TakenOver())
            {
                return ClusterDiagnostics.DrainTakenOver;
            }

            await RunAwaitedAsync(DrainStep.AwaitRequests, context).ConfigureAwait(false);
            await ReachAsync(MembershipFaultPoints.DrainBeforeDrainingWrite).ConfigureAwait(false);
            if (draining && !gate.BeginDrainStep4())
            {
                logger.LogWarning("A fence took over the drain of epoch {Epoch}; the draining write is skipped.", epoch);
                return ClusterDiagnostics.DrainTakenOver;
            }
        }

        string? outcome = null;
        if (draining || cell.Effects.HasFlag(MemberEffects.WriteDrainingRow))
        {
            var answered = await WriteRowAsync("draining", epoch, store.WriteDrainingAsync).ConfigureAwait(false);
            outcome = answered ? ClusterDiagnostics.DrainDrained : ClusterDiagnostics.DrainUnanswered;
        }

        if (draining)
        {
            gate.CompleteDrain();
        }

        return outcome;
    }

    private bool TakenOver()
    {
        if (gate.Phase == ReplicaPhase.Draining)
        {
            return false;
        }

        logger.LogWarning("A fence took over the drain; its remaining steps and its row write are skipped.");
        return true;
    }

    private async Task ReleaseUnitsAsync(DrainContext context)
    {
        var works = participants[DrainStep.ReleaseUnits].Select(p => (Func<CancellationToken, Task>)(ct => RunParticipantAsync(p, context, ct))).ToList();
        if (works.Count == 0)
        {
            return;
        }

        var inTime = await BoundedWork.RunAsync(time, options.StopBudget, works).ConfigureAwait(false);
        if (inTime.Contains(false))
        {
            logger.LogWarning("Drain step 2 did not finish within StopBudget; the drain goes on without it.");
        }
    }

    private async Task RunAwaitedAsync(DrainStep step, DrainContext context) =>
        await Task.WhenAll(participants[step].Select(p => RunParticipantAsync(p, context, CancellationToken.None))).ConfigureAwait(false);

    private async Task RunParticipantAsync(IDrainParticipant participant, DrainContext context, CancellationToken cancellationToken)
    {
        try
        {
            await participant.RunAsync(context, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                logger.LogError(exception, "Drain participant {Participant} of step {Step} failed.", participant.GetType().Name, (int)participant.Step);
            }
        }
    }

    private async Task<bool> WriteRowAsync(string what, long epoch, Func<string, long, CancellationToken, Task<bool>> write)
    {
        using var bound = new CancellationTokenSource(options.RenewTimeout, time);
        try
        {
            if (await write(replicaId, epoch, bound.Token).ConfigureAwait(false))
            {
                return true;
            }

            logger.LogWarning("The {What} write of epoch {Epoch} found the row no longer this process's.", what, epoch);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "The {What} write of epoch {Epoch} got no answer.", what, epoch);
        }

        return false;
    }

    private async ValueTask ReachAsync(FaultPoint point)
    {
        try
        {
            await points.ReachAsync(point).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Fault point {Point} threw; the drain goes on.", point.Name);
        }
    }

    private void EmitStep(bool draining, int step)
    {
        if (draining)
        {
            events.Emit(new ModelEvent(MembershipActions.Drain23, new Dictionary<string, object?> { ["step"] = step, ["epoch"] = gate.Epoch }));
        }
    }
}
