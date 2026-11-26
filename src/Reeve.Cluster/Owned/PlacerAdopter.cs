using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Owned;

internal sealed class PlacerAdopter : IHostedService, IDisposable
{
    private static readonly string Key = Placement.SingletonKey(Placement.PlacerKind);

    private readonly bool enabled;
    private readonly OwnedStore store;
    private readonly UnitRuntime runtime;
    private readonly LocalPhaseGate gate;
    private readonly IFaultPoints points;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly IDisposable phases;
    private readonly CancellationTokenSource stopping = new();
    private TaskCompletionSource wake = NewWake();
    private Task? running;
    private int disposed;

    public PlacerAdopter(
        IEnumerable<UnitKind> registered,
        OwnedStore store,
        UnitRuntime runtime,
        LocalPhaseGate gate,
        IFaultPoints points,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<PlacerAdopter> logger)
    {
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(options);
        enabled = registered.Any(k => k.Kind == Placement.PlacerKind);
        this.store = store;
        this.runtime = runtime;
        this.gate = gate;
        this.points = points;
        this.options = options.Value;
        this.time = time;
        this.logger = logger;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
        phases = gate.Subscribe(change =>
        {
            if (change.Kind == PhaseEventKind.Transition && change.To == ReplicaPhase.Serving)
            {
                Volatile.Read(ref wake).TrySetResult();
            }
        });
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (enabled)
        {
            Volatile.Write(ref running, Task.Run(() => LoopAsync(stopping.Token), CancellationToken.None));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);
        if (Volatile.Read(ref running) is { } loop)
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        phases.Dispose();
        stopping.Cancel();
    }

    internal async Task<long?> AdoptAsync(CancellationToken cancellationToken)
    {
        await points.ReachAsync(OwnedFaultPoints.PlacerAdoptBeforeRead, cancellationToken).ConfigureAwait(false);
        if (gate.Phase != ReplicaPhase.Serving || runtime.Holds(Placement.PlacerKind, Key))
        {
            return null;
        }

        using var timer = new CancellationTokenSource(options.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
        var read = await store.ReadAsync(Placement.PlacerKind, Key, bound.Token).ConfigureAwait(false);

        if (read.Condition is not (OwnedCondition.Absent or OwnedCondition.Free or OwnedCondition.Orphaned)
            || gate.CheckServing() is not { IsServing: true, Epoch: { } epoch })
        {
            return null;
        }

        var token = read.Condition switch
        {
            OwnedCondition.Absent => await store.InsertAsync(Placement.PlacerKind, Key, replicaId, epoch, bound.Token).ConfigureAwait(false),
            OwnedCondition.Free or OwnedCondition.Orphaned => await store.TakeAsync(Placement.PlacerKind, Key, read.Row!.Value, replicaId, epoch, bound.Token).ConfigureAwait(false),
            _ => null,
        };
        if (token is not null)
        {
            logger.LogInformation("This replica took the placer's row, tenure {Token} of epoch {Epoch}.", token, epoch);
            runtime.Nudge();
        }

        return token;
    }

    private async Task LoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var next = NewWake();
            Interlocked.Exchange(ref wake, next);
            try
            {
                await AdoptAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogWarning(exception, "The take-over of the placer's row got no answer; the next attempt reads again.");
            }

            try
            {
                await next.Task.WaitAsync(options.PlacementInterval, time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private static TaskCompletionSource NewWake() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
