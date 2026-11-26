using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Owned;

internal sealed class PlacerUnit : IOwnedUnit
{
    private readonly OwnedStore store;
    private readonly IWakeupChannel wakeups;
    private readonly IFaultPoints points;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ReleaseOverdueReport report;
    private readonly ILogger logger;
    private readonly Dictionary<string, PlacedKind> kinds;
    private readonly string[] kindNames;
    private Tenure? current;

    public PlacerUnit(
        IEnumerable<UnitKind> registered,
        OwnedStore store,
        IWakeupChannel wakeups,
        IFaultPoints points,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ReleaseOverdueReport report,
        ILogger<PlacerUnit> logger)
    {
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(options);
        this.store = store;
        this.wakeups = wakeups;
        this.points = points;
        this.options = options.Value;
        this.time = time;
        this.report = report;
        this.logger = logger;
        kinds = Placement.PlacedKinds(registered).ToDictionary(k => k.Kind, StringComparer.Ordinal);
        kindNames = [.. kinds.Keys];
    }

    public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        var tenure = new Tenure(FencedTransactions.Owner(context), new PlacerMemory(options.StabilisationWindow, time));
        Volatile.Write(ref current, tenure);
        logger.LogInformation("This replica is the placer, tenure {Token}.", context.Token);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await PassAsync(tenure, stop).ConfigureAwait(false);
                }
                catch (Exception) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (OwnershipLostException exception)
                {
                    logger.LogInformation(exception, "The placer's pass was refused its tenure {Token}; the pass ends.", context.Token);
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    logger.LogWarning(exception, "The placer's pass failed; the next pass reads again.");
                }

                try
                {
                    await Task.Delay(options.PlacementInterval, time, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
            }
        }
        finally
        {
            report.Clear();
        }
    }

    public Task OnLostAsync(OwnedContext context)
    {
        if (Volatile.Read(ref current) is { } tenure && ReferenceEquals(tenure.Placer, context))
        {
            Interlocked.CompareExchange(ref current, null, tenure);
        }

        return Task.CompletedTask;
    }

    internal async Task<bool> RequestHandoverAsync(string kind, string key, CancellationToken cancellationToken)
    {
        var tenure = Volatile.Read(ref current) ?? throw new InvalidOperationException("This replica does not run the placer.");
        OwnedRead read;
        using (var bound = Bound(cancellationToken))
        {
            read = await store.ReadAsync(kind, key, bound.Token).ConfigureAwait(false);
        }

        if (read.Condition != OwnedCondition.Owned || read.Row is not { OwnerReplica: { } owner } row)
        {
            return false;
        }

        return await RequestAsync(tenure, new Handover(kind, key, row.Token, owner), cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> RequestAsync(Tenure tenure, Handover handover, CancellationToken cancellationToken)
    {
        tenure.Memory.Requested(handover);
        bool stored;
        try
        {
            using var bound = Bound(cancellationToken);
            stored = await store.RequestReleaseAsync(tenure.Placer, handover.Kind, handover.Key, handover.Token, bound.Token).ConfigureAwait(false);
        }
        catch
        {
            tenure.Memory.Forget(handover);
            throw;
        }

        if (stored)
        {
            logger.LogInformation(
                "The release of {Kind}/{Key} tenure {Token} is requested from {Owner}.", handover.Kind, handover.Key, handover.Token, handover.From);
            await wakeups.PublishAsync(Wakeup.Wakeup.Nudge(WakeupKind.OwnerNudge, handover.From), cancellationToken).ConfigureAwait(false);
        }

        return stored;
    }

    private async Task PassAsync(Tenure tenure, CancellationToken stop)
    {
        await points.ReachAsync(OwnedFaultPoints.PlacerPassBeforeRead, stop).ConfigureAwait(false);
        if (kinds.Count == 0)
        {
            return;
        }

        PlacementRead read;
        using (var bound = Bound(stop))
        {
            read = await store.ReadPlacementAsync(kindNames, bound.Token).ConfigureAwait(false);
        }

        var memory = tenure.Memory;
        memory.Observe(read);
        report.Apply(read.Overdue, time.GetUtcNow());

        var barred = memory.Barred;
        var plan = Placement.Plan([.. kinds.Values], read.Rows, read.Members, memory.Handovers, barred);
        foreach (var assignment in plan)
        {
            await points.ReachAsync(OwnedFaultPoints.PlacerBeforeAssign, stop).ConfigureAwait(false);
            long? token;
            using (var bound = Bound(stop))
            {
                token = await store.AssignAsync(tenure.Placer, assignment, kinds[assignment.Kind].Eligibility, bound.Token).ConfigureAwait(false);
            }

            if (token is null)
            {
                logger.LogInformation(
                    "{Kind}/{Key} was not assigned to {Target}: the row or the target changed since the pass read them.",
                    assignment.Kind,
                    assignment.Key,
                    assignment.Target.ReplicaId);
                continue;
            }

            memory.Assigned((assignment.Kind, assignment.Key), token.Value, assignment.Target.ReplicaId);
            logger.LogInformation("{Kind}/{Key} tenure {Token} is assigned to {Target}.", assignment.Kind, assignment.Key, token, assignment.Target.ReplicaId);
            await wakeups.PublishAsync(Wakeup.Wakeup.Nudge(WakeupKind.OwnerNudge, assignment.Target.ReplicaId), stop).ConfigureAwait(false);
        }

        if (plan.Count == 0
            && memory.MayRebalance()
            && Placement.Rebalance([.. kinds.Values], read.Rows, read.Members, barred) is { } move)
        {
            await RequestAsync(tenure, move, stop).ConfigureAwait(false);
        }
    }

    private Bounded Bound(CancellationToken cancellationToken)
    {
        var timer = new CancellationTokenSource(options.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);
        return new Bounded(timer, CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token));
    }

    private sealed class Bounded(CancellationTokenSource timer, CancellationTokenSource linked) : IDisposable
    {
        public CancellationToken Token => linked.Token;

        public void Dispose()
        {
            linked.Dispose();
            timer.Dispose();
        }
    }

    private sealed record Tenure(IFencedOwner Placer, PlacerMemory Memory);
}
