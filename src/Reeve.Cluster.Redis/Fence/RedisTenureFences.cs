using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Fence;

internal sealed class RedisTenureFences : ITenureFence, IDisposable
{
    private static readonly ConditionalWeakTable<OwnedContext, TenureFence> ByContext = [];

    private readonly IDatabase database;
    private readonly IFaultPoints points;
    private readonly ILogger logger;
    private readonly string prefix;
    private readonly TimeSpan ttl;
    private readonly TimeSpan installWait;
    private readonly TimeProvider time;
    private readonly IDisposable renewals;
    private readonly Dictionary<TenureFence, RaiseReport> active = [];

    public RedisTenureFences(
        IConnectionMultiplexer redis,
        LocalPhaseGate gate,
        IFaultPoints points,
        IOptions<ClusterRedisOptions> redisOptions,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<RedisTenureFences> logger)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentNullException.ThrowIfNull(gate);
        database = redis.GetDatabase();
        this.points = points;
        this.logger = logger;
        prefix = redisOptions.Value.KeyPrefix;
        ttl = options.Value.LeaseTtl;
        installWait = options.Value.FencedTransactionTimeout;
        this.time = time;
        renewals = gate.OnRenewed(_ => Refresh());
    }

    public static TenureFence? Of(OwnedContext context) => ByContext.TryGetValue(context, out var fence) ? fence : null;

    public async Task OnAcquiringAsync(OwnedContext tenure, CancellationToken stop)
    {
        var fence = new TenureFence(database, tenure, points, prefix, ttl);
        ByContext.AddOrUpdate(tenure, fence);
        lock (active)
        {
            active[fence] = new RaiseReport();
        }

        var raise = Task.Run(() => RaiseAsync(fence), CancellationToken.None);
        try
        {
            await raise.WaitAsync(installWait, time, stop).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        catch (TimeoutException)
        {
            logger.LogWarning(
                "The Redis raise of {Kind}/{Key} tenure {Token} has not returned within FencedTransactionTimeout; the unit starts and its fenced Redis writes are refused until a raise succeeds.",
                tenure.Kind,
                tenure.Key,
                tenure.Token);
        }
    }

    public async Task OnFencedAsync(OwnedContext tenure, CancellationToken cancellationToken)
    {
        if (Of(tenure) is not { } fence)
        {
            return;
        }

        lock (active)
        {
            active.Remove(fence);
        }

        try
        {
            var deleted = await Task.Run(() => fence.DeleteOwnAsync(cancellationToken), CancellationToken.None)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            logger.LogInformation(
                deleted ? "Fence step 5 deleted the Redis fence key of {Kind}/{Key} tenure {Token}." : "Fence step 5 left the Redis fence key of {Kind}/{Key}: it no longer holds tenure {Token}.",
                tenure.Kind,
                tenure.Key,
                tenure.Token);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "Fence step 5 could not delete the Redis fence key of {Kind}/{Key} tenure {Token}; it expires on its TTL.", tenure.Kind, tenure.Key, tenure.Token);
        }
    }

    public void Dispose() => renewals.Dispose();

    private void Refresh()
    {
        TenureFence[] fences;
        lock (active)
        {
            fences = [.. active.Keys];
        }

        foreach (var fence in fences)
        {
            _ = Task.Run(() => RaiseAsync(fence));
        }
    }

    private async Task RaiseAsync(TenureFence fence)
    {
        RaiseOutcome? outcome;
        Exception? failure = null;
        try
        {
            outcome = await fence.RaiseAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (OwnershipLostException)
        {
            lock (active)
            {
                active.Remove(fence);
            }

            return;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            outcome = null;
            failure = exception;
        }

        lock (active)
        {
            if (!active.TryGetValue(fence, out var report) || (report.Logged && report.Last == outcome))
            {
                return;
            }

            report.Logged = true;
            report.Last = outcome;
        }

        var owner = fence.Owner;
        switch (outcome)
        {
            case RaiseOutcome.Installed:
                logger.LogInformation("The Redis fence of {Kind}/{Key} holds tenure {Token}.", owner.Kind, owner.Key, owner.Token);
                break;
            case RaiseOutcome.Refused:
                logger.LogWarning(
                    "The Redis fence of {Kind}/{Key} holds a token above tenure {Token}: its fenced Redis writes are refused.", owner.Kind, owner.Key, owner.Token);
                break;
            default:
                logger.LogWarning(
                    failure,
                    "The Redis raise of {Kind}/{Key} tenure {Token} did not decide ({Outcome}); the next renewal raises again.",
                    owner.Kind,
                    owner.Key,
                    owner.Token,
                    outcome?.ToString() ?? "no answer");
                break;
        }
    }

    private sealed class RaiseReport
    {
        public bool Logged { get; set; }

        public RaiseOutcome? Last { get; set; }
    }
}
