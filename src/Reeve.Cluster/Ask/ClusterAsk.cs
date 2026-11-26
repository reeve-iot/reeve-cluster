using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Ask;

internal sealed class ClusterAsk : IClusterAsk
{
    internal static readonly TimeSpan OwnerPollInterval = TimeSpan.FromMilliseconds(500);

    private readonly IAskDirectory directory;
    private readonly IAskInbound inbound;
    private readonly IAskTransport transport;
    private readonly MemberIdentity identity;
    private readonly Dictionary<string, UnitKind> kinds;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ILogger<ClusterAsk> logger;
    private readonly string replicaId;

    public ClusterAsk(
        IAskDirectory directory,
        IAskInbound inbound,
        IAskTransport transport,
        MemberIdentity identity,
        IEnumerable<UnitKind> registered,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<ClusterAsk> logger)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(inbound);
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);
        this.directory = directory;
        this.inbound = inbound;
        this.transport = transport;
        this.identity = identity;
        kinds = registered.ToDictionary(k => k.Kind, StringComparer.Ordinal);
        this.options = options.Value;
        this.time = time;
        this.logger = logger;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    }

    public async Task<AskResult> AskAsync(string kind, string key, ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        ArgumentNullException.ThrowIfNull(key);
        RequireFinite(timeout);

        var started = time.GetTimestamp();
        using var deadline = new CancellationTokenSource(timeout, time);
        using var call = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var failoverBound = FailoverBound(kind);
        AskOwner? declined = null;
        var wait = ReplacementWait.OtherOwner;
        while (true)
        {
            var remaining = timeout - time.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero || call.IsCancellationRequested)
            {
                return AskResult.NotTaken;
            }

            var resolved = await ResolveOwnerAsync(kind, key, AskOutcomes.OwnerWait(remaining, failoverBound), call.Token, cancellationToken, declined, wait).ConfigureAwait(false);
            if (resolved.Result is { } ended)
            {
                return ended;
            }

            var left = timeout - time.GetElapsedTime(started);
            if (left <= TimeSpan.Zero || call.IsCancellationRequested)
            {
                return AskResult.NotTaken;
            }

            var owner = resolved.Owner!;
            var attempt = await DeliverAsync(new AskTarget(owner.ReplicaId, owner.ProcessId), new AskDelivery(kind, key, owner.Token, request), left, takeTimeout: null, call.Token).ConfigureAwait(false);
            var step = AskOutcomes.Next(attempt, keyed: true, reResolved: declined is not null);
            if (step.Result is { } result)
            {
                return result;
            }

            declined = owner;
            wait = AskOutcomes.AfterDecline(attempt.Reason);
            if (wait == ReplacementWait.None)
            {
                return AskResult.NotTaken;
            }
        }
    }

    public Task<AskResult> AskMemberAsync(string replicaId, ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        AskMemberAsync(replicaId, request, timeout, takeTimeout: null, cancellationToken);

    public Task<AskResult> AskMemberAsync(string replicaId, ReadOnlyMemory<byte> request, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken = default) =>
        AskMemberAsync(replicaId, request, timeout, (TimeSpan?)takeTimeout, cancellationToken);

    private async Task<AskResult> AskMemberAsync(string replicaId, ReadOnlyMemory<byte> request, TimeSpan timeout, TimeSpan? takeTimeout, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        RequireFinite(timeout);
        if (takeTimeout is { } bound)
        {
            RequireFinite(bound);
        }

        var started = time.GetTimestamp();
        using var deadline = new CancellationTokenSource(timeout, time);
        using var call = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        AskMemberRow? row;
        try
        {
            row = await directory.ResolveMemberAsync(replicaId, call.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!call.IsCancellationRequested)
            {
                logger.LogWarning(exception, "The member row of {Replica} was not read; the request was not delivered.", replicaId);
            }

            return AskResult.NotTaken;
        }

        if (AskOutcomes.MemberTarget(replicaId, row) is not { } target)
        {
            return AskResult.NotTaken;
        }

        var left = timeout - time.GetElapsedTime(started);
        if (left <= TimeSpan.Zero || call.IsCancellationRequested)
        {
            return AskResult.NotTaken;
        }

        var takeLeft = takeTimeout - time.GetElapsedTime(started);
        if (takeLeft <= TimeSpan.Zero)
        {
            return AskResult.NotTaken;
        }

        if (takeLeft > left)
        {
            takeLeft = left;
        }

        var attempt = await DeliverAsync(target, new AskDelivery(null, null, null, request), left, takeLeft, call.Token).ConfigureAwait(false);
        return AskOutcomes.Next(attempt, keyed: false, reResolved: false).Result!;
    }

    private static void RequireFinite(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
    }

    private TimeSpan FailoverBound(string kind) =>
        (kinds.TryGetValue(kind, out var registered) ? registered : new UnitKind(kind, typeof(IOwnedUnit), new UnitKindOptions())).FailoverBound(options);

    private async Task<(AskOwner? Owner, AskResult? Result)> ResolveOwnerAsync(
        string kind, string key, TimeSpan wait, CancellationToken call, CancellationToken caller, AskOwner? declined, ReplacementWait replacement)
    {
        var begun = time.GetTimestamp();
        while (true)
        {
            try
            {
                if (await directory.ResolveOwnerAsync(kind, key, call).ConfigureAwait(false) is { } owner
                    && (declined is null || AskOutcomes.IsAnother(owner, declined)
                        || (replacement == ReplacementWait.OtherOwnerOrGrace && time.GetElapsedTime(begun) >= OwnerPollInterval)))
                {
                    return (owner, null);
                }

                var left = wait - time.GetElapsedTime(begun);
                if (left <= TimeSpan.Zero)
                {
                    return (null, AskResult.NoOwner);
                }

                await Task.Delay(left < OwnerPollInterval ? left : OwnerPollInterval, time, call).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (call.IsCancellationRequested)
            {
                return (null, caller.IsCancellationRequested ? AskResult.NotTaken : AskResult.NoOwner);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogWarning(exception, "The owner of {Kind}/{Key} was not read; the request was not delivered.", kind, key);
                return (null, AskResult.NotTaken);
            }
        }
    }

    private async Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan remaining, TimeSpan? takeTimeout, CancellationToken call)
    {
        try
        {
            if (target.ReplicaId == replicaId && target.ProcessId == identity.ProcessId)
            {
                return await LocalAskDispatch.DeliverAsync(inbound, delivery, remaining, time, call).ConfigureAwait(false);
            }

            return takeTimeout is { } take
                ? await transport.DeliverAsync(target, delivery, remaining, take, call).ConfigureAwait(false)
                : await transport.DeliverAsync(target, delivery, remaining, call).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "Delivery of an Ask to {Replica} threw; the request may have been taken.", target.ReplicaId);
            return new DeliveryOutcome(DeliveryKind.TakenUnanswered, null);
        }
    }
}

internal static class LocalAskDispatch
{
    public static async Task<DeliveryOutcome> DeliverAsync(IAskInbound inbound, AskDelivery delivery, TimeSpan timeout, TimeProvider time, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(inbound);
        ArgumentNullException.ThrowIfNull(delivery);
        using var expiry = new CancellationTokenSource(timeout, time);
        using var answerBy = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, expiry.Token);

        var taking = await inbound.TakeAsync(delivery, answerBy.Token).ConfigureAwait(false);
        if (!taking.IsTaken)
        {
            return new DeliveryOutcome(DeliveryKind.Declined, null, taking.Reason);
        }

        try
        {
            var answer = await taking.Answer.WaitAsync(answerBy.Token).ConfigureAwait(false);

            return answer.Kind == AskAnswerKind.Unanswered
                ? new DeliveryOutcome(DeliveryKind.TakenUnanswered, null)
                : new DeliveryOutcome(DeliveryKind.Answered, answer);
        }
        catch (OperationCanceledException) when (answerBy.IsCancellationRequested)
        {
            return new DeliveryOutcome(DeliveryKind.TakenUnanswered, null);
        }
    }
}
