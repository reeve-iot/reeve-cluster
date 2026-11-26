using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Slots;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal sealed partial class RedisSlots(
    IRedisSlotRows rows, ISlotGate gate, SlotRegistry registry, OnRedisUnavailable policy, IOptions<ClusterOptions> options, TimeProvider time,
    ILogger<RedisSlots> logger, ISlotObserver observer) : IRedisSlots
{
    private readonly OnRedisUnavailable policy = RedisDecisions.RequirePolicy(policy);

    public async Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ThrowIfTtlTooShort(ttl);
        if (gate.CheckServing() is not { } seat)
        {
            return Observed(new SlotAcquireResult.NotServing());
        }

        var started = time.GetTimestamp();
        Guid token;
        try
        {
            var read = await rows.ReadAsync(key, id, cancellationToken).ConfigureAwait(false);

            if (read.IdToken is { } named && await rows.RenewAsync(key, id, named, read.Now, ttl, cancellationToken).ConfigureAwait(false))
            {
                registry.Record(key, id, named, seat.Epoch, started, ttl);
                return Observed(new SlotAcquireResult.Acquired(named));
            }

            token = RedisTokens.Draw();
            var count = await rows.GrantAsync(key, token, read.Now, ttl, cancellationToken).ConfigureAwait(false);
            if (!RedisDecisions.SlotAdmitted(count, limit))
            {
                await RefuseQuietlyAsync(key, token).ConfigureAwait(false);
                return Observed(new SlotAcquireResult.Full());
            }
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return Observed(Unavailable());
        }

        await AdmitQuietlyAsync(key, id, token, ttl).ConfigureAwait(false);
        registry.Record(key, id, token, seat.Epoch, started, ttl);
        return Observed(new SlotAcquireResult.Acquired(token));
    }

    public async Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        ThrowIfTtlTooShort(ttl);
        if (registry.TryGet(key, id, token) is not { } entry)
        {
            return Observed(SlotRenewResult.Ended);
        }

        if (gate.CheckHolder(entry.Epoch) is null)
        {
            registry.Remove(key, id, token);
            return Observed(SlotRenewResult.NotServing);
        }

        var started = time.GetTimestamp();
        try
        {
            var now = await rows.TimeAsync(key, cancellationToken).ConfigureAwait(false);
            if (await rows.RenewAsync(key, id, token, now, ttl, cancellationToken).ConfigureAwait(false))
            {
                registry.Extend(key, id, token, started, ttl);
                return Observed(SlotRenewResult.Renewed);
            }

            registry.Remove(key, id, token);
            return Observed(SlotRenewResult.Ended);
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return Observed(SlotRenewResult.Unknown);
        }
    }

    public async Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        try
        {
            return await rows.ReleaseAsync(key, id, token, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            throw new CommitOutcomeUnknownException($"The release of the Redis slot {id} of {key} was not answered: its outcome is unknown.", e);
        }
        finally
        {
            registry.Remove(key, id, token);
        }
    }

    public async Task<RedisEffectResult> ExecuteAsync(string key, string id, Guid token, Action<ITransaction> effect, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        ArgumentNullException.ThrowIfNull(effect);
        if (registry.TryGet(key, id, token) is not { } entry || gate.CheckHolder(entry.Epoch) is null)
        {
            return RedisEffectResult.NotServing;
        }

        try
        {
            return await rows.ExecuteAsync(key, token, effect, cancellationToken).ConfigureAwait(false)
                ? RedisEffectResult.Committed
                : RedisEffectResult.Refused;
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return RedisEffectResult.Unknown;
        }
    }

    private SlotAcquireResult Unavailable()
    {
        RedisDiagnostics.PolicyUsed(RedisDiagnostics.Slot, policy);
        return RedisDecisions.OnUnavailable(policy) == UnavailableAction.Proceed ? new SlotAcquireResult.Acquired(RedisTokens.Draw()) : new SlotAcquireResult.Unknown();
    }

    private async Task RefuseQuietlyAsync(string key, Guid token)
    {
        try
        {
            await rows.RefuseAsync(key, token, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) when (e is RedisException or TimeoutException)
        {
            LogRefuseFailed(logger, e, key);
        }
    }

    private async Task AdmitQuietlyAsync(string key, string id, Guid token, TimeSpan ttl)
    {
        try
        {
            await rows.AdmitAsync(key, id, token, ttl, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) when (e is RedisException or TimeoutException)
        {
            LogAdmitFailed(logger, e, id, key);
        }
    }

    private SlotAcquireResult Observed(SlotAcquireResult result)
    {
        observer.Acquired(result);
        return result;
    }

    private SlotRenewResult Observed(SlotRenewResult result)
    {
        observer.Renewed(result);
        return result;
    }

    private static void ThrowIfInvalid(string key, string id)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(id);
        if (key.AsSpan().IndexOfAny('{', '}') >= 0 || id.AsSpan().IndexOfAny('{', '}') >= 0)
        {
            throw new ArgumentException("A Redis slot's key and id may not contain '{' or '}': the braces delimit the hash tag.");
        }
    }

    private void ThrowIfTtlTooShort(TimeSpan ttl) => ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, options.Value.SafetyMargin);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The refused member of a slot of {Key} could not be removed; it stays counted until its time to live and a grant is refused meanwhile.")]
    private static partial void LogRefuseFailed(ILogger logger, Exception exception, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The key of the slot {Id} of {Key} could not be written after an admitted grant; a retry with the same id grants again and is refused until the first member ends.")]
    private static partial void LogAdmitFailed(ILogger logger, Exception exception, string id, string key);
}
