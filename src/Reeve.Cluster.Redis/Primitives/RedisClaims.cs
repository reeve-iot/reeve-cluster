using Microsoft.Extensions.Logging;
using Reeve.Cluster.Claims;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal sealed class RedisClaims(IRedisClaimRows rows, IClaimGate gate, OnRedisUnavailable policy) : IRedisClaims
{
    private readonly OnRedisUnavailable policy = RedisDecisions.RequirePolicy(policy);

    public async Task<ClaimResult> ClaimAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);

        for (var attempt = 0; attempt < RedisDecisions.MaxGrantAttempts; attempt++)
        {
            if (gate.CheckServing() is not { } seat)
            {
                return new ClaimResult.NotServing();
            }

            var token = RedisTokens.Draw();
            bool granted;
            try
            {
                granted = await rows.GrantAsync(RedisSpace.Claim, key, seat.ReplicaId, token, ttl, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (RedisFailure.IsUnavailable(e))
            {
                return Unavailable();
            }

            if (granted)
            {
                return new ClaimResult.Won(token);
            }

            RedisRecord record;
            try
            {
                record = await rows.ReadAsync(RedisSpace.Claim, key, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (RedisFailure.IsUnavailable(e))
            {
                return Unavailable();
            }

            if (record.TokenPresent)
            {
                return new ClaimResult.Lost(record.Holder ?? string.Empty);
            }
        }

        return Unavailable();
    }

    private ClaimResult Unavailable()
    {
        RedisDiagnostics.PolicyUsed(RedisDiagnostics.Claim, policy);
        return RedisDecisions.OnUnavailable(policy) == UnavailableAction.Proceed ? new ClaimResult.Won(RedisTokens.Draw()) : new ClaimResult.Unknown();
    }

    public async Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        try
        {
            return await rows.ReleaseAsync(RedisSpace.Claim, key, token, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            throw new CommitOutcomeUnknownException($"The release of the Redis claim {key} was not answered: its outcome is unknown.", e);
        }
    }

    public async Task<RedisEffectResult> ExecuteAsync(string key, Guid token, Action<ITransaction> effect, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(effect);
        if (gate.CheckServing() is null)
        {
            return RedisEffectResult.NotServing;
        }

        try
        {
            return await rows.ExecuteAsync(RedisSpace.Claim, key, token, effect, cancellationToken).ConfigureAwait(false)
                ? RedisEffectResult.Committed
                : RedisEffectResult.Refused;
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return RedisEffectResult.Unknown;
        }
    }
}

internal sealed class RedisSingleFlight(IRedisClaimRows rows, IClaimGate gate, OnRedisUnavailable policy, TimeProvider time) : IRedisSingleFlight
{
    internal static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    private readonly OnRedisUnavailable policy = RedisDecisions.RequirePolicy(policy);

    public async Task<SingleFlightResult> RunAsync(
        string key, Func<CancellationToken, Task<byte[]>> compute, TimeSpan claimTtl, TimeSpan resultTtl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(compute);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(claimTtl, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(resultTtl, TimeSpan.Zero);

        var vanished = 0;
        while (vanished < RedisDecisions.MaxGrantAttempts)
        {
            Step step;
            try
            {
                step = await AcquireAsync(key, claimTtl, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (RedisFailure.IsUnavailable(e))
            {
                return await UnavailableAsync(compute, cancellationToken).ConfigureAwait(false);
            }

            if (step.Answer is { } answer)
            {
                return answer;
            }

            if (step.Token is { } token)
            {
                return await ComputeAsync(key, token, compute, resultTtl, cancellationToken).ConfigureAwait(false);
            }

            if (step.Vanished)
            {
                vanished++;
            }
        }

        return await UnavailableAsync(compute, cancellationToken).ConfigureAwait(false);
    }

    private readonly record struct Step(SingleFlightResult? Answer, Guid? Token, bool Vanished = false);

    private async Task<Step> AcquireAsync(string key, TimeSpan claimTtl, CancellationToken cancellationToken)
    {
        if (gate.CheckServing() is not { } seat)
        {
            return new Step(new SingleFlightResult.NotServing(), null);
        }

        var token = RedisTokens.Draw();
        if (await rows.GrantAsync(RedisSpace.Flight, key, seat.ReplicaId, token, claimTtl, cancellationToken).ConfigureAwait(false))
        {
            var held = await rows.ReadAsync(RedisSpace.Flight, key, cancellationToken).ConfigureAwait(false);
            if (held.Outcome is not { } live)
            {
                return new Step(null, token);
            }

            await ReleaseQuietlyAsync(key, token).ConfigureAwait(false);
            return new Step(new SingleFlightResult.Waited(live), null);
        }

        for (var waited = false; ; waited = true)
        {
            var record = await rows.ReadAsync(RedisSpace.Flight, key, cancellationToken).ConfigureAwait(false);
            if (record.Outcome is { } result)
            {
                return new Step(new SingleFlightResult.Waited(result), null);
            }

            if (!record.TokenPresent)
            {
                return new Step(null, null, !waited);
            }

            await Task.Delay(PollInterval, time, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is null)
            {
                return new Step(new SingleFlightResult.NotServing(), null);
            }
        }
    }

    private async Task<SingleFlightResult> UnavailableAsync(Func<CancellationToken, Task<byte[]>> compute, CancellationToken cancellationToken)
    {
        RedisDiagnostics.PolicyUsed(RedisDiagnostics.SingleFlight, policy);
        if (RedisDecisions.OnUnavailable(policy) == UnavailableAction.Stop)
        {
            return new SingleFlightResult.Unknown();
        }

        var value = await compute(cancellationToken).ConfigureAwait(false) ?? throw new InvalidOperationException("The computation returned null.");
        return new SingleFlightResult.ComputedUnknown(value);
    }

    private async Task<SingleFlightResult> ComputeAsync(
        string key, Guid token, Func<CancellationToken, Task<byte[]>> compute, TimeSpan resultTtl, CancellationToken cancellationToken)
    {
        byte[] value;
        try
        {
            value = await compute(cancellationToken).ConfigureAwait(false) ?? throw new InvalidOperationException($"The computation of {key} returned null.");
        }
        catch
        {
            await ReleaseQuietlyAsync(key, token).ConfigureAwait(false);
            throw;
        }

        if (gate.CheckServing() is null)
        {
            return new SingleFlightResult.ComputedRefused(value);
        }

        try
        {
            return await rows.CompleteAsync(RedisSpace.Flight, key, token, value, resultTtl, cancellationToken).ConfigureAwait(false)
                ? new SingleFlightResult.Computed(value)
                : new SingleFlightResult.ComputedRefused(value);
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return new SingleFlightResult.ComputedUnknown(value);
        }
    }

    private async Task ReleaseQuietlyAsync(string key, Guid token)
    {
        try
        {
            _ = await rows.ReleaseAsync(RedisSpace.Flight, key, token, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) when (e is RedisException or TimeoutException)
        {
        }
    }
}

internal sealed partial class RedisBudget(IRedisBudgetRows rows, IClaimGate gate, OnRedisUnavailable policy, ILogger<RedisBudget> logger) : IRedisBudget
{
    private readonly OnRedisUnavailable policy = RedisDecisions.RequirePolicy(policy);

    public async Task<BudgetResult> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(id);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(period, TimeSpan.FromMilliseconds(1));
        if (gate.CheckServing() is null)
        {
            return new BudgetResult.NotServing();
        }

        RedisBudgetStatement statement;
        try
        {
            statement = await rows.ConsumeAsync(key, id, period, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            RedisDiagnostics.PolicyUsed(RedisDiagnostics.Budget, policy);
            return RedisDecisions.OnUnavailable(policy) == UnavailableAction.Proceed
                ? new BudgetResult.Granted(RedisDecisions.UncountedGrant)
                : new BudgetResult.Unknown();
        }

        var verdict = RedisDecisions.AfterConsume(statement, limit);
        if (verdict is BudgetResult.Exhausted)
        {
            try
            {
                await rows.UndoAsync(key, id, statement.Window, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception e) when (e is RedisException or TimeoutException)
            {
                LogUndoFailed(logger, e, id, key);
            }
        }

        return verdict;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The id {Id} the limit stopped could not be removed from the window of the Redis budget {Key}; it stays in the window's set.")]
    private static partial void LogUndoFailed(ILogger logger, Exception exception, string id, string key);
}

internal sealed class RedisCounter(IRedisCounterRows rows) : IRedisCounter
{
    public async Task<CounterResult> IncrementAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        try
        {
            return new CounterResult.Incremented(await rows.IncrementAsync(key, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception e) when (RedisFailure.IsUnavailable(e))
        {
            return new CounterResult.Unknown();
        }
    }
}
