using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal sealed class RedisClaimStore(IDatabase database, string prefix) : IRedisClaimRows
{
    public async Task<bool> GrantAsync(RedisSpace space, string key, string holder, Guid token, TimeSpan ttl, CancellationToken cancellationToken) =>
        await database.StringSetAsync(TokenKey(space, key), RedisClaimValue.Encode(token, holder), ttl, When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false);

    public async Task<RedisRecord> ReadAsync(RedisSpace space, string key, CancellationToken cancellationToken)
    {
        if (space == RedisSpace.Claim)
        {
            var value = (string?)await database.StringGetAsync(TokenKey(space, key)).WaitAsync(cancellationToken).ConfigureAwait(false);
            return new RedisRecord(value is not null, value is null ? null : RedisClaimValue.Holder(value), null);
        }

        var values = await database.StringGetAsync([TokenKey(space, key), ResultKey(space, key)]).WaitAsync(cancellationToken).ConfigureAwait(false);
        var token = (string?)values[0];
        return new RedisRecord(token is not null, token is null ? null : RedisClaimValue.Holder(token), values[1].IsNull ? null : (byte[]?)values[1]);
    }

    public async Task<bool> ReleaseAsync(RedisSpace space, string key, Guid token, CancellationToken cancellationToken)
    {
        var tokenKey = TokenKey(space, key);
        if (await HeldAsync(tokenKey, token, cancellationToken).ConfigureAwait(false) is not { } held)
        {
            return false;
        }

        var transaction = database.CreateTransaction();
        transaction.AddCondition(Condition.StringEqual(tokenKey, held));
        _ = transaction.KeyDeleteAsync(tokenKey);
        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ExecuteAsync(RedisSpace space, string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken)
    {
        var tokenKey = TokenKey(space, key);
        if (await HeldAsync(tokenKey, token, cancellationToken).ConfigureAwait(false) is not { } held)
        {
            return false;
        }

        var transaction = database.CreateTransaction();
        transaction.AddCondition(Condition.StringEqual(tokenKey, held));
        queue(transaction);

        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> CompleteAsync(RedisSpace space, string key, Guid token, byte[] outcome, TimeSpan outcomeTtl, CancellationToken cancellationToken)
    {
        var tokenKey = TokenKey(space, key);
        if (await HeldAsync(tokenKey, token, cancellationToken).ConfigureAwait(false) is not { } held)
        {
            return false;
        }

        var transaction = database.CreateTransaction();
        transaction.AddCondition(Condition.StringEqual(tokenKey, held));
        _ = transaction.StringSetAsync(ResultKey(space, key), outcome, outcomeTtl);
        _ = transaction.KeyDeleteAsync(tokenKey);
        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<RedisValue?> HeldAsync(RedisKey tokenKey, Guid token, CancellationToken cancellationToken)
    {
        var value = await database.StringGetAsync(tokenKey).WaitAsync(cancellationToken).ConfigureAwait(false);
        return !value.IsNull && RedisClaimValue.Holds((string)value!, token) ? value : (RedisValue?)null;
    }

    private RedisKey TokenKey(RedisSpace space, string key) => Tag(space, key) + ":token";

    private RedisKey ResultKey(RedisSpace space, string key) => Tag(space, key) + ":result";

    private string Tag(RedisSpace space, string key) => space == RedisSpace.Claim
        ? RedisPrimitiveKeys.ClaimHashTag(prefix, key)
        : RedisPrimitiveKeys.FlightHashTag(prefix, key);
}

internal sealed class RedisBudgetStore(IDatabase database, string prefix) : IRedisBudgetRows
{
    private static readonly TimeSpan Slack = TimeSpan.FromSeconds(1);

    public async Task<RedisBudgetStatement> ConsumeAsync(string key, string id, TimeSpan period, CancellationToken cancellationToken)
    {
        var time = (RedisResult[])(await database.ExecuteAsync("TIME").WaitAsync(cancellationToken).ConfigureAwait(false))!;
        var nowMilliseconds = (long)time[0] * 1000 + (long)time[1] / 1000;
        var periodMilliseconds = Math.Max(1, (long)period.TotalMilliseconds);
        var window = nowMilliseconds / periodMilliseconds * periodMilliseconds;
        RedisKey set = SetKey(key, window);

        var transaction = database.CreateTransaction();
        var added = transaction.SetAddAsync(set, id);
        var count = transaction.SetLengthAsync(set);
        _ = transaction.KeyExpireAsync(set, TimeSpan.FromMilliseconds(window + periodMilliseconds - nowMilliseconds) + Slack);
        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"The unconditional transaction of budget {key} was not executed.");
        }

        return new RedisBudgetStatement(await added.ConfigureAwait(false), await count.ConfigureAwait(false), window);
    }

    public async Task UndoAsync(string key, string id, long window, CancellationToken cancellationToken) =>
        _ = await database.SetRemoveAsync(SetKey(key, window), id).WaitAsync(cancellationToken).ConfigureAwait(false);

    private string SetKey(string key, long window) => RedisPrimitiveKeys.BudgetHashTag(prefix, key) + ":" + window.ToString(System.Globalization.CultureInfo.InvariantCulture);
}

internal sealed class RedisCounterStore(IDatabase database, string prefix, TimeSpan? retention = null) : IRedisCounterRows
{
    public async Task<long> IncrementAsync(string key, CancellationToken cancellationToken)
    {
        RedisKey counter = RedisPrimitiveKeys.CounterHashTag(prefix, key) + ":value";
        if (retention is not { } idle)
        {
            return await database.StringIncrementAsync(counter).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        var transaction = database.CreateTransaction();
        var value = transaction.StringIncrementAsync(counter);
        _ = transaction.KeyExpireAsync(counter, idle);
        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"The unconditional transaction of counter {key} was not executed.");
        }

        return await value.ConfigureAwait(false);
    }
}
