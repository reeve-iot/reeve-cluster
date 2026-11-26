using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal sealed class RedisSlotStore(IDatabase database, string prefix) : IRedisSlotRows
{
    public async Task<long> TimeAsync(string key, CancellationToken cancellationToken) =>
        Milliseconds(await StartTime(key).WaitAsync(cancellationToken).ConfigureAwait(false));

    public async Task<RedisSlotRead> ReadAsync(string key, string id, CancellationToken cancellationToken)
    {
        var time = StartTime(key);
        var value = database.StringGetAsync(IdKey(key, id));
        var now = Milliseconds(await time.WaitAsync(cancellationToken).ConfigureAwait(false));
        var token = await value.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new RedisSlotRead(now, !token.IsNull && Guid.TryParseExact((string)token!, "N", out var named) ? named : null);
    }

    public async Task<bool> RenewAsync(string key, string id, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken)
    {
        RedisKey held = HeldKey(key);
        var member = Member(token);

        var transaction = database.CreateTransaction();
        _ = transaction.SortedSetRemoveRangeByScoreAsync(held, double.NegativeInfinity, now);
        _ = transaction.SortedSetAddAsync(held, member, Expiry(now, ttl), SortedSetWhen.Exists);
        var score = transaction.SortedSetScoreAsync(held, member);
        _ = transaction.KeyExpireAsync(IdKey(key, id), ttl);
        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"The unconditional transaction of the renewal of slot {id} of {key} was not executed.");
        }

        return RedisDecisions.SlotRenewed(await score.ConfigureAwait(false));
    }

    public async Task<long> GrantAsync(string key, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken)
    {
        RedisKey held = HeldKey(key);

        var transaction = database.CreateTransaction();
        _ = transaction.SortedSetRemoveRangeByScoreAsync(held, double.NegativeInfinity, now);
        _ = transaction.SortedSetAddAsync(held, Member(token), Expiry(now, ttl), SortedSetWhen.NotExists);
        var count = transaction.SortedSetLengthAsync(held);
        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"The unconditional transaction of the grant of a slot of {key} was not executed.");
        }

        return await count.ConfigureAwait(false);
    }

    public async Task AdmitAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken) =>
        _ = await database.StringSetAsync(IdKey(key, id), Member(token), ttl, When.Always).WaitAsync(cancellationToken).ConfigureAwait(false);

    public async Task RefuseAsync(string key, Guid token, CancellationToken cancellationToken) =>
        _ = await database.SortedSetRemoveAsync(HeldKey(key), Member(token)).WaitAsync(cancellationToken).ConfigureAwait(false);

    public async Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken)
    {
        var named = token;
        if (named is null)
        {
            var value = await database.StringGetAsync(IdKey(key, id)).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (value.IsNull || !Guid.TryParseExact((string)value!, "N", out var parsed))
            {
                return false;
            }

            named = parsed;
        }

        return await database.SortedSetRemoveAsync(HeldKey(key), Member(named.Value)).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ExecuteAsync(string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken)
    {
        RedisKey held = HeldKey(key);
        var member = Member(token);
        var time = StartTime(key);
        var scored = database.SortedSetScoreAsync(held, member);
        var now = Milliseconds(await time.WaitAsync(cancellationToken).ConfigureAwait(false));
        if (await scored.WaitAsync(cancellationToken).ConfigureAwait(false) is not { } score || score <= now)
        {
            return false;
        }

        var transaction = database.CreateTransaction();
        transaction.AddCondition(Condition.SortedSetEqual(held, member, score % 1 == 0 ? (RedisValue)(long)score : score));
        queue(transaction);
        return await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task<RedisResult> StartTime(string key)
    {
        IServer server;
        try
        {
            server = database.Multiplexer.GetServer(HeldKey(key));
        }
        catch (InvalidOperationException)
        {
            return database.ExecuteAsync("TIME");
        }

        return server.ExecuteAsync("TIME");
    }

    private static long Milliseconds(RedisResult time)
    {
        var parts = (RedisResult[])time!;
        return (long)parts[0] * 1000 + (long)parts[1] / 1000;
    }

    private static long Expiry(long now, TimeSpan ttl) => now + (long)ttl.TotalMilliseconds;

    private static string Member(Guid token) => token.ToString("N");

    private RedisKey HeldKey(string key) => RedisPrimitiveKeys.SlotHashTag(prefix, key) + ":held";

    private RedisKey IdKey(string key, string id) => RedisPrimitiveKeys.SlotHashTag(prefix, key) + ":id:" + id;
}
