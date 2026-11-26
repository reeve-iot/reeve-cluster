using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal enum RedisSpace
{
    Claim,

    Flight,
}

internal readonly record struct RedisRecord(bool TokenPresent, string? Holder, byte[]? Outcome);

internal interface IRedisClaimRows
{
    Task<bool> GrantAsync(RedisSpace space, string key, string holder, Guid token, TimeSpan ttl, CancellationToken cancellationToken);

    Task<RedisRecord> ReadAsync(RedisSpace space, string key, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(RedisSpace space, string key, Guid token, CancellationToken cancellationToken);

    Task<bool> ExecuteAsync(RedisSpace space, string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken);

    Task<bool> CompleteAsync(RedisSpace space, string key, Guid token, byte[] outcome, TimeSpan outcomeTtl, CancellationToken cancellationToken);
}

internal readonly record struct RedisBudgetStatement(bool Added, long Count, long Window);

internal interface IRedisBudgetRows
{
    Task<RedisBudgetStatement> ConsumeAsync(string key, string id, TimeSpan period, CancellationToken cancellationToken);

    Task UndoAsync(string key, string id, long window, CancellationToken cancellationToken);
}

internal interface IRedisCounterRows
{
    Task<long> IncrementAsync(string key, CancellationToken cancellationToken);
}

