using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal readonly record struct RedisSlotRead(long Now, Guid? IdToken);

internal interface IRedisSlotRows
{
    Task<long> TimeAsync(string key, CancellationToken cancellationToken);

    Task<RedisSlotRead> ReadAsync(string key, string id, CancellationToken cancellationToken);

    Task<bool> RenewAsync(string key, string id, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken);

    Task<long> GrantAsync(string key, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken);

    Task AdmitAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken);

    Task RefuseAsync(string key, Guid token, CancellationToken cancellationToken);

    Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken);

    Task<bool> ExecuteAsync(string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken);
}
