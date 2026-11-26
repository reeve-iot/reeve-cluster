using Reeve.Cluster.Redis.Fence;
using StackExchange.Redis;

namespace Reeve.Cluster;

/// <summary>
/// <c>FenceRedis</c>: writes of state a unit keeps in Redis, conditioned on the tenure's token
/// stored in the key's Redis fence. The fence orders tenures — nothing of a previous tenure lands after its successor's raise —
/// and does not end one at the lease: a write delayed on the wire lands only within <c>LeaseTtl</c> of its owner's last refresh,
/// on Redis's clock, and before a successor installed. A Redis effect that must not outlive the lease goes through PostgreSQL.
/// The kind declares <see cref="UnitKindOptions.RedisFence"/>, and the Redis package is registered. The fence needs one Redis
/// deployment (standalone, Sentinel or Cluster): a connection group that fails over between deployments finds no high-water
/// keys on the other one, as when Redis loses acknowledged writes.
/// </summary>
public static class RedisFenceExtensions
{
    /// <summary>
    /// Runs <paramref name="write"/>'s commands in one Redis transaction (<c>MULTI</c>/<c>EXEC</c>) on the library's
    /// <see cref="IConnectionMultiplexer"/>, conditioned on the fence key holding this tenure's token, after
    /// <see cref="OwnedContext.EnsureOwner"/>, which runs again before each send. A transaction whose <c>EXEC</c> the server
    /// rejected because a watched key changed is built and sent again, so <paramref name="write"/> may run more than once: it only
    /// queues commands and conditions, and the results are those of the tasks its last run queued. Under Redis Cluster every key
    /// it writes carries <see cref="RedisHashTag"/>. A Redis error (<see cref="RedisTimeoutException"/>,
    /// <see cref="RedisConnectionException"/>) leaves the outcome unknown: the transaction may have committed.
    /// </summary>
    /// <param name="context">The context the library passed to <see cref="IOwnedUnit.OnAcquiredAsync"/>.</param>
    /// <param name="write">Queues the commands, and any conditions of the caller's, on the transaction.</param>
    /// <param name="cancellationToken">Checked before each send; once a transaction is sent it only stops the wait, and the
    /// transaction may still commit.</param>
    /// <returns>Whether the transaction committed; <see langword="false"/> when a condition of the caller's failed, or keys it
    /// watches kept changing.</returns>
    /// <exception cref="RedisFenceRefusedException">The fence is not raised for this tenure, or no longer holds its token. Nothing was written.</exception>
    /// <exception cref="OwnershipLostException">The unit is being stopped, or the replica no longer serves.</exception>
    /// <exception cref="InvalidOperationException">The kind does not declare <see cref="UnitKindOptions.RedisFence"/>, or the Redis package is not registered.</exception>
    public static Task<bool> FenceRedisAsync(this OwnedContext context, Action<ITransaction> write, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(write);
        return FenceOf(context).WriteAsync(write, cancellationToken);
    }

    /// <summary>
    /// The hash tag of the key's Redis fence, <c>{&lt;prefix&gt;owned:&lt;kind&gt;:&lt;key&gt;}</c>: under Redis Cluster the keys a
    /// <see cref="FenceRedisAsync"/> transaction writes contain it, so the transaction addresses one slot.
    /// </summary>
    /// <param name="context">The context the library passed to <see cref="IOwnedUnit.OnAcquiredAsync"/>.</param>
    /// <exception cref="InvalidOperationException">The kind does not declare <see cref="UnitKindOptions.RedisFence"/>, or the Redis package is not registered.</exception>
    public static string RedisHashTag(this OwnedContext context) => FenceOf(context).HashTag;

    private static TenureFence FenceOf(OwnedContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return RedisTenureFences.Of(context)
            ?? throw new InvalidOperationException(
                $"{context.Kind}/{context.Key} has no Redis fence: the kind declares RedisFence, the Redis package is registered, and the context is the one the library passed to OnAcquiredAsync.");
    }
}
