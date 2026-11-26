using Reeve.Cluster.Owned;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Fence;

internal enum RaiseOutcome
{
    Installed,

    Refused,

    Contended,
}

internal sealed class TenureFence
{
    internal const int RaiseAttempts = 16;

    internal const int WriteAttempts = 3;

    private readonly IDatabase database;
    private readonly OwnedContext owner;
    private readonly IFaultPoints points;
    private readonly TimeSpan ttl;
    private int raised;

    public TenureFence(IDatabase database, OwnedContext owner, IFaultPoints points, string prefix, TimeSpan ttl)
    {
        this.database = database;
        this.owner = owner;
        this.points = points;
        this.ttl = ttl;
        HashTag = HashTagOf(prefix, owner.Kind, owner.Key);
        FenceKey = HashTag + ":fence";
        HighWaterKey = HashTag + ":hw";
    }

    public string HashTag { get; }

    public RedisKey FenceKey { get; }

    public RedisKey HighWaterKey { get; }

    public OwnedContext Owner => owner;

    public bool Raised => Volatile.Read(ref raised) == 1;

    public static string HashTagOf(string prefix, string kind, string key) => "{" + prefix + "owned:" + kind + ":" + key + "}";

    public async Task<RaiseOutcome> RaiseAsync(CancellationToken cancellationToken)
    {
        var token = owner.Token;
        for (var attempt = 0; attempt < RaiseAttempts; attempt++)
        {
            owner.EnsureOwner();
            var stored = await database.StringGetAsync([FenceKey, HighWaterKey]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (IsAbove(stored[0], token) || IsAbove(stored[1], token))
            {
                Volatile.Write(ref raised, 0);
                return RaiseOutcome.Refused;
            }

            var transaction = database.CreateTransaction();
            transaction.AddCondition(Unchanged(FenceKey, stored[0]));
            transaction.AddCondition(Unchanged(HighWaterKey, stored[1]));
            _ = transaction.StringSetAsync(FenceKey, token, ttl);
            _ = transaction.StringSetAsync(HighWaterKey, token);
            await points.ReachAsync(OwnedFaultPoints.RedisBeforeExec, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            if (await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                Volatile.Write(ref raised, 1);
                return RaiseOutcome.Installed;
            }
        }

        return RaiseOutcome.Contended;
    }

    public async Task<bool> WriteAsync(Action<ITransaction> write, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < WriteAttempts; attempt++)
        {
            owner.EnsureOwner();
            if (!Raised)
            {
                throw new RedisFenceRefusedException($"The Redis fence of {owner.Kind}/{owner.Key} is not raised for tenure {owner.Token}: nothing was written.");
            }

            var transaction = database.CreateTransaction();
            var fence = transaction.AddCondition(Condition.StringEqual(FenceKey, owner.Token));
            write(transaction);
            await points.ReachAsync(OwnedFaultPoints.RedisBeforeExec, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                return true;
            }

            if (!fence.WasSatisfied)
            {
                throw new RedisFenceRefusedException(
                    $"The Redis fence of {owner.Kind}/{owner.Key} does not hold tenure {owner.Token} (a successor installed, or the key expired before a refresh): nothing was written.");
            }

            if (!transaction.WasWatchConflict)
            {
                return false;
            }
        }

        return false;
    }

    public async Task<bool> DeleteOwnAsync(CancellationToken cancellationToken)
    {
        Volatile.Write(ref raised, 0);
        for (var attempt = 0; attempt < WriteAttempts; attempt++)
        {
            var transaction = database.CreateTransaction();
            transaction.AddCondition(Condition.StringEqual(FenceKey, owner.Token));
            _ = transaction.KeyDeleteAsync(FenceKey);
            await points.ReachAsync(OwnedFaultPoints.RedisBeforeExec, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                return true;
            }

            if (!transaction.WasWatchConflict)
            {
                return false;
            }
        }

        return false;
    }

    private static bool IsAbove(RedisValue stored, long token) => !stored.IsNull && (long)stored > token;

    private static Condition Unchanged(RedisKey key, RedisValue read) =>
        read.IsNull ? Condition.KeyNotExists(key) : Condition.StringEqual(key, read);
}
