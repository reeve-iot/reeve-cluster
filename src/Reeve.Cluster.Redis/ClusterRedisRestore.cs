using System.Text;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis;

/// <summary>
/// The Redis step of the restore procedure: with every process that
/// runs the library stopped, after the database is restored and before any replica starts, clear the library's Redis keys — every key matching
/// <c>{&lt;prefix&gt;owned:*</c>, the fence keys and the high-water keys — so that the tenures drawn after the restore, whose tokens
/// rewind with the database, can raise their fences. Run with such a process still running, it voids the Redis fence's ordering.
/// </summary>
public static class ClusterRedisRestore
{
    /// <summary>
    /// Deletes every key matching <c>{&lt;prefix&gt;owned:*</c> on every Redis primary <paramref name="redis"/> lists, by
    /// <c>SCAN</c> and <c>UNLINK</c>, in the connection's default database, which must be the one the replicas use.
    /// </summary>
    /// <param name="redis">A connection to the deployment the replicas use.</param>
    /// <param name="keyPrefix">The replicas' <see cref="ClusterRedisOptions.KeyPrefix"/>; <c>reeve:</c> by default.</param>
    /// <param name="cancellationToken">Stops the clearing between keys; the keys deleted so far stay deleted.</param>
    /// <returns>How many keys were deleted.</returns>
    /// <exception cref="InvalidOperationException">A server the connection lists is not connected, or none is a primary; nothing is deleted.</exception>
    public static async Task<long> ClearOwnedKeysAsync(IConnectionMultiplexer redis, string keyPrefix = "reeve:", CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentNullException.ThrowIfNull(keyPrefix);
        var pattern = "{" + EscapeGlob(keyPrefix) + "owned:*";
        var database = redis.GetDatabase();
        long deleted = 0;

        var servers = redis.GetServers();
        if (servers.Length == 0 || servers.Any(s => !s.IsConnected) || !servers.Any(s => !s.IsReplica))
        {
            throw new InvalidOperationException(
                "The restore clearing needs every Redis server connected and a primary among them; nothing was deleted: "
                + string.Join(", ", servers.Select(s => $"{s.EndPoint} {(s.IsConnected ? (s.IsReplica ? "replica" : "primary") : "not connected")}")));
        }

        foreach (var server in servers.Where(s => !s.IsReplica))
        {
            await foreach (var key in server.KeysAsync(pattern: pattern).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                deleted += (long)await database.ExecuteAsync("UNLINK", key).ConfigureAwait(false);
            }
        }

        return deleted;
    }

    private static string EscapeGlob(string value)
    {
        var escaped = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            if (c is '*' or '?' or '[' or ']' or '\\')
            {
                escaped.Append('\\');
            }

            escaped.Append(c);
        }

        return escaped.ToString();
    }
}
