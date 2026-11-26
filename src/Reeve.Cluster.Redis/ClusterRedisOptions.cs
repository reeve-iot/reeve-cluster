namespace Reeve.Cluster.Redis;

/// <summary>Options of the Redis package. Redis is never the authority: nothing here changes what the library guarantees.</summary>
public sealed class ClusterRedisOptions
{
    /// <summary>
    /// The prefix of every key and channel the library uses in Redis; <c>reeve:</c> by default. Replicas that are to hear each
    /// other share it, and two clusters on one Redis differ in it.
    /// </summary>
    public string KeyPrefix { get; set; } = "reeve:";

    /// <summary>
    /// Whether the start checks that the <c>maxmemory-policy</c> of every Redis primary is <c>noeviction</c> when a kind declares
    /// <c>RedisFence</c> or a Redis claim, single-flight, budget, counter or slot is registered; on by default. Any other policy may
    /// evict a key under memory pressure: a claim, flight or budget key carries a TTL (a counter's, with <c>CounterRetention</c>), which a <c>volatile-*</c> policy evicts
    /// (an evicted claim key ends the grant early, an evicted budget window restarts its count), and a <c>FenceRedis</c> high-water key
    /// and a held slot's set have none, which an <c>allkeys-*</c> policy evicts. A policy the check cannot read (<c>INFO</c> refused by an
    /// ACL or the provider) fails the start as well. A deployment that
    /// turns it off answers for the policy itself. The check's short-lived connection is a clone parsed from the application's
    /// configuration string; settings that string does not carry, such as a password containing <c>,</c> set in code, may be
    /// needed for the check's connection. If they are, the connection cannot authenticate or connect, and the start is refused.
    /// </summary>
    public bool CheckEvictionPolicy { get; set; } = true;
}
