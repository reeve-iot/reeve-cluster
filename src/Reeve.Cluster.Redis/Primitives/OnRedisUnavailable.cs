namespace Reeve.Cluster.Redis;

/// <summary>
/// What a Redis primitive does when Redis does not decide a grant: it does not answer the
/// grant or the read that follows it, or a claim or single-flight lost three grants in a row to a claim that had expired by the time it was
/// read. It is required wherever a Redis primitive is registered, and every use of it is counted in <c>reeve_cluster.redis.policy_uses</c>.
/// The default value is not a policy: nothing is granted or refused by accident.
/// </summary>
public enum OnRedisUnavailable
{
    /// <summary>Fail open: the call proceeds as if granted, without the store's protection. The caller chose this; the at-most-once guarantee does not cover it.</summary>
    Grant = 1,

    /// <summary>Fail closed: the call is refused with an unknown outcome and nothing proceeds.</summary>
    Refuse = 2,
}
