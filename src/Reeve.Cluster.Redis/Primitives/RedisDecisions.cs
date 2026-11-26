using System.Security.Cryptography;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Primitives;

internal enum UnavailableAction
{
    Proceed,

    Stop,
}

internal static class RedisFailure
{
    public static bool IsUnavailable(Exception exception) => exception switch
    {
        RedisConnectionException or RedisTimeoutException or TimeoutException => true,
        RedisServerException server => server.Kind is RedisErrorKind.Loading or RedisErrorKind.ClusterDown or RedisErrorKind.MasterDown
            or RedisErrorKind.TryAgain or RedisErrorKind.NoReplicas or RedisErrorKind.ReadOnly or RedisErrorKind.ConnectionFault or RedisErrorKind.Timeout,
        _ => false,
    };
}

internal static class RedisDecisions
{
    public const int MaxGrantAttempts = 3;

    public static UnavailableAction OnUnavailable(OnRedisUnavailable policy) => policy switch
    {
        OnRedisUnavailable.Grant => UnavailableAction.Proceed,
        OnRedisUnavailable.Refuse => UnavailableAction.Stop,
        _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, "Not an OnRedisUnavailable policy."),
    };

    public static OnRedisUnavailable RequirePolicy(OnRedisUnavailable policy) =>
        policy is OnRedisUnavailable.Grant or OnRedisUnavailable.Refuse
            ? policy
            : throw new ClusterConfigurationException(
                ClusterConfigurationReason.RedisPolicyRequired,
                $"OnRedisUnavailable must be Grant or Refuse; {(int)policy} is neither. A Redis primitive's policy is required and is never assumed.");

    public static BudgetResult AfterConsume(RedisBudgetStatement statement, int limit) => (statement.Added, statement.Count <= limit) switch
    {
        (false, _) => new BudgetResult.AlreadyCounted(),
        (true, true) => new BudgetResult.Granted((int)statement.Count),
        _ => new BudgetResult.Exhausted(),
    };

    public static bool SlotAdmitted(long count, int limit) => count <= limit;

    public static bool SlotRenewed(double? score) => score.HasValue;

    public const int UncountedGrant = 0;
}

internal static class RedisTokens
{
    public static Guid Draw() => new(RandomNumberGenerator.GetBytes(16));
}

internal static class RedisClaimValue
{
    private const int TokenLength = 32;

    public static string Encode(Guid token, string holder) => Text(token) + ":" + holder;

    public static string Holder(string value) => value.Length > TokenLength && value[TokenLength] == ':' ? value[(TokenLength + 1)..] : string.Empty;

    public static bool Holds(string value, Guid token) =>
        value.Length > TokenLength && value[TokenLength] == ':' && value.AsSpan(0, TokenLength).SequenceEqual(Text(token));

    private static string Text(Guid token) => token.ToString("N");
}
