using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Reeve.Cluster.Redis.Primitives;

internal static class RedisDiagnostics
{
    internal const string PolicyUsesName = "reeve_cluster.redis.policy_uses";

    internal const string Claim = "claim", SingleFlight = "single_flight", Budget = "budget", Slot = "slot";

    internal static readonly Counter<long> PolicyUses = ClusterDiagnostics.Meter.CreateCounter<long>(
        PolicyUsesName, unit: "{use}", description: "Uses of the OnRedisUnavailable policy by a Redis primitive.");

    public static void PolicyUsed(string primitive, OnRedisUnavailable policy) =>
        PolicyUses.Add(1, new TagList { { "primitive", primitive }, { "policy", policy == OnRedisUnavailable.Grant ? "grant" : "refuse" } });
}
