using Reeve.Cluster;

namespace Int.Membership;

internal static class StartupBound
{
    public static TimeSpan For(ClusterOptions options) => options.LeaseTtl + options.RenewInterval
        + options.ReconcileTimeout!.Value + options.ApplyStallLimit;
}
