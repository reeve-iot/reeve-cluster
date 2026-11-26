namespace Reeve.Cluster.Core;

internal readonly record struct LeaseTiming(TimeSpan LeaseTtl, TimeSpan SafetyMargin, TimeSpan StopBudget, TimeSpan RenewTimeout)
{
    public static LeaseTiming From(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new LeaseTiming(options.LeaseTtl, options.SafetyMargin, options.StopBudget, options.RenewTimeout);
    }
}
