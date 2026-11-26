namespace Reeve.Cluster.Harness.Timing;

internal static class TestProfile
{
    public static readonly TimeSpan LeaseTtl = TimeSpan.FromSeconds(8);
    public static readonly TimeSpan SafetyMargin = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan StopBudget = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan RenewTimeout = TimeSpan.FromMilliseconds(500);
    public static readonly TimeSpan RenewInterval = TimeSpan.FromMilliseconds(1500);
    public static readonly TimeSpan ReconcileTimeout = TimeSpan.FromSeconds(1);

    public static readonly TimeSpan RejoinBackoff = TimeSpan.FromSeconds(2);

    public static readonly TimeSpan TolerantLeaseTtl = TimeSpan.FromSeconds(20);

    public static readonly TimeSpan TolerantApplyStallLimit = TimeSpan.FromSeconds(10);

    public static void ApplyTight(ClusterOptions options) => Apply(options);

    public static void ApplyStallTolerant(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.LeaseTtl = TolerantLeaseTtl;
        options.SafetyMargin = TimeSpan.FromSeconds(2);
        options.StopBudget = TimeSpan.FromSeconds(2);
        options.RenewTimeout = TimeSpan.FromSeconds(2);
        options.RenewInterval = TimeSpan.FromSeconds(3);
        options.ReconcileTimeout = ReconcileTimeout;
        options.ApplyStallLimit = TolerantApplyStallLimit;
        options.RejoinBackoff = RejoinBackoff;
    }

    public static void Apply(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.LeaseTtl = LeaseTtl;
        options.SafetyMargin = SafetyMargin;
        options.StopBudget = StopBudget;
        options.RenewTimeout = RenewTimeout;
        options.RenewInterval = RenewInterval;
        options.ReconcileTimeout = ReconcileTimeout;
        options.RejoinBackoff = RejoinBackoff;
    }

    public static void ApplyLibraryDefaults(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ReconcileTimeout = ReconcileTimeout;
    }

    public static ClusterOptions Options(string replicaId, string connectionString)
    {
        var options = new ClusterOptions { ReplicaId = replicaId, ConnectionString = connectionString };
        Apply(options);
        return options;
    }

    public static ClusterOptions Options(string replicaId, string connectionString, Action<ClusterOptions>? profile)
    {
        var options = new ClusterOptions { ReplicaId = replicaId, ConnectionString = connectionString };
        (profile ?? ApplyStallTolerant)(options);
        return options;
    }
}
