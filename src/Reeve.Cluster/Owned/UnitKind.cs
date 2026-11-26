namespace Reeve.Cluster.Owned;

internal sealed record UnitKind(string Kind, Type UnitType, UnitKindOptions Options)
{
    public bool Keyed { get; init; }

    public TimeSpan FailoverBound(ClusterOptions cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        var raise = Options.RedisFence ? cluster.FencedTransactionTimeout : TimeSpan.Zero;
        return cluster.LeaseTtl + 3 * cluster.PlacementInterval + cluster.FencedTransactionTimeout + Options.StartTime + raise;
    }
}
