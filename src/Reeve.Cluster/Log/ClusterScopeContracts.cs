using System.Data.Common;

namespace Reeve.Cluster.Log;

internal interface IClusterScopeWriter : IClusterScope
{
    bool Ended { get; }

    long FenceStepsAtBegin { get; }

    void RecordSeq(long seq);

    void RecordRefusedCommit();

    void OptOut();

    void Pin(IGeneration generation);

    IGeneration PinOrGet(IGeneration generation);

    void TransactionOpened(DbTransaction transaction);

    void TransactionEnded(DbTransaction transaction);

    void ClassifyAsExempt();
}

internal static class EndedScopes
{
    public static IClusterScopeWriter? UnlessEnded(this IClusterScopeWriter? scope) => scope is { Ended: true } ? null : scope;
}

internal sealed class FenceSteps
{
    private long count;

    public long Count => Interlocked.Read(ref count);

    public void Increment() => Interlocked.Increment(ref count);
}

internal interface IClusterScopeSource : IClusterScopeAccessor
{
    IClusterScopeWriter? CurrentWriter { get; }

    IClusterScopeHandle Begin(ClusterScopeKind kind);
}
