using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;

namespace Unit.EntityFrameworkCore;

public sealed class AggregateReconcilerCheckTests
{
    private sealed class Row;
    private sealed class Reconciler : IReconciler
    {
        public string AggregateType => "handled";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(new object()));
    }

    [Fact]
    public async Task A_mapped_type_without_a_reconciler_fails_start()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Row>("unhandled").NoKey().AllColumns());
        var check = new AggregateReconcilerCheck(mapping, []);
        var exception = await Assert.ThrowsAsync<ClusterConfigurationException>(() => check.StartingAsync(default));
        Assert.Equal(ClusterConfigurationReason.AggregateTypeWithoutReconciler, exception.Reason);
        Assert.Contains("unhandled", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task No_mapped_types_can_start_without_reconcilers()
    {
        await new AggregateReconcilerCheck(ClusterMapping.Build(_ => { }), []).StartingAsync(default);
    }

    [Fact]
    public async Task Every_mapped_type_with_a_reconciler_passes_start()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Row>("handled").NoKey().AllColumns());
        await new AggregateReconcilerCheck(mapping, [new Reconciler()]).StartingAsync(default);
    }
}
