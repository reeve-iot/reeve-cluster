using Reeve.Cluster;
using Reeve.Cluster.Log;

namespace Unit.Log;

public sealed class ReconcilerGraphTests
{
    private sealed class Declared(string type, IReadOnlyList<string>? dependsOn) : IReconciler
    {
        public string AggregateType => type;

        IReadOnlyList<string> IReconciler.DependsOn => dependsOn!;

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(type));
    }

    private sealed class Plain(string type) : IReconciler
    {
        public string AggregateType => type;

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(type));
    }

    private static Declared Reconciler(string type, params string[] dependsOn) => new(type, dependsOn);

    [Fact]
    public void Build_refuses_a_null_DependsOn_as_ReconcilerDependencyUnknown()
    {
        var ex = Assert.Throws<ClusterConfigurationException>(() => ReconcilerGraph.Build([new Plain("a"), new Declared("b", null)]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyUnknown, ex.Reason);
        Assert.Contains("'b'", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("ghost")]
    public void Build_refuses_a_null_or_empty_or_unregistered_dependency_as_ReconcilerDependencyUnknown(string? dependency)
    {
        var ex = Assert.Throws<ClusterConfigurationException>(
            () => ReconcilerGraph.Build([new Plain("a"), new Declared("b", [dependency!])]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyUnknown, ex.Reason);
        Assert.Contains("'b'", ex.Message, StringComparison.Ordinal);
        if (!string.IsNullOrEmpty(dependency))
        {
            Assert.Contains($"'{dependency}'", ex.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Build_refuses_a_dependency_on_itself_as_ReconcilerDependencyNotEarlier()
    {
        var ex = Assert.Throws<ClusterConfigurationException>(() => ReconcilerGraph.Build([new Plain("a"), Reconciler("b", "b")]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyNotEarlier, ex.Reason);
        Assert.Contains("itself", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_refuses_a_dependency_on_a_type_declared_later_as_ReconcilerDependencyNotEarlier()
    {
        var ex = Assert.Throws<ClusterConfigurationException>(() => ReconcilerGraph.Build([Reconciler("a", "b"), new Plain("b")]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyNotEarlier, ex.Reason);
        Assert.Contains("'a'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'b'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("position 0", ex.Message, StringComparison.Ordinal);
        Assert.Contains("position 1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_refuses_a_two_type_cycle_as_ReconcilerDependencyNotEarlier()
    {
        var ex = Assert.Throws<ClusterConfigurationException>(() => ReconcilerGraph.Build([Reconciler("a", "b"), Reconciler("b", "a")]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyNotEarlier, ex.Reason);
    }

    [Fact]
    public void Build_accepts_a_reconciler_without_DependsOn_through_the_default_member()
    {
        var graph = ReconcilerGraph.Build([new Plain("a"), new Plain("b")]);

        Assert.Equal(["a", "b"], graph.Types);
        Assert.Empty(graph.DependenciesOf("a"));
        Assert.Empty(graph.DependenciesOf("b"));
    }

    [Fact]
    public void Build_compares_names_ordinally()
    {
        var ex = Assert.Throws<ClusterConfigurationException>(() => ReconcilerGraph.Build([new Plain("a"), Reconciler("b", "A")]));

        Assert.Equal(ClusterConfigurationReason.ReconcilerDependencyUnknown, ex.Reason);
        Assert.Equal(["a"], ReconcilerGraph.Build([new Plain("a"), new Plain("A"), Reconciler("b", "a")]).DependenciesOf("b"));
    }

    [Fact]
    public void Build_deduplicates_the_names_of_one_list()
    {
        var graph = ReconcilerGraph.Build([new Plain("a"), Reconciler("b", "a", "a")]);

        Assert.Equal(["a"], graph.DependenciesOf("b"));
    }

    [Fact]
    public void The_closure_of_a_type_lists_its_transitive_dependencies_and_then_it_in_declared_order()
    {
        var graph = ReconcilerGraph.Build(
            [new Plain("a"), Reconciler("b", "a"), new Plain("x"), Reconciler("c", "b", "a"), Reconciler("d", "c")]);

        Assert.Equal(["a", "b", "c", "d"], graph.ClosureOf("d"));
        Assert.Equal(["a", "b"], graph.ClosureOf("b"));
        Assert.Equal(["x"], graph.ClosureOf("x"));
    }
}
