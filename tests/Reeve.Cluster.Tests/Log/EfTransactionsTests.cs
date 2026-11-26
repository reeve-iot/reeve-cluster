using Reeve.Cluster;
using Reeve.Cluster.Log;

namespace Unit.Log;

public sealed class EfTransactionsTests
{
    private readonly EfTransactions registry = new();

    [Fact]
    public void A_transaction_the_library_did_not_see_begin_is_not_listed()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        using var raw = new FakeTransaction();

        registry.Holds(raw, scope);

        Assert.False(registry.IsOpen(raw));
        Assert.Empty(scope.OpenTransactions);
    }

    [Fact]
    public void A_scope_lists_an_open_EF_transaction_it_holds_until_it_ends()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        using var transaction = new FakeTransaction();
        registry.Began(transaction);

        registry.Holds(transaction, scope);
        registry.Holds(transaction, scope);

        Assert.True(registry.IsOpen(transaction));
        Assert.Same(transaction, Assert.Single(scope.OpenTransactions));

        registry.Ended(transaction);

        Assert.False(registry.IsOpen(transaction));
        Assert.Empty(scope.OpenTransactions);
    }

    [Fact]
    public void Every_scope_that_holds_the_transaction_lets_it_go_at_its_end()
    {
        var first = new ClusterScope(ClusterScopeKind.ClientRequest);
        var second = new ClusterScope(ClusterScopeKind.UnitOfWork);
        using var transaction = new FakeTransaction();
        registry.Began(transaction);

        registry.Holds(transaction, first);
        registry.Holds(transaction, second);
        registry.Ended(transaction);

        Assert.Empty(first.OpenTransactions);
        Assert.Empty(second.OpenTransactions);
    }

    [Fact]
    public void A_begin_on_a_reused_transaction_object_ends_the_earlier_transaction_on_it()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        using var transaction = new FakeTransaction();
        registry.Began(transaction);
        registry.Holds(transaction, scope);

        registry.Began(transaction);

        Assert.True(registry.IsOpen(transaction));
        Assert.Empty(scope.OpenTransactions);
    }

    [Fact]
    public void Holding_outside_every_scope_lists_nothing()
    {
        using var transaction = new FakeTransaction();
        registry.Began(transaction);

        registry.Holds(transaction, null);
        registry.Ended(transaction);

        Assert.False(registry.IsOpen(transaction));
    }

    [Fact]
    public void An_end_of_a_transaction_that_is_not_listed_does_nothing()
    {
        using var transaction = new FakeTransaction();

        registry.Ended(transaction);

        Assert.False(registry.IsOpen(transaction));
    }
}
