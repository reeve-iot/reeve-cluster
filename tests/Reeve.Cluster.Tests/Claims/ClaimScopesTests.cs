using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClaimScopesTests
{
    [Fact]
    public void A_key_is_open_from_the_scopes_opening_to_its_disposal()
    {
        var scopes = new ClaimScopes();
        Assert.False(scopes.IsOpen("k"));

        var scope = scopes.Open("k");
        Assert.True(scopes.IsOpen("k"));

        scope.Dispose();
        Assert.False(scopes.IsOpen("k"));
    }

    [Fact]
    public void A_key_stays_open_until_the_last_of_its_scopes_is_disposed()
    {
        var scopes = new ClaimScopes();
        var first = scopes.Open("k");
        var second = scopes.Open("k");

        first.Dispose();
        Assert.True(scopes.IsOpen("k"));

        second.Dispose();
        Assert.False(scopes.IsOpen("k"));
    }

    [Fact]
    public void Disposing_a_scope_twice_closes_it_once()
    {
        var scopes = new ClaimScopes();
        var first = scopes.Open("k");
        _ = scopes.Open("k");

        first.Dispose();
        first.Dispose();

        Assert.True(scopes.IsOpen("k"));
    }

    [Fact]
    public void A_scope_of_one_key_does_not_open_another()
    {
        var scopes = new ClaimScopes();
        using var scope = scopes.Open("a");

        Assert.False(scopes.IsOpen("b"));
    }
}
