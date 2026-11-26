using Reeve.Cluster;
using Reeve.Cluster.Owned;

namespace Unit.Owned;

public sealed class OwnedRowConditionTests
{
    private const long Token = 42;

    private static readonly OwnedRow Held = new("a", 7, Token);

    private static OwnerMember Member(long epoch = 7, MemberRowState state = MemberRowState.Active, bool leaseValid = true) =>
        new(epoch, state, leaseValid);

    [Fact]
    public void No_row_is_absent() =>
        Assert.Equal(OwnedCondition.Absent, OwnedRowCondition.Of(null, null, null));

    [Fact]
    public void A_row_without_an_owner_is_free_whatever_the_request_says() =>
        Assert.Equal(OwnedCondition.Free, OwnedRowCondition.Of(new OwnedRow(null, null, Token), null, Token));

    [Fact]
    public void A_valid_hold_is_owned() =>
        Assert.Equal(OwnedCondition.Owned, OwnedRowCondition.Of(Held, Member(), null));

    [Fact]
    public void A_valid_hold_with_a_request_for_its_token_is_release_requested() =>
        Assert.Equal(OwnedCondition.ReleaseRequested, OwnedRowCondition.Of(Held, Member(), Token));

    [Theory]
    [InlineData(Token - 1)]
    [InlineData(Token + 1)]
    public void A_request_for_another_token_is_void(long requestToken) =>
        Assert.Equal(OwnedCondition.Owned, OwnedRowCondition.Of(Held, Member(), requestToken));

    [Fact]
    public void An_owner_without_a_member_row_is_orphaned() =>
        Assert.Equal(OwnedCondition.Orphaned, OwnedRowCondition.Of(Held, null, null));

    [Theory]
    [InlineData(6, MemberRowState.Active, true)]
    [InlineData(8, MemberRowState.Active, true)]
    [InlineData(7, MemberRowState.Active, false)]
    [InlineData(7, MemberRowState.Joining, true)]
    [InlineData(7, MemberRowState.Fenced, true)]
    [InlineData(7, MemberRowState.Draining, true)]
    public void A_hold_that_is_not_valid_is_orphaned_also_with_a_request(long epoch, MemberRowState state, bool leaseValid)
    {
        Assert.Equal(OwnedCondition.Orphaned, OwnedRowCondition.Of(Held, Member(epoch, state, leaseValid), null));
        Assert.Equal(OwnedCondition.Orphaned, OwnedRowCondition.Of(Held, Member(epoch, state, leaseValid), Token));
    }
}
