using Reeve.Cluster.Core;

namespace Unit.Membership;

public sealed class FloorRulesTests
{
    [Fact]
    public void I14_a_raise_with_every_leased_member_at_or_above_writes_the_floor()
    {
        Assert.Equal(VersionRaiseDecision.Raise, FloorRules.DecideVersionRaise("1.10.0", "1.9.0", ["1.10.0", "2.0.0", "1.10.1"]));
    }

    [Fact]
    public void I14_a_raise_with_one_member_below_is_refused_by_SemVer_where_text_order_would_pass_it()
    {
        Assert.Equal(VersionRaiseDecision.RefusedByMember, FloorRules.DecideVersionRaise("1.10.0", null, ["1.10.0", "1.9.0"]));
    }

    [Theory]
    [InlineData("1.0.0", "1.0.0-rc.1")]
    [InlineData("1.0.0", "0.9.99")]
    public void I14_a_pre_release_is_below_its_release(string required, string member)
    {
        Assert.Equal(VersionRaiseDecision.RefusedByMember, FloorRules.DecideVersionRaise(required, null, [member]));
    }

    [Theory]
    [InlineData("")]
    [InlineData("latest")]
    [InlineData("1.0")]
    [InlineData("01.0.0")]
    public void I14_a_member_whose_version_is_not_SemVer_is_not_at_or_above(string member)
    {
        Assert.Equal(VersionRaiseDecision.RefusedByMember, FloorRules.DecideVersionRaise("1.0.0", null, ["1.0.0", member]));
    }

    [Theory]
    [InlineData("1.9.0", "1.9.0")]
    [InlineData("1.9.0", "1.10.0")]
    [InlineData("1.0.0-rc.1", "1.0.0")]
    public void I14_a_floor_already_at_or_above_answers_without_looking_at_the_members(string required, string floor)
    {
        Assert.Equal(VersionRaiseDecision.AlreadyAtOrAbove, FloorRules.DecideVersionRaise(required, floor, ["0.1.0"]));
    }

    [Theory]
    [InlineData("")]
    [InlineData("v1")]
    [InlineData("1.x")]
    public void I14_a_stored_floor_that_is_not_SemVer_refuses_the_raise(string floor)
    {
        Assert.Equal(VersionRaiseDecision.FloorNotSemVer, FloorRules.DecideVersionRaise("1.0.0", floor, ["2.0.0"]));
    }

    [Fact]
    public void A_raise_with_no_leased_member_writes_the_floor()
    {
        Assert.Equal(VersionRaiseDecision.Raise, FloorRules.DecideVersionRaise("1.0.0", null, []));
    }

    [Fact]
    public void The_observation_is_true_for_no_member_and_false_for_one_below_or_not_SemVer()
    {
        Assert.True(FloorRules.AllAtLeast("1.2.0", []));
        Assert.True(FloorRules.AllAtLeast("1.2.0", ["1.2.0", "1.10.0"]));
        Assert.False(FloorRules.AllAtLeast("1.10.0", ["1.10.0", "1.9.0"]));
        Assert.False(FloorRules.AllAtLeast("1.2.0", ["1.2.0", "nightly"]));
    }

    [Theory]
    [InlineData("1.9.0", "1.10.0", true)]
    [InlineData("1.10.0", "1.10.0", false)]
    [InlineData("1.10.0", "1.9.0", false)]
    [InlineData("1.0.0-rc.1", "1.0.0", true)]
    [InlineData(null, "1.0.0", true)]
    [InlineData(null, null, false)]
    [InlineData("1.0.0", null, false)]
    [InlineData("1.0.0", "garbage", true)]
    public void The_lowering_writes_only_a_lower_value_than_a_floor_that_is_set(string? value, string? floor, bool lowers)
    {
        Assert.Equal(lowers ? VersionLowerDecision.Lower : VersionLowerDecision.NotAbove, FloorRules.DecideVersionLower(value, floor));
    }
}
