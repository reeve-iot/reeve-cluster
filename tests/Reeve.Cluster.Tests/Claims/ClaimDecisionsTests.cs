using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClaimDecisionsTests
{
    private static readonly Guid Token = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Fact]
    public void A_conflict_whose_row_is_gone_inserts_again()
    {
        Assert.Equal(ConflictAction.InsertAgain, ClaimDecisions.AfterConflict(null));
    }

    [Fact]
    public void A_conflict_with_a_valid_claim_is_lost_and_takes_nothing()
    {
        Assert.Equal(ConflictAction.Lost, ClaimDecisions.AfterConflict(new ClaimRead("r2", 7, Token, Valid: true)));
    }

    [Fact]
    public void A_conflict_with_a_claim_that_is_not_valid_takes_it_over()
    {
        Assert.Equal(ConflictAction.Takeover, ClaimDecisions.AfterConflict(new ClaimRead("r2", 7, Token, Valid: false)));
    }

    [Theory]
    [InlineData("RC003", "ClaimLost")]
    [InlineData("RC002", "OtherTimeline")]
    [InlineData("RC001", "None")]
    [InlineData("40001", "None")]
    [InlineData("25P04", "None")]
    [InlineData(null, "None")]
    public void The_database_refusals_map_to_the_claims_two_and_nothing_else(string? sqlState, string expected)
    {
        Assert.Equal(expected, ClaimDecisions.Refusal(sqlState).ToString());
    }

    [Fact]
    public void A_written_token_is_won_when_no_cancel_was_requested_before_the_call_returned()
    {
        Assert.Equal(new ClaimResult.Won(Token), ClaimDecisions.GrantAnswer(Token, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void I33_a_written_token_is_not_won_when_a_cancel_was_requested_before_the_call_returned()
    {
        var answer = ClaimDecisions.GrantAnswer(Token, cancellationRequestedBeforeReturn: true);

        Assert.IsType<ClaimResult.Unknown>(answer);
    }

    [Fact]
    public void Only_a_won_claim_is_won_and_only_it_carries_a_token()
    {
        Assert.True(new ClaimResult.Won(Token).IsWon);
        Assert.True(new ClaimResult.Won(Token).TryGetToken(out var token));
        Assert.Equal(Token, token);

        foreach (ClaimResult other in new ClaimResult[] { new ClaimResult.Lost("r2"), new ClaimResult.NotServing(), new ClaimResult.Unknown() })
        {
            Assert.False(other.IsWon);
            Assert.False(other.TryGetToken(out var none));
            Assert.Equal(Guid.Empty, none);
        }
    }
}
