using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class FlightDecisionsTests
{
    private static readonly Guid Token = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly byte[] Result = [1, 2, 3];

    [Fact]
    public void A_conflict_whose_row_is_gone_inserts_again()
    {
        Assert.Equal(FlightAction.InsertAgain, FlightDecisions.AfterConflict(null));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_live_result_is_used_whatever_the_claims_validity(bool valid)
    {
        Assert.Equal(FlightAction.UseResult, FlightDecisions.AfterConflict(new FlightRead("r2", 7, Token, valid, Result)));
    }

    [Fact]
    public void A_valid_claim_without_a_live_result_is_waited_for()
    {
        Assert.Equal(FlightAction.Wait, FlightDecisions.AfterConflict(new FlightRead("r2", 7, Token, Valid: true, Result: null)));
    }

    [Fact]
    public void A_claim_that_is_not_valid_and_holds_no_live_result_is_taken_over()
    {
        Assert.Equal(FlightAction.Takeover, FlightDecisions.AfterConflict(new FlightRead("r2", 7, Token, Valid: false, Result: null)));
    }

    [Fact]
    public void I33_a_grant_whose_commit_the_library_cancelled_before_it_returned_may_not_compute()
    {
        Assert.False(FlightDecisions.MayCompute(cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void A_grant_that_returned_uncancelled_may_compute()
    {
        Assert.True(FlightDecisions.MayCompute(cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_result_write_that_updated_its_row_uncancelled_is_stored()
    {
        Assert.Equal(ResultAction.Stored, FlightDecisions.AfterResultWrite(updated: true, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_result_write_that_matched_no_row_is_refused()
    {
        Assert.Equal(ResultAction.Refused, FlightDecisions.AfterResultWrite(updated: false, cancellationRequestedBeforeReturn: false));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void I33_a_result_write_the_library_cancelled_before_it_returned_is_unknown_whatever_it_returned(bool updated)
    {
        Assert.Equal(ResultAction.Unknown, FlightDecisions.AfterResultWrite(updated, cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void The_flight_key_space_has_its_own_prefix_apart_from_the_once_runs()
    {
        Assert.Equal("reeve.flight:k", FlightKeys.Store("k"));
        Assert.True(FlightKeys.IsReserved(FlightKeys.Store("k")));
        Assert.False(FlightKeys.IsReserved("k"));
        Assert.False(FlightKeys.IsReserved(OnceKeys.Store("k")));
        Assert.False(OnceKeys.IsReserved(FlightKeys.Store("k")));
    }
}
