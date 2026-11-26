using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class OnceDecisionsTests
{
    private static readonly Guid Token = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Fact]
    public void A_conflict_whose_row_is_gone_inserts_again()
    {
        Assert.Equal(OnceConflictAction.InsertAgain, OnceDecisions.AfterConflict(null));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_complete_run_stays_complete_whatever_its_claims_validity(bool valid)
    {
        Assert.Equal(OnceConflictAction.AlreadyCompleted, OnceDecisions.AfterConflict(new OnceRead("r2", 7, Token, valid, Completed: true)));
    }

    [Fact]
    public void A_run_whose_claim_is_valid_is_running_under_its_executor()
    {
        Assert.Equal(OnceConflictAction.Running, OnceDecisions.AfterConflict(new OnceRead("r2", 7, Token, Valid: true, Completed: false)));
    }

    [Fact]
    public void A_run_that_is_neither_valid_nor_complete_is_in_doubt_and_not_taken_over_by_the_caller()
    {
        Assert.Equal(OnceConflictAction.InDoubt, OnceDecisions.AfterConflict(new OnceRead("r2", 7, Token, Valid: false, Completed: false)));
    }

    [Fact]
    public void I33_a_grant_whose_commit_the_library_cancelled_before_it_returned_may_not_start()
    {
        Assert.False(OnceDecisions.MayStart(cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void A_grant_that_returned_uncancelled_may_start()
    {
        Assert.True(OnceDecisions.MayStart(cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_completion_that_updated_its_row_uncancelled_is_completed()
    {
        Assert.Equal(new OnceResult.Completed(), OnceDecisions.AfterCompletion(updated: true, cancellationRequestedBeforeReturn: false));
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void Every_other_completion_has_an_unknown_outcome(bool updated, bool cancelled)
    {
        Assert.Equal(new OnceResult.CompletionUnknown(), OnceDecisions.AfterCompletion(updated, cancelled));
    }

    [Fact]
    public void A_retake_that_wrote_a_token_uncancelled_runs_the_effect()
    {
        Assert.Equal(RetakeAction.Run, OnceDecisions.AfterRetake(Token, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void I33_a_retake_whose_commit_the_library_cancelled_is_not_run_although_it_wrote_a_token()
    {
        Assert.Equal(RetakeAction.NotRun, OnceDecisions.AfterRetake(Token, cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void A_retake_that_matched_no_row_is_skipped()
    {
        Assert.Equal(RetakeAction.Skip, OnceDecisions.AfterRetake(null, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_cancelled_retake_with_no_token_is_not_run()
    {
        Assert.Equal(RetakeAction.NotRun, OnceDecisions.AfterRetake(null, cancellationRequestedBeforeReturn: true));
    }
}
