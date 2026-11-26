using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class BudgetDecisionsTests
{
    [Fact]
    public void An_id_the_statement_recorded_and_counted_is_granted_with_the_period_count()
    {
        Assert.Equal(new BudgetResult.Granted(3), BudgetDecisions.Verdict(new BudgetStatement(Inserted: true, Used: 3), cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void An_id_counted_earlier_in_the_period_is_already_counted_and_not_counted_again()
    {
        Assert.Equal(new BudgetResult.AlreadyCounted(), BudgetDecisions.Verdict(new BudgetStatement(Inserted: false, Used: null), cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_new_id_the_limit_stopped_is_exhausted()
    {
        Assert.Equal(new BudgetResult.Exhausted(), BudgetDecisions.Verdict(new BudgetStatement(Inserted: true, Used: null), cancellationRequestedBeforeReturn: false));
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(true, null)]
    [InlineData(false, null)]
    public void I33_a_consume_the_library_cancelled_before_it_returned_is_unknown_whatever_the_statement_reported(bool inserted, int? used)
    {
        Assert.Equal(new BudgetResult.Unknown(), BudgetDecisions.Verdict(new BudgetStatement(inserted, used), cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void Granted_and_already_counted_admit_the_id_and_the_other_answers_do_not()
    {
        Assert.True(new BudgetResult.Granted(1).IsAllowed);
        Assert.True(new BudgetResult.AlreadyCounted().IsAllowed);
        Assert.False(new BudgetResult.Exhausted().IsAllowed);
        Assert.False(new BudgetResult.NotServing().IsAllowed);
        Assert.False(new BudgetResult.Unknown().IsAllowed);
    }
}
