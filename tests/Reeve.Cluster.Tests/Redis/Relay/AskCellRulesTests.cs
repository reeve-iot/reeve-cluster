using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;

namespace Unit.Redis.Relay;

public sealed class AskCellRulesTests
{
    private static readonly TimeSpan Margin = TimeSpan.FromSeconds(5);
    private static readonly AskAnswer Response = new(AskAnswerKind.Response, new byte[] { 1 }, null);

    private static AskCell? CellOf(int index) => index switch
    {
        0 => null,
        1 => AskCell.Taken,
        2 => AskCell.Withdrawn,
        3 => AskCell.Declined(DeclineReason.NotServing),
        _ => AskCell.Answered(Response),
    };

    [Fact]
    public void A_pending_request_is_taken_while_the_caller_can_still_withdraw_it()
    {
        Assert.Equal(TakeVerdict.Taken, AskCellRules.Take(AskCell.Pending, TimeSpan.FromSeconds(6), Margin));
        Assert.Equal(TakeVerdict.Taken, AskCellRules.Take(AskCell.Pending, null, Margin));
    }

    [Theory]
    [InlineData(5000)]
    [InlineData(1)]
    [InlineData(0)]
    public void A_pending_request_whose_record_is_within_the_margin_of_its_end_is_not_taken(int leftMs) =>
        Assert.Equal(TakeVerdict.TooLate, AskCellRules.Take(AskCell.Pending, TimeSpan.FromMilliseconds(leftMs), Margin));

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Only_a_pending_request_is_taken(int cell) =>
        Assert.Equal(TakeVerdict.NotPending, AskCellRules.Take(CellOf(cell), TimeSpan.FromMinutes(1), Margin));

    [Fact]
    public void Only_a_pending_request_is_declined_and_only_a_taken_one_is_answered()
    {
        Assert.True(AskCellRules.CanDecline(AskCell.Pending));
        Assert.True(AskCellRules.CanAnswer(AskCell.Taken));
        foreach (var cell in new AskCell?[] { null, AskCell.Taken, AskCell.Withdrawn, AskCell.Declined(DeclineReason.Absent), AskCell.Answered(Response) })
        {
            Assert.False(AskCellRules.CanDecline(cell));
        }

        foreach (var cell in new AskCell?[] { null, AskCell.Pending, AskCell.Withdrawn, AskCell.Declined(DeclineReason.Absent), AskCell.Answered(Response) })
        {
            Assert.False(AskCellRules.CanAnswer(cell));
        }
    }

    [Fact]
    public void Withdrawing_a_pending_request_wins_and_a_withdrawn_one_stays_withdrawn()
    {
        Assert.Equal(WithdrawKind.Withdrew, AskCellRules.Withdraw(AskCell.Pending).Kind);
        Assert.Equal(WithdrawKind.Withdrew, AskCellRules.Withdraw(AskCell.Withdrawn).Kind);
    }

    [Fact]
    public void Withdrawing_a_request_the_target_took_reports_the_receipt_not_a_withdrawal()
    {
        Assert.Equal(WithdrawKind.AlreadyTaken, AskCellRules.Withdraw(AskCell.Taken).Kind);
        var answered = AskCellRules.Withdraw(AskCell.Answered(Response));
        Assert.Equal(WithdrawKind.Answered, answered.Kind);
        Assert.Same(Response, answered.Answer);
    }

    [Fact]
    public void Withdrawing_a_declined_request_reports_why_and_a_missing_record_is_missing()
    {
        var declined = AskCellRules.Withdraw(AskCell.Declined(DeclineReason.Stopping));
        Assert.Equal(WithdrawKind.Declined, declined.Kind);
        Assert.Equal(DeclineReason.Stopping, declined.Reason);
        Assert.Equal(WithdrawKind.Missing, AskCellRules.Withdraw(null).Kind);
    }

    [Fact]
    public void A_receipt_before_the_withdrawal_leaves_the_withdrawal_with_a_taken_request()
    {
        var cell = (AskCell?)AskCell.Pending;
        Assert.Equal(TakeVerdict.Taken, AskCellRules.Take(cell, TimeSpan.FromMinutes(1), Margin));
        cell = AskCell.Taken;

        Assert.Equal(WithdrawKind.AlreadyTaken, AskCellRules.Withdraw(cell).Kind);
    }

    [Fact]
    public void A_withdrawal_before_the_receipt_leaves_the_target_with_nothing_to_take()
    {
        var cell = (AskCell?)AskCell.Pending;
        Assert.Equal(WithdrawKind.Withdrew, AskCellRules.Withdraw(cell).Kind);
        cell = AskCell.Withdrawn;

        Assert.Equal(TakeVerdict.NotPending, AskCellRules.Take(cell, TimeSpan.FromMinutes(1), Margin));
    }
}
