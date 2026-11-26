using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;

namespace Unit.Redis.Relay;

public sealed class AskRelayDecisionsTests
{
    private static readonly AskAnswer Response = new(AskAnswerKind.Response, new byte[] { 7 }, null);

    [Fact]
    public void A_submit_that_never_reached_Redis_ends_NotTaken_at_once()
    {
        var step = AskRelayDecisions.AfterSubmit(SubmitEnd.NeverSent);

        Assert.Equal(CallerAction.End, step.Action);
        Assert.Equal(DeliveryKind.NotTaken, step.Outcome!.Value.Kind);
    }

    [Fact]
    public void A_submit_whose_fate_is_unknown_is_settled_by_the_withdrawal()
    {
        var step = AskRelayDecisions.AfterSubmit(SubmitEnd.Unknown);

        Assert.Equal(CallerAction.Withdraw, step.Action);
        Assert.Null(step.Outcome);
    }

    [Fact]
    public void A_submitted_request_is_waited_for()
    {
        var step = AskRelayDecisions.AfterSubmit(SubmitEnd.Submitted);

        Assert.Equal(CallerAction.Wait, step.Action);
        Assert.Null(step.Outcome);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void A_record_that_is_pending_or_taken_is_waited_on(int kind) =>
        Assert.Equal(CallerAction.Wait, AskRelayDecisions.OnObserved(new AskCell((AskCellKind)kind, null, null)).Action);

    [Fact]
    public void A_record_that_is_not_there_is_waited_on_until_the_withdrawal_decides()
    {
        Assert.Equal(CallerAction.Wait, AskRelayDecisions.OnObserved(null).Action);
    }

    [Fact]
    public void An_answer_ends_the_delivery_Answered()
    {
        var step = AskRelayDecisions.OnObserved(AskCell.Answered(Response));

        Assert.Equal(CallerAction.End, step.Action);
        Assert.Equal(DeliveryKind.Answered, step.Outcome!.Value.Kind);
        Assert.Same(Response, step.Outcome.Value.Answer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void A_decline_ends_the_delivery_Declined_with_its_reason(int code)
    {
        var reason = (DeclineReason)code;
        var outcome = AskRelayDecisions.OnObserved(AskCell.Declined(reason)).Outcome!.Value;

        Assert.Equal(DeliveryKind.Declined, outcome.Kind);
        Assert.Equal(reason, outcome.Reason);
    }

    [Fact]
    public void A_withdrawn_request_ends_NotTaken()
    {
        var outcome = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.Withdrew, null, null), false);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
    }

    [Fact]
    public void A_withdrawn_request_after_a_connection_event_ends_TakenUnanswered()
    {
        var outcome = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.Withdrew, null, null), true);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public void A_request_taken_before_the_withdrawal_ends_TakenUnanswered()
    {
        var outcome = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.AlreadyTaken, null, null), false);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public void An_answer_that_arrived_before_the_withdrawal_is_the_answer()
    {
        var outcome = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.Answered, Response, null), false);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Same(Response, outcome.Answer);
    }

    [Fact]
    public void A_decline_that_arrived_before_the_withdrawal_is_the_decline()
    {
        var outcome = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.Declined, null, DeclineReason.Stopping), false);

        Assert.Equal(DeliveryKind.Declined, outcome.Kind);
        Assert.Equal(DeclineReason.Stopping, outcome.Reason);
    }

    [Fact]
    public void A_withdrawal_that_Redis_did_not_answer_leaves_the_request_TakenUnanswered()
    {
        Assert.Equal(DeliveryKind.TakenUnanswered, AskRelayDecisions.AfterWithdrawal(null, false).Kind);
    }

    [Fact]
    public void A_record_that_is_gone_at_the_withdrawal_cannot_be_called_NotTaken()
    {
        var gone = new Withdrawal(WithdrawKind.Missing, null, null);

        Assert.Equal(DeliveryKind.TakenUnanswered, AskRelayDecisions.AfterWithdrawal(gone, false).Kind);
    }

    [Theory]
    [InlineData(WithdrawKind.AlreadyTaken)]
    [InlineData(WithdrawKind.Answered)]
    [InlineData(WithdrawKind.Declined)]
    [InlineData(WithdrawKind.Missing)]
    public void A_connection_event_does_not_change_other_withdrawal_results(object rawKind)
    {
        var kind = (WithdrawKind)rawKind;
        var withdrawal = new Withdrawal(kind, kind == WithdrawKind.Answered ? Response : null, kind == WithdrawKind.Declined ? DeclineReason.Stopping : null);

        Assert.Equal(AskRelayDecisions.AfterWithdrawal(withdrawal, false), AskRelayDecisions.AfterWithdrawal(withdrawal, true));
        Assert.Equal(AskRelayDecisions.AtTakeBound(withdrawal, false), AskRelayDecisions.AtTakeBound(withdrawal, true));
    }

    [Fact]
    public void A_connection_event_does_not_change_null_withdrawal_results()
    {
        Assert.Equal(AskRelayDecisions.AfterWithdrawal(null, false), AskRelayDecisions.AfterWithdrawal(null, true));
        Assert.Equal(AskRelayDecisions.AtTakeBound(null, false), AskRelayDecisions.AtTakeBound(null, true));
    }

    [Fact]
    public void An_answer_is_passed_on_as_it_is_and_the_outcome_mapping_reads_it()
    {
        var late = new AskAnswer(AskAnswerKind.Unanswered, default, null);

        var observed = AskRelayDecisions.OnObserved(AskCell.Answered(late)).Outcome!.Value;
        var withdrawn = AskRelayDecisions.AfterWithdrawal(new Withdrawal(WithdrawKind.Answered, late, null), false);

        Assert.Equal(DeliveryKind.Answered, observed.Kind);
        Assert.Equal(DeliveryKind.Answered, withdrawn.Kind);
        Assert.Equal(AskOutcome.TakenUnanswered, AskOutcomes.Next(observed, keyed: true, reResolved: false).Result!.Outcome);
    }

    [Fact]
    public void The_answer_window_leaves_the_withdrawal_inside_the_callers_wait()
    {
        var budget = TimeSpan.FromSeconds(1);

        Assert.Equal(TimeSpan.FromSeconds(29), AskRelayDecisions.AnswerWindow(TimeSpan.FromSeconds(30), budget));
        Assert.Equal(TimeSpan.FromMilliseconds(300), AskRelayDecisions.AnswerWindow(TimeSpan.FromMilliseconds(400), budget));
    }

    [Fact]
    public void The_record_outlives_the_wait_by_the_margin()
    {
        Assert.Equal(TimeSpan.FromSeconds(35), AskRelayDecisions.RecordTtl(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5)));
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(3, false)]
    public void A_handler_starts_only_when_the_receipt_is_written(int end, bool starts) =>
        Assert.Equal(starts, AskRelayDecisions.StartsHandler((TakeEnd)end));

    [Fact]
    public void The_take_ends_are_the_four_the_decision_covers() =>
        Assert.Equal([TakeEnd.Written, TakeEnd.NotPending, TakeEnd.TooLate, TakeEnd.Fault], Enum.GetValues<TakeEnd>());

    [Theory]
    [InlineData(0, false, false)]
    [InlineData(1, false, false)]
    [InlineData(2, false, true)]
    [InlineData(2, true, false)]
    [InlineData(0, true, false)]
    public void An_answer_that_Redis_did_not_take_is_written_again_while_the_caller_waits(int end, bool waitEnded, bool retries) =>
        Assert.Equal(retries ? AnswerStep.Retry : AnswerStep.Done, AskRelayDecisions.AfterAnswerWrite((AnswerEnd)end, waitEnded));

    [Fact]
    public void A_request_withdrawn_at_the_take_bound_ends_NotTaken()
    {
        var step = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.Withdrew, null, null), false);

        Assert.Equal(CallerAction.End, step.Action);
        Assert.Equal(DeliveryKind.NotTaken, step.Outcome!.Value.Kind);
    }

    [Fact]
    public void A_request_withdrawn_at_the_take_bound_after_a_connection_event_ends_TakenUnanswered()
    {
        var step = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.Withdrew, null, null), true);

        Assert.Equal((CallerAction.End, DeliveryKind.TakenUnanswered), (step.Action, step.Outcome!.Value.Kind));
    }

    [Fact]
    public void A_request_taken_by_the_take_bound_is_waited_on_for_its_answer()
    {
        var step = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.AlreadyTaken, null, null), false);

        Assert.Equal(CallerAction.Wait, step.Action);
        Assert.Null(step.Outcome);
    }

    [Fact]
    public void An_answer_or_a_decline_found_at_the_take_bound_ends_the_delivery_with_it()
    {
        var answered = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.Answered, Response, null), false);
        var declined = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.Declined, null, DeclineReason.NotServing), false);

        Assert.Equal((CallerAction.End, DeliveryKind.Answered), (answered.Action, answered.Outcome!.Value.Kind));
        Assert.Same(Response, answered.Outcome.Value.Answer);
        Assert.Equal((CallerAction.End, DeliveryKind.Declined), (declined.Action, declined.Outcome!.Value.Kind));
        Assert.Equal(DeclineReason.NotServing, declined.Outcome.Value.Reason);
    }

    [Fact]
    public void A_record_gone_at_the_take_bound_can_hold_no_answer_and_ends_TakenUnanswered()
    {
        var step = AskRelayDecisions.AtTakeBound(new Withdrawal(WithdrawKind.Missing, null, null), false);

        Assert.Equal((CallerAction.End, DeliveryKind.TakenUnanswered), (step.Action, step.Outcome!.Value.Kind));
    }

    [Fact]
    public void A_withdrawal_Redis_did_not_answer_at_the_take_bound_is_left_to_the_withdrawal_at_the_end()
    {
        Assert.Equal(CallerAction.Wait, AskRelayDecisions.AtTakeBound(null, false).Action);
    }
}
