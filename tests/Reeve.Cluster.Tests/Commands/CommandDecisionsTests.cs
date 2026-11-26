using Reeve.Cluster;
using Reeve.Cluster.Commands;

namespace Unit.Commands;

public sealed class CommandDecisionsTests
{
    private static readonly byte[] Payload = [1, 2, 3];

    [Fact]
    public void A_send_whose_row_is_gone_inserts_again()
    {
        Assert.Equal(SendAction.InsertAgain, SendDecisions.AfterConflict(null, Payload));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    [InlineData("expired")]
    public void A_different_payload_under_the_same_key_is_an_idempotency_conflict_in_every_state(string sql)
    {
        Assert.Equal(SendAction.Conflict, SendDecisions.AfterConflict(new SentCommand(7, CommandStates.Parse(sql), new byte[] { 9 }), Payload));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    [InlineData("expired")]
    public void A_payload_of_the_same_length_that_differs_in_one_byte_is_an_idempotency_conflict_in_every_state(string sql)
    {
        Assert.Equal(SendAction.Conflict, SendDecisions.AfterConflict(new SentCommand(7, CommandStates.Parse(sql), new byte[] { 1, 2, 4 }), Payload));
        Assert.Equal(SendAction.Conflict, SendDecisions.AfterConflict(new SentCommand(7, CommandStates.Parse(sql), new byte[] { 0, 2, 3 }), Payload));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    public void The_same_payload_returns_the_existing_command(string sql)
    {
        Assert.Equal(SendAction.Existing, SendDecisions.AfterConflict(new SentCommand(7, CommandStates.Parse(sql), Payload.ToArray()), Payload));
    }

    [Fact]
    public void The_same_payload_after_expiry_pends_the_command_again()
    {
        Assert.Equal(SendAction.Resend, SendDecisions.AfterConflict(new SentCommand(7, CommandState.Expired, Payload.ToArray()), Payload));
    }

    [Fact]
    public void A_resend_that_matched_no_row_reads_again()
    {
        Assert.Equal(SendAction.ReadAgain, SendDecisions.AfterResend(updated: false));
        Assert.Equal(SendAction.Resent, SendDecisions.AfterResend(updated: true));
    }

    [Fact]
    public void A_take_that_committed_uncancelled_runs_the_handler()
    {
        Assert.Equal(TakeAction.Run, TakeDecisions.AfterTake(updated: true, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_take_that_matched_no_row_runs_nothing()
    {
        Assert.Equal(TakeAction.NotTaken, TakeDecisions.AfterTake(updated: false, cancellationRequestedBeforeReturn: false));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_take_cancelled_before_it_returned_has_an_unknown_outcome_whatever_it_returned(bool updated)
    {
        Assert.Equal(TakeAction.Unknown, TakeDecisions.AfterTake(updated, cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void A_completion_that_updated_its_row_and_was_not_cancelled_is_completed()
    {
        Assert.Equal(CompleteAction.Completed, CompleteDecisions.AfterComplete(updated: true, cancellationRequestedBeforeReturn: false));
    }

    [Fact]
    public void A_completion_that_matched_no_row_is_refused()
    {
        Assert.Equal(CompleteAction.Refused, CompleteDecisions.AfterComplete(updated: false, cancellationRequestedBeforeReturn: false));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_completion_cancelled_before_it_returned_is_unknown_whatever_it_returned(bool updated)
    {
        Assert.Equal(CompleteAction.Unknown, CompleteDecisions.AfterComplete(updated, cancellationRequestedBeforeReturn: true));
    }

    private static ActiveCommand Pending(long id) => new(id, CommandState.Pending, null);

    private static ActiveCommand Taken(long id, long token) => new(id, CommandState.Taken, token);

    [Fact]
    public void A_key_with_no_active_command_is_idle()
    {
        Assert.Equal(new PickupStep(PickupAction.Idle, 0), PickupDecisions.Next([], ownToken: 9, flight: null));
    }

    [Fact]
    public void The_oldest_pending_command_is_taken_first()
    {
        Assert.Equal(new PickupStep(PickupAction.Take, 3), PickupDecisions.Next([Pending(3), Pending(4)], ownToken: 9, flight: null));
    }

    [Fact]
    public void One_command_at_a_time_a_taken_command_blocks_the_take_of_a_pending_one()
    {
        var step = PickupDecisions.Next([Taken(2, 9), Pending(3)], ownToken: 9, new KeyFlight(9, 2, FlightPhase.Running));

        Assert.Equal(PickupAction.Wait, step.Action);
    }

    [Fact]
    public void A_command_taken_under_the_own_token_that_this_tenure_never_started_is_run_without_a_new_take()
    {
        Assert.Equal(new PickupStep(PickupAction.Run, 2), PickupDecisions.Next([Taken(2, 9), Pending(3)], ownToken: 9, flight: null));
    }

    [Fact]
    public void A_flight_of_an_earlier_tenure_does_not_count_for_this_one()
    {
        Assert.Equal(new PickupStep(PickupAction.Run, 2), PickupDecisions.Next([Taken(2, 9)], ownToken: 9, new KeyFlight(7, 2, FlightPhase.Returned)));
    }

    [Fact]
    public void A_command_whose_handler_returned_and_whose_completion_is_unknown_is_completed_again()
    {
        Assert.Equal(new PickupStep(PickupAction.CompleteAgain, 2), PickupDecisions.Next([Taken(2, 9)], ownToken: 9, new KeyFlight(9, 2, FlightPhase.Returned)));
    }

    [Fact]
    public void A_command_whose_handler_failed_stays_taken_and_blocks_the_key_until_the_tenure_changes()
    {
        Assert.Equal(PickupAction.Wait, PickupDecisions.Next([Taken(2, 9), Pending(3)], ownToken: 9, new KeyFlight(9, 2, FlightPhase.Failed)).Action);
    }

    [Fact]
    public void A_command_taken_under_an_older_token_is_the_new_owners_to_classify_and_blocks_the_key()
    {
        Assert.Equal(new PickupStep(PickupAction.ClassifyOlder, 2), PickupDecisions.Next([Taken(2, 5), Pending(3)], ownToken: 9, flight: null));
    }

    [Fact]
    public void A_command_taken_under_a_newer_token_is_not_this_tenures_and_blocks_it()
    {
        Assert.Equal(PickupAction.Wait, PickupDecisions.Next([Taken(2, 12), Pending(3)], ownToken: 9, flight: null).Action);
    }

    [Fact]
    public void A_handler_that_returned_completes_the_command_with_its_result()
    {
        Assert.Equal(HandlerAction.Complete, HandlerDecisions.After(HandlerEnd.Returned, stopRequested: false));
    }

    [Theory]
    [InlineData("Threw")]
    [InlineData("Cancelled")]
    public void A_handler_that_threw_or_cancelled_itself_with_no_stop_requested_completes_the_command_with_an_error(string end)
    {
        Assert.Equal(HandlerAction.CompleteWithError, HandlerDecisions.After(Enum.Parse<HandlerEnd>(end), stopRequested: false));
    }

    [Theory]
    [InlineData("Returned")]
    [InlineData("Threw")]
    [InlineData("Cancelled")]
    [InlineData("Abandoned")]
    public void A_handler_that_ended_under_the_tenures_or_the_hosts_stop_leaves_the_command_taken_for_the_next_owner(string name)
    {
        var end = Enum.Parse<HandlerEnd>(name);
        Assert.Equal(HandlerAction.LeaveTaken, HandlerDecisions.After(end, stopRequested: true));
    }

    [Fact]
    public void A_handler_still_running_when_the_tenure_stops_leaves_the_command_taken()
    {
        Assert.Equal(HandlerAction.LeaveTaken, HandlerDecisions.After(HandlerEnd.Abandoned, stopRequested: false));
    }

    [Fact]
    public void A_command_whose_handler_threw_is_completed_again_from_the_remembered_error_and_not_run_again()
    {
        Assert.Equal(new PickupStep(PickupAction.CompleteAgain, 2), PickupDecisions.Next([Taken(2, 9)], ownToken: 9, new KeyFlight(9, 2, FlightPhase.Faulted)));
    }

    [Fact]
    public void A_command_taken_by_an_older_token_is_taken_again_under_ReRun_and_recorded_in_doubt_under_Complete()
    {
        Assert.Equal(ClassifyAction.Retake, ClassifyDecisions.Classify(InDoubtPolicy.ReRun, takenToken: 5, ownToken: 9));
        Assert.Equal(ClassifyAction.MarkInDoubt, ClassifyDecisions.Classify(InDoubtPolicy.Complete, takenToken: 5, ownToken: 9));
    }

    [Theory]
    [InlineData(InDoubtPolicy.ReRun)]
    [InlineData(InDoubtPolicy.Complete)]
    public void A_command_taken_by_the_own_or_a_newer_token_is_not_classified(InDoubtPolicy policy)
    {
        Assert.Equal(ClassifyAction.None, ClassifyDecisions.Classify(policy, takenToken: 9, ownToken: 9));
        Assert.Equal(ClassifyAction.None, ClassifyDecisions.Classify(policy, takenToken: 12, ownToken: 9));
    }

    [Fact]
    public void Tokens_a_command_retained_with_the_old_tenures_token_is_classified_as_the_old_tenures_even_when_the_key_was_created_again()
    {
        const long oldTenure = 4;
        const long recreated = 5;
        Assert.Equal(new PickupStep(PickupAction.ClassifyOlder, 2), PickupDecisions.Next([Taken(2, oldTenure), Pending(3)], recreated, flight: null));
        Assert.Equal(ClassifyAction.MarkInDoubt, ClassifyDecisions.Classify(InDoubtPolicy.Complete, oldTenure, recreated));
        Assert.Null(CommandStateMachine.Decide(CommandWriter.Complete, new CommandRow(CommandState.Taken, oldTenure), recreated));
    }

    [Fact]
    public void A_retake_decides_as_a_take_does_and_runs_nothing_on_a_cancelled_commit()
    {
        Assert.Equal(TakeAction.Run, TakeDecisions.AfterTake(updated: true, cancellationRequestedBeforeReturn: false));
        Assert.Equal(TakeAction.NotTaken, TakeDecisions.AfterTake(updated: false, cancellationRequestedBeforeReturn: false));
        Assert.Equal(TakeAction.Unknown, TakeDecisions.AfterTake(updated: true, cancellationRequestedBeforeReturn: true));
    }

    [Fact]
    public void An_in_doubt_verdict_follows_the_outcome_rule()
    {
        Assert.Equal(MarkAction.Marked, MarkDecisions.AfterMark(updated: true, cancellationRequestedBeforeReturn: false));
        Assert.Equal(MarkAction.Refused, MarkDecisions.AfterMark(updated: false, cancellationRequestedBeforeReturn: false));
        Assert.Equal(MarkAction.Unknown, MarkDecisions.AfterMark(updated: true, cancellationRequestedBeforeReturn: true));
        Assert.Equal(MarkAction.Unknown, MarkDecisions.AfterMark(updated: false, cancellationRequestedBeforeReturn: true));
    }
}
