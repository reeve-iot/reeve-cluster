using Reeve.Cluster;
using Reeve.Cluster.Ask;

namespace Unit.Ask;

public sealed class AskOutcomesTests
{
    private static readonly byte[] Payload = [1, 2, 3];

    private static DeliveryOutcome Answered(AskAnswerKind kind, string? error = null) =>
        new(DeliveryKind.Answered, new AskAnswer(kind, Payload, error));

    [Fact]
    public void A_response_is_Answered_and_carries_the_bytes()
    {
        var step = AskOutcomes.Next(Answered(AskAnswerKind.Response), keyed: true, reResolved: false);

        Assert.False(step.ResolveAgain);
        Assert.Equal(AskOutcome.Answered, step.Result!.Outcome);
        Assert.Equal(Payload, step.Result.Response.ToArray());
        Assert.Null(step.Result.Error);
    }

    [Fact]
    public void A_handler_exception_is_an_answer_carrying_the_error()
    {
        var step = AskOutcomes.Next(Answered(AskAnswerKind.HandlerError, "boom"), keyed: false, reResolved: false);

        Assert.Equal(AskOutcome.Answered, step.Result!.Outcome);
        Assert.Equal("boom", step.Result.Error);
    }

    [Fact]
    public void An_answer_that_the_owner_was_lost_is_OwnerLost()
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.Answered, new AskAnswer(AskAnswerKind.OwnerLost, default, null)), keyed: true, reResolved: false);

        Assert.Equal(AskOutcome.OwnerLost, step.Result!.Outcome);
        Assert.False(step.ResolveAgain);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void A_request_nobody_took_is_NotTaken(bool keyed, bool reResolved)
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.NotTaken, null), keyed, reResolved);

        Assert.Equal(AskOutcome.NotTaken, step.Result!.Outcome);
        Assert.False(step.ResolveAgain);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void A_request_taken_and_not_answered_is_TakenUnanswered(bool keyed, bool reResolved)
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.TakenUnanswered, null), keyed, reResolved);

        Assert.Equal(AskOutcome.TakenUnanswered, step.Result!.Outcome);
        Assert.False(step.ResolveAgain);
    }

    [Fact]
    public void A_keyed_decline_resolves_again_once()
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.Declined, null), keyed: true, reResolved: false);

        Assert.True(step.ResolveAgain);
        Assert.Null(step.Result);
    }

    [Fact]
    public void A_second_keyed_decline_is_NotTaken()
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.Declined, null), keyed: true, reResolved: true);

        Assert.False(step.ResolveAgain);
        Assert.Equal(AskOutcome.NotTaken, step.Result!.Outcome);
    }

    [Fact]
    public void A_named_request_that_is_declined_is_NotTaken_with_no_second_resolution()
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.Declined, null), keyed: false, reResolved: false);

        Assert.False(step.ResolveAgain);
        Assert.Equal(AskOutcome.NotTaken, step.Result!.Outcome);
    }

    [Fact]
    public void Only_NotTaken_and_NoOwner_are_safe_to_retry()
    {
        Assert.True(AskResult.NotTaken.SafeToRetry);
        Assert.True(AskResult.NoOwner.SafeToRetry);
        Assert.False(AskResult.TakenUnanswered.SafeToRetry);
        Assert.False(AskResult.OwnerLost.SafeToRetry);
        Assert.False(AskResult.Answered(Payload).SafeToRetry);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void A_handler_that_ends_after_the_wait_ended_is_Unanswered_whatever_it_did(bool handlerReturned, bool tenureStopped)
    {
        Assert.Equal(AskAnswerKind.Unanswered, AskOutcomes.AnswerOf(handlerReturned, tenureStopped, waitEnded: true));
    }

    [Theory]
    [InlineData(true, false, "Response")]
    [InlineData(false, false, "HandlerError")]
    [InlineData(true, true, "OwnerLost")]
    [InlineData(false, true, "OwnerLost")]
    public void The_answer_is_unchanged_while_the_wait_has_not_ended(bool handlerReturned, bool tenureStopped, string expected)
    {
        Assert.Equal(Enum.Parse<AskAnswerKind>(expected), AskOutcomes.AnswerOf(handlerReturned, tenureStopped, waitEnded: false));
    }

    [Fact]
    public void An_Unanswered_answer_is_TakenUnanswered_to_the_caller()
    {
        var step = AskOutcomes.Next(new DeliveryOutcome(DeliveryKind.Answered, new AskAnswer(AskAnswerKind.Unanswered, default, null)), keyed: true, reResolved: false);

        Assert.False(step.ResolveAgain);
        Assert.Equal(AskOutcome.TakenUnanswered, step.Result!.Outcome);
    }

    [Theory]
    [InlineData(5, 30, 5)]
    [InlineData(60, 30, 30)]
    [InlineData(30, 30, 30)]
    public void The_wait_for_an_owner_is_the_timeout_at_most_the_failover_bound(int timeoutSeconds, int boundSeconds, int expectedSeconds)
    {
        Assert.Equal(
            TimeSpan.FromSeconds(expectedSeconds),
            AskOutcomes.OwnerWait(TimeSpan.FromSeconds(timeoutSeconds), TimeSpan.FromSeconds(boundSeconds)));
    }

    [Fact]
    public void No_member_row_resolves_to_no_target()
    {
        Assert.Null(AskOutcomes.MemberTarget("b", null));
    }

    [Fact]
    public void A_draining_row_resolves_to_no_target()
    {
        Assert.Null(AskOutcomes.MemberTarget("b", new AskMemberRow(Guid.NewGuid(), MemberRowState.Draining)));
    }

    [Theory]
    [InlineData(MemberRowState.Joining)]
    [InlineData(MemberRowState.Active)]
    [InlineData(MemberRowState.Fenced)]
    public void A_row_in_any_other_state_resolves_to_its_process(MemberRowState state)
    {
        var process = Guid.NewGuid();

        Assert.Equal(new AskTarget("b", process), AskOutcomes.MemberTarget("b", new AskMemberRow(process, state)));
    }

    [Theory]
    [InlineData("NotServing", "OtherOwner")]
    [InlineData("Stopping", "OtherOwner")]
    [InlineData("Absent", "OtherOwnerOrGrace")]
    [InlineData("NoHandler", "None")]
    [InlineData("WaitEnded", "None")]
    public void What_the_caller_waits_for_follows_the_reason_of_the_decline(string reason, string expected)
    {
        Assert.Equal(Enum.Parse<ReplacementWait>(expected), AskOutcomes.AfterDecline(Enum.Parse<DeclineReason>(reason)));
    }

    [Fact]
    public void A_decline_with_no_reason_is_read_as_a_tenure_that_is_absent()
    {
        Assert.Equal(ReplacementWait.OtherOwnerOrGrace, AskOutcomes.AfterDecline(null));
    }

    [Fact]
    public void An_owner_is_another_when_its_process_or_its_token_differs()
    {
        var declined = new AskOwner("b", Guid.NewGuid(), 7);

        Assert.False(AskOutcomes.IsAnother(declined with { }, declined));
        Assert.True(AskOutcomes.IsAnother(declined with { Token = 8 }, declined));
        Assert.True(AskOutcomes.IsAnother(declined with { ProcessId = Guid.NewGuid() }, declined));
    }
}
