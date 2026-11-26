using Reeve.Cluster;
using Reeve.Cluster.Ask;

namespace Unit.Ask;

public sealed class AskWaitEndedTests
{
    private static readonly byte[] Request = [9];

    private enum Ending
    {
        Returns,
        Throws,
        HonoursItsToken,
    }

    private static LambdaHandler HandlerEnding(Ending ending, TaskCompletionSource started, TaskCompletionSource release) => new(async (_, ct) =>
    {
        started.TrySetResult();
        switch (ending)
        {
            case Ending.HonoursItsToken:
                await Task.Delay(Timeout.Infinite, ct);
                break;
            default:
                await release.Task;
                break;
        }

        return ending == Ending.Throws ? throw new InvalidOperationException("boom") : (ReadOnlyMemory<byte>)new byte[] { 1 };
    });

    [Theory]
    [InlineData("Returns", true)]
    [InlineData("Throws", true)]
    [InlineData("HonoursItsToken", true)]
    [InlineData("Returns", false)]
    [InlineData("Throws", false)]
    [InlineData("HonoursItsToken", false)]
    public async Task A_handler_ending_after_the_wait_ended_is_Unanswered(string ending, bool keyed)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = HandlerEnding(Enum.Parse<Ending>(ending), started, release);
        using var rig = new AskRig(TimeProvider.System, keyed ? null : handler, keyed ? handler : null);
        rig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
        using var answerBy = new CancellationTokenSource();
        var delivery = keyed ? new AskDelivery(AskRig.Kind, AskRig.Key, 7, Request) : new AskDelivery(null, null, null, Request);

        var taking = await rig.Inbound.TakeAsync(delivery, answerBy.Token);
        Assert.True(taking.IsTaken);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30));

        await answerBy.CancelAsync();
        release.SetResult();
        var answer = await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(AskAnswerKind.Unanswered, answer.Kind);
    }

    [Fact]
    public async Task A_handler_ending_before_the_wait_ended_answers_as_it_did()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        release.SetResult();
        var handler = HandlerEnding(Ending.Returns, started, release);
        using var rig = new AskRig(TimeProvider.System, handler, handler);
        using var answerBy = new CancellationTokenSource();

        var taking = await rig.Inbound.TakeAsync(new AskDelivery(null, null, null, Request), answerBy.Token);
        var answer = await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(AskAnswerKind.Response, answer.Kind);
    }

    private sealed class EndedInbound(Task<AskAnswer> answer) : IAskInbound
    {
        public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy) => ValueTask.FromResult(AskTaking.Taken(answer));

        public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy) => throw new NotSupportedException();
    }

    [Fact]
    public async Task An_Unanswered_answer_ends_the_local_delivery_TakenUnanswered_whether_or_not_the_wait_has_ended()
    {
        var unanswered = Task.FromResult(new AskAnswer(AskAnswerKind.Unanswered, default, null));
        var delivery = new AskDelivery(AskRig.Kind, AskRig.Key, 7, Request);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        var open = await LocalAskDispatch.DeliverAsync(new EndedInbound(unanswered), delivery, TimeSpan.FromSeconds(30), TimeProvider.System, CancellationToken.None);
        var ended = await LocalAskDispatch.DeliverAsync(new EndedInbound(unanswered), delivery, TimeSpan.FromSeconds(30), TimeProvider.System, cancelled.Token);

        Assert.Equal(DeliveryKind.TakenUnanswered, open.Kind);
        Assert.Equal(DeliveryKind.TakenUnanswered, ended.Kind);
    }
}
