using Reeve.Cluster.Ask;

namespace Reeve.Cluster.Redis.Relay;

internal enum SubmitEnd
{
    Submitted,

    NeverSent,

    Unknown,
}

internal enum CallerAction
{
    Wait,

    Withdraw,

    End,
}

internal readonly record struct CallerStep(CallerAction Action, DeliveryOutcome? Outcome);

internal enum TakeEnd
{
    Written,

    NotPending,

    TooLate,

    Fault,
}

internal enum AnswerEnd
{
    Written,

    Refused,

    Fault,
}

internal enum AnswerStep
{
    Done,

    Retry,
}

internal static class AskRelayDecisions
{
    public static CallerStep AfterSubmit(SubmitEnd end) => end switch
    {
        SubmitEnd.Submitted => new CallerStep(CallerAction.Wait, null),
        SubmitEnd.NeverSent => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.NotTaken, null)),
        _ => new CallerStep(CallerAction.Withdraw, null),
    };

    public static CallerStep OnObserved(AskCell? cell) => cell switch
    {
        { Kind: AskCellKind.Answered } => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.Answered, cell.Answer)),
        { Kind: AskCellKind.Declined } => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.Declined, null, cell.Reason)),
        _ => new CallerStep(CallerAction.Wait, null),
    };

    public static DeliveryOutcome AfterWithdrawal(Withdrawal? withdrawal, bool connectionChanged) =>
        (withdrawal, connectionChanged) switch
    {
        (null, _) => new DeliveryOutcome(DeliveryKind.TakenUnanswered, null),
        ({ Kind: WithdrawKind.Withdrew }, true) => new DeliveryOutcome(DeliveryKind.TakenUnanswered, null),
        ({ Kind: WithdrawKind.Withdrew }, false) => new DeliveryOutcome(DeliveryKind.NotTaken, null),
        ({ Kind: WithdrawKind.Answered } w, _) => new DeliveryOutcome(DeliveryKind.Answered, w.Answer),
        ({ Kind: WithdrawKind.Declined } w, _) => new DeliveryOutcome(DeliveryKind.Declined, null, w.Reason),
        _ => new DeliveryOutcome(DeliveryKind.TakenUnanswered, null),
    };

    public static CallerStep AtTakeBound(Withdrawal? withdrawal, bool connectionChanged) =>
        (withdrawal, connectionChanged) switch
    {
        ({ Kind: WithdrawKind.Withdrew }, true) => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.TakenUnanswered, null)),
        ({ Kind: WithdrawKind.Withdrew }, false) => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.NotTaken, null)),
        ({ Kind: WithdrawKind.Answered } w, _) => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.Answered, w.Answer)),
        ({ Kind: WithdrawKind.Declined } w, _) => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.Declined, null, w.Reason)),
        ({ Kind: WithdrawKind.Missing }, _) => new CallerStep(CallerAction.End, new DeliveryOutcome(DeliveryKind.TakenUnanswered, null)),
        _ => new CallerStep(CallerAction.Wait, null),
    };

    public static TimeSpan AnswerWindow(TimeSpan timeout, TimeSpan withdrawBudget)
    {
        var quarter = timeout / 4;
        return timeout - (withdrawBudget < quarter ? withdrawBudget : quarter);
    }

    public static TimeSpan RecordTtl(TimeSpan timeout, TimeSpan margin) => timeout + margin;

    public static bool StartsHandler(TakeEnd end) => end == TakeEnd.Written;

    public static AnswerStep AfterAnswerWrite(AnswerEnd end, bool waitEnded) =>
        end == AnswerEnd.Fault && !waitEnded ? AnswerStep.Retry : AnswerStep.Done;
}
