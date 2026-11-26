namespace Reeve.Cluster.Ask;

internal enum ReplacementWait
{
    OtherOwner,

    OtherOwnerOrGrace,

    None,
}

internal static class AskOutcomes
{
    public static AskStep Next(DeliveryOutcome attempt, bool keyed, bool reResolved) => attempt.Kind switch
    {
        DeliveryKind.Answered => new AskStep(false, Answered(attempt.Answer)),
        DeliveryKind.TakenUnanswered => new AskStep(false, AskResult.TakenUnanswered),
        DeliveryKind.NotTaken => new AskStep(false, AskResult.NotTaken),
        DeliveryKind.Declined when keyed && !reResolved => new AskStep(true, null),
        DeliveryKind.Declined => new AskStep(false, AskResult.NotTaken),
        _ => throw new ArgumentOutOfRangeException(nameof(attempt), attempt.Kind, "Not a delivery outcome."),
    };

    public static AskAnswerKind AnswerOf(bool handlerReturned, bool tenureStopped, bool waitEnded) =>
        waitEnded ? AskAnswerKind.Unanswered
        : tenureStopped ? AskAnswerKind.OwnerLost
        : handlerReturned ? AskAnswerKind.Response
        : AskAnswerKind.HandlerError;

    public static ReplacementWait AfterDecline(DeclineReason? reason) => reason switch
    {
        DeclineReason.NotServing or DeclineReason.Stopping => ReplacementWait.OtherOwner,
        DeclineReason.NoHandler or DeclineReason.WaitEnded => ReplacementWait.None,
        _ => ReplacementWait.OtherOwnerOrGrace,
    };

    public static bool IsAnother(AskOwner resolved, AskOwner declined) => resolved.ProcessId != declined.ProcessId || resolved.Token != declined.Token;

    public static TimeSpan OwnerWait(TimeSpan timeout, TimeSpan failoverBound) => timeout < failoverBound ? timeout : failoverBound;

    public static AskTarget? MemberTarget(string replicaId, AskMemberRow? row) =>
        row is null || row.State == MemberRowState.Draining ? null : new AskTarget(replicaId, row.ProcessId);

    private static AskResult Answered(AskAnswer? answer) => answer switch
    {
        { Kind: AskAnswerKind.Response } => AskResult.Answered(answer.Response),
        { Kind: AskAnswerKind.HandlerError } => AskResult.AnsweredWithError(answer.Error ?? string.Empty),
        { Kind: AskAnswerKind.OwnerLost } => AskResult.OwnerLost,
        { Kind: AskAnswerKind.Unanswered } => AskResult.TakenUnanswered,
        _ => throw new ArgumentException("An answered delivery carries its answer.", nameof(answer)),
    };
}
