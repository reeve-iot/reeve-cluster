using System.Diagnostics.CodeAnalysis;

namespace Reeve.Cluster.Ask;

internal readonly record struct AskTarget(string ReplicaId, Guid ProcessId);

internal sealed record AskDelivery(string? Kind, string? Key, long? Token, ReadOnlyMemory<byte> Payload)
{
    public bool IsMember => Kind is null;
}

internal enum AskAnswerKind
{
    Response,

    HandlerError,

    OwnerLost,

    Unanswered,
}

internal sealed record AskAnswer(AskAnswerKind Kind, ReadOnlyMemory<byte> Response, string? Error);

internal enum DeclineReason
{
    NotServing,

    Stopping,

    Absent,

    NoHandler,

    WaitEnded,
}

internal sealed class AskTaking
{
    private static readonly AskTaking NotServing = new(null, DeclineReason.NotServing);
    private static readonly AskTaking Stopping = new(null, DeclineReason.Stopping);
    private static readonly AskTaking Absent = new(null, DeclineReason.Absent);
    private static readonly AskTaking NoHandler = new(null, DeclineReason.NoHandler);
    private static readonly AskTaking WaitEnded = new(null, DeclineReason.WaitEnded);

    private readonly Task<AskAnswer>? answer;

    private AskTaking(Task<AskAnswer>? answer, DeclineReason? reason)
    {
        this.answer = answer;
        Reason = reason;
    }

    public static AskTaking Declined => Absent;

    public static AskTaking Decline(DeclineReason reason) => reason switch
    {
        DeclineReason.NotServing => NotServing,
        DeclineReason.Stopping => Stopping,
        DeclineReason.NoHandler => NoHandler,
        DeclineReason.WaitEnded => WaitEnded,
        _ => Absent,
    };

    public static AskTaking Taken(Task<AskAnswer> answer)
    {
        ArgumentNullException.ThrowIfNull(answer);
        return new(answer, null);
    }

    public DeclineReason? Reason { get; }

    public bool IsTaken => answer is not null;

    public Task<AskAnswer> Answer => answer ?? throw new InvalidOperationException("The request was declined; there is no answer.");
}

internal sealed class AskReservation
{
    private readonly Func<Task<AskAnswer>>? start;
    private int started;

    private AskReservation(Func<Task<AskAnswer>>? start, DeclineReason? reason)
    {
        this.start = start;
        Reason = reason;
    }

    public static AskReservation Decline(DeclineReason reason) => new(null, reason);

    public static AskReservation Reserve(Func<Task<AskAnswer>> start)
    {
        ArgumentNullException.ThrowIfNull(start);
        return new(start, null);
    }

    public DeclineReason? Reason { get; }

    public bool IsReserved => start is not null;

    public Task<AskAnswer> Start()
    {
        if (start is null)
        {
            throw new InvalidOperationException("The request was declined; there is no handler to start.");
        }

        if (Interlocked.Exchange(ref started, 1) != 0)
        {
            throw new InvalidOperationException("The request was started already.");
        }

        return start();
    }
}

internal enum DeliveryKind
{
    Declined,

    NotTaken,

    Answered,

    TakenUnanswered,
}

internal readonly record struct DeliveryOutcome(DeliveryKind Kind, AskAnswer? Answer, DeclineReason? Reason = null);

internal readonly record struct AskStep(bool ResolveAgain, AskResult? Result);

internal sealed record AskOwner(string ReplicaId, Guid ProcessId, long Token);

internal sealed record AskMemberRow(Guid ProcessId, MemberRowState State);

internal interface IAskInbound
{
    ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy);

    ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy);
}

internal interface IAskTransport
{
    Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, CancellationToken cancellationToken);

    Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken);
}

internal interface IAskDirectory
{
    Task<AskOwner?> ResolveOwnerAsync(string kind, string key, CancellationToken cancellationToken);

    Task<AskMemberRow?> ResolveMemberAsync(string replicaId, CancellationToken cancellationToken);
}

internal interface IRunningTenures
{
    bool TryGetRunning(string kind, string key, long token, out CancellationToken stopped, [NotNullWhen(true)] out OwnedContext? owner);

    bool IsStopping(string kind, string key, long token);
}

internal sealed class NoAskTransport : IAskTransport
{
    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, CancellationToken cancellationToken) =>
        Task.FromResult(new DeliveryOutcome(DeliveryKind.NotTaken, null));

    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken) =>
        DeliverAsync(target, delivery, timeout, cancellationToken);
}
