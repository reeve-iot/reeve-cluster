namespace Reeve.Cluster.Commands;

internal sealed record SentCommand(long Id, CommandState State, byte[] Payload);

internal enum SendAction
{
    InsertAgain,

    Conflict,

    Resend,

    Existing,

    ReadAgain,

    Resent,
}

internal static class SendDecisions
{
    public static SendAction AfterConflict(SentCommand? row, ReadOnlySpan<byte> payload)
    {
        if (row is null)
        {
            return SendAction.InsertAgain;
        }

        if (!payload.SequenceEqual(row.Payload))
        {
            return SendAction.Conflict;
        }

        return row.State == CommandState.Expired ? SendAction.Resend : SendAction.Existing;
    }

    public static SendAction AfterResend(bool updated) => updated ? SendAction.Resent : SendAction.ReadAgain;
}

internal enum TakeAction
{
    Run,

    NotTaken,

    Unknown,
}

internal static class TakeDecisions
{
    public static TakeAction AfterTake(bool updated, bool cancellationRequestedBeforeReturn) => (updated, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => TakeAction.Unknown,
        (false, false) => TakeAction.NotTaken,
        _ => TakeAction.Run,
    };
}

internal enum CompleteAction
{
    Completed,

    Refused,

    Unknown,
}

internal static class CompleteDecisions
{
    public static CompleteAction AfterComplete(bool updated, bool cancellationRequestedBeforeReturn) => (updated, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => CompleteAction.Unknown,
        (false, false) => CompleteAction.Refused,
        _ => CompleteAction.Completed,
    };
}

internal readonly record struct ActiveCommand(long Id, CommandState State, long? TakenToken);

internal enum FlightPhase
{
    Running,

    Failed,

    Returned,

    Faulted,
}

internal sealed record KeyFlight(long Token, long Id, FlightPhase Phase);

internal enum PickupAction
{
    Idle,

    Wait,

    Take,

    Run,

    CompleteAgain,

    ClassifyOlder,
}

internal readonly record struct PickupStep(PickupAction Action, long Id);

internal static class PickupDecisions
{
    public static PickupStep Next(IReadOnlyList<ActiveCommand> rows, long ownToken, KeyFlight? flight)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var own = flight is { } f && f.Token == ownToken ? f : null;

        foreach (var row in rows)
        {
            if (row.State != CommandState.Taken)
            {
                continue;
            }

            if (row.TakenToken == ownToken)
            {
                return own is { } mine && mine.Id == row.Id
                    ? mine.Phase is FlightPhase.Returned or FlightPhase.Faulted ? new PickupStep(PickupAction.CompleteAgain, row.Id) : new PickupStep(PickupAction.Wait, row.Id)
                    : new PickupStep(PickupAction.Run, row.Id);
            }

            return row.TakenToken < ownToken ? new PickupStep(PickupAction.ClassifyOlder, row.Id) : new PickupStep(PickupAction.Wait, row.Id);
        }

        foreach (var row in rows)
        {
            if (row.State == CommandState.Pending)
            {
                return new PickupStep(PickupAction.Take, row.Id);
            }
        }

        return new PickupStep(PickupAction.Idle, 0);
    }
}

internal enum HandlerEnd
{
    Returned,

    Threw,

    Cancelled,

    Abandoned,
}

internal enum HandlerAction
{
    Complete,

    CompleteWithError,

    LeaveTaken,
}

internal static class HandlerDecisions
{
    public static HandlerAction After(HandlerEnd end, bool stopRequested) => (end, stopRequested) switch
    {
        (HandlerEnd.Abandoned, _) => HandlerAction.LeaveTaken,

        (_, true) => HandlerAction.LeaveTaken,
        (HandlerEnd.Returned, false) => HandlerAction.Complete,
        _ => HandlerAction.CompleteWithError,
    };
}

internal enum ClassifyAction
{
    None,

    Retake,

    MarkInDoubt,
}

internal static class ClassifyDecisions
{
    public static ClassifyAction Classify(InDoubtPolicy policy, long takenToken, long ownToken) =>
        takenToken >= ownToken ? ClassifyAction.None : policy == InDoubtPolicy.ReRun ? ClassifyAction.Retake : ClassifyAction.MarkInDoubt;
}

internal enum MarkAction
{
    Marked,

    Refused,

    Unknown,
}

internal static class MarkDecisions
{
    public static MarkAction AfterMark(bool updated, bool cancellationRequestedBeforeReturn) => (updated, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => MarkAction.Unknown,
        (false, false) => MarkAction.Refused,
        _ => MarkAction.Marked,
    };
}

internal readonly record struct CommandOutcome(ReadOnlyMemory<byte> Result, string? Error)
{
    public static CommandOutcome Returned(ReadOnlyMemory<byte> result) => new(result, null);
}

internal readonly record struct ExpirableCommand(long Id, string Kind, string Key);

internal readonly record struct CommandStatus(long Id, CommandState State, byte[]? Result, string? Error);

internal static class CommandNudges
{
    private const string CompletedPrefix = "completed:";

    public static string Completed(string kind, string key) => CompletedPrefix + kind + ":" + key;
}

internal static class WaitDecisions
{
    public static DurableCommandResult? Finished(CommandStatus? status) => status switch
    {
        null => new DurableCommandResult(DurableCommandOutcome.NotFound, 0, null, default, null),
        { State: CommandState.Done, Error: { } error } s => new DurableCommandResult(DurableCommandOutcome.Failed, s.Id, DurableCommandState.Done, default, error),
        { State: CommandState.Done } s => new DurableCommandResult(DurableCommandOutcome.Completed, s.Id, DurableCommandState.Done, s.Result ?? [], null),
        { State: CommandState.InDoubt } s => new DurableCommandResult(DurableCommandOutcome.InDoubt, s.Id, DurableCommandState.InDoubt, default, null),
        { State: CommandState.Expired } s => new DurableCommandResult(DurableCommandOutcome.NoOwner, s.Id, DurableCommandState.Expired, default, null),
        _ => null,
    };
}
