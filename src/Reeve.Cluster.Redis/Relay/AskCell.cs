using Reeve.Cluster.Ask;

namespace Reeve.Cluster.Redis.Relay;

internal enum AskCellKind
{
    Pending,

    Taken,

    Withdrawn,

    Declined,

    Answered,
}

internal sealed record AskCell(AskCellKind Kind, DeclineReason? Reason, AskAnswer? Answer)
{
    public static AskCell Pending { get; } = new(AskCellKind.Pending, null, null);

    public static AskCell Taken { get; } = new(AskCellKind.Taken, null, null);

    public static AskCell Withdrawn { get; } = new(AskCellKind.Withdrawn, null, null);

    public static AskCell Declined(DeclineReason reason) => new(AskCellKind.Declined, reason, null);

    public static AskCell Answered(AskAnswer answer)
    {
        ArgumentNullException.ThrowIfNull(answer);
        return new(AskCellKind.Answered, null, answer);
    }
}

internal enum TakeVerdict
{
    Taken,

    NotPending,

    TooLate,
}

internal enum WithdrawKind
{
    Withdrew,

    AlreadyTaken,

    Answered,

    Declined,

    Missing,
}

internal readonly record struct Withdrawal(WithdrawKind Kind, AskAnswer? Answer, DeclineReason? Reason);

internal static class AskCellRules
{
    public static TakeVerdict Take(AskCell? cell, TimeSpan? ttlLeft, TimeSpan margin) =>
        cell is not { Kind: AskCellKind.Pending } ? TakeVerdict.NotPending
        : ttlLeft is { } left && left <= margin ? TakeVerdict.TooLate
        : TakeVerdict.Taken;

    public static bool CanDecline(AskCell? cell) => cell is { Kind: AskCellKind.Pending };

    public static bool CanAnswer(AskCell? cell) => cell is { Kind: AskCellKind.Taken };

    public static Withdrawal Withdraw(AskCell? cell) => cell switch
    {
        null => new Withdrawal(WithdrawKind.Missing, null, null),
        { Kind: AskCellKind.Pending or AskCellKind.Withdrawn } => new Withdrawal(WithdrawKind.Withdrew, null, null),
        { Kind: AskCellKind.Taken } => new Withdrawal(WithdrawKind.AlreadyTaken, null, null),
        { Kind: AskCellKind.Answered } => new Withdrawal(WithdrawKind.Answered, cell.Answer, null),
        _ => new Withdrawal(WithdrawKind.Declined, null, cell.Reason),
    };
}
