namespace Reeve.Cluster.Commands;

internal enum CommandState
{
    Pending,

    Taken,

    Done,

    InDoubt,

    Expired,
}

internal static class CommandStates
{
    public static string ToSql(CommandState state) => state switch
    {
        CommandState.Pending => "pending",
        CommandState.Taken => "taken",
        CommandState.Done => "done",
        CommandState.InDoubt => "in_doubt",
        CommandState.Expired => "expired",
        _ => throw new ArgumentOutOfRangeException(nameof(state)),
    };

    public static CommandState Parse(string sql) => sql switch
    {
        "pending" => CommandState.Pending,
        "taken" => CommandState.Taken,
        "done" => CommandState.Done,
        "in_doubt" => CommandState.InDoubt,
        "expired" => CommandState.Expired,
        _ => throw new ArgumentException($"'{sql}' is not a command state.", nameof(sql)),
    };
}

internal enum CommandWriter
{
    Send,

    Resend,

    Take,

    Complete,

    Retake,

    MarkInDoubt,

    Expire,

    Retention,
}

internal readonly record struct CommandRow(CommandState State, long? TakenToken);

internal readonly record struct CommandStep(CommandState? To, long? TakenToken);

internal static class CommandStateMachine
{
    public static CommandStep? Decide(CommandWriter writer, CommandRow? row, long actor = 0) => (writer, row) switch
    {
        (CommandWriter.Send, null) => new CommandStep(CommandState.Pending, null),
        (CommandWriter.Resend, { State: CommandState.Expired }) => new CommandStep(CommandState.Pending, null),
        (CommandWriter.Take, { State: CommandState.Pending }) => new CommandStep(CommandState.Taken, actor),
        (CommandWriter.Complete, { State: CommandState.Taken, TakenToken: { } held }) when held == actor => new CommandStep(CommandState.Done, held),
        (CommandWriter.Retake, { State: CommandState.Taken, TakenToken: { } held }) when held < actor => new CommandStep(CommandState.Taken, actor),
        (CommandWriter.MarkInDoubt, { State: CommandState.Taken, TakenToken: { } held }) when held < actor => new CommandStep(CommandState.InDoubt, held),
        (CommandWriter.Expire, { State: CommandState.Pending }) => new CommandStep(CommandState.Expired, null),
        (CommandWriter.Retention, { State: CommandState.Done or CommandState.InDoubt or CommandState.Expired }) => new CommandStep(null, null),
        _ => null,
    };
}
