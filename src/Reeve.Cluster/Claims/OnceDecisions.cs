namespace Reeve.Cluster.Claims;

internal readonly record struct OnceRead(string HolderReplica, long HolderEpoch, Guid Token, bool Valid, bool Completed);

internal readonly record struct OnceCandidate(string Key, Guid Token, string Effect);

internal enum OnceConflictAction
{
    InsertAgain,

    AlreadyCompleted,

    Running,

    InDoubt,
}

internal enum RetakeAction
{
    Run,

    Skip,

    NotRun,
}

internal static class OnceDecisions
{
    public static OnceConflictAction AfterConflict(OnceRead? row) => row switch
    {
        null => OnceConflictAction.InsertAgain,
        { Completed: true } => OnceConflictAction.AlreadyCompleted,
        { Valid: true } => OnceConflictAction.Running,
        _ => OnceConflictAction.InDoubt,
    };

    public static bool MayStart(bool cancellationRequestedBeforeReturn) => !cancellationRequestedBeforeReturn;

    public static OnceResult AfterCompletion(bool updated, bool cancellationRequestedBeforeReturn) =>
        updated && !cancellationRequestedBeforeReturn ? new OnceResult.Completed() : new OnceResult.CompletionUnknown();

    public static RetakeAction AfterRetake(Guid? token, bool cancellationRequestedBeforeReturn) => (token, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => RetakeAction.NotRun,
        (null, false) => RetakeAction.Skip,
        _ => RetakeAction.Run,
    };
}

internal static class OnceKeys
{
    public const string Prefix = "reeve.once:";

    public static string Store(string key) => Prefix + key;

    public static bool IsReserved(string key) => key.StartsWith(Prefix, StringComparison.Ordinal);

    public static string Application(string stored) => stored[Prefix.Length..];

    public static void ThrowIfReserved(string key)
    {
        if (IsReserved(key))
        {
            throw new ArgumentException($"Keys starting with '{Prefix}' belong to once-runs; a plain claim cannot use them.", nameof(key));
        }
    }
}
