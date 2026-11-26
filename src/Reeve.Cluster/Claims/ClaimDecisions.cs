namespace Reeve.Cluster.Claims;

internal readonly record struct ClaimRead(string HolderReplica, long HolderEpoch, Guid Token, bool Valid);

internal enum ConflictAction
{
    InsertAgain,

    Lost,

    Takeover,
}

internal enum DatabaseRefusal
{
    None,

    ClaimLost,

    OtherTimeline,
}

internal static class ClaimDecisions
{
    public static ConflictAction AfterConflict(ClaimRead? row) => row switch
    {
        null => ConflictAction.InsertAgain,
        { Valid: true } => ConflictAction.Lost,
        _ => ConflictAction.Takeover,
    };

    public static DatabaseRefusal Refusal(string? sqlState) => sqlState switch
    {
        "RC003" => DatabaseRefusal.ClaimLost,
        "RC002" => DatabaseRefusal.OtherTimeline,
        _ => DatabaseRefusal.None,
    };

    public static ClaimResult GrantAnswer(Guid token, bool cancellationRequestedBeforeReturn) =>
        cancellationRequestedBeforeReturn ? new ClaimResult.Unknown() : new ClaimResult.Won(token);
}

internal static class ClaimKeys
{
    public static void ThrowIfReserved(string key)
    {
        OnceKeys.ThrowIfReserved(key);
        FlightKeys.ThrowIfReserved(key);
    }
}
