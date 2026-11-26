namespace Reeve.Cluster.Claims;

internal readonly record struct FlightRead(string HolderReplica, long HolderEpoch, Guid Token, bool Valid, byte[]? Result);

internal enum FlightAction
{
    InsertAgain,

    UseResult,

    Wait,

    Takeover,
}

internal enum ResultAction
{
    Stored,

    Refused,

    Unknown,
}

internal static class FlightDecisions
{
    public static FlightAction AfterConflict(FlightRead? row) => row switch
    {
        null => FlightAction.InsertAgain,
        { Result: not null } => FlightAction.UseResult,
        { Valid: true } => FlightAction.Wait,
        _ => FlightAction.Takeover,
    };

    public static bool MayCompute(bool cancellationRequestedBeforeReturn) => !cancellationRequestedBeforeReturn;

    public static ResultAction AfterResultWrite(bool updated, bool cancellationRequestedBeforeReturn) => (updated, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => ResultAction.Unknown,
        (true, false) => ResultAction.Stored,
        _ => ResultAction.Refused,
    };
}

internal static class FlightKeys
{
    public const string Prefix = "reeve.flight:";

    public static string Store(string key) => Prefix + key;

    public static bool IsReserved(string key) => key.StartsWith(Prefix, StringComparison.Ordinal);

    public static void ThrowIfReserved(string key)
    {
        if (IsReserved(key))
        {
            throw new ArgumentException($"Keys starting with '{Prefix}' belong to single-flight; a plain claim cannot use them.", nameof(key));
        }
    }
}
