using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Relay;

internal static class AskFault
{
    public static bool NeverSent(Exception exception) =>
        exception is RedisConnectionException
        {
            FailureType: ConnectionFailureType.UnableToResolvePhysicalConnection
                or ConnectionFailureType.UnableToConnect
                or ConnectionFailureType.AuthenticationFailure,
        }
        or RedisTimeoutException { Commandstatus: CommandStatus.WaitingToBeSent or CommandStatus.WaitingInBacklog };

    public static SubmitEnd ClassifySubmit(Exception exception) => NeverSent(exception) ? SubmitEnd.NeverSent : SubmitEnd.Unknown;
}
