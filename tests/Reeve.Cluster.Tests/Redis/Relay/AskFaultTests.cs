using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;

namespace Unit.Redis.Relay;

public sealed class AskFaultTests
{
    [Theory]
    [InlineData(ConnectionFailureType.UnableToResolvePhysicalConnection)]
    [InlineData(ConnectionFailureType.UnableToConnect)]
    [InlineData(ConnectionFailureType.AuthenticationFailure)]
    public void A_connection_that_never_existed_proves_the_command_was_not_sent(ConnectionFailureType type)
    {
        var fault = new RedisConnectionException(type, CommandFlags.None, "down", null, CommandStatus.Unknown);

        Assert.True(AskFault.NeverSent(fault));
        Assert.Equal(SubmitEnd.NeverSent, AskFault.ClassifySubmit(fault));
    }

    [Theory]
    [InlineData(ConnectionFailureType.SocketFailure)]
    [InlineData(ConnectionFailureType.SocketClosed)]
    [InlineData(ConnectionFailureType.InternalFailure)]
    public void A_connection_that_failed_under_the_command_leaves_it_unknown(ConnectionFailureType type)
    {
        var fault = new RedisConnectionException(type, CommandFlags.None, "lost", null, CommandStatus.Unknown);

        Assert.False(AskFault.NeverSent(fault));
        Assert.Equal(SubmitEnd.Unknown, AskFault.ClassifySubmit(fault));
    }

    [Theory]
    [InlineData(CommandStatus.WaitingToBeSent)]
    [InlineData(CommandStatus.WaitingInBacklog)]
    public void A_command_that_timed_out_before_it_was_written_was_never_sent(CommandStatus status) =>
        Assert.Equal(SubmitEnd.NeverSent, AskFault.ClassifySubmit(new RedisTimeoutException(CommandFlags.None, "timeout", status)));

    [Fact]
    public void A_timeout_after_the_write_and_a_server_error_leave_the_command_unknown()
    {
        Assert.Equal(SubmitEnd.Unknown, AskFault.ClassifySubmit(new RedisTimeoutException(CommandFlags.None, "timeout", CommandStatus.Sent)));
        Assert.Equal(SubmitEnd.Unknown, AskFault.ClassifySubmit(new RedisTimeoutException(CommandFlags.None, "timeout", CommandStatus.Unknown)));
        Assert.Equal(SubmitEnd.Unknown, AskFault.ClassifySubmit(new RedisServerException(RedisErrorKind.ReadOnly, CommandFlags.None, "READONLY")));
        Assert.Equal(SubmitEnd.Unknown, AskFault.ClassifySubmit(new InvalidOperationException()));
    }
}
