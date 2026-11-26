using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;

namespace Unit.Redis.Relay;

public sealed class AskCellCodecTests
{
    [Fact]
    public void Every_state_of_the_record_survives_its_encoding()
    {
        foreach (var reason in Enum.GetValues<DeclineReason>())
        {
            var decoded = AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Declined(reason)));
            Assert.Equal(AskCellKind.Declined, decoded!.Kind);
            Assert.Equal(reason, decoded.Reason);
        }

        Assert.Equal(AskCellKind.Pending, AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Pending))!.Kind);
        Assert.Equal(AskCellKind.Taken, AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Taken))!.Kind);
        Assert.Equal(AskCellKind.Withdrawn, AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Withdrawn))!.Kind);
    }

    [Theory]
    [InlineData(0, "r", null)]
    [InlineData(1, "", "boom — ünïcode")]
    [InlineData(2, "", null)]
    [InlineData(3, "", null)]
    public void An_answer_survives_its_encoding(int code, string response, string? error)
    {
        var kind = (AskAnswerKind)code;
        var answer = new AskAnswer(kind, System.Text.Encoding.UTF8.GetBytes(response), error);

        var decoded = AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Answered(answer)));

        Assert.Equal(AskCellKind.Answered, decoded!.Kind);
        Assert.Equal(kind, decoded.Answer!.Kind);
        Assert.Equal(response, System.Text.Encoding.UTF8.GetString(decoded.Answer.Response.Span));
        Assert.Equal(error, decoded.Answer.Error);
    }

    [Fact]
    public void An_empty_response_and_an_empty_error_stay_distinct_from_no_error()
    {
        var empty = AskCellCodec.Decode(AskCellCodec.Encode(AskCell.Answered(new AskAnswer(AskAnswerKind.HandlerError, default, string.Empty))));

        Assert.Equal(string.Empty, empty!.Answer!.Error);
    }

    [Theory]
    [InlineData(new byte[0])]
    [InlineData(new byte[] { 0xFF })]
    [InlineData(new byte[] { 1 })]
    [InlineData(new byte[] { 1, (byte)'A' })]
    [InlineData(new byte[] { 1, (byte)'D' })]
    [InlineData(new byte[] { (byte)'P' })]
    [InlineData(new byte[] { 2, (byte)'P' })]
    public void Bytes_that_are_not_a_record_of_this_version_decode_to_nothing(byte[] bytes) =>
        Assert.Null(AskCellCodec.Decode(bytes));

    [Fact]
    public void A_record_and_a_message_start_with_the_version_of_their_format()
    {
        Assert.Equal(AskCellCodec.Version, AskCellCodec.Encode(AskCell.Pending)[0]);
        var message = new AskRequestMessage(Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromSeconds(1), new AskDelivery(null, null, null, default)).Encode();
        Assert.Equal(AskCellCodec.Version, message[0]);
        message[0] = 2;
        Assert.Null(AskRequestMessage.TryDecode(message));
    }

    [Fact]
    public void A_request_message_carries_the_delivery_the_callers_address_and_the_window()
    {
        var caller = Guid.NewGuid();
        var id = Guid.NewGuid();
        var keyed = new AskRequestMessage(id, caller, TimeSpan.FromMilliseconds(2500), new AskDelivery("scheduler", "k1", 42, new byte[] { 1, 2, 3 }));
        var member = new AskRequestMessage(id, caller, TimeSpan.FromSeconds(1), new AskDelivery(null, null, null, ReadOnlyMemory<byte>.Empty));

        var keyedBack = AskRequestMessage.TryDecode(keyed.Encode());
        var memberBack = AskRequestMessage.TryDecode(member.Encode());

        Assert.NotNull(keyedBack);
        Assert.Equal(id, keyedBack.RequestId);
        Assert.Equal(caller, keyedBack.CallerProcess);
        Assert.Equal(TimeSpan.FromMilliseconds(2500), keyedBack.Window);
        Assert.Equal("scheduler", keyedBack.Delivery.Kind);
        Assert.Equal("k1", keyedBack.Delivery.Key);
        Assert.Equal(42, keyedBack.Delivery.Token);
        Assert.Equal(new byte[] { 1, 2, 3 }, keyedBack.Delivery.Payload.ToArray());
        Assert.NotNull(memberBack);
        Assert.True(memberBack.Delivery.IsMember);
        Assert.Null(memberBack.Delivery.Key);
        Assert.Null(memberBack.Delivery.Token);
    }

    [Theory]
    [InlineData(new byte[0])]
    [InlineData(new byte[] { 1, 2, 3 })]
    public void Bytes_that_are_not_a_request_message_decode_to_nothing(byte[] bytes) =>
        Assert.Null(AskRequestMessage.TryDecode(bytes));

    [Fact]
    public void The_channels_name_the_process_not_the_replica_alone()
    {
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        Assert.NotEqual(AskAddress.RequestChannel("a", first), AskAddress.RequestChannel("a", second));
        Assert.NotEqual(AskAddress.RequestChannel("a", first), AskAddress.RequestChannel("b", first));
        Assert.NotEqual(AskAddress.RequestChannel("a", first), AskAddress.ReplyChannel(first));
    }
}
