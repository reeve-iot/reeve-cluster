using System.Text;
using Reeve.Cluster.Ask;

namespace Reeve.Cluster.Redis.Relay;

internal static class AskCellCodec
{
    internal const byte Version = 1;

    private const byte PendingTag = (byte)'P';
    private const byte TakenTag = (byte)'T';
    private const byte WithdrawnTag = (byte)'W';
    private const byte DeclinedTag = (byte)'D';
    private const byte AnsweredTag = (byte)'A';

    public static byte[] Encode(AskCell cell)
    {
        ArgumentNullException.ThrowIfNull(cell);
        switch (cell.Kind)
        {
            case AskCellKind.Pending:
                return [Version, PendingTag];
            case AskCellKind.Taken:
                return [Version, TakenTag];
            case AskCellKind.Withdrawn:
                return [Version, WithdrawnTag];
            case AskCellKind.Declined:
                return [Version, DeclinedTag, (byte)(cell.Reason ?? DeclineReason.Absent)];
            default:
                var answer = cell.Answer!;
                var error = answer.Error is null ? null : Encoding.UTF8.GetBytes(answer.Error);
                var bytes = new byte[3 + 4 + answer.Response.Length + 1 + (error?.Length ?? 0)];
                bytes[0] = Version;
                bytes[1] = AnsweredTag;
                bytes[2] = (byte)answer.Kind;
                BitConverter.TryWriteBytes(bytes.AsSpan(3, 4), answer.Response.Length);
                answer.Response.Span.CopyTo(bytes.AsSpan(7));
                bytes[7 + answer.Response.Length] = (byte)(error is null ? 0 : 1);
                error?.CopyTo(bytes, 8 + answer.Response.Length);
                return bytes;
        }
    }

    public static AskCell? Decode(ReadOnlyMemory<byte> value)
    {
        var span = value.Span;
        if (span.Length < 2 || span[0] != Version)
        {
            return null;
        }

        span = span[1..];
        switch (span[0])
        {
            case PendingTag when span.Length == 1:
                return AskCell.Pending;
            case TakenTag when span.Length == 1:
                return AskCell.Taken;
            case WithdrawnTag when span.Length == 1:
                return AskCell.Withdrawn;
            case DeclinedTag when span.Length == 2 && Enum.IsDefined((DeclineReason)span[1]):
                return AskCell.Declined((DeclineReason)span[1]);
            case AnsweredTag when span.Length >= 7:
                var kind = (AskAnswerKind)span[1];
                var length = BitConverter.ToInt32(span.Slice(2, 4));
                if (!Enum.IsDefined(kind) || length < 0 || span.Length < 7 + length)
                {
                    return null;
                }

                var response = span.Slice(6, length).ToArray();
                var hasError = span[6 + length] != 0;
                var error = hasError ? Encoding.UTF8.GetString(span[(7 + length)..]) : null;
                return span.Length != 7 + length + (error is null ? 0 : Encoding.UTF8.GetByteCount(error))
                    ? null
                    : AskCell.Answered(new AskAnswer(kind, response, error));
            default:
                return null;
        }
    }
}

internal static class AskAddress
{
    public static string RequestChannel(string replicaId, Guid processId) => $"ask:q:{processId:N}:{replicaId}";

    public static string ReplyChannel(Guid processId) => $"ask:r:{processId:N}";

    public static string Record(Guid requestId) => $"ask:c:{requestId:N}";
}

internal sealed record AskRequestMessage(Guid RequestId, Guid CallerProcess, TimeSpan Window, AskDelivery Delivery)
{
    private const byte Tag = (byte)'Q';

    public byte[] Encode()
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(AskCellCodec.Version);
            writer.Write(Tag);
            writer.Write(RequestId.ToByteArray());
            writer.Write(CallerProcess.ToByteArray());
            writer.Write((long)Window.TotalMilliseconds);
            writer.Write(Delivery.IsMember);
            if (!Delivery.IsMember)
            {
                writer.Write(Delivery.Kind!);
                writer.Write(Delivery.Key ?? string.Empty);
                writer.Write(Delivery.Token ?? 0);
            }

            writer.Write(Delivery.Payload.Length);
            writer.Write(Delivery.Payload.Span);
        }

        return stream.ToArray();
    }

    public static AskRequestMessage? TryDecode(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            using var stream = new MemoryStream(bytes.ToArray());
            using var reader = new BinaryReader(stream, Encoding.UTF8);
            if (reader.ReadByte() != AskCellCodec.Version || reader.ReadByte() != Tag)
            {
                return null;
            }

            var id = new Guid(reader.ReadBytes(16));
            var caller = new Guid(reader.ReadBytes(16));
            var window = TimeSpan.FromMilliseconds(reader.ReadInt64());
            string? kind = null;
            string? key = null;
            long? token = null;
            if (!reader.ReadBoolean())
            {
                kind = reader.ReadString();
                key = reader.ReadString();
                token = reader.ReadInt64();
            }

            var length = reader.ReadInt32();
            if (length < 0 || stream.Length - stream.Position != length)
            {
                return null;
            }

            return new AskRequestMessage(id, caller, window, new AskDelivery(kind, key, token, reader.ReadBytes(length)));
        }
        catch (Exception exception) when (exception is EndOfStreamException or ArgumentException or FormatException or IOException)
        {
            return null;
        }
    }
}
