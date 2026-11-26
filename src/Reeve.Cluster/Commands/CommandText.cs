using System.Text;
using Npgsql;

namespace Reeve.Cluster.Commands;

internal static class CommandText
{
    internal const int MaxErrorLength = 4096;

    internal const char Replacement = '�';

    internal const string Unstorable = "The command's outcome could not be stored.";

    internal const string Cancelled = "The handler was cancelled.";

    public static string ErrorOf(Exception thrown)
    {
        ArgumentNullException.ThrowIfNull(thrown);
        var inner = thrown.GetBaseException();
        string? message;
        try
        {
            message = inner.Message;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            message = null;
        }

        return Normalise(message, $"The handler threw {inner.GetType().Name}.");
    }

    public static string Normalise(string? message, string fallback)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fallback);
        if (string.IsNullOrWhiteSpace(message))
        {
            return fallback;
        }

        var text = new StringBuilder(Math.Min(message.Length, MaxErrorLength));
        var limit = message.Length > MaxErrorLength ? MaxErrorLength - 1 : MaxErrorLength;
        for (var i = 0; i < message.Length; i++)
        {
            var c = message[i];
            if (char.IsHighSurrogate(c) && i + 1 < message.Length && char.IsLowSurrogate(message[i + 1]))
            {
                if (text.Length + 2 > limit)
                {
                    break;
                }

                text.Append(c).Append(message[++i]);
                continue;
            }

            if (text.Length + 1 > limit)
            {
                break;
            }

            text.Append(c == '\0' || char.IsSurrogate(c) ? Replacement : c);
        }

        if (message.Length > MaxErrorLength)
        {
            text.Append('…');
        }

        return text.ToString();
    }

    public static bool IsStorable(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (c == '\0')
            {
                return false;
            }

            if (char.IsHighSurrogate(c) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
            {
                i++;
            }
            else if (char.IsSurrogate(c))
            {
                return false;
            }
        }

        return true;
    }

    public static void ThrowIfUnstorable(string value, string name)
    {
        if (!IsStorable(value))
        {
            throw new ArgumentException("The text holds U+0000 or an unpaired surrogate, which the command table cannot store.", name);
        }
    }

    public static bool IsPermanent(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is EncoderFallbackException || e is PostgresException { SqlState: { } state } && state.StartsWith("22", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
