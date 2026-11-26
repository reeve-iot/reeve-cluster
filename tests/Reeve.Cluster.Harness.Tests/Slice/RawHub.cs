using System.Net.WebSockets;
using System.Text;

namespace Harness.Slice;

internal sealed class RawHub(ClientWebSocket socket) : IAsyncDisposable
{
    private const char Separator = '\u001e';
    private readonly StringBuilder pending = new();
    private int invocations;

    public static async Task<RawHub> ConnectAsync(Uri url)
    {
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(new UriBuilder(url) { Scheme = "ws" }.Uri, CancellationToken.None).ConfigureAwait(false);
        var hub = new RawHub(socket);
        await hub.SendAsync("""{"protocol":"json","version":1}""").ConfigureAwait(false);
        if (await hub.ReceiveAsync().ConfigureAwait(false) != "{}")
        {
            throw new InvalidOperationException("The hub refused the handshake.");
        }

        return hub;
    }

    public bool IsOpen => socket.State == WebSocketState.Open;

    public Task SendAsync(string json) =>
        socket.SendAsync(Encoding.UTF8.GetBytes(json + Separator), WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);

    public async Task<string> ReceiveAsync(TimeSpan? limit = null)
    {
        using var bound = new CancellationTokenSource(limit ?? TimeSpan.FromSeconds(30));
        var buffer = new byte[4096];
        while (true)
        {
            var text = pending.ToString();
            var end = text.IndexOf(Separator, StringComparison.Ordinal);
            if (end >= 0)
            {
                pending.Clear().Append(text[(end + 1)..]);
                return text[..end];
            }

            WebSocketReceiveResult result;
            try
            {
                result = await socket.ReceiveAsync(buffer, bound.Token).ConfigureAwait(false);
            }
            catch (WebSocketException)
            {
                return "closed";
            }

            if (result.MessageType == WebSocketMessageType.Close)
            {
                return "closed";
            }

            pending.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
        }
    }

    public async Task<string> InvokeAsync(string target, params string[] arguments)
    {
        var id = (++invocations).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var json = string.Join(",", arguments.Select(a => System.Text.Json.JsonSerializer.Serialize(a)));
        await SendAsync($$"""{"type":1,"invocationId":"{{id}}","target":"{{target}}","arguments":[{{json}}]}""").ConfigureAwait(false);
        while (true)
        {
            var message = await ReceiveAsync().ConfigureAwait(false);
            if (message == "closed" || message.Contains("\"type\":3", StringComparison.Ordinal))
            {
                return message;
            }
        }
    }

    public async Task<bool> ClosedWithinAsync(TimeSpan limit)
    {
        var deadline = DateTime.UtcNow + limit;
        while (DateTime.UtcNow < deadline)
        {
            if (socket.State is WebSocketState.Aborted or WebSocketState.Closed or WebSocketState.CloseReceived)
            {
                return true;
            }

            try
            {
                if (await ReceiveAsync(deadline - DateTime.UtcNow).ConfigureAwait(false) == "closed")
                {
                    return true;
                }
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        return false;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (socket.State == WebSocketState.Open)
            {
                using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, bound.Token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
        {
        }

        socket.Dispose();
    }
}
