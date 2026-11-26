using System.Net;
using System.Net.Sockets;

namespace Int.Ask;

internal sealed class CutProxy : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly CancellationTokenSource stopping = new();
    private readonly List<TcpClient> open = [];
    private volatile bool cut;
    private volatile bool paused;

    public CutProxy(string target)
    {
        var endpoint = target.Split(',')[0];
        var colon = endpoint.LastIndexOf(':');
        targetHost = endpoint[..colon];
        targetPort = int.Parse(endpoint[(colon + 1)..], System.Globalization.CultureInfo.InvariantCulture);
        listener.Start();
        _ = Task.Run(AcceptLoopAsync);
    }

    public string Endpoint => $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";

    public void Cut()
    {
        cut = true;
        lock (open)
        {
            foreach (var client in open)
            {
                client.Close();
            }

            open.Clear();
        }
    }

    public void Restore() => cut = false;

    public void Pause() => paused = true;

    public void Resume() => paused = false;

    public async ValueTask DisposeAsync()
    {
        Cut();
        await stopping.CancelAsync();
        listener.Stop();
        stopping.Dispose();
    }

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stopping.Token);
                if (cut)
                {
                    client.Close();
                    continue;
                }

                _ = Task.Run(() => ForwardAsync(client));
            }
        }
        catch (Exception exception) when (exception is OperationCanceledException or ObjectDisposedException or SocketException)
        {
        }
    }

    private async Task PumpAsync(Stream from, Stream to)
    {
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var read = await from.ReadAsync(buffer, stopping.Token);
            if (read == 0)
            {
                return;
            }

            while (paused)
            {
                await Task.Delay(20, stopping.Token);
            }

            await to.WriteAsync(buffer.AsMemory(0, read), stopping.Token);
        }
    }

    private async Task ForwardAsync(TcpClient client)
    {
        var server = new TcpClient();
        try
        {
            await server.ConnectAsync(targetHost, targetPort, stopping.Token);
            lock (open)
            {
                open.Add(client);
                open.Add(server);
            }

            if (cut)
            {
                client.Close();
                server.Close();
                return;
            }

            await Task.WhenAny(
                PumpAsync(client.GetStream(), server.GetStream()),
                PumpAsync(server.GetStream(), client.GetStream()));
        }
        catch (Exception exception) when (exception is OperationCanceledException or IOException or SocketException or ObjectDisposedException or InvalidOperationException)
        {
        }
        finally
        {
            client.Close();
            server.Close();
        }
    }
}
