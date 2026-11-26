using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Int.Redis;

internal sealed class MultiHoldProxy : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly CancellationTokenSource stopping = new();
    private readonly object gate = new();
    private Hold? armed;

    public MultiHoldProxy(string target)
    {
        var endpoint = target.Split(',')[0];
        var colon = endpoint.LastIndexOf(':');
        targetHost = endpoint[..colon];
        targetPort = int.Parse(endpoint[(colon + 1)..], System.Globalization.CultureInfo.InvariantCulture);
        listener.Start();
        _ = Task.Run(AcceptLoopAsync);
    }

    public string Endpoint => $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";

    public Hold HoldNextMulti()
    {
        var hold = new Hold();
        lock (gate)
        {
            armed = hold;
        }

        return hold;
    }

    public async ValueTask DisposeAsync()
    {
        Hold? left;
        lock (gate)
        {
            left = armed;
            armed = null;
        }

        left?.Release();
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
                _ = Task.Run(() => ServeAsync(client));
            }
        }
        catch (Exception) when (stopping.IsCancellationRequested)
        {
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using var c = client;
        using var server = new TcpClient { NoDelay = true };
        try
        {
            c.NoDelay = true;
            await server.ConnectAsync(targetHost, targetPort, stopping.Token);
            var up = PumpAsync(c.GetStream(), server.GetStream(), clientToServer: true);
            var down = PumpAsync(server.GetStream(), c.GetStream(), clientToServer: false);
            await Task.WhenAny(up, down);
        }
        catch (Exception)
        {
        }
    }

    private async Task PumpAsync(Stream from, Stream to, bool clientToServer)
    {
        var buffer = new byte[64 * 1024];
        while (true)
        {
            var n = await from.ReadAsync(buffer, stopping.Token);
            if (n == 0)
            {
                return;
            }

            if (clientToServer && Take(buffer, n) is { } hold)
            {
                hold.Reached();
                await hold.Released.WaitAsync(stopping.Token);
            }

            await to.WriteAsync(buffer.AsMemory(0, n), stopping.Token);
            await to.FlushAsync(stopping.Token);
        }
    }

    private Hold? Take(byte[] buffer, int n)
    {
        lock (gate)
        {
            if (armed is null || !Encoding.ASCII.GetString(buffer, 0, n).Contains("MULTI", StringComparison.Ordinal))
            {
                return null;
            }

            var hold = armed;
            armed = null;
            return hold;
        }
    }

    public sealed class Hold
    {
        private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Held => reached.Task;

        internal Task Released => released.Task;

        public void Release() => released.TrySetResult();

        internal void Reached() => reached.TrySetResult();
    }
}
