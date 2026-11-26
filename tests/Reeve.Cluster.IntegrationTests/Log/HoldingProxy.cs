using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

internal sealed class HoldingProxy : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly string upstreamHost;
    private readonly int upstreamPort;
    private readonly CancellationTokenSource stopping = new();
    private readonly object sync = new();
    private readonly List<TcpClient> sockets = [];
    private readonly List<TcpClient> parked = [];
    private readonly Task accepting;
    private TaskCompletionSource open = Opened();
    private TaskCompletionSource bytesHeld = Signal();
    private int acceptedWhileHeld;

    public HoldingProxy(string upstreamHost, int upstreamPort)
    {
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
        listener.Start();
        accepting = AcceptAsync();
    }

    public int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

    public int AcceptedWhileHeld
    {
        get
        {
            lock (sync)
            {
                return acceptedWhileHeld;
            }
        }
    }

    public void Hold()
    {
        lock (sync)
        {
            if (open.Task.IsCompleted)
            {
                open = Signal();
                bytesHeld = Signal();
            }
        }
    }

    public bool BytesHeld
    {
        get
        {
            lock (sync)
            {
                return bytesHeld.Task.IsCompleted;
            }
        }
    }

    public Task UntilBytesHeldAsync(TimeSpan bound)
    {
        lock (sync)
        {
            return bytesHeld.Task.WaitAsync(bound);
        }
    }

    public void Release()
    {
        TaskCompletionSource gate;
        TcpClient[] held;
        lock (sync)
        {
            gate = open;
            held = [.. parked];
            parked.Clear();
        }

        foreach (var client in held)
        {
            client.Dispose();
        }

        gate.TrySetResult();
    }

    public void Reset()
    {
        TcpClient[] all;
        lock (sync)
        {
            all = [.. sockets];
            sockets.Clear();
        }

        foreach (var client in all)
        {
            try
            {
                client.Client.LingerState = new LingerOption(true, 0);
            }
            catch (Exception)
            {
            }

            client.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        Release();
        await stopping.CancelAsync();
        listener.Stop();
        try
        {
            await accepting;
        }
        catch (Exception)
        {
        }

        lock (sync)
        {
            foreach (var socket in sockets)
            {
                socket.Dispose();
            }

            sockets.Clear();
        }

        stopping.Dispose();
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static TaskCompletionSource Opened()
    {
        var opened = Signal();
        opened.SetResult();
        return opened;
    }

    private async Task AcceptAsync()
    {
        while (!stopping.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync(stopping.Token);
            }
            catch (Exception)
            {
                return;
            }

            bool held;
            lock (sync)
            {
                held = !open.Task.IsCompleted;
                if (held)
                {
                    parked.Add(client);
                    acceptedWhileHeld++;
                }
                else
                {
                    sockets.Add(client);
                }
            }

            if (!held)
            {
                _ = PipeAsync(client);
            }
        }
    }

    private async Task PipeAsync(TcpClient client)
    {
        var upstream = new TcpClient();
        lock (sync)
        {
            sockets.Add(upstream);
        }

        try
        {
            await upstream.ConnectAsync(upstreamHost, upstreamPort, stopping.Token);
            var down = client.GetStream();
            var up = upstream.GetStream();
            await Task.WhenAny(PumpAsync(down, up), PumpAsync(up, down));
        }
        catch (Exception)
        {
        }
        finally
        {
            client.Dispose();
            upstream.Dispose();
        }
    }

    private async Task PumpAsync(NetworkStream from, NetworkStream to)
    {
        var buffer = new byte[64 * 1024];
        try
        {
            while (true)
            {
                var n = await from.ReadAsync(buffer, stopping.Token);
                if (n == 0)
                {
                    return;
                }

                Task gate;
                lock (sync)
                {
                    gate = open.Task;
                    if (!gate.IsCompleted)
                    {
                        bytesHeld.TrySetResult();
                    }
                }

                await gate.WaitAsync(stopping.Token);
                await to.WriteAsync(buffer.AsMemory(0, n), stopping.Token);
            }
        }
        catch (Exception)
        {
        }
    }
}
