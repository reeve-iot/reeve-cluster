using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class HubReconnectTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(HttpTransportType.WebSockets)]
    [InlineData(HttpTransportType.ServerSentEvents)]
    public async Task A_hub_client_with_automatic_reconnect_reconnects_by_itself_after_fence_step_4_on_WebSockets_and_SSE(HttpTransportType transport)
    {
        await using var kestrel = HttpRig.CreateKestrel(services => services.AddSingleton<IFenceSink, AspNetCoreFenceSink>());
        await using var client = await StartAsync(kestrel, transport);
        var first = client.Connection.ConnectionId;
        var connections = kestrel.Services.GetRequiredService<ClusterConnections>();
        await TrackedAsync(connections);

        await kestrel.Services.GetRequiredService<IFenceSink>().AbortConnectionsAsync(CancellationToken.None);

        await client.Reconnected.Task.WaitAsync(Wait);
        Assert.Equal(HubConnectionState.Connected, client.Connection.State);
        Assert.NotEqual(first, client.Connection.ConnectionId);
        Assert.Equal("pong", await client.Connection.InvokeAsync<string>("Ping").WaitAsync(Wait));
        Assert.Equal(1, connections.Count);
    }

    [Fact]
    public async Task A_hub_client_on_long_polling_stays_closed_after_fence_step_4()
    {
        await using var kestrel = HttpRig.CreateKestrel(services => services.AddSingleton<IFenceSink, AspNetCoreFenceSink>());
        await using var client = await StartAsync(kestrel, HttpTransportType.LongPolling);
        await TrackedAsync(kestrel.Services.GetRequiredService<ClusterConnections>());

        await kestrel.Services.GetRequiredService<IFenceSink>().AbortConnectionsAsync(CancellationToken.None);

        Assert.Null(await client.Closed.Task.WaitAsync(Wait));
        Assert.Equal(HubConnectionState.Disconnected, client.Connection.State);
        Assert.False(client.Reconnecting);
        Assert.Equal(0, kestrel.Services.GetRequiredService<ClusterConnections>().Count);
    }

    private static async Task TrackedAsync(ClusterConnections connections)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (connections.Count < 1 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(1, connections.Count);
    }

    private static async Task<Client> StartAsync(HttpRig kestrel, HttpTransportType transport)
    {
        await kestrel.PublishAsync(5);
        kestrel.ToServing();
        var connection = new HubConnectionBuilder()
            .WithUrl(new Uri(kestrel.BaseAddress, "/hub/closed"), options => options.Transports = transport)
            .WithAutomaticReconnect()
            .Build();
        var client = new Client(connection);
        await connection.StartAsync().WaitAsync(Wait);
        return client;
    }

    private sealed class Client : IAsyncDisposable
    {
        public Client(HubConnection connection)
        {
            Connection = connection;
            connection.Reconnecting += _ =>
            {
                Reconnecting = true;
                return Task.CompletedTask;
            };
            connection.Reconnected += _ =>
            {
                Reconnected.TrySetResult();
                return Task.CompletedTask;
            };
            connection.Closed += error =>
            {
                Closed.TrySetResult(error);
                return Task.CompletedTask;
            };
        }

        public HubConnection Connection { get; }

        public bool Reconnecting { get; private set; }

        public TaskCompletionSource Reconnected { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<Exception?> Closed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask DisposeAsync() => Connection.DisposeAsync();
    }
}
