using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Reeve.Cluster.Harness.Processes;

namespace Reeve.Cluster.Harness.Linearizability;

internal sealed class HubConfigReader(Uri hubUrl, string? first, HistoryRecorder recorder, ConfigRegisters registers) : IAsyncDisposable
{
    private HubConnection? connection;

    public async Task<bool> ReadAsync(int client, int replica, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        HubConnection open;
        try
        {
            open = await ConnectedAsync(cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (IsConnectionFailure(exception))
        {
            await DropAsync().ConfigureAwait(false);
            return false;
        }

        var pending = recorder.BeginRead(client, replica);
        JsonElement? answer = null;
        try
        {
            answer = await open.InvokeAsync<JsonElement>("GetConfig", cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is HubException || IsConnectionFailure(exception))
        {
            await DropAsync().ConfigureAwait(false);
        }

        var values = answer is { } body ? registers.Parse(body) : null;
        recorder.EndRead(pending, values);
        return values is not null;
    }

    public ValueTask DisposeAsync() => DropAsync();

    private static bool IsConnectionFailure(Exception exception) =>
        exception is HttpRequestException or OperationCanceledException or IOException or InvalidOperationException or ObjectDisposedException
            or System.Net.WebSockets.WebSocketException or System.Net.Sockets.SocketException;

    private async Task<HubConnection> ConnectedAsync(CancellationToken cancellationToken)
    {
        if (connection is { State: HubConnectionState.Connected } existing)
        {
            return existing;
        }

        await DropAsync().ConfigureAwait(false);
        var created = new HubConnectionBuilder()
            .WithUrl(hubUrl, options =>
            {
                options.SkipNegotiation = true;
                options.Transports = HttpTransportType.WebSockets;
                if (first is not null)
                {
                    options.Headers[Balancer.FirstHeader] = first;
                }
            })
            .Build();
        await created.StartAsync(cancellationToken).ConfigureAwait(false);
        return connection = created;
    }

    private async ValueTask DropAsync()
    {
        var old = connection;
        connection = null;
        if (old is not null)
        {
            await old.DisposeAsync().ConfigureAwait(false);
        }
    }
}
