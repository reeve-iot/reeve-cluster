using System.Net.WebSockets;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class HubAcknowledgementTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly HttpRig rig = HttpRig.Create(services => services.AddSingleton<IFenceSink, AspNetCoreFenceSink>());

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    private async Task<RawHub> ConnectAsync()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        return await rig.ConnectHubAsync("/hub/write");
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

    private static string? ErrorOf(string completion)
    {
        using var message = JsonDocument.Parse(completion);
        return message.RootElement.TryGetProperty("error", out var error) ? error.GetString() : null;
    }

    private static string? ResultOf(string completion)
    {
        using var message = JsonDocument.Parse(completion);
        return message.RootElement.TryGetProperty("result", out var result) ? result.GetString() : null;
    }

    [Fact]
    public async Task An_acknowledged_invocation_returns_its_value_after_one_wait_by_the_highest_seq()
    {
        await using var hub = await ConnectAsync();

        var completion = await hub.InvokeAsync("Write", "seqs=3,8");

        Assert.Equal("done", ResultOf(completion));
        Assert.Equal([8L], rig.Ack.Waits);
    }

    [Fact]
    public async Task An_unconfirmed_invocation_throws_write_unconfirmed_and_loses_its_value()
    {
        await using var hub = await ConnectAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var completion = await hub.InvokeAsync("Write", "seqs=3");

        Assert.Null(ResultOf(completion));
        Assert.Contains("write_unconfirmed", ErrorOf(completion), StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_invocation_whose_commit_was_refused_throws_write_refused_when_it_appended_nothing_and_write_unconfirmed_when_it_did()
    {
        await using var hub = await ConnectAsync();

        Assert.Contains("write_refused", ErrorOf(await hub.InvokeAsync("Write", "refuse")), StringComparison.Ordinal);
        Assert.Contains("write_unconfirmed", ErrorOf(await hub.InvokeAsync("Write", "seqs=2&refuse")), StringComparison.Ordinal);
        Assert.Empty(rig.Ack.Waits);
    }

    [Fact]
    public async Task An_exception_after_a_refused_commit_is_answered_by_the_rule()
    {
        await using var hub = await ConnectAsync();

        Assert.Contains("write_refused", ErrorOf(await hub.InvokeAsync("Throw", "refuse")), StringComparison.Ordinal);
        Assert.Contains("write_unconfirmed", ErrorOf(await hub.InvokeAsync("Throw", "seqs=2&refuse")), StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_opted_out_or_exempt_invocation_is_not_awaited_and_returns_its_value()
    {
        await using var hub = await ConnectAsync();

        Assert.Equal("done", ResultOf(await hub.InvokeAsync("Write", "seqs=3&optout")));
        Assert.Equal("done", ResultOf(await hub.InvokeAsync("WriteExempt", "seqs=4")));
        Assert.Empty(rig.Ack.Waits);
    }

    [Fact]
    public async Task An_invocation_that_wrote_nothing_waits_for_nothing()
    {
        await using var hub = await ConnectAsync();

        Assert.Equal("done", ResultOf(await hub.InvokeAsync("Write", "")));
        Assert.Empty(rig.Ack.Waits);
    }

    [Fact]
    public async Task Fence_step_4_aborts_a_hub_connection_and_the_connection_is_forgotten()
    {
        await using var hub = await ConnectAsync();
        var connections = rig.Services.GetRequiredService<ClusterConnections>();
        await TrackedAsync(connections);

        await rig.Services.GetRequiredService<IFenceSink>().AbortConnectionsAsync(CancellationToken.None);

        Assert.Equal(0, connections.Count);
    }

    [Fact]
    public async Task A_connection_the_hub_refuses_by_throwing_is_not_tracked_and_a_failing_abort_does_not_stop_step_4()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var connections = rig.Services.GetRequiredService<ClusterConnections>();

        await using (var refused = await rig.ConnectHubAsync("/hub/throwing"))
        {
            while (await refused.ReceiveAsync() != "closed")
            {
            }
        }

        var deadline = DateTime.UtcNow + Wait;
        while (connections.Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(0, connections.Count);
        var aborted = 0;
        connections.Track("fails", () => throw new InvalidOperationException("gone"));
        connections.Track("works", () => aborted++);
        Assert.Equal(2, connections.AbortAll());
        Assert.Equal(1, aborted);
    }

    [Fact]
    public async Task A_hub_connection_that_ended_is_not_tracked_any_more()
    {
        var hub = await ConnectAsync();
        var connections = rig.Services.GetRequiredService<ClusterConnections>();
        await TrackedAsync(connections);

        await hub.DisposeAsync();
        var deadline = DateTime.UtcNow + Wait;
        while (connections.Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(0, connections.Count);
    }

    [Fact]
    public async Task Fence_step_4_aborts_a_raw_WebSocket_at_the_transport_on_Kestrel()
    {
        await using var kestrel = HttpRig.CreateKestrel(services => services.AddSingleton<IFenceSink, AspNetCoreFenceSink>());
        await kestrel.PublishAsync(5);
        kestrel.ToServing();
        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri(new UriBuilder(kestrel.BaseAddress) { Scheme = "ws", Path = "/min/ws" }.ToString()), CancellationToken.None);
        await kestrel.WsOpen.Task.WaitAsync(Wait);
        var connections = kestrel.Services.GetRequiredService<ClusterConnections>();
        Assert.Equal(1, connections.Count);

        await kestrel.Services.GetRequiredService<IFenceSink>().AbortConnectionsAsync(CancellationToken.None);

        var buffer = new byte[16];
        using var bound = new CancellationTokenSource(Wait);
        var ended = await Record.ExceptionAsync(async () => await socket.ReceiveAsync(buffer, bound.Token));
        Assert.IsType<WebSocketException>(ended);
        Assert.NotEqual(WebSocketState.Open, socket.State);
        Assert.Equal(0, connections.Count);
    }

    [Fact]
    public async Task A_connection_tracked_by_the_application_is_aborted_with_the_rest_and_released_by_its_handle()
    {
        var connections = rig.Services.GetRequiredService<ClusterConnections>();
        var aborted = 0;
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
        context.Features.Set<Microsoft.AspNetCore.Http.Features.IHttpRequestLifetimeFeature>(new Lifetime(() => aborted++));

        using (connections.Track(context))
        {
            Assert.Equal(1, connections.Count);
        }

        Assert.Equal(0, connections.Count);
        using var again = connections.Track(context);
        await rig.Services.GetRequiredService<IFenceSink>().AbortConnectionsAsync(CancellationToken.None);
        Assert.Equal(1, aborted);
    }

    private sealed class Lifetime(Action abort) : Microsoft.AspNetCore.Http.Features.IHttpRequestLifetimeFeature
    {
        public CancellationToken RequestAborted { get; set; }

        public void Abort() => abort();
    }
}
