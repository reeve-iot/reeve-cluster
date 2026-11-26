using System.Net;
using System.Net.WebSockets;
using System.Text.Json;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Membership;

namespace Unit.Http;

public sealed class HubTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly HttpRig rig = HttpRig.Create();

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    private static Seen Result(string completion)
    {
        using var message = JsonDocument.Parse(completion);
        Assert.False(message.RootElement.TryGetProperty("error", out var error), $"invocation failed: {error}");
        return message.RootElement.GetProperty("result").Deserialize<Seen>(new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
    }

    private static string Error(string completion)
    {
        using var message = JsonDocument.Parse(completion);
        return message.RootElement.GetProperty("error").GetString()!;
    }

    [Fact]
    public async Task An_invocation_on_a_serving_replica_has_a_scope_of_its_own_and_pins_after_the_read_index()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        var connected = rig.Store.Reads;
        var seen = Result(await hub.InvokeAsync("Plain"));

        Assert.Equal(new Seen("ClientRequest", 5), seen);
        Assert.Equal(connected + 1, rig.Store.Reads);
    }

    [Fact]
    public async Task Each_invocation_reads_the_head_again_and_pins_what_it_finds()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        Assert.Equal(5, Result(await hub.InvokeAsync("Plain")).PinnedHead);

        await rig.PublishAsync(8);
        await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Applied, 8));

        Assert.Equal(8, Result(await hub.InvokeAsync("Plain")).PinnedHead);
    }

    [Fact]
    public async Task An_invocation_that_does_not_reach_the_head_answers_replica_behind_and_does_not_run()
    {
        await rig.PublishAsync(3);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        rig.Store.Head = 9;

        var invocation = hub.InvokeAsync("Plain");
        await rig.ExpireReadIndexAsync();
        var error = Error(await invocation.WaitAsync(Wait));

        Assert.Contains("replica_behind", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_method_that_turned_the_wait_off_pins_without_reading()
    {
        await rig.PublishAsync(3);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        rig.Store.Head = 9;
        var connected = rig.Store.Reads;

        var seen = Result(await hub.InvokeAsync("Skipped"));

        Assert.Equal(new Seen("ClientRequest", 3), seen);
        Assert.Equal(connected, rig.Store.Reads);
    }

    [Fact]
    public async Task A_WebSocket_upgrade_to_a_hub_that_nothing_marks_is_refused_on_a_replica_that_is_not_serving()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.ToDraining();

        var exception = await Assert.ThrowsAnyAsync<Exception>(() => rig.ConnectHubAsync("/hub/closed"));

        Assert.Contains("503", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_negotiate_request_of_a_hub_that_nothing_marks_answers_503_on_a_fenced_replica()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.ToFenced();

        var response = await rig.Client.PostAsync("/hub/closed/negotiate?negotiateVersion=1", null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("1", response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.Contains("replica_fenced", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_replica_that_is_not_serving_accepts_the_connection_of_a_hub_with_a_marked_method_and_answers_only_the_marked_invocation()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();
        rig.ToFenced();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");

        var marked = Result(await hub.InvokeAsync("Marked"));
        var unmarked = Error(await hub.InvokeAsync("Plain"));

        Assert.Equal(new Seen("ExemptRequest", 3), marked);
        Assert.Contains("replica_fenced", unmarked, StringComparison.Ordinal);
        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task A_hub_marked_on_the_class_is_exempt_for_the_connection_and_every_method()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();
        rig.ToFenced();
        await using var hub = await rig.ConnectHubAsync("/hub/exempt");

        var seen = Result(await hub.InvokeAsync("Plain"));

        Assert.Equal(new Seen("ExemptRequest", 3), seen);
    }

    [Fact]
    public async Task An_exempt_invocation_on_a_serving_replica_waits_on_the_read_index_like_any_invocation()
    {
        await rig.PublishAsync(3);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        rig.Store.Head = 9;

        var invocation = hub.InvokeAsync("Marked");
        await rig.ExpireReadIndexAsync();
        var error = Error(await invocation.WaitAsync(Wait));

        Assert.Contains("replica_behind", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connection_that_stays_open_across_a_fence_answers_a_non_exempt_invocation_as_fenced_without_running_it()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");
        Assert.Equal(5, Result(await hub.InvokeAsync("Plain")).PinnedHead);

        rig.ToFencing();

        Assert.Contains("replica_fenced", Error(await hub.InvokeAsync("Plain")), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connection_that_stays_open_across_a_drain_answers_a_non_exempt_invocation_as_not_serving()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        await using var hub = await rig.ConnectHubAsync("/hub/probe");

        rig.ToDraining();

        Assert.Contains("replica_not_serving", Error(await hub.InvokeAsync("Plain")), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connections_request_pins_nothing_and_takes_no_ticket_and_OnConnectedAsync_does_not_read_the_head()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var tracker = (RequestTracker)rig.Services.GetService(typeof(RequestTracker))!;

        await using var hub = await rig.ConnectHubAsync("/hub/probe");

        using var bound = new CancellationTokenSource(Wait);
        while (tracker.Running != 0)
        {
            await Task.Delay(1, bound.Token);
        }

        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task A_hub_connection_admitted_through_a_marked_method_runs_its_lifecycle_as_a_client_request_not_as_exempt()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.ToFenced();
        var log = (LifecycleLog)rig.Services.GetService(typeof(LifecycleLog))!;

        var hub = await rig.ConnectHubAsync("/hub/lifecycle");

        Assert.Equal("ClientRequest", await log.Connected.WaitAsync(Wait));
        await hub.DisposeAsync();
        Assert.Equal("ClientRequest", await log.Disconnected.WaitAsync(Wait));
    }

    [Fact]
    public async Task OnDisconnectedAsync_runs_in_a_client_request_scope()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var log = (LifecycleLog)rig.Services.GetService(typeof(LifecycleLog))!;
        var hub = await rig.ConnectHubAsync("/hub/lifecycle");
        Assert.Equal("ClientRequest", await log.Connected.WaitAsync(Wait));

        await hub.DisposeAsync();

        Assert.Equal("ClientRequest", await log.Disconnected.WaitAsync(Wait));
    }

    [Fact]
    public async Task The_lifecycle_of_a_hub_marked_through_its_endpoint_runs_as_exempt()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var log = (LifecycleLog)rig.Services.GetService(typeof(LifecycleLog))!;
        var hub = await rig.ConnectHubAsync("/hub/lifecycle-marked");
        Assert.Equal("ExemptRequest", await log.Connected.WaitAsync(Wait));

        await hub.DisposeAsync();

        Assert.Equal("ExemptRequest", await log.Disconnected.WaitAsync(Wait));
    }

    [Fact]
    public async Task OnConnectedAsync_does_not_wait_on_the_read_index_so_a_replica_that_is_behind_keeps_the_connection_open()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();
        var log = (LifecycleLog)rig.Services.GetService(typeof(LifecycleLog))!;

        await using var hub = await rig.ConnectHubAsync("/hub/lifecycle");

        Assert.Equal("ClientRequest", await log.Connected.WaitAsync(Wait));
        Assert.Equal(0, rig.Store.Reads);
        var invocation = hub.InvokeAsync("Marked");
        await rig.ExpireReadIndexAsync();
        Assert.Contains("replica_behind", Error(await invocation.WaitAsync(Wait)), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_hub_marked_through_its_endpoint_is_exempt_for_the_connection_and_its_invocations()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.ToFenced();
        await using var hub = await rig.ConnectHubAsync("/hub/convention");

        var completion = await hub.InvokeAsync("Ping");

        Assert.Contains("pong", completion, StringComparison.Ordinal);
    }
}
