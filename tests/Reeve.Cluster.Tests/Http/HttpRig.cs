using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Reeve.Cluster.Wakeup;
using Unit.Generations;

namespace Unit.Http;

public sealed record Seen(string? Kind, long? PinnedHead);

[ApiController]
public sealed class ProbeController(IClusterScopeAccessor scopes) : ControllerBase
{
    private Seen Now() => new(scopes.Current?.Kind.ToString(), scopes.Current?.PinnedGeneration?.Head);

    [HttpGet("/mvc/plain")]
    public Seen Plain() => Now();

    [HttpGet("/mvc/exempt")]
    [AllowWhenFenced]
    public Seen Exempt() => Now();

    [HttpGet("/mvc/skip")]
    [SkipReadIndex]
    public Seen Skip() => Now();

    [HttpGet("/mvc/exempt-skip")]
    [AllowWhenFenced]
    [SkipReadIndex]
    public Seen ExemptSkip() => Now();
}

[ApiController]
[AllowWhenFenced]
public sealed class ExemptController(IClusterScopeAccessor scopes) : ControllerBase
{
    [HttpGet("/mvc/class-exempt")]
    public Seen Action() => new(scopes.Current?.Kind.ToString(), scopes.Current?.PinnedGeneration?.Head);
}

public class ProbeHub(IClusterScopeAccessor scopes) : Hub
{
    private Seen Now() => new(scopes.Current?.Kind.ToString(), scopes.Current?.PinnedGeneration?.Head);

    public Seen Plain() => Now();

    [AllowWhenFenced]
    public Seen Marked() => Now();

    [SkipReadIndex]
    public Seen Skipped() => Now();
}

internal static class WriteScript
{
    public static int Run(IClusterScopeSource scopes, IQueryCollection query)
    {
        var scope = scopes.CurrentWriter!;
        foreach (var seq in query["seqs"].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            scope.RecordSeq(long.Parse(seq, System.Globalization.CultureInfo.InvariantCulture));
        }

        if (query.ContainsKey("refuse"))
        {
            scope.RecordRefusedCommit();
        }

        if (query.ContainsKey("optout"))
        {
            scope.OptOut();
        }

        if (query.ContainsKey("opentx"))
        {
            scope.TransactionOpened(new Unit.Log.FakeTransaction());
        }

        return query.TryGetValue("status", out var status) ? int.Parse(status.ToString(), System.Globalization.CultureInfo.InvariantCulture) : 200;
    }
}

[ApiController]
public sealed class WriteController(IServiceProvider services) : ControllerBase
{
    [HttpPost("/mvc/write")]
    public IActionResult Write() => StatusCode(WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query), "body");

    [HttpPost("/mvc/write-created")]
    public IActionResult WriteCreated()
    {
        WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query);
        return CreatedAtAction(nameof(WriteCreated), "created");
    }

    [HttpPost("/mvc/write-throw")]
    public IActionResult Throw()
    {
        WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query);
        Response.Headers["X-Handler"] = "set before the failure";
        throw new InvalidOperationException("the refusal");
    }

    [HttpPost("/mvc/write-empty")]
    public IActionResult WriteEmpty()
    {
        Response.StatusCode = WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query);
        return new EmptyResult();
    }

    [HttpPost("/mvc/write-iresult")]
    public IResult IResult() =>
        Microsoft.AspNetCore.Http.Results.StatusCode(WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query));

    [HttpPost("/mvc/write-redirect")]
    public IActionResult Redirected()
    {
        WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query);
        return Redirect("/elsewhere");
    }

    [HttpPost("/mvc/write-exempt")]
    [AllowWhenFenced]
    public IActionResult Exempt() => StatusCode(WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), Request.Query), "body");
}

public sealed class WriteHub(IServiceProvider services) : Hub
{
    public string Write(string script) => Run(script);

    [AllowWhenFenced]
    public string WriteExempt(string script) => Run(script);

    public string Throw(string script)
    {
        Run(script);
        throw new InvalidOperationException("the refusal");
    }

    private string Run(string script)
    {
        WriteScript.Run(services.GetRequiredService<IClusterScopeSource>(), new QueryCollection(Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(script)));
        return "done";
    }
}

internal sealed class ScriptedAck(IClusterScopeAccessor scopes) : IChangeAck
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<long> waits = new();
    private volatile TaskCompletionSource? hold;

    public Func<long, AckOutcome> Outcome { get; set; } = _ => AckOutcome.Acknowledged.None;

    public IReadOnlyList<long> Waits => [.. waits];

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyList<long> Seqs => scopes.Current?.Seqs ?? [];

    public void Hold() => hold = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Release() => hold?.TrySetResult();

    public void AcknowledgeAtCommit() => throw new NotSupportedException("The script opts out through the scope's writer.");

    public async Task<AckOutcome> WaitAppliedAsync(long seq, CancellationToken cancellationToken = default)
    {
        waits.Enqueue(seq);
        Entered.TrySetResult();
        if (hold is { } held)
        {
            await held.Task.WaitAsync(cancellationToken);
        }

        return Outcome(seq);
    }
}

internal sealed class ScriptedSkip : IChangeSkip
{
    public int Opens { get; private set; }
    public int Closes { get; private set; }

    public Task<SkipOpenResult> OpenAsync(SkipRequest request, CancellationToken cancellationToken = default)
    {
        Opens++;
        return Task.FromResult<SkipOpenResult>(new SkipOpenResult.Opened(1));
    }

    public Task<long> CloseAsync(long skipId, string closedBy, CancellationToken cancellationToken = default)
    {
        Closes++;
        return Task.FromResult(2L);
    }
}

[AllowWhenFenced]
public sealed class ExemptHub(IClusterScopeAccessor scopes) : Hub
{
    public Seen Plain() => new(scopes.Current?.Kind.ToString(), scopes.Current?.PinnedGeneration?.Head);
}

public sealed class LifecycleLog
{
    private readonly TaskCompletionSource<string?> connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<string?> disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<string?> Connected => connected.Task;

    public Task<string?> Disconnected => disconnected.Task;

    internal void Connect(string? kind) => connected.TrySetResult(kind);

    internal void Disconnect(string? kind) => disconnected.TrySetResult(kind);
}

public sealed class LifecycleHub(IClusterScopeAccessor scopes, LifecycleLog log) : Hub
{
    public override Task OnConnectedAsync()
    {
        log.Connect(scopes.Current?.Kind.ToString());
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        log.Disconnect(scopes.Current?.Kind.ToString());
        return base.OnDisconnectedAsync(exception);
    }

    [AllowWhenFenced]
    public string Marked() => "marked";
}

public sealed class ThrowingHub : Hub
{
    public override Task OnConnectedAsync() => throw new HubException("no");
}

public sealed class ClosedHub : Hub
{
    public string Ping() => "pong";
}

internal sealed class HttpRig : IAsyncDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

    public static readonly TimeSpan ReadIndexTimeout = TimeSpan.FromMilliseconds(400);

    private readonly WebApplication app;

    private HttpRig(Action<IServiceCollection>? before, Action<IServiceCollection>? configure, Action<ReeveClusterHttpOptions>? configureHttp, Action<IApplicationBuilder>? beforeFence, Action<IApplicationBuilder>? beforeCluster, Action<IApplicationBuilder>? afterFence, bool kestrel, params string[] types)
    {
        Generations = Stores.Create(types);
        Store = new FakeIndexStore();
        Points = new ProbeFaultPoints();
        Gate = new LocalPhaseGate(Timing, Time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        var options = new ClusterOptions { ReadIndexTimeout = ReadIndexTimeout, ApplyPollInterval = TimeSpan.FromMilliseconds(50), ProxyReadTimeout = TimeSpan.FromSeconds(60), ActionTimeBudget = TimeSpan.FromSeconds(30) };
        Cluster = options;
        Channel = new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance);
        Index = new ReadIndex(Gate, Generations, Store, Channel, Microsoft.Extensions.Options.Options.Create(options), Time, Points, NoModelEvents.Instance, NullLogger<ReadIndex>.Instance);
        Scopes = new ClusterScopeSource(new FenceSteps());

        var builder = WebApplication.CreateSlimBuilder();
        if (kestrel)
        {
            builder.WebHost.UseKestrel(o => o.Listen(System.Net.IPAddress.Loopback, 0));
        }
        else
        {
            builder.WebHost.UseTestServer();
        }

        builder.Logging.ClearProviders();
        before?.Invoke(builder.Services);
        builder.Services.AddSingleton<IPhaseGate>(Gate);
        builder.Services.AddSingleton<IGenerationSource>(Generations);
        builder.Services.AddSingleton(Index);
        builder.Services.AddSingleton<IClusterScopeSource>(Scopes);
        builder.Services.AddSingleton<IClusterScopeAccessor>(Scopes);
        builder.Services.AddSingleton<IFaultPoints>(Points);
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(Microsoft.Extensions.Options.Options.Create(options));
        builder.Services.AddSingleton<ScriptedAck>();
        builder.Services.AddSingleton<IChangeAck>(sp => sp.GetRequiredService<ScriptedAck>());
        builder.Services.AddSingleton<ScriptedSkip>();
        builder.Services.AddSingleton<IChangeSkip>(sp => sp.GetRequiredService<ScriptedSkip>());
        builder.Services.AddReeveClusterAspNetCore(configureHttp);
        builder.Services.AddControllers().AddApplicationPart(typeof(ProbeController).Assembly);
        builder.Services.AddSignalR();
        builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);
        builder.Services.AddSingleton<LifecycleLog>();
        configure?.Invoke(builder.Services);

        builder.Services.AddSingleton<IClusterStatus>(sp => new ClusterStatus(
            sp.GetRequiredService<IStatusStore>(), sp.GetRequiredService<IOptions<ClusterOptions>>(), sp.GetRequiredService<TimeProvider>()));

        app = builder.Build();
        if (kestrel)
        {
            app.UseWebSockets();
        }

        beforeCluster?.Invoke(app);
        beforeFence?.Invoke(app);
        app.UseReeveCluster();
        afterFence?.Invoke(app);
        app.MapControllers();
        Seen Now(HttpContext c) => new(c.RequestServices.GetRequiredService<IClusterScopeAccessor>().Current?.Kind.ToString(), c.RequestServices.GetRequiredService<IClusterScopeAccessor>().Current?.PinnedGeneration?.Head);
        app.MapGet("/min/plain", Now);
        app.MapGet("/min/exempt", Now).AllowWhenFenced();
        app.MapGet("/min/attribute", [AllowWhenFenced] (HttpContext c) => Now(c));
        var group = app.MapGroup("/min/off").SkipReadIndex();
        group.MapGet("/plain", Now);
        group.MapGet("/exempt", Now).AllowWhenFenced();
        app.MapGet("/min/marker", () => HttpRequestFlow.IsInside.ToString().ToLowerInvariant());
        app.MapGet("/min/capture", (HttpContext c) =>
        {
            Captured.Enqueue(c.RequestServices.GetRequiredService<IClusterScopeAccessor>().Current!);
            return Now(c);
        });
        app.MapGet("/min/slow", async (HttpContext c) =>
        {
            Entered.TrySetResult();
            await Finish.Task;
            return Now(c);
        });
        app.MapPost("/min/write", (HttpContext c) => Results.StatusCode(WriteScript.Run(c.RequestServices.GetRequiredService<IClusterScopeSource>(), c.Request.Query))).AcknowledgeWrites();
        app.MapReeveClusterSkips("/skip-unprotected");
        app.MapReeveClusterSkips("/skip-anonymous").AllowAnonymous();
        app.MapReeveClusterSkips("/skip-authorized").RequireAuthorization();
        app.MapPost("/min/write-bare-body", async (HttpContext c) =>
        {
            c.Response.ContentType = "text/plain";
            c.Response.StatusCode = WriteScript.Run(c.RequestServices.GetRequiredService<IClusterScopeSource>(), c.Request.Query);
            await c.Response.WriteAsync("handler body");
        });
        app.MapPost("/min/write-bare", (HttpContext c) => Results.StatusCode(WriteScript.Run(c.RequestServices.GetRequiredService<IClusterScopeSource>(), c.Request.Query)));
        app.MapPost("/min/union", (HttpContext c) =>
            {
                var status = WriteScript.Run(c.RequestServices.GetRequiredService<IClusterScopeSource>(), c.Request.Query);
                return status == 409
                    ? (Microsoft.AspNetCore.Http.HttpResults.Results<Microsoft.AspNetCore.Http.HttpResults.Ok<string>, Microsoft.AspNetCore.Http.HttpResults.Conflict<string>>)TypedResults.Conflict("conflict")
                    : TypedResults.Ok("ok");
            }).AcknowledgeWrites();
        var acknowledged = app.MapGroup("/min/group").AcknowledgeWrites();
        acknowledged.MapPost("/write", (HttpContext c) => Results.StatusCode(WriteScript.Run(c.RequestServices.GetRequiredService<IClusterScopeSource>(), c.Request.Query))).AcknowledgeWrites();
        app.MapHub<ThrowingHub>("/hub/throwing");
        app.MapHub<WriteHub>("/hub/write");
        app.MapHub<ProbeHub>("/hub/probe");
        app.MapHub<ExemptHub>("/hub/exempt");
        app.MapHub<ClosedHub>("/hub/closed");
        app.MapHub<ClosedHub>("/hub/convention").AllowWhenFenced();
        app.MapHub<LifecycleHub>("/hub/lifecycle");
        app.MapHub<LifecycleHub>("/hub/lifecycle-marked").AllowWhenFenced();
        app.Map("/min/ws", async (HttpContext c) =>
        {
            using var socket = await c.WebSockets.AcceptWebSocketAsync();
            WsOpen.TrySetResult();
            await Finish.Task;
        });
        app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") }).AllowWhenFenced().SkipReadIndex();
        app.StartAsync().GetAwaiter().GetResult();
        if (kestrel)
        {
            BaseAddress = new Uri(app.Urls.Single());
            Client = new HttpClient { BaseAddress = BaseAddress };
        }
        else
        {
            server = app.GetTestServer();
            BaseAddress = server.BaseAddress;
            Client = server.CreateClient();
        }
    }

    private readonly TestServer? server;

    public Uri BaseAddress { get; }

    public FakeTimeProvider Time { get; } = new();

    public GenerationStore Generations { get; }

    public FakeIndexStore Store { get; }

    public ProbeFaultPoints Points { get; }

    public ScriptedAck Ack => Services.GetRequiredService<ScriptedAck>();

    public ScriptedSkip Skips => Services.GetRequiredService<ScriptedSkip>();

    public LocalPhaseGate Gate { get; }

    public ClusterOptions Cluster { get; }

    public InProcessWakeupChannel Channel { get; }

    public ReadIndex Index { get; }

    public ClusterScopeSource Scopes { get; }

    public TestServer Server => server ?? throw new InvalidOperationException("This rig runs on Kestrel.");

    public HttpClient Client { get; }

    public IServiceProvider Services => app.Services;

    public System.Collections.Concurrent.ConcurrentQueue<IClusterScope> Captured { get; } = new();

    public TaskCompletionSource WsOpen { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static HttpRig Create(
        Action<IServiceCollection>? configure = null, Action<ReeveClusterHttpOptions>? configureHttp = null, Action<IApplicationBuilder>? afterFence = null,
        Action<IServiceCollection>? before = null, Action<IApplicationBuilder>? beforeFence = null, Action<IApplicationBuilder>? beforeCluster = null) =>
        new(before, configure, configureHttp, beforeFence, beforeCluster, afterFence, kestrel: false, "a", "b");

    public static HttpRig CreateKestrel(Action<IServiceCollection>? configure = null) =>
        new(null, configure, null, null, null, null, kestrel: true, "a", "b");

    public async Task<IGeneration> PublishAsync(long head, bool alsoHead = true)
    {
        if (alsoHead)
        {
            Store.Head = head;
        }

        return await Generations.PublishAsync(head, Token(head), ("a", $"a{head}"), ("b", $"b{head}"));
    }

    public static Guid Token(long seq) => new((int)seq, 0, 0, [0, 0, 0, 0, 0, 0, 0, 0]);

    public void ToServing()
    {
        ToCatchingUp();
        Gate.Apply(MemberTrigger.ActivationCommitted);
        Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, Gate.Phase);
    }

    public void ToLoading()
    {
        Assert.True(Gate.Joined(1, 1, Gate.Now));
        Assert.Equal(ReplicaPhase.Loading, Gate.Phase);
    }

    public void ToCatchingUp()
    {
        ToLoading();
        Gate.Apply(MemberTrigger.StateLoaded);
    }

    public void ToActivated()
    {
        ToCatchingUp();
        Gate.Apply(MemberTrigger.ActivationCommitted);
    }

    public void ToFencing() => Gate.RequestFence(new FenceRequest(FenceTrigger.ApplierStall));

    public void ToFenced()
    {
        ToFencing();
        Gate.CompleteFence();
        Assert.Equal(ReplicaPhase.Fenced, Gate.Phase);
    }

    public void ToDraining() => Gate.Apply(MemberTrigger.GracefulStop);

    public async Task ReadsAsync(int count)
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (Store.Reads < count)
        {
            await Task.Delay(1, bound.Token);
        }
    }

    public async Task ExpireReadIndexAsync(int after = 0)
    {
        await ReadsAsync(after + 1);
        Time.Advance(ReadIndexTimeout);
    }

    public ValueTask NotifyAppliedAsync(long head) => Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Applied, head));

    public void PassFenceStart() => Time.Advance(TimeSpan.FromSeconds(23));

    public Task<HttpResponseMessage> GetAsync(string path) => Client.GetAsync(path);

    public async Task<RawHub> ConnectHubAsync(string path)
    {
        var socket = await Server.CreateWebSocketClient().ConnectAsync(new Uri(Server.BaseAddress, path), CancellationToken.None);
        var hub = new RawHub(socket);
        await hub.SendAsync("""{"protocol":"json","version":1}""");
        Assert.Equal("{}", await hub.ReceiveAsync());
        return hub;
    }

    public async ValueTask DisposeAsync()
    {
        Finish.TrySetResult();
        Client.Dispose();
        await app.DisposeAsync();
        Index.Dispose();
        Gate.Dispose();
    }
}

internal sealed class FakeIndexStore : IReadIndexStore
{
    private int reads;

    public long Head { get; set; }

    public int Reads => Volatile.Read(ref reads);

    public Task<ReadIndexRead> ReadAsync(Cursor cursor, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref reads);
        return Task.FromResult(new ReadIndexRead(Head, CursorValid: true));
    }
}

internal sealed class ProbeFaultPoints : IFaultPoints
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> reached = new();
    private readonly TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile string? held;

    public IReadOnlyList<string> Reached => [.. reached];

    public Task Arrived => arrived.Task;

    public void Hold(FaultPoint point) => held = point.Name;

    public void Release() => released.TrySetResult();

    public async ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        reached.Enqueue(point);
        if (point == held)
        {
            arrived.TrySetResult();
            await released.Task.WaitAsync(cancellationToken);
        }
    }
}

internal sealed class RawHub(WebSocket socket) : IAsyncDisposable
{
    private const char Separator = '\u001e';
    private readonly StringBuilder pending = new();
    private int invocations;

    public Task SendAsync(string json) =>
        socket.SendAsync(Encoding.UTF8.GetBytes(json + Separator), WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);

    public async Task<string> ReceiveAsync()
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
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

            var result = await socket.ReceiveAsync(buffer, bound.Token);
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
        await SendAsync($$"""{"type":1,"invocationId":"{{id}}","target":"{{target}}","arguments":[{{json}}]}""");
        while (true)
        {
            var message = await ReceiveAsync();
            if (message.Contains("\"type\":3", StringComparison.Ordinal))
            {
                return message;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }
        catch (WebSocketException)
        {
        }

        socket.Dispose();
    }
}
