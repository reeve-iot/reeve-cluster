using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Core;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Membership;

namespace Unit.Http;

public sealed class PackageTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly HttpRig rig = HttpRig.Create();

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    [Fact]
    public async Task The_readiness_endpoint_is_healthy_only_while_the_replica_serves()
    {
        await rig.PublishAsync(5);
        rig.ToCatchingUp();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await rig.GetAsync("/health/ready")).StatusCode);

        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await rig.GetAsync("/health/ready")).StatusCode);

        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(HttpStatusCode.OK, (await rig.GetAsync("/health/ready")).StatusCode);

        rig.ToFencing();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await rig.GetAsync("/health/ready")).StatusCode);
    }

    [Fact]
    public async Task Readiness_names_the_phase_and_every_reason_the_registry_holds_and_a_reason_alone_makes_a_serving_replica_unready()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var check = new ReadinessCheck(rig.Gate, []);
        var context = new HealthCheckContext();

        var healthy = await check.CheckHealthAsync(context);
        rig.Gate.Readiness.Raise(new ReadinessReason("replication.not-synchronous", "The standby is not synchronous."));
        var unready = await check.CheckHealthAsync(context);
        rig.Gate.Readiness.Raise(new ReadinessReason("joining.refused", "The floor refused the join."));
        var two = await check.CheckHealthAsync(context);
        rig.Gate.Readiness.Clear("replication.not-synchronous");
        rig.Gate.Readiness.Clear("joining.refused");
        var cleared = await check.CheckHealthAsync(context);

        Assert.Equal(HealthStatus.Healthy, healthy.Status);
        Assert.Equal(HealthStatus.Unhealthy, unready.Status);
        Assert.Contains("replication.not-synchronous", unready.Description, StringComparison.Ordinal);
        Assert.Equal("The standby is not synchronous.", unready.Data["replication.not-synchronous"]);
        Assert.Contains("joining.refused", two.Description, StringComparison.Ordinal);
        Assert.Contains("replication.not-synchronous", two.Description, StringComparison.Ordinal);
        Assert.Equal(HealthStatus.Healthy, cleared.Status);
    }

    [Fact]
    public async Task Readiness_of_a_replica_that_is_not_serving_names_its_phase()
    {
        await rig.PublishAsync(5);
        rig.ToActivated();

        var result = await new ReadinessCheck(rig.Gate, []).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("Activated", result.Description, StringComparison.Ordinal);
        Assert.Equal("Activated", result.Data["phase"]);
    }

    [Fact]
    public async Task A_paused_replica_whose_fence_start_passed_reports_unready_at_its_first_probe_and_starts_the_fence()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.PassFenceStart();

        var result = await new ReadinessCheck(rig.Gate, []).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
    }

    [Fact]
    public void The_tracker_waits_at_a_drain_for_the_requests_running_at_step_1_and_not_for_the_ones_after()
    {
        var tracker = new RequestTracker();
        var before = tracker.Enter();
        var ended = tracker.Enter();
        ended.Dispose();

        Assert.Equal(1, tracker.CloseAtDrain());
        var after = tracker.Enter();

        Assert.Single(tracker.RunningAtMark());
        Assert.False(tracker.RunningAtMark().Single().IsCompleted);
        before.Dispose();
        Assert.True(tracker.RunningAtMark().Count == 0 && tracker.Running == 1);
        after.Dispose();
        Assert.Equal(0, tracker.Running);
    }

    [Fact]
    public void The_tracker_has_nothing_to_wait_for_before_step_1()
    {
        var tracker = new RequestTracker();
        using var running = tracker.Enter();

        Assert.Empty(tracker.RunningAtMark());
    }

    [Fact]
    public async Task A_running_request_is_counted_and_the_drains_step_4_waits_for_it_until_it_ends()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var tracker = (RequestTracker)rig.Services.GetRequiredService(typeof(RequestTracker));
        var request = rig.GetAsync("/min/slow");
        await rig.Entered.Task.WaitAsync(Wait);
        Assert.Equal(1, tracker.Running);

        var participants = rig.Services.GetServices<IDrainParticipant>().ToDictionary(p => p.Step);
        rig.ToDraining();
        await participants[DrainStep.CloseTraffic].RunAsync(new DrainContext(), CancellationToken.None);
        var waiting = participants[DrainStep.AwaitRequests].RunAsync(new DrainContext(), CancellationToken.None).AsTask();
        Assert.False(waiting.IsCompleted, "step 4 waits for the request running at step 1");

        rig.Finish.SetResult();
        await waiting.WaitAsync(Wait);
        Assert.Equal(HttpStatusCode.OK, (await request).StatusCode);
        Assert.Equal(0, tracker.Running);
    }

    [Fact]
    public async Task A_request_that_has_passed_the_serving_check_and_is_held_before_its_handler_is_counted_and_waited_for()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.Points.Hold(HttpFaultPoints.BetweenServingCheckAndHandler);
        var participants = rig.Services.GetServices<IDrainParticipant>().ToDictionary(p => p.Step);
        var request = rig.GetAsync("/mvc/plain");
        await rig.Points.Arrived.WaitAsync(Wait);

        rig.ToDraining();
        await participants[DrainStep.CloseTraffic].RunAsync(new DrainContext(), CancellationToken.None);
        var waiting = participants[DrainStep.AwaitRequests].RunAsync(new DrainContext(), CancellationToken.None).AsTask();
        Assert.False(waiting.IsCompleted, "step 4 waits for a request admitted before the drain, though its handler has not started");

        rig.Points.Release();
        await waiting.WaitAsync(Wait);
        await request.WaitAsync(Wait);
    }

    [Fact]
    public async Task An_open_raw_WebSocket_is_not_a_request_that_drain_step_4_waits_for()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var tracker = (RequestTracker)rig.Services.GetRequiredService(typeof(RequestTracker));

        using var socket = await rig.Server.CreateWebSocketClient().ConnectAsync(new Uri(rig.Server.BaseAddress, "/min/ws"), CancellationToken.None);
        await rig.WsOpen.Task.WaitAsync(Wait);

        Assert.Equal(0, tracker.Running);
    }

    [Fact]
    public async Task Step_4_gives_up_at_ProxyReadTimeout_and_leaves_the_request_running()
    {
        var time = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        var tracker = new RequestTracker();
        var options = Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ProxyReadTimeout = TimeSpan.FromSeconds(30) });
        var step4 = new HttpDrainAwaitParticipant(tracker, new ClusterConnections(), options, time, NullLogger<HttpDrainAwaitParticipant>.Instance);
        using var running = tracker.Enter();
        tracker.CloseAtDrain();

        var waiting = step4.RunAsync(new DrainContext(), CancellationToken.None).AsTask();
        Assert.False(waiting.IsCompleted);
        time.Advance(TimeSpan.FromSeconds(29));
        Assert.False(waiting.IsCompleted);
        time.Advance(TimeSpan.FromSeconds(2));

        await waiting.WaitAsync(Wait);
        Assert.Equal(1, tracker.Running);
    }

    [Fact]
    public async Task Step_4_aborts_the_connections_that_stay_open_once_the_running_requests_have_finished()
    {
        var tracker = new RequestTracker();
        var connections = new ClusterConnections();
        var aborted = 0;
        using var socket = connections.Track(new object(), () => aborted++);
        var options = Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ProxyReadTimeout = TimeSpan.FromSeconds(30) });
        var step4 = new HttpDrainAwaitParticipant(tracker, connections, options, new Microsoft.Extensions.Time.Testing.FakeTimeProvider(), NullLogger<HttpDrainAwaitParticipant>.Instance);
        var running = tracker.Enter();
        tracker.CloseAtDrain();

        var waiting = step4.RunAsync(new DrainContext(), CancellationToken.None).AsTask();
        Assert.Equal(0, aborted);
        running.Dispose();

        await waiting.WaitAsync(Wait);
        Assert.Equal(1, aborted);
        Assert.Equal(0, connections.Count);
    }

    [Fact]
    public async Task Step_4_does_not_wait_for_a_response_the_application_tracks_as_an_open_connection()
    {
        var tracker = new RequestTracker();
        var connections = new ClusterConnections();
        var time = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        var options = Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ProxyReadTimeout = TimeSpan.FromSeconds(30) });
        var step4 = new HttpDrainAwaitParticipant(tracker, connections, options, time, NullLogger<HttpDrainAwaitParticipant>.Instance);
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
        var abortable = new AbortableLifetime();
        context.Features.Set<Microsoft.AspNetCore.Http.Features.IHttpRequestLifetimeFeature>(abortable);
        using var ticket = tracker.Enter();
        context.Items[RequestTracker.TicketKey] = ticket;
        using var stream = connections.Track(context);
        tracker.CloseAtDrain();

        await step4.RunAsync(new DrainContext(), CancellationToken.None).AsTask().WaitAsync(Wait);

        Assert.True(abortable.Aborted);
        Assert.Equal(0, connections.Count);
    }

    [Fact]
    public async Task A_request_that_began_after_step_1_is_refused_and_leaves_nothing_for_step_4()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        var tracker = (RequestTracker)rig.Services.GetRequiredService(typeof(RequestTracker));
        rig.ToDraining();
        tracker.CloseAtDrain();

        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await rig.GetAsync("/mvc/plain")).StatusCode);

        Assert.Equal(0, tracker.Running);
        Assert.Empty(tracker.RunningAtMark());
    }

    [Fact]
    public void The_package_registers_one_participant_for_fence_step_1_and_for_drain_steps_1_and_4()
    {
        var fence = rig.Services.GetServices<IFenceParticipant>();
        var drain = rig.Services.GetServices<IDrainParticipant>();

        Assert.Equal(FenceStep.CloseTraffic, Assert.Single(fence).Step);
        Assert.Equal([DrainStep.CloseTraffic, DrainStep.AwaitRequests], drain.Select(p => p.Step).Order());
    }

    [Theory]
    [InlineData("Reeve-", true)]
    [InlineData("Acme-Cluster-", true)]
    [InlineData("X-", true)]
    [InlineData("", false)]
    [InlineData("Reeve", false)]
    [InlineData("Re eve-", false)]
    [InlineData("Reeve_-", false)]
    public void The_header_prefix_must_make_a_header_name_with_each_suffix(string prefix, bool valid)
    {
        var options = new ReeveClusterHttpOptions { HeaderPrefix = prefix };

        Assert.Equal(valid, options.Problems().Count == 0);
        if (valid)
        {
            Assert.Equal(prefix + "Seq", options.HeaderName("Seq"));
        }
    }

    [Fact]
    public void The_default_header_prefix_is_Reeve_and_an_invalid_one_refuses_to_start()
    {
        Assert.Equal("Reeve-", new ReeveClusterHttpOptions().HeaderPrefix);
        var services = new ServiceCollection().AddReeveClusterAspNetCore(o => o.HeaderPrefix = "bad prefix");
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ReeveClusterHttpOptions>>().Value);
    }

    [Fact]
    public void Registering_the_package_tells_the_validator_that_AspNetCore_is_in_use()
    {
        var plain = new ServiceCollection().AddOptions<ClusterOptions>().Services;
        Assert.Empty(plain.BuildServiceProvider().GetServices<AspNetCoreMarker>());

        var services = new ServiceCollection().AddReeveClusterAspNetCore();

        Assert.Single(services.BuildServiceProvider().GetServices<AspNetCoreMarker>());
        var validation = new ClusterOptionsValidation([new AspNetCoreMarker()], [], []);
        var withoutBound = new ClusterOptions { ReplicaId = "r1", ConnectionString = "Host=x", ReconcileTimeout = TimeSpan.FromSeconds(1) };
        Assert.True(validation.Validate(null, withoutBound).Failed);
    }

    [Fact]
    public void The_packages_exception_handler_is_registered_once_and_first()
    {
        var services = new ServiceCollection();
        services.AddExceptionHandler<NoExceptionHandler>();
        services.AddReeveClusterAspNetCore();
        services.AddReeveClusterAspNetCore();
        services.AddExceptionHandler<NoExceptionHandler>();

        var handlers = services.Where(d => d.ServiceType == typeof(Microsoft.AspNetCore.Diagnostics.IExceptionHandler)).Select(d => d.ImplementationType).ToArray();

        Assert.Equal([typeof(RefusedRequestExceptionHandler), typeof(NoExceptionHandler), typeof(NoExceptionHandler)], handlers);
    }

    [Fact]
    public void A_keyed_exception_handler_registered_before_the_package_is_left_alone()
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<Microsoft.AspNetCore.Diagnostics.IExceptionHandler, NoExceptionHandler>("k");
        services.AddReeveClusterAspNetCore();
        services.AddReeveClusterAspNetCore();

        var handlers = services.Where(d => d.ServiceType == typeof(Microsoft.AspNetCore.Diagnostics.IExceptionHandler)).ToArray();

        Assert.Equal(2, handlers.Length);
        Assert.Equal(typeof(RefusedRequestExceptionHandler), handlers[0].ImplementationType);
        Assert.True(handlers[1].IsKeyedService);
    }

    private sealed class NoExceptionHandler : Microsoft.AspNetCore.Diagnostics.IExceptionHandler
    {
        public ValueTask<bool> TryHandleAsync(Microsoft.AspNetCore.Http.HttpContext httpContext, Exception exception, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }
}

internal sealed class AbortableLifetime : Microsoft.AspNetCore.Http.Features.IHttpRequestLifetimeFeature
{
    public bool Aborted { get; private set; }

    public CancellationToken RequestAborted { get; set; }

    public void Abort() => Aborted = true;
}
