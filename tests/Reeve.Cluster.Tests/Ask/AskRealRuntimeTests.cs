using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Generations;
using Unit.Membership;
using Unit.Owned;
using Unit.Testing;
using static Unit.Ask.ScriptedTransport;

namespace Unit.Ask;

public sealed class AskRealRuntimeTests : IDisposable
{
    private const long Epoch = 5;
    private const string Kind = "widget";
    private const string Key = "k";

    private static readonly TimeSpan Wait = Bounds.Safety;
    private static readonly byte[] Request = [9];

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);
    private readonly LocalPhaseGate gate;
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly ScriptedDirectory directory = new();
    private readonly ScriptedTransport transport = new();
    private readonly MemberIdentity self = new(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
    private readonly UnitRuntime runtime;
    private readonly ClusterAsk ask;
    private readonly TaskCompletionSource handlerStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public AskRealRuntimeTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new Unit.Membership.RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var scopes = new ClusterScopeSource(new FenceSteps());
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var registered = new UnitKind(Kind, typeof(RecordingUnit), new UnitKindOptions());
        runtime = new UnitRuntime(
            [registered], services, rows, gate, scopes, Stores.Create("config"), NoFaultPoints.Instance, capabilities,
            new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), new NoTenureFence(), Options.Create(options), time,
            NullLogger<UnitRuntime>.Instance);
        var echo = new LambdaHandler(async (request, ct) =>
        {
            if (request.Payload.Length > 1)
            {
                handlerStarted.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
            }

            return request.Payload.ToArray();
        });
        var inbound = new AskInbound(
            gate, scopes, runtime, new AskHandlers(new Dictionary<string, IAskHandler> { [Kind] = echo }, null), NullLogger<AskInbound>.Instance);
        ask = new ClusterAsk(directory, inbound, transport, self, [registered], Options.Create(options), time, NullLogger<ClusterAsk>.Instance);

        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        fenceTimer.Release();
        runtime.Dispose();
        gate.Dispose();
    }

    private AskOwner Mine(long token) => new("r1", self.ProcessId, token);

    private async Task AdvanceAsync(Task<AskResult> call, TimeSpan total)
    {
        for (var moved = TimeSpan.Zero; moved < total && !call.IsCompleted; moved += TimeSpan.FromMilliseconds(100))
        {
            await Task.Delay(1);
            time.Advance(TimeSpan.FromMilliseconds(100));
        }
    }

    private async Task StartTenureAsync(long token)
    {
        rows.Held = [new HeldRow(Kind, Key, token, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
    }

    private async Task StopTenureHangingAsync(long token)
    {
        unit.HangLost(Key);
        rows.Held = [new HeldRow(Kind, Key, token, true)];
        await runtime.PassAsync(CancellationToken.None);
    }

    [Fact]
    public async Task During_a_handover_the_caller_waits_and_is_answered_by_the_new_owner()
    {
        await StartTenureAsync(10);
        await StopTenureHangingAsync(10);
        directory.Otherwise = Mine(10);
        transport.Then(Answers(1));

        var call = ask.AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(10));
        await AdvanceAsync(call, TimeSpan.FromSeconds(1.5));

        Assert.False(call.IsCompleted);

        unit.ReleaseLost(Key);
        directory.Otherwise = new AskOwner("r2", Guid.NewGuid(), 11);
        await AdvanceAsync(call, TimeSpan.FromSeconds(2));

        var result = await call.WaitAsync(Wait);
        Assert.Equal(AskOutcome.Answered, result.Outcome);
        var (target, delivery) = Assert.Single(transport.Deliveries);
        Assert.Equal(("r2", 11L), (target.ReplicaId, delivery.Token!.Value));
    }

    [Fact]
    public async Task A_handover_that_does_not_end_within_the_wait_is_NoOwner_and_the_stopping_owner_was_asked_once()
    {
        await StartTenureAsync(10);
        await StopTenureHangingAsync(10);
        directory.Otherwise = Mine(10);

        var call = ask.AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(3));
        await AdvanceAsync(call, TimeSpan.FromSeconds(6));

        Assert.Equal(AskOutcome.NoOwner, (await call.WaitAsync(Wait)).Outcome);
        Assert.Empty(transport.Deliveries);
    }

    [Fact]
    public async Task A_tenure_that_has_not_started_yet_is_given_time_to_start_and_then_answers()
    {
        directory.Otherwise = Mine(10);

        var call = ask.AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(10));
        using var bound = new CancellationTokenSource(Wait);
        while (directory.OwnerResolutions < 2)
        {
            await Task.Delay(1, bound.Token);
        }

        Assert.False(call.IsCompleted);
        await StartTenureAsync(10);
        await AdvanceAsync(call, TimeSpan.FromSeconds(3));

        var result = await call.WaitAsync(Wait);
        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(Request, result.Response.ToArray());
    }

    [Fact]
    public async Task A_tenure_that_stops_under_a_running_handler_ends_the_call_OwnerLost()
    {
        await StartTenureAsync(10);
        directory.Otherwise = Mine(10);

        var call = ask.AskAsync(Kind, Key, new byte[] { 1, 2 }, TimeSpan.FromSeconds(10));
        await handlerStarted.Task.WaitAsync(Wait);
        await StopTenureHangingAsync(10);

        Assert.Equal(AskOutcome.OwnerLost, (await call.WaitAsync(Wait)).Outcome);
    }

    [Fact]
    public async Task The_runtime_reports_a_running_tenure_by_kind_key_and_token()
    {
        await StartTenureAsync(10);

        Assert.True(runtime.TryGetRunning(Kind, Key, 10, out var stopped, out _));
        Assert.False(stopped.IsCancellationRequested);
        Assert.False(runtime.TryGetRunning(Kind, Key, 11, out _, out _));
        Assert.False(runtime.TryGetRunning(Kind, "other", 10, out _, out _));
        Assert.False(runtime.TryGetRunning("other", Key, 10, out _, out _));
    }

    [Fact]
    public async Task The_runtime_does_not_report_a_tenure_whose_stop_has_begun()
    {
        await StartTenureAsync(10);
        Assert.True(runtime.TryGetRunning(Kind, Key, 10, out var stopped, out _));

        await StopTenureHangingAsync(10);

        Assert.True(stopped.IsCancellationRequested);
        Assert.False(runtime.TryGetRunning(Kind, Key, 10, out _, out _));
    }
}
