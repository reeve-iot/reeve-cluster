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

namespace Unit.Ask;

public sealed class AskReadyTenureTests : IDisposable
{
    private const long Epoch = 5;
    private const string Kind = "widget";
    private const string Key = "k";

    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);
    private readonly LocalPhaseGate gate;
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly HeldTenureFence fence = new();
    private readonly UnitRuntime runtime;
    private readonly AskInbound inbound;
    private readonly List<AskRequest> seen = [];

    public AskReadyTenureTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new Unit.Membership.RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var scopes = new ClusterScopeSource(new FenceSteps());
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var registered = new UnitKind(Kind, typeof(RecordingUnit), new UnitKindOptions { RedisFence = true });
        runtime = new UnitRuntime(
            [registered], services, rows, gate, scopes, Stores.Create("config"), NoFaultPoints.Instance, capabilities,
            new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), fence, Options.Create(options), time,
            NullLogger<UnitRuntime>.Instance);
        var handler = new LambdaHandler((request, _) =>
        {
            lock (seen)
            {
                seen.Add(request);
            }

            return Task.FromResult<ReadOnlyMemory<byte>>(request.Payload.ToArray());
        });
        inbound = new AskInbound(
            gate, scopes, runtime, new AskHandlers(new Dictionary<string, IAskHandler> { [Kind] = handler }, handler), NullLogger<AskInbound>.Instance);

        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    public void Dispose()
    {
        fence.Release();
        unit.ReleaseAll();
        fenceTimer.Release();
        runtime.Dispose();
        gate.Dispose();
    }

    private static AskDelivery Keyed(long token) => new(Kind, Key, token, new byte[] { 1 });

    [Fact]
    public async Task A_keyed_request_in_the_window_of_the_install_raise_is_declined_and_afterwards_is_delivered_with_the_tenure()
    {
        rows.Held = [new HeldRow(Kind, Key, 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var raised = await fence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        var early = await inbound.TakeAsync(Keyed(10), CancellationToken.None);

        Assert.False(early.IsTaken);
        Assert.Equal(DeclineReason.Absent, early.Reason);
        Assert.Empty(seen);

        fence.Release();
        var acquired = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var taking = await inbound.TakeAsync(Keyed(10), CancellationToken.None);

        Assert.True(taking.IsTaken);
        Assert.Equal(AskAnswerKind.Response, (await taking.Answer.WaitAsync(Wait)).Kind);
        var request = Assert.Single(seen);
        Assert.Same(raised, request.Owner);
        Assert.Same(acquired.Context, request.Owner);
        Assert.Equal((Kind, Key, 10L), (request.Owner!.Kind, request.Owner.Key, request.Owner.Token));
    }

    [Fact]
    public async Task A_request_to_a_replica_has_no_tenure()
    {
        var taking = await inbound.TakeAsync(new AskDelivery(null, null, null, new byte[] { 1 }), CancellationToken.None);

        Assert.True(taking.IsTaken);
        Assert.Equal(AskAnswerKind.Response, (await taking.Answer.WaitAsync(Wait)).Kind);
        var request = Assert.Single(seen);
        Assert.True(request.IsMemberRequest);
        Assert.Null(request.Owner);
    }
}
