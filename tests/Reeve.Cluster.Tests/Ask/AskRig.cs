using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Generations;
using Unit.Testing;

namespace Unit.Ask;

internal sealed class AskRig : IDisposable
{
    public const string Kind = "scheduler";
    public const string Key = "k1";

    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);

    public AskRig(TimeProvider askTime, IAskHandler? member, IAskHandler? keyed, ScriptedDirectory? directory = null, IAskTransport? transport = null, ILogger<AskInbound>? inboundLog = null)
    {
        var timing = new LeaseTiming(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
        fenceTimer.Arm();
        Gate = new LocalPhaseGate(timing, GateTime, NoModelEvents.Instance, fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        Assert.True(Gate.Joined(1, 1, Gate.Now));
        Gate.Apply(MemberTrigger.StateLoaded);
        Gate.Apply(MemberTrigger.ActivationCommitted);
        Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, Gate.Phase);

        Scopes = new ClusterScopeSource(new FenceSteps());
        Directory = directory ?? new ScriptedDirectory();
        Self = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var keyedHandlers = keyed is null ? new Dictionary<string, IAskHandler>() : new Dictionary<string, IAskHandler> { [Kind] = keyed };
        Inbound = new AskInbound(Gate, Scopes, Tenures, new AskHandlers(keyedHandlers, member), inboundLog ?? NullLogger<AskInbound>.Instance);
        Ask = new ClusterAsk(
            Directory, Inbound, transport ?? new ScriptedTransport(), Self, [], Options.Create(new ClusterOptions { ReplicaId = "a" }), askTime,
            NullLogger<ClusterAsk>.Instance);
    }

    public CountingTimeProvider GateTime { get; } = new();

    public LocalPhaseGate Gate { get; }

    public ClusterScopeSource Scopes { get; }

    public ScriptedDirectory Directory { get; }

    public MemberIdentity Self { get; }

    public FakeTenures Tenures { get; } = new();

    public AskInbound Inbound { get; }

    public ClusterAsk Ask { get; }

    public AskOwner Mine(long token) => new("a", Self.ProcessId, token);

    public AskMemberRow MyRow => new(Self.ProcessId, MemberRowState.Active);

    public void Dispose()
    {
        fenceTimer.Release();
        Gate.Dispose();
    }
}
