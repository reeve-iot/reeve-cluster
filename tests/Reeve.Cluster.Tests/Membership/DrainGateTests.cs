using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class DrainGateTests : IDisposable
{
    private const long Epoch = 7;

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly LocalPhaseGate gate;
    private readonly List<string> order = [];

    public DrainGateTests()
    {
        gate = new LocalPhaseGate(new LeaseTiming(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2)), time, events, NullLogger<LocalPhaseGate>.Instance);
        gate.Subscribe(e => order.Add($"{e.Kind}:{e.To}:passed={gate.DrainStep4Passed}:events={events.All.Count(m => m.Action == MembershipActions.Drain4)}"));
    }

    public void Dispose() => gate.Dispose();

    private void ToServing()
    {
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        order.Clear();
    }

    [Fact]
    public void M12_the_stop_from_Serving_enters_Draining_and_emits_Drain1_for_the_epoch()
    {
        ToServing();

        var cell = gate.Apply(MemberTrigger.GracefulStop);

        Assert.Equal("M12", cell.RowId);
        Assert.Equal(ReplicaPhase.Draining, gate.Phase);
        var drain1 = Assert.Single(events.All, e => e.Action == MembershipActions.Drain1);
        Assert.Equal(Epoch, drain1.State!["epoch"]);
        Assert.False(gate.DrainStep4Passed);
    }

    [Fact]
    public void M13_drain_step_4_emits_Drain4_then_raises_its_event_with_the_flag_already_set_and_stops_renewal()
    {
        ToServing();
        gate.Apply(MemberTrigger.GracefulStop);
        order.Clear();

        Assert.True(gate.BeginDrainStep4());

        Assert.Equal(["DrainStep4:Draining:passed=True:events=1"], order);
        Assert.True(gate.DrainStep4Passed);
        Assert.Equal(ReplicaPhase.Draining, gate.Phase);
        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public void M13_drain_step_4_happens_once()
    {
        ToServing();
        gate.Apply(MemberTrigger.GracefulStop);
        Assert.True(gate.BeginDrainStep4());
        order.Clear();

        Assert.False(gate.BeginDrainStep4());

        Assert.Empty(order);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void S2_drain_step_4_after_a_fence_took_over_the_drain_raises_nothing(bool serving)
    {
        ToServing();
        if (!serving)
        {
            gate.Apply(MemberTrigger.GracefulStop);
        }

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        order.Clear();

        Assert.False(gate.BeginDrainStep4());

        Assert.Empty(order);
        Assert.False(gate.DrainStep4Passed);
        Assert.DoesNotContain(events.All, e => e.Action == MembershipActions.Drain4);
    }

    [Fact]
    public void After_drain_step_4_a_fence_trigger_changes_nothing()
    {
        ToServing();
        gate.Apply(MemberTrigger.GracefulStop);
        Assert.True(gate.BeginDrainStep4());

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        Assert.Equal(ReplicaPhase.Draining, gate.Phase);
    }
}
