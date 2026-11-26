using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Slots;

namespace Unit.Slots;

public sealed class LocalSlotGateTests
{
    private const long Epoch = 7;
    private const long Timeline = 3;
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

    private readonly FakeTimeProvider time = new();
    private readonly LocalPhaseGate phase;
    private readonly LocalSlotGate gate;

    public LocalSlotGateTests()
    {
        phase = new LocalPhaseGate(Timing, time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        gate = new LocalSlotGate(phase, Options.Create(new ClusterOptions { ReplicaId = "r1" }));
        Assert.True(phase.Joined(Epoch, Timeline, phase.Now));
        phase.Apply(MemberTrigger.StateLoaded);
        phase.Apply(MemberTrigger.ActivationCommitted);
        phase.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, phase.Phase);
    }

    [Fact]
    public void A_serving_replica_passes_for_a_slot_of_its_epoch_and_answers_the_seat_with_the_joined_timeline()
    {
        Assert.Equal(new ClaimSeat("r1", Epoch, Timeline), gate.CheckHolder(Epoch));
    }

    [Fact]
    public void A_slot_of_an_earlier_epoch_is_refused_and_asks_for_no_fence()
    {
        Assert.Null(gate.CheckHolder(Epoch - 1));
        Assert.Equal(ReplicaPhase.Serving, phase.Phase);
    }

    [Fact]
    public void I1_a_check_that_finds_fence_start_passed_refuses_and_starts_the_fence_at_step_0()
    {
        time.Advance(TimeSpan.FromSeconds(22));

        Assert.Null(gate.CheckHolder(Epoch));

        Assert.Equal(ReplicaPhase.Fencing, phase.Phase);
        Assert.NotNull(phase.CurrentFence);
    }

    [Fact]
    public void A_fenced_member_never_passes_again_in_its_epoch()
    {
        phase.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        Assert.Equal(ReplicaPhase.Fencing, phase.Phase);
        Assert.Null(gate.CheckHolder(Epoch));

        phase.CompleteFence();

        Assert.Null(gate.CheckHolder(Epoch));
    }

    [Fact]
    public void B1_a_draining_replica_passes_before_and_after_drain_step_4_and_in_Exited()
    {
        phase.Apply(MemberTrigger.GracefulStop);
        Assert.Equal(ReplicaPhase.Draining, phase.Phase);
        Assert.NotNull(gate.CheckHolder(Epoch));

        Assert.True(phase.BeginDrainStep4());
        time.Advance(TimeSpan.FromMinutes(5));
        Assert.NotNull(gate.CheckHolder(Epoch));

        phase.CompleteDrain();
        Assert.Equal(ReplicaPhase.Exited, phase.Phase);
        Assert.Equal(new ClaimSeat("r1", Epoch, Timeline), gate.CheckHolder(Epoch));
    }

    [Fact]
    public void A_draining_replica_whose_fence_start_passed_before_step_4_is_fenced_by_the_check()
    {
        phase.Apply(MemberTrigger.GracefulStop);
        time.Advance(TimeSpan.FromSeconds(22));

        Assert.Null(gate.CheckHolder(Epoch));

        Assert.Equal(ReplicaPhase.Fencing, phase.Phase);
    }

    [Fact]
    public void A_fence_that_took_over_the_drain_refuses_the_slot_for_good()
    {
        phase.Apply(MemberTrigger.GracefulStop);
        phase.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        phase.CompleteFence();

        Assert.Null(gate.CheckHolder(Epoch));
    }

    [Fact]
    public void The_serving_check_of_the_slot_gate_is_the_claims()
    {
        Assert.Equal(new ClaimSeat("r1", Epoch, Timeline), gate.CheckServing());
        phase.Apply(MemberTrigger.GracefulStop);
        Assert.Null(gate.CheckServing());
    }

    [Fact]
    public void I32_a_statement_on_another_timeline_fences_through_the_gate()
    {
        gate.OnOtherTimeline(Epoch);

        Assert.Equal(ReplicaPhase.Fencing, phase.Phase);
    }
}
