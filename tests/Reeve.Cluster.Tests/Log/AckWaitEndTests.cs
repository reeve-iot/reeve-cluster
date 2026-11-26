using Reeve.Cluster;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Unit.Log;

public sealed class AckWaitEndTests
{
    [Fact]
    public void A_fence_ends_the_wait_in_Unconfirmed_Fenced()
    {
        Assert.Equal(UnconfirmedReason.Fenced, AckWaitEnd.On(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Serving, ReplicaPhase.Fencing)));
        Assert.Equal(UnconfirmedReason.Fenced, AckWaitEnd.On(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Draining, ReplicaPhase.Fencing)));
    }

    [Fact]
    public void Drain_step_4_ends_the_wait_in_Unconfirmed_Draining()
    {
        Assert.Equal(UnconfirmedReason.Draining, AckWaitEnd.On(new PhaseEvent(PhaseEventKind.DrainStep4, ReplicaPhase.Draining, ReplicaPhase.Draining)));
    }

    [Fact]
    public void Drain_step_1_lets_the_wait_go_on()
    {
        Assert.Null(AckWaitEnd.On(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Serving, ReplicaPhase.Draining)));
    }

    [Fact]
    public void A_transition_to_Serving_lets_the_wait_go_on() =>
        Assert.Null(AckWaitEnd.On(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Activated, ReplicaPhase.Serving)));

    [Theory]
    [InlineData(ReplicaPhase.Fenced)]
    [InlineData(ReplicaPhase.Exited)]
    [InlineData(ReplicaPhase.Starting)]
    public void Any_other_change_of_phase_ends_the_wait_as_a_fence(ReplicaPhase to)
    {
        Assert.Equal(UnconfirmedReason.Fenced, AckWaitEnd.On(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Draining, to)));
    }
}
