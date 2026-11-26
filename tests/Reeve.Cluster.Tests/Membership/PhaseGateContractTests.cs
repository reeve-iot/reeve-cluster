using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class PhaseGateContractTests
{
    private sealed class StubGate(ServingCheckResult result) : IPhaseGate
    {
        public int Checks { get; private set; }

        public ReplicaPhase Phase => result.Phase;

        public long? FenceStartTimestamp => null;

        public IReadinessReasons Readiness => throw new NotSupportedException();

        public ServingCheckResult CheckServing()
        {
            Checks++;
            return result;
        }

        public bool ExitAfterFence => false;

        public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

        public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

        public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

        public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable Subscribe(Action<PhaseEvent> handler) => throw new NotSupportedException();
    }

    [Fact]
    public void EnsureServing_runs_the_check_on_every_call_and_passes_while_serving()
    {
        var gate = new StubGate(new ServingCheckResult(ServingCheckOutcome.Serving, ReplicaPhase.Serving));

        gate.EnsureServing();
        gate.EnsureServing();

        Assert.Equal(2, gate.Checks);
    }

    [Theory]
    [InlineData("NotServing", ReplicaPhase.CatchingUp)]
    [InlineData("NotServing", ReplicaPhase.Draining)]
    [InlineData("FenceStartPassed", ReplicaPhase.Fencing)]
    public void EnsureServing_throws_with_what_the_check_found(string outcomeName, ReplicaPhase phase)
    {
        var outcome = Enum.Parse<ServingCheckOutcome>(outcomeName);
        var gate = new StubGate(new ServingCheckResult(outcome, phase));

        var ex = Assert.Throws<NotServingException>(gate.EnsureServing);

        Assert.Equal(outcome, ex.Result.Outcome);
        Assert.Equal(phase, ex.Result.Phase);
        Assert.False(ex.Result.IsServing);
    }

    [Fact]
    public void The_two_phase_vocabularies_do_not_overlap_by_name_except_where_the_spec_says()
    {
        var phases = Enum.GetNames<ReplicaPhase>();
        var states = Enum.GetNames<MemberRowState>();

        Assert.Equal(["Joining", "Active", "Fenced", "Draining"], states);
        Assert.Equal(
            ["Starting", "Loading", "CatchingUp", "Activated", "Serving", "Fencing", "Fenced", "Draining", "Exited"],
            phases);
    }

    [Fact]
    public void Fence_and_drain_steps_carry_the_specs_numbers()
    {
        Assert.Equal([1, 2, 3, 4, 5, 6], Enum.GetValues<FenceStep>().Select(s => (int)s));
        Assert.Equal([1, 2, 4], Enum.GetValues<DrainStep>().Select(s => (int)s));
    }
}
