using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Log;

internal sealed class CommitRefusal : IDisposable
{
    private readonly IDisposable subscription;
    private readonly FenceSteps steps;
    private volatile bool refusing;

    public CommitRefusal(IPhaseGate gate, FenceSteps steps)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(steps);
        this.steps = steps;
        subscription = gate.Subscribe(OnPhase);
    }

    public bool Refusing => refusing;

    public bool Refuses(IClusterScopeWriter? scope) =>
        scope.UnlessEnded() is { Kind: ClusterScopeKind.ClientRequest } request && (refusing || request.FenceStepsAtBegin < steps.Count);

    public bool RanThroughStep2(IClusterScopeWriter? scope) =>
        scope.UnlessEnded() is { Kind: ClusterScopeKind.ClientRequest } request && request.FenceStepsAtBegin < steps.Count;

    public void Start()
    {
        steps.Increment();
        refusing = true;
    }

    public void Dispose() => subscription.Dispose();

    private void OnPhase(PhaseEvent phaseEvent)
    {
        if (phaseEvent.Kind == PhaseEventKind.Transition
            && phaseEvent.To is not (ReplicaPhase.Fencing or ReplicaPhase.Fenced or ReplicaPhase.Exited))
        {
            refusing = false;
        }
    }
}

internal sealed class AppendRefusalParticipant(CommitRefusal refusal) : IFenceParticipant
{
    public FenceStep Step => FenceStep.RefuseCommits;

    public ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken)
    {
        refusal.Start();
        return ValueTask.CompletedTask;
    }
}
