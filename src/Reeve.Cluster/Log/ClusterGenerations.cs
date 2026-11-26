using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Log;

internal sealed class ClusterGenerations : IClusterGenerations
{
    private readonly IGenerationSource source;
    private readonly IPhaseGate gate;
    private readonly IClusterScopeSource scopes;

    public ClusterGenerations(IGenerationSource source, IPhaseGate gate, IClusterScopeSource scopes)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(scopes);
        this.source = source;
        this.gate = gate;
        this.scopes = scopes;
    }

    public IGeneration? Current => source.Current;

    public IGeneration? TryPin()
    {
        source.EnsureNotInsideTheSwap();
        var scope = scopes.CurrentWriter.UnlessEnded()
            ?? throw new InvalidOperationException(
                "TryPin needs a live scope: begin one per action with IClusterScopeAccessor.BeginUnitOfWork() or BeginBackground().");
        if (scope.Kind == ClusterScopeKind.Applier)
        {
            throw new InvalidOperationException(
                "TryPin is not for a reconciler: it reads its snapshot and Previous, and code it shares with background readers takes the generation as a parameter.");
        }

        if (scope.PinnedGeneration is { } pinned)
        {
            return pinned;
        }

        var before = gate.CheckServing();
        if (!before.IsServing)
        {
            return null;
        }

        var current = source.Current;
        var after = gate.CheckServing();
        if (!after.IsServing || before.Epoch is null || before.Epoch != after.Epoch || current is null)
        {
            return null;
        }

        return scope.Kind is ClusterScopeKind.UnitOfWork or ClusterScopeKind.Background ? scope.PinOrGet(current) : current;
    }
}
