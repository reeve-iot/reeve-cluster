using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.AspNetCore;

internal enum Refusal
{
    NotServing,

    Fenced,

    Behind,
}

internal readonly record struct Admitted(Refusal? Refusal)
{
    public bool IsAdmitted => Refusal is null;

    public static Admitted Yes => new(null);

    public static Admitted No(Refusal refusal) => new(refusal);
}

internal sealed class Admission(IPhaseGate gate, IGenerationSource generations, ReadIndex readIndex, IFaultPoints points)
{
    public async ValueTask<Admitted> AdmitAsync(IClusterScopeWriter scope, bool exempt, bool skipReadIndex, CancellationToken cancellationToken)
    {
        var admitted = await DecideAsync(scope, exempt, skipReadIndex, cancellationToken).ConfigureAwait(false);
        if (admitted.IsAdmitted)
        {
            await points.ReachAsync(HttpFaultPoints.BetweenServingCheckAndHandler, cancellationToken).ConfigureAwait(false);
        }
        else if (admitted.Refusal is { } refusal) HttpDiagnostics.AdmissionRefusals.Add(1, new KeyValuePair<string, object?>("reason", RefusalResponse.CodeOf(refusal)));

        return admitted;
    }

    public async ValueTask<Admitted> AdmitConnectionAsync(bool exempt, CancellationToken cancellationToken)
    {
        var admitted = Admitted.No(Refusal.NotServing);
        if (generations.Current is not null)
        {
            var check = gate.CheckServing();
            admitted = check.IsServing || exempt ? Admitted.Yes : Admitted.No(RefusalFor(check));
        }

        if (admitted.IsAdmitted)
        {
            await points.ReachAsync(HttpFaultPoints.BetweenServingCheckAndHandler, cancellationToken).ConfigureAwait(false);
        }
        else if (admitted.Refusal is { } refusal) HttpDiagnostics.AdmissionRefusals.Add(1, new KeyValuePair<string, object?>("reason", RefusalResponse.CodeOf(refusal)));

        return admitted;
    }

    private async ValueTask<Admitted> DecideAsync(IClusterScopeWriter scope, bool exempt, bool skipReadIndex, CancellationToken cancellationToken)
    {
        if (generations.Current is null)
        {
            return Admitted.No(Refusal.NotServing);
        }

        var check = gate.CheckServing();
        if (!check.IsServing)
        {
            return exempt ? await PinWithoutWaitAsync(scope, cancellationToken).ConfigureAwait(false) : Admitted.No(RefusalFor(check));
        }

        if (skipReadIndex)
        {
            return await PinWithoutWaitAsync(scope, cancellationToken).ConfigureAwait(false);
        }

        var result = await readIndex.EnterAsync(scope, wait: true, cancellationToken).ConfigureAwait(false);
        return result.Outcome switch
        {
            ReadIndexOutcome.Pinned => Admitted.Yes,
            ReadIndexOutcome.Behind => Admitted.No(Refusal.Behind),
            _ => exempt ? await PinWithoutWaitAsync(scope, cancellationToken).ConfigureAwait(false) : Admitted.No(RefusalFor(result.Refusal!.Value)),
        };
    }

    private async ValueTask<Admitted> PinWithoutWaitAsync(IClusterScopeWriter scope, CancellationToken cancellationToken)
    {
        await readIndex.EnterAsync(scope, wait: false, cancellationToken).ConfigureAwait(false);
        return Admitted.Yes;
    }

    internal static Refusal RefusalFor(ServingCheckResult check) =>
        check.Outcome == ServingCheckOutcome.FenceStartPassed || check.Phase is ReplicaPhase.Fencing or ReplicaPhase.Fenced
            ? Refusal.Fenced
            : Refusal.NotServing;
}
