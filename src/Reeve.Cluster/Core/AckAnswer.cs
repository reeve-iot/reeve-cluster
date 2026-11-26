namespace Reeve.Cluster.Core;

internal readonly record struct AckAnswer(AckOutcome? Outcome)
{
    public static AckAnswer Wait => default;

    public bool Waits => Outcome is null;
}

internal readonly record struct AckCaller(
    ClusterScopeKind? ScopeKind,
    bool AcknowledgedAtCommit = false,
    bool TrackerHoldsSeq = false,
    bool CommitRefused = false,
    bool RanThroughStep2 = false);

internal static class AckAnswerPolicy
{
    private static readonly AckAnswer Applier = new(new AckOutcome.NotAwaited(NotAwaitedReason.Applier));

    private static readonly AckAnswer AtCommit = new(new AckOutcome.NotAwaited(NotAwaitedReason.AtCommit));

    private static readonly AckAnswer Joining = new(new AckOutcome.NotAwaited(NotAwaitedReason.Joining));

    private static readonly AckAnswer Draining = new(new AckOutcome.NotAwaited(NotAwaitedReason.Draining));

    private static readonly AckAnswer FencedNotAwaited = new(new AckOutcome.NotAwaited(NotAwaitedReason.Fenced));

    private static readonly AckAnswer FencedUnconfirmed = new(new AckOutcome.Unconfirmed(UnconfirmedReason.Fenced));

    public static AckAnswer Decide(ReplicaPhase phase, AckCaller caller)
    {
        if (caller.ScopeKind == ClusterScopeKind.Applier)
        {
            return Applier;
        }

        if (caller is { ScopeKind: ClusterScopeKind.ClientRequest, RanThroughStep2: true })
        {
            return FencedUnconfirmed;
        }

        if (caller is { AcknowledgedAtCommit: true, TrackerHoldsSeq: true, CommitRefused: false })
        {
            return AtCommit;
        }

        var isRequest = caller.ScopeKind is ClusterScopeKind.ClientRequest or ClusterScopeKind.ExemptRequest;
        switch (phase)
        {
            case ReplicaPhase.Starting:
            case ReplicaPhase.Loading:
            case ReplicaPhase.CatchingUp:
            case ReplicaPhase.Activated:
                return Joining;

            case ReplicaPhase.Serving:
                return AckAnswer.Wait;

            case ReplicaPhase.Draining:
                return isRequest ? AckAnswer.Wait : Draining;

            case ReplicaPhase.Fencing:
            case ReplicaPhase.Fenced:
            case ReplicaPhase.Exited:
                return caller.ScopeKind == ClusterScopeKind.ClientRequest ? FencedUnconfirmed : FencedNotAwaited;

            default:
                throw new ArgumentOutOfRangeException(nameof(phase), phase, "Not a replica phase.");
        }
    }
}
