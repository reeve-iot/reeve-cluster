namespace Reeve.Cluster.AspNetCore;

internal enum VerdictKind
{
    Pass,

    Acknowledged,

    AtCommit,

    WriteRefused,

    Unconfirmed,
}

internal readonly record struct Verdict(VerdictKind Kind, long? Seq, IReadOnlyList<string> Skipped)
{
    public Verdict(VerdictKind kind, long? seq = null)
        : this(kind, seq, [])
    {
    }

    public static Verdict Pass => new(VerdictKind.Pass);

    public bool Equals(Verdict other) => Kind == other.Kind && Seq == other.Seq && Skipped.SequenceEqual(other.Skipped, StringComparer.Ordinal);

    public override int GetHashCode() => HashCode.Combine(Kind, Seq, Skipped.Count);
}

internal readonly record struct TrackerView(bool Exempt, IReadOnlyList<long> Seqs, bool CommitRefused, bool OptedOut, bool OpenTransaction)
{
    public static TrackerView Of(IClusterScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var seqs = scope.Seqs;
        var refused = scope.CommitRefused;
        var optedOut = scope.AcknowledgedAtCommit;
        var open = scope.OpenTransactions.Count > 0;
        return new TrackerView(scope.Kind == ClusterScopeKind.ExemptRequest, seqs, refused, optedOut, open);
    }

    public long? Highest => Seqs.Count == 0 ? null : Seqs.Max();
}

internal readonly record struct AckPlan(Verdict? Settled, long AwaitSeq = 0);

internal static class FencedRequestRule
{
    public static AckPlan Plan(TrackerView tracker)
    {
        if (tracker.Exempt)
        {
            return new AckPlan(Verdict.Pass);
        }

        var seq = tracker.Highest;
        if (tracker.CommitRefused)
        {
            return new AckPlan(seq is null ? new Verdict(VerdictKind.WriteRefused) : new Verdict(VerdictKind.Unconfirmed, seq));
        }

        if (seq is null && !tracker.OpenTransaction)
        {
            return new AckPlan(Verdict.Pass);
        }

        if (tracker.OptedOut)
        {
            return new AckPlan(seq is null ? Verdict.Pass : new Verdict(VerdictKind.AtCommit, seq));
        }

        if (tracker.OpenTransaction)
        {
            return new AckPlan(new Verdict(VerdictKind.Unconfirmed, seq));
        }

        return new AckPlan(null, seq!.Value);
    }

    public static Verdict Settle(long seq, AckOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);
        return outcome switch
        {
            AckOutcome.Acknowledged { Skipped.Count: > 0 } acknowledged => new Verdict(VerdictKind.Acknowledged, seq, acknowledged.Skipped),
            AckOutcome.Acknowledged => Verdict.Pass,
            AckOutcome.NotAwaited { Reason: NotAwaitedReason.AtCommit } => new Verdict(VerdictKind.AtCommit, seq),
            _ => new Verdict(VerdictKind.Unconfirmed, seq),
        };
    }
}
