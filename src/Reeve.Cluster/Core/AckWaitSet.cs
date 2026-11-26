namespace Reeve.Cluster.Core;

internal readonly record struct AckCursor(long Seq, bool Valid)
{
    public bool HasApplied(long seq) => Valid && Seq >= seq;
}

internal readonly record struct AckMember(MemberRowState State, DateTimeOffset LeaseExpiresAt, long ActivatedAtSeq, AckCursor Cursor);

internal static class AckWaitSet
{
    public static bool Includes(AckMember member, long seq, DateTimeOffset now) =>
        member.State == MemberRowState.Active
        && member.LeaseExpiresAt > now
        && member.ActivatedAtSeq < seq;

    public static long? ReadyBound(long seq, AckCursor local, IEnumerable<AckMember> members, DateTimeOffset now)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(seq, 1);

        if (!local.HasApplied(seq))
        {
            return null;
        }

        var bound = local.Seq;
        foreach (var member in members)
        {
            if (!Includes(member, seq, now))
            {
                continue;
            }

            if (!member.Cursor.HasApplied(seq))
            {
                return null;
            }

            bound = Math.Min(bound, member.Cursor.Seq);
        }

        return bound;
    }
}
