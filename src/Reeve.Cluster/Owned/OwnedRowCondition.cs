namespace Reeve.Cluster.Owned;

internal enum OwnedCondition
{
    Absent,

    Free,

    Owned,

    ReleaseRequested,

    Orphaned,
}

internal readonly record struct OwnedRow(string? OwnerReplica, long? OwnerEpoch, long Token);

internal readonly record struct OwnerMember(long Epoch, MemberRowState State, bool LeaseValid);

internal static class OwnedRowCondition
{
    public static OwnedCondition Of(OwnedRow? row, OwnerMember? owner, long? requestToken)
    {
        if (row is not { } r)
        {
            return OwnedCondition.Absent;
        }

        if (r.OwnerReplica is null)
        {
            return OwnedCondition.Free;
        }

        if (owner is not { } m || m.Epoch != r.OwnerEpoch || m.State != MemberRowState.Active || !m.LeaseValid)
        {
            return OwnedCondition.Orphaned;
        }

        return requestToken == r.Token ? OwnedCondition.ReleaseRequested : OwnedCondition.Owned;
    }
}
