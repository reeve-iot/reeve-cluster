namespace Reeve.Cluster.Claims;

internal readonly record struct ClaimSeat(string ReplicaId, long Epoch, long Timeline);

internal interface IClaimGate
{
    ClaimSeat? CheckServing();

    void OnOtherTimeline(long epoch);
}
