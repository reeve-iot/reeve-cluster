using Microsoft.Extensions.Options;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Slots;

internal interface ISlotGate : IClaimGate
{
    ClaimSeat? CheckHolder(long grantedEpoch);
}

internal sealed class LocalSlotGate(LocalPhaseGate gate, IOptions<ClusterOptions> options) : ISlotGate
{
    private readonly LocalClaimGate serving = new(gate, options);

    public ClaimSeat? CheckServing() => serving.CheckServing();

    public ClaimSeat? CheckHolder(long grantedEpoch)
    {
        if (gate.CheckSlotHolder(grantedEpoch) != SlotHolderOutcome.Passed || !gate.TryGetTimeline(grantedEpoch, out var timeline))
        {
            return null;
        }

        return new ClaimSeat(options.Value.ReplicaId ?? throw new InvalidOperationException("ReplicaId is required."), grantedEpoch, timeline);
    }

    public void OnOtherTimeline(long epoch) => serving.OnOtherTimeline(epoch);
}
