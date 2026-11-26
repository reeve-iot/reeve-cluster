using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Core;

internal static class ServingCheck
{
    public static ServingCheckOutcome Evaluate(ReplicaPhase phase, TimeSpan now, TimeSpan? fenceStart)
    {
        if (phase != ReplicaPhase.Serving)
        {
            return ServingCheckOutcome.NotServing;
        }

        return fenceStart is { } start && now < start
            ? ServingCheckOutcome.Serving
            : ServingCheckOutcome.FenceStartPassed;
    }

    public static bool MayEnterServing(TimeSpan now, TimeSpan? fenceStart) => fenceStart is { } start && now < start;
}
