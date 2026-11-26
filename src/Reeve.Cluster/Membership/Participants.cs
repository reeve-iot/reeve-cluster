namespace Reeve.Cluster.Membership;

internal enum FenceStep
{
    CloseTraffic = 1,

    RefuseCommits = 2,

    ReleaseLease = 3,

    AbortConnections = 4,

    StopUnits = 5,

    StopApplier = 6,
}

internal sealed record FenceContext(FenceRequest Request);

internal interface IFenceParticipant
{
    FenceStep Step { get; }

    ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken);
}

internal enum DrainStep
{
    CloseTraffic = 1,

    ReleaseUnits = 2,

    AwaitRequests = 4,
}

internal sealed record DrainContext;

internal interface IDrainParticipant
{
    DrainStep Step { get; }

    ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken);
}
