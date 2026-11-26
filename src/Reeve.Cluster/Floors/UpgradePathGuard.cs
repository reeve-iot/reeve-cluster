using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Floors;

internal sealed record UpgradePathDeclaration(int MinimumPeerProtocol)
{
    public static UpgradePathDeclaration ThisRelease { get; } = new(MemberIdentity.UpgradePathProtocol);
}

internal interface IUpgradePathGuard
{
    int MinimumPeerProtocol { get; }

    Task<OlderActiveMember?> FindBlockerAsync(string replicaId, CancellationToken cancellationToken);
}

internal static class UpgradePathRules
{
    public const string Reason = "activation.upgrade-path";

    public static ReadinessReason ReasonFor(OlderActiveMember blocker, int required)
    {
        ArgumentNullException.ThrowIfNull(blocker);
        return new ReadinessReason(
            Reason,
            $"Member '{blocker.ReplicaId}' is active and speaks protocol {blocker.Protocol}; this release needs every other member to speak at least {required}. " +
            "It stays joining until that member leaves (a skipped release: upgrade through the release in between).");
    }
}

internal sealed class UpgradePathGuard(IFloorStore store, UpgradePathDeclaration declaration) : IUpgradePathGuard
{
    public int MinimumPeerProtocol => declaration.MinimumPeerProtocol;

    public Task<OlderActiveMember?> FindBlockerAsync(string replicaId, CancellationToken cancellationToken) =>
        store.FindOlderActiveMemberAsync(replicaId, declaration.MinimumPeerProtocol, cancellationToken);
}

internal sealed class NoUpgradePathGuard : IUpgradePathGuard
{
    public static NoUpgradePathGuard Instance { get; } = new();

    public int MinimumPeerProtocol => 0;

    public Task<OlderActiveMember?> FindBlockerAsync(string replicaId, CancellationToken cancellationToken) => Task.FromResult<OlderActiveMember?>(null);
}
