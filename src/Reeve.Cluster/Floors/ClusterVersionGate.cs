using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Floors;

internal sealed class ClusterVersionGate(IFloorStore store, IOptions<ClusterOptions> options, MemberIdentity self) : IClusterVersionGate
{
    internal const string UnjoinedReplica = "unjoined";

    internal static string RaisedBy(string? replicaId) => "raise:" + (string.IsNullOrWhiteSpace(replicaId) ? UnjoinedReplica : replicaId);

    public Task<bool> RequireProtocolAsync(int protocol, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(protocol);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(protocol, self.Protocol);
        return store.RaiseProtocolAsync(protocol, RaisedBy(options.Value.ReplicaId), cancellationToken);
    }

    public Task<bool> RequireVersionAsync(string version, CancellationToken cancellationToken = default)
    {
        RequireSemVer(version);
        if (SemanticVersion.TryParse(self.Version, out var own) && SemanticVersion.Parse(version).CompareTo(own) > 0)
        {
            throw new ArgumentException($"'{version}' is above the version of the calling binary ('{self.Version}'): the raise would refuse the caller too.", nameof(version));
        }

        return store.RaiseVersionAsync(version, RaisedBy(options.Value.ReplicaId), cancellationToken);
    }

    public Task<bool> AllMembersSpeakAsync(int protocol, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(protocol);
        return store.AllMembersSpeakAsync(protocol, cancellationToken);
    }

    public Task<bool> AllMembersAtLeastAsync(string version, CancellationToken cancellationToken = default)
    {
        RequireSemVer(version);
        return store.AllMembersAtLeastAsync(version, cancellationToken);
    }

    internal static void RequireSemVer(string? version, string parameter = "version")
    {
        if (!SemanticVersion.TryParse(version, out _))
        {
            throw new ArgumentException($"'{version}' is not a SemVer 2.0 version.", parameter);
        }
    }
}

internal sealed class ClusterFloorCommand(IFloorStore store) : IClusterFloorCommand
{
    internal static string LoweredBy(string who)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(who);
        return "lower:" + who;
    }

    public Task<FloorLowering> LowerProtocolFloorAsync(int protocol, string loweredBy, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(protocol);
        return store.LowerProtocolAsync(protocol, LoweredBy(loweredBy), cancellationToken);
    }

    public Task<FloorLowering> LowerVersionFloorAsync(string? version, string loweredBy, CancellationToken cancellationToken = default)
    {
        var by = LoweredBy(loweredBy);
        if (version is not null)
        {
            ClusterVersionGate.RequireSemVer(version);
        }

        return store.LowerVersionAsync(version, by, cancellationToken);
    }
}
