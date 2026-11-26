using Microsoft.Extensions.Options;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Membership;

internal sealed class LeaseStatusSection(LocalPhaseGate gate, IOptions<ClusterOptions> options) : IStatusSectionSource
{
    public const string Name = "lease";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var deadline = gate.Deadline;
        var o = options.Value;
        var values = new Dictionary<string, object?>
        {
            ["epoch"] = gate.Epoch,
            ["timeline"] = gate.JoinedTimeline,
            ["leaseRemainingSeconds"] = deadline is { } d ? (d - gate.Now).TotalSeconds : null,
            ["leaseTtlSeconds"] = o.LeaseTtl.TotalSeconds,
            ["renewIntervalSeconds"] = o.RenewInterval.TotalSeconds,
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}

internal sealed class PhaseStatusSection(LocalPhaseGate gate, MemberActivation activation) : IStatusSectionSource
{
    public const string Name = "phase";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var current = activation.Current;
        var phase = gate.Phase;
        var values = new Dictionary<string, object?>
        {
            ["phase"] = phase.ToString(),
            ["epoch"] = gate.Epoch,
            ["activatedAtSeq"] = current?.Head,
            ["servingSince"] = current?.ServingSince,
            ["ready"] = gate.WouldServe(phase),
            ["readinessReasons"] = gate.Readiness.Current.Select(r => new Dictionary<string, object?> { ["code"] = r.Code, ["description"] = r.Description }).ToList(),
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}

internal sealed class FloorStatusSection(MemberActivation activation, MemberIdentity identity) : IStatusSectionSource
{
    public const string Name = "floors";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var floors = activation.Floors;
        var values = new Dictionary<string, object?>
        {
            ["protocolFloor"] = floors?.ProtocolFloor,
            ["versionFloor"] = floors?.VersionFloor,
            ["readAt"] = floors?.ReadAt,
            ["movedBy"] = floors?.Moved?.By,
            ["movedAt"] = floors?.Moved?.At,
            ["protocol"] = identity.Protocol,
            ["version"] = identity.Version,
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}
