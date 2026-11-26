namespace Reeve.Cluster.Migrations;

internal sealed class MemberRow
{
    public string ReplicaId { get; set; } = "";
    public long Epoch { get; set; }
    public string State { get; set; } = "";
    public DateTimeOffset LeaseExpiresAt { get; set; }
    public DateTimeOffset? ServingSince { get; set; }
    public long? ActivatedAtSeq { get; set; }
    public long AppliedSeq { get; set; }
    public Guid? AppliedToken { get; set; }
    public string[] Capabilities { get; set; } = [];
    public string Version { get; set; } = "";
    public int Protocol { get; set; }
    public Guid ProcessId { get; set; }
}

internal sealed class ClusterRow
{
    public bool Id { get; set; }
    public int ProtocolFloor { get; set; }
    public string? VersionFloor { get; set; }
    public long HorizonSeq { get; set; }
    public string? FloorMovedBy { get; set; }
    public DateTimeOffset? FloorMovedAt { get; set; }
}

internal sealed class FenceHistoryRow
{
    public string ReplicaId { get; set; } = "";
    public long Epoch { get; set; }
    public string Trigger { get; set; } = "";
    public DateTimeOffset FencedAt { get; set; }
    public TimeSpan Span { get; set; }
}

internal sealed class ChangeLogRow
{
    public long Seq { get; set; }
    public Guid EntryToken { get; set; }
    public ulong Xid { get; set; }
    public string AggregateType { get; set; } = "";
    public string? AggregateKey { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class OwnedRow
{
    public string Kind { get; set; } = "";
    public string Key { get; set; } = "";
    public string? OwnerReplica { get; set; }
    public long? OwnerEpoch { get; set; }
    public long Token { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

internal sealed class ReleaseRequestRow
{
    public string Kind { get; set; } = "";
    public string Key { get; set; } = "";
    public long Token { get; set; }
    public DateTimeOffset RequestedAt { get; set; }
}

internal sealed class ChangeAckRow
{
    public bool Id { get; set; }
    public long AckedSeq { get; set; }
}

internal sealed class ChangeSkipRow
{
    public long Id { get; set; }
    public string AggregateType { get; set; } = "";
    public long FromSeq { get; set; }
    public long? UntilSeq { get; set; }
    public string Reason { get; set; } = "";
    public string OpenedBy { get; set; } = "";
    public DateTimeOffset OpenedAt { get; set; }
    public string? ClosedBy { get; set; }
    public DateTimeOffset? ClosedAt { get; set; }
}

internal sealed class ClaimRow
{
    public string Key { get; set; } = "";
    public string HolderReplica { get; set; } = "";
    public long HolderEpoch { get; set; }
    public Guid Token { get; set; }
    public DateTimeOffset ExpiresAt { get; set; }
    public string? OnceMode { get; set; }
    public string? OnceEffect { get; set; }
    public TimeSpan? OnceTtl { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public byte[]? Result { get; set; }
    public DateTimeOffset? ResultExpiresAt { get; set; }
}

internal sealed class BudgetRow
{
    public string Key { get; set; } = "";
    public DateTimeOffset WindowStart { get; set; }
    public int Used { get; set; }
    public DateTimeOffset WindowEnd { get; set; }
}

internal sealed class BudgetIdRow
{
    public string Key { get; set; } = "";
    public DateTimeOffset WindowStart { get; set; }
    public string Id { get; set; } = "";
}

internal sealed class CounterRow
{
    public string Key { get; set; } = "";
    public long Value { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

internal sealed class SlotRow
{
    public string Key { get; set; } = "";
    public string Id { get; set; } = "";
    public string HolderReplica { get; set; } = "";
    public long HolderEpoch { get; set; }
    public Guid Token { get; set; }
    public DateTimeOffset ExpiresAt { get; set; }
}

internal sealed class CommandRow
{
    public long Id { get; set; }
    public string Kind { get; set; } = "";
    public string Key { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public byte[] Payload { get; set; } = [];
    public string State { get; set; } = "";
    public long? TakenToken { get; set; }
    public DateTimeOffset? TakenAt { get; set; }
    public byte[]? Result { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
}

internal sealed class MemberStatusRow
{
    public string ReplicaId { get; set; } = "";
    public long Epoch { get; set; }
    public string Section { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTimeOffset UpdatedAt { get; set; }
}
