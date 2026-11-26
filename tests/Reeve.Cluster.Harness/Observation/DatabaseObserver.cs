using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Replicas;

namespace Reeve.Cluster.Harness.Observation;

internal sealed record ObservedMember(
    string ReplicaId,
    long Epoch,
    MemberRowState State,
    DateTimeOffset LeaseExpiresAt,
    DateTimeOffset? ServingSince,
    long? ActivatedAtSeq,
    long AppliedSeq,
    Guid ProcessId);

internal sealed record ObservedFence(string ReplicaId, long Epoch, string Trigger, DateTimeOffset FencedAt, TimeSpan Span);

internal sealed class DatabaseObserver(HarnessRun run)
{
    public async Task<(IReadOnlyList<ObservedMember> Members, DateTimeOffset? DatabaseNow)> MembersAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await run.OpenObserverConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            SELECT replica_id, epoch, state, lease_expires_at, serving_since, activated_at_seq, applied_seq, process_id,
                   clock_timestamp() AS db_now
            FROM reeve_cluster.member
            ORDER BY replica_id
            """,
            connection);
        var members = new List<ObservedMember>();
        DateTimeOffset? now = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            now = reader.GetFieldValue<DateTimeOffset>(8);
            members.Add(new ObservedMember(
                reader.GetString(0),
                reader.GetInt64(1),
                Enum.Parse<MemberRowState>(reader.GetString(2), ignoreCase: true),
                reader.GetFieldValue<DateTimeOffset>(3),
                reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetGuid(7)));
        }

        return (members, now);
    }

    public async Task<ObservedMember?> MemberAsync(string replicaId, CancellationToken cancellationToken = default) =>
        (await MembersAsync(cancellationToken).ConfigureAwait(false)).Members.FirstOrDefault(m => m.ReplicaId == replicaId);

    public async Task<IReadOnlySet<string>> WaitSetAsync(long seq, CancellationToken cancellationToken = default)
    {
        var (members, now) = await MembersAsync(cancellationToken).ConfigureAwait(false);
        if (now is null)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        return members
            .Where(m => AckWaitSet.Includes(new AckMember(m.State, m.LeaseExpiresAt, m.ActivatedAtSeq ?? long.MaxValue, new AckCursor(m.AppliedSeq, true)), seq, now.Value))
            .Select(m => m.ReplicaId)
            .ToHashSet(StringComparer.Ordinal);
    }

    public async Task<IReadOnlyList<ObservedFence>> FenceHistoryAsync(string? replicaId = null, CancellationToken cancellationToken = default)
    {
        await using var connection = await run.OpenObserverConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            "SELECT replica_id, epoch, trigger, fenced_at, span FROM reeve_cluster.fence_history WHERE (@id::text IS NULL OR replica_id = @id) ORDER BY replica_id, epoch",
            connection);
        command.Parameters.AddWithValue("id", (object?)replicaId ?? DBNull.Value);
        var fences = new List<ObservedFence>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            fences.Add(new ObservedFence(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3),
                reader.GetFieldValue<TimeSpan>(4)));
        }

        return fences;
    }
}
