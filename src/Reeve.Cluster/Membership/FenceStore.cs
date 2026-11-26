using Npgsql;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Membership;

internal sealed record FenceRecord(string ReplicaId, long Epoch, string Trigger, TimeSpan Span);

internal interface IFenceStore
{
    Task<bool> ReleaseLeaseAsync(string replicaId, long epoch, CancellationToken cancellationToken);

    Task RecordFenceAsync(FenceRecord record, TimeSpan age, CancellationToken cancellationToken);

    Task<OwnRowObservation> ReadRejoinStateAsync(string replicaId, CancellationToken cancellationToken);
}

internal static class FenceTriggerNames
{
    public static string Of(FenceTrigger trigger) => trigger switch
    {
        FenceTrigger.FenceStart => "fence-start",
        FenceTrigger.ApplierStall => "applier-stall",
        FenceTrigger.StuckAttempt => "stuck-attempt",
        FenceTrigger.FatalFault => "fatal-fault",
        FenceTrigger.ZeroRowRenewal => "zero-row-renewal",
        FenceTrigger.Timeline => "timeline",
        FenceTrigger.UnansweredActivation => "unanswered-activation",
        _ => throw new ArgumentOutOfRangeException(nameof(trigger), trigger, null),
    };
}

internal sealed class FenceStore(ClusterDataSource data, RenewalDataSource renewal, MemberWriteDataSource writes) : IFenceStore
{
    internal const int HistoryRows = 10;

    internal const string ReleaseSql = """
        UPDATE reeve_cluster.member SET state = 'fenced', lease_expires_at = clock_timestamp(), serving_since = NULL
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active')
        """;

    internal const string RecordSql = """
        WITH recorded AS (
            INSERT INTO reeve_cluster.fence_history (replica_id, epoch, trigger, fenced_at, span)
            VALUES (@id, @epoch, @trigger, clock_timestamp() - @age, @span)
            ON CONFLICT (replica_id, epoch) DO NOTHING
        )
        DELETE FROM reeve_cluster.fence_history
        WHERE replica_id = @id
          AND epoch < (SELECT min(kept.epoch) FROM (
                SELECT epoch FROM reeve_cluster.fence_history WHERE replica_id = @id AND epoch < @epoch
                ORDER BY epoch DESC LIMIT @keep) kept)
        """;

    internal const string RejoinStateSql = """
        SELECT epoch, state, lease_expires_at > clock_timestamp() FROM reeve_cluster.member WHERE replica_id = @id
        """;

    public async Task<bool> ReleaseLeaseAsync(string replicaId, long epoch, CancellationToken cancellationToken)
    {
        await using var connection = await renewal.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReleaseSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    public async Task RecordFenceAsync(FenceRecord record, TimeSpan age, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(RecordSql, connection);
        command.Parameters.AddWithValue("id", record.ReplicaId);
        command.Parameters.AddWithValue("epoch", record.Epoch);
        command.Parameters.AddWithValue("trigger", record.Trigger);
        command.Parameters.AddWithValue("span", record.Span);
        command.Parameters.AddWithValue("age", age < TimeSpan.Zero ? TimeSpan.Zero : age);
        command.Parameters.AddWithValue("keep", HistoryRows - 1);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
    }

    public async Task<OwnRowObservation> ReadRejoinStateAsync(string replicaId, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(RejoinStateSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new OwnRowObservation(true, reader.GetInt64(0), Enum.Parse<MemberRowState>(reader.GetString(1), ignoreCase: true), reader.GetBoolean(2))
            : OwnRowObservation.Absent;
    }
}
