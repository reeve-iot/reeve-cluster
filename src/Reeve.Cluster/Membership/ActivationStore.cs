using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;

#pragma warning disable CA2007

namespace Reeve.Cluster.Membership;

internal readonly record struct Backlog(bool CursorValid, long EntriesAfter);

internal sealed record ActivationRequest(string ReplicaId, long Epoch, string Version, int Protocol);

internal readonly record struct ActivationAnswer(
    ActivationOutcome Outcome, long Head, int? ProtocolFloor, string? VersionFloor, bool FloorsRead, FloorMove? Moved = null);

internal readonly record struct FloorMove(string By, DateTimeOffset At);

internal interface IActivationStore
{
    Task<Backlog> ReadBacklogAsync(Cursor cursor, long threshold, CancellationToken cancellationToken);

    Task<ActivationAnswer> ActivateAsync(ActivationRequest request, Func<bool> mayCommit, CancellationToken cancellationToken);

    Task<bool> SetServingSinceAsync(string replicaId, long epoch, Cursor applied, CancellationToken cancellationToken);
}

internal sealed class ActivationStore(ClusterDataSource data, MemberWriteDataSource writes, IOptions<ClusterOptions> options, IFaultPoints points)
    : IActivationStore
{
    private readonly int boundMilliseconds = (int)Math.Min(
        int.MaxValue, Math.Ceiling((options.Value.RenewTimeout + TimeSpan.FromSeconds(1)).TotalMilliseconds));

    internal const string BacklogSql = """
        SELECT reeve_cluster.cursor_valid(@seq, @token),
               (SELECT count(*) FROM (SELECT 1 FROM reeve_cluster.change_log WHERE seq > @seq LIMIT @limit) AS after)
        """;

    internal static readonly string LockSql = $"SELECT pg_advisory_xact_lock({ChangeLogLock.Key}), current_setting('transaction_timeout')";

    internal const string HeadSql = """
        SELECT coalesce((SELECT max(seq) FROM reeve_cluster.change_log), 0), c.protocol_floor, c.version_floor,
               c.floor_moved_by, c.floor_moved_at
        FROM reeve_cluster.cluster c
        """;

    internal const string ActivateSql = """
        UPDATE reeve_cluster.member SET state = 'active', activated_at_seq = @head
        WHERE replica_id = @id AND epoch = @epoch AND state = 'joining'
          AND @protocol >= (SELECT protocol_floor FROM reeve_cluster.cluster)
          AND (SELECT version_floor FROM reeve_cluster.cluster) IS NOT DISTINCT FROM @floor_read
        """;

    internal const string ServingSql = """
        UPDATE reeve_cluster.member SET serving_since = coalesce(serving_since, clock_timestamp())
        WHERE replica_id = @id AND epoch = @epoch AND state = 'active'
          AND activated_at_seq IS NOT NULL AND @seq >= activated_at_seq AND reeve_cluster.cursor_valid(@seq, @token)
        """;

    public async Task<Backlog> ReadBacklogAsync(Cursor cursor, long threshold, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(threshold);
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(BacklogSql, connection);
        command.Parameters.AddWithValue("seq", cursor.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)cursor.Token ?? DBNull.Value });
        command.Parameters.AddWithValue("limit", threshold == long.MaxValue ? threshold : threshold + 1);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new Backlog(reader.GetBoolean(0), reader.GetInt64(1));
    }

    public async Task<ActivationAnswer> ActivateAsync(ActivationRequest request, Func<bool> mayCommit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(mayCommit);

        await points.ReachAsync(MembershipFaultPoints.ActivateBeforeLock, cancellationToken).ConfigureAwait(false);
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);

        _ = await TransactionTimeout.FirstAsync(
                connection, transaction, boundMilliseconds, new NpgsqlBatchCommand(LockSql), static _ => true, cancellationToken)
            .ConfigureAwait(false) ?? throw new InvalidOperationException("The change-log lock statement returned no row.");

        long head;
        int protocolFloor;
        string? versionFloor;
        FloorMove? moved;
        await using (var read = new NpgsqlCommand(HeadSql, connection, transaction))
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException("reeve_cluster.cluster has no row.");
            }

            head = reader.GetInt64(0);
            protocolFloor = reader.GetInt32(1);
            versionFloor = reader.IsDBNull(2) ? null : reader.GetString(2);
            moved = reader.IsDBNull(3) || reader.IsDBNull(4) ? null : new FloorMove(reader.GetString(3), reader.GetFieldValue<DateTimeOffset>(4));
        }

        await points.ReachAsync(MembershipFaultPoints.ActivateAfterHeadRead, cancellationToken).ConfigureAwait(false);

        if (ActivationRules.Admit(request.Version, versionFloor) != Admission.Admitted)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new ActivationAnswer(ActivationOutcome.RefusedByVersionFloor, head, protocolFloor, versionFloor, FloorsRead: true, moved);
        }

        await points.ReachAsync(MembershipFaultPoints.ActivateBeforeUpdate, cancellationToken).ConfigureAwait(false);
        int rows;
        await using (var update = new NpgsqlCommand(ActivateSql, connection, transaction))
        {
            update.Parameters.AddWithValue("id", request.ReplicaId);
            update.Parameters.AddWithValue("epoch", request.Epoch);
            update.Parameters.AddWithValue("head", head);
            update.Parameters.AddWithValue("protocol", request.Protocol);
            update.Parameters.Add(new NpgsqlParameter("floor_read", NpgsqlDbType.Text) { Value = (object?)versionFloor ?? DBNull.Value });
            rows = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await points.ReachAsync(MembershipFaultPoints.ActivateAfterUpdate, cancellationToken).ConfigureAwait(false);
        if (rows == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new ActivationAnswer(ActivationRules.OnZeroRows(request.Protocol, protocolFloor), head, protocolFloor, versionFloor, FloorsRead: true, moved);
        }

        await points.ReachAsync(MembershipFaultPoints.ActivateBeforeCommit, cancellationToken).ConfigureAwait(false);
        if (!mayCommit())
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new ActivationAnswer(ActivationOutcome.LeftCatchingUp, head, protocolFloor, versionFloor, FloorsRead: true, moved);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return new ActivationAnswer(ActivationOutcome.Committed, head, protocolFloor, versionFloor, FloorsRead: true, moved);
    }

    public async Task<bool> SetServingSinceAsync(string replicaId, long epoch, Cursor applied, CancellationToken cancellationToken)
    {
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ServingSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        command.Parameters.AddWithValue("seq", applied.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)applied.Token ?? DBNull.Value });
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return rows == 1;
    }
}
