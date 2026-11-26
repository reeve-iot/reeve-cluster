using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

#pragma warning disable CA2007

namespace Reeve.Cluster.Floors;

internal sealed record FloorRow(int ProtocolFloor, string? VersionFloor, string? MovedBy, DateTimeOffset? MovedAt);

internal sealed record OlderActiveMember(string ReplicaId, int Protocol);

internal interface IFloorStore
{
    Task<FloorRow> ReadAsync(CancellationToken cancellationToken);

    Task<bool> RaiseProtocolAsync(int protocol, string by, CancellationToken cancellationToken);

    Task<bool> RaiseVersionAsync(string version, string by, CancellationToken cancellationToken);

    Task<FloorLowering> LowerProtocolAsync(int protocol, string by, CancellationToken cancellationToken);

    Task<FloorLowering> LowerVersionAsync(string? version, string by, CancellationToken cancellationToken);

    Task<bool> AllMembersSpeakAsync(int protocol, CancellationToken cancellationToken);

    Task<bool> AllMembersAtLeastAsync(string version, CancellationToken cancellationToken);

    Task<OlderActiveMember?> FindOlderActiveMemberAsync(string selfReplicaId, int minimumProtocol, CancellationToken cancellationToken);
}

internal sealed class FloorStore(ClusterDataSource data, MemberWriteDataSource writes, IOptions<ClusterOptions> options, IFaultPoints points) : IFloorStore
{
    private const string MemberFilter = "state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()";

    internal const string ReadSql = "SELECT protocol_floor, version_floor, floor_moved_by, floor_moved_at FROM reeve_cluster.cluster";

    internal const string FloorsSql = "SELECT protocol_floor, version_floor FROM reeve_cluster.cluster";

    internal const string AnyBelowProtocolSql = $"SELECT EXISTS (SELECT 1 FROM reeve_cluster.member WHERE {MemberFilter} AND protocol < @protocol)";

    internal const string MemberVersionsSql = $"SELECT version FROM reeve_cluster.member WHERE {MemberFilter}";

    internal const string RaiseProtocolSql = """
        UPDATE reeve_cluster.cluster
        SET protocol_floor = @protocol, floor_moved_by = @by, floor_moved_at = clock_timestamp()
        WHERE protocol_floor < @protocol
        """;

    internal const string RaiseVersionSql = """
        UPDATE reeve_cluster.cluster
        SET version_floor = @version, floor_moved_by = @by, floor_moved_at = clock_timestamp()
        WHERE version_floor IS NOT DISTINCT FROM @floor_read
        """;

    internal const string LowerProtocolSql = """
        UPDATE reeve_cluster.cluster
        SET protocol_floor = @protocol, floor_moved_by = @by, floor_moved_at = clock_timestamp()
        WHERE protocol_floor > @protocol
        """;

    internal const string LowerVersionSql = """
        UPDATE reeve_cluster.cluster
        SET version_floor = @version, floor_moved_by = @by, floor_moved_at = clock_timestamp()
        WHERE version_floor IS NOT DISTINCT FROM @floor_read
        """;

    internal const string OlderActiveSql = """
        SELECT replica_id, protocol FROM reeve_cluster.member
        WHERE state = 'active' AND lease_expires_at > clock_timestamp() AND protocol < @minimum AND replica_id <> @self
        ORDER BY protocol, replica_id LIMIT 1
        """;

    private readonly int boundMilliseconds = (int)Math.Min(
        int.MaxValue, Math.Ceiling((options.Value.RenewTimeout + TimeSpan.FromSeconds(1)).TotalMilliseconds));

    public async Task<FloorRow> ReadAsync(CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("reeve_cluster.cluster has no row.");
        }

        return new FloorRow(
            reader.GetInt32(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3));
    }

    public async Task<bool> RaiseProtocolAsync(int protocol, string by, CancellationToken cancellationToken)
    {
        if ((await ReadAsync(cancellationToken).ConfigureAwait(false)).ProtocolFloor >= protocol)
        {
            return true;
        }

        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await BeginUnderLockAsync(connection, cancellationToken).ConfigureAwait(false);

        var (floor, _) = await ReadFloorsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        if (floor >= protocol)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }

        bool anyBelow;
        await using (var members = new NpgsqlCommand(AnyBelowProtocolSql, connection, transaction))
        {
            members.Parameters.AddWithValue("protocol", protocol);
            anyBelow = (bool)(await members.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        await points.ReachAsync(MembershipFaultPoints.FloorRaiseAfterMembersRead, cancellationToken).ConfigureAwait(false);
        if (anyBelow)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }

        await using (var update = new NpgsqlCommand(RaiseProtocolSql, connection, transaction))
        {
            update.Parameters.AddWithValue("protocol", protocol);
            update.Parameters.AddWithValue("by", by);
            await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await points.ReachAsync(MembershipFaultPoints.FloorRaiseBeforeCommit, cancellationToken).ConfigureAwait(false);
        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> RaiseVersionAsync(string version, string by, CancellationToken cancellationToken)
    {
        var read = await ReadAsync(cancellationToken).ConfigureAwait(false);
        switch (FloorRules.DecideVersionRaise(version, read.VersionFloor, []))
        {
            case VersionRaiseDecision.AlreadyAtOrAbove:
                return true;
            case VersionRaiseDecision.FloorNotSemVer:
                return false;
        }

        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await BeginUnderLockAsync(connection, cancellationToken).ConfigureAwait(false);

        var (_, floorRead) = await ReadFloorsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var versions = new List<string>();
        await using (var members = new NpgsqlCommand(MemberVersionsSql, connection, transaction))
        await using (var reader = await members.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                versions.Add(reader.GetString(0));
            }
        }

        await points.ReachAsync(MembershipFaultPoints.FloorRaiseAfterMembersRead, cancellationToken).ConfigureAwait(false);
        var decision = FloorRules.DecideVersionRaise(version, floorRead, versions);
        if (decision != VersionRaiseDecision.Raise)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return decision == VersionRaiseDecision.AlreadyAtOrAbove;
        }

        int rows;
        await using (var update = new NpgsqlCommand(RaiseVersionSql, connection, transaction))
        {
            update.Parameters.AddWithValue("version", version);
            update.Parameters.AddWithValue("by", by);
            update.Parameters.Add(new NpgsqlParameter("floor_read", NpgsqlDbType.Text) { Value = (object?)floorRead ?? DBNull.Value });
            rows = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        if (rows == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }

        await points.ReachAsync(MembershipFaultPoints.FloorRaiseBeforeCommit, cancellationToken).ConfigureAwait(false);
        await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<FloorLowering> LowerProtocolAsync(int protocol, string by, CancellationToken cancellationToken)
    {
        await points.ReachAsync(MembershipFaultPoints.FloorLowerBeforeWrite, cancellationToken).ConfigureAwait(false);
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(LowerProtocolSql, connection);
        command.Parameters.AddWithValue("protocol", protocol);
        command.Parameters.AddWithValue("by", by);
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return rows == 1 ? FloorLowering.Lowered : FloorLowering.NotAbove;
    }

    public async Task<FloorLowering> LowerVersionAsync(string? version, string by, CancellationToken cancellationToken)
    {
        var floorRead = (await ReadAsync(cancellationToken).ConfigureAwait(false)).VersionFloor;
        if (FloorRules.DecideVersionLower(version, floorRead) == VersionLowerDecision.NotAbove)
        {
            return FloorLowering.NotAbove;
        }

        await points.ReachAsync(MembershipFaultPoints.FloorLowerBeforeWrite, cancellationToken).ConfigureAwait(false);
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(LowerVersionSql, connection);
        command.Parameters.Add(new NpgsqlParameter("version", NpgsqlDbType.Text) { Value = (object?)version ?? DBNull.Value });
        command.Parameters.AddWithValue("by", by);
        command.Parameters.Add(new NpgsqlParameter("floor_read", NpgsqlDbType.Text) { Value = (object?)floorRead ?? DBNull.Value });
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return rows == 1 ? FloorLowering.Lowered : FloorLowering.Changed;
    }

    public async Task<bool> AllMembersSpeakAsync(int protocol, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(AnyBelowProtocolSql, connection);
        command.Parameters.AddWithValue("protocol", protocol);
        return !(bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    public async Task<bool> AllMembersAtLeastAsync(string version, CancellationToken cancellationToken)
    {
        var versions = new List<string>();
        await using (var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
        await using (var command = new NpgsqlCommand(MemberVersionsSql, connection))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                versions.Add(reader.GetString(0));
            }
        }

        return FloorRules.AllAtLeast(version, versions);
    }

    public async Task<OlderActiveMember?> FindOlderActiveMemberAsync(string selfReplicaId, int minimumProtocol, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(OlderActiveSql, connection);
        command.Parameters.AddWithValue("minimum", minimumProtocol);
        command.Parameters.AddWithValue("self", selfReplicaId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? new OlderActiveMember(reader.GetString(0), reader.GetInt32(1)) : null;
    }

    private async Task<NpgsqlTransaction> BeginUnderLockAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await points.ReachAsync(MembershipFaultPoints.FloorRaiseBeforeLock, cancellationToken).ConfigureAwait(false);
        var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        try
        {
            _ = await TransactionTimeout.FirstAsync(
                    connection, transaction, boundMilliseconds, new NpgsqlBatchCommand(ActivationStore.LockSql), static _ => true, cancellationToken)
                .ConfigureAwait(false) ?? throw new InvalidOperationException("The change-log lock statement returned no row.");
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static async Task<(int Protocol, string? Version)> ReadFloorsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(FloorsSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("reeve_cluster.cluster has no row.");
        }

        return (reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }
}
