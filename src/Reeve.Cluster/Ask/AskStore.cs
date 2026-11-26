using Npgsql;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Ask;

internal sealed class AskStore(ClusterDataSource data) : IAskDirectory
{
    internal const string OwnerSql = """
        SELECT m.replica_id, m.process_id, o.token
        FROM reeve_cluster.owned o
        JOIN reeve_cluster.member m ON m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch
        WHERE o.kind = @kind AND o.key = @key
          AND m.state = 'active' AND m.lease_expires_at > clock_timestamp() AND m.serving_since IS NOT NULL
        """;

    internal const string MemberSql = """
        SELECT process_id, state FROM reeve_cluster.member WHERE replica_id = @id
        """;

    public async Task<AskOwner?> ResolveOwnerAsync(string kind, string key, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(OwnerSql, connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new AskOwner(reader.GetString(0), reader.GetGuid(1), reader.GetInt64(2))
            : null;
    }

    public async Task<AskMemberRow?> ResolveMemberAsync(string replicaId, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(MemberSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new AskMemberRow(reader.GetGuid(0), Enum.Parse<MemberRowState>(reader.GetString(1), ignoreCase: true))
            : null;
    }
}
