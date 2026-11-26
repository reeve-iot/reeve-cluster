using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Status;

internal sealed record ReportedSection(string Name, string Json);

internal interface IStatusStore
{
    Task<int> ReportAsync(string replicaId, long epoch, IReadOnlyList<ReportedSection> sections, CancellationToken cancellationToken);

    Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken);
}

internal sealed class StatusStore(ClusterDataSource data, ClusterDataSource readData, IOptions<ClusterOptions> options) : IStatusStore
{
    internal const string WriteSql = """
        INSERT INTO reeve_cluster.member_status (replica_id, epoch, section, payload, updated_at)
        SELECT @id, @epoch, s.section, s.payload::jsonb, clock_timestamp()
        FROM unnest(@sections, @payloads) AS s(section, payload)
        WHERE EXISTS (SELECT 1 FROM reeve_cluster.member m
                      WHERE m.replica_id = @id AND m.epoch = @epoch AND m.lease_expires_at + @ttl > clock_timestamp())
        ON CONFLICT (replica_id, epoch, section) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at
        """;

    internal const string SweepSql = """
        DELETE FROM reeve_cluster.member_status s
        WHERE NOT EXISTS (SELECT 1 FROM reeve_cluster.member m
                          WHERE m.replica_id = s.replica_id AND m.epoch = s.epoch AND m.lease_expires_at + @ttl > clock_timestamp())
        """;

    internal const string ClockSql = "SELECT clock_timestamp()";

    internal const string MembersSql = """
        SELECT replica_id, epoch, state, lease_expires_at, lease_expires_at > clock_timestamp(), applied_seq, serving_since, version, protocol
        FROM reeve_cluster.member ORDER BY replica_id
        """;

    internal const string SectionsSql = """
        SELECT s.replica_id, s.epoch, s.section, s.payload::text, clock_timestamp() - s.updated_at
        FROM reeve_cluster.member_status s
        JOIN reeve_cluster.member m ON m.replica_id = s.replica_id AND m.epoch = s.epoch
        WHERE m.lease_expires_at + @ttl > clock_timestamp()
        ORDER BY s.replica_id, s.section
        """;

    internal const string FencesSql = """
        SELECT replica_id, epoch, trigger, fenced_at, span FROM reeve_cluster.fence_history
        WHERE replica_id IN (SELECT replica_id FROM reeve_cluster.member)
        ORDER BY replica_id, epoch DESC
        """;

    public async Task<int> ReportAsync(string replicaId, long epoch, IReadOnlyList<ReportedSection> sections, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(replicaId);
        ArgumentNullException.ThrowIfNull(sections);
        var ttl = options.Value.LeaseTtl;
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var batch = new NpgsqlBatch(connection);
        if (sections.Count > 0)
        {
            var write = new NpgsqlBatchCommand(WriteSql);
            write.Parameters.AddWithValue("id", replicaId);
            write.Parameters.AddWithValue("epoch", epoch);
            write.Parameters.Add(new NpgsqlParameter("sections", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = sections.Select(s => s.Name).ToArray() });
            write.Parameters.Add(new NpgsqlParameter("payloads", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = sections.Select(s => s.Json).ToArray() });
            write.Parameters.Add(new NpgsqlParameter("ttl", NpgsqlDbType.Interval) { Value = ttl });
            batch.BatchCommands.Add(write);
        }

        var sweep = new NpgsqlBatchCommand(SweepSql);
        sweep.Parameters.Add(new NpgsqlParameter("ttl", NpgsqlDbType.Interval) { Value = ttl });
        batch.BatchCommands.Add(sweep);

        await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return sections.Count > 0 ? (int)batch.BatchCommands[0].Rows : 0;
    }

    public async Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken)
    {
        var ttl = options.Value.LeaseTtl;
        await using var connection = await readData.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var batch = new NpgsqlBatch(connection)
        {
            BatchCommands =
            {
                new NpgsqlBatchCommand(ClockSql),
                new NpgsqlBatchCommand(MembersSql),
                new NpgsqlBatchCommand(SectionsSql) { Parameters = { new NpgsqlParameter("ttl", NpgsqlDbType.Interval) { Value = ttl } } },
                new NpgsqlBatchCommand(FencesSql),
            },
        };
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var readAt = reader.GetFieldValue<DateTimeOffset>(0);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        var members = new List<(string Id, long Epoch, string State, DateTimeOffset LeaseEnd, bool Valid, long Applied, DateTimeOffset? Serving, string Version, int Protocol)>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            members.Add((reader.GetString(0), reader.GetInt64(1), reader.GetString(2), reader.GetFieldValue<DateTimeOffset>(3), reader.GetBoolean(4),
                reader.GetInt64(5), await reader.IsDBNullAsync(6, cancellationToken).ConfigureAwait(false) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                reader.GetString(7), reader.GetInt32(8)));
        }

        var sections = new Dictionary<(string, long), List<ClusterStatusSection>>();
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = (reader.GetString(0), reader.GetInt64(1));
            if (!sections.TryGetValue(key, out var list))
            {
                sections[key] = list = [];
            }

            using var document = JsonDocument.Parse(reader.GetString(3));
            list.Add(new ClusterStatusSection { Name = reader.GetString(2), Values = document.RootElement.Clone(), Age = reader.GetFieldValue<TimeSpan>(4) });
        }

        var fences = new Dictionary<string, List<ClusterFenceRecord>>(StringComparer.Ordinal);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var id = reader.GetString(0);
            if (!fences.TryGetValue(id, out var list))
            {
                fences[id] = list = [];
            }

            list.Add(new ClusterFenceRecord { Epoch = reader.GetInt64(1), Trigger = reader.GetString(2), FencedAt = reader.GetFieldValue<DateTimeOffset>(3), Span = reader.GetFieldValue<TimeSpan>(4) });
        }

        return new ClusterStatusRead
        {
            ReadAt = readAt,
            Members = [.. members.Select(m => new ClusterMemberStatus
            {
                ReplicaId = m.Id,
                Epoch = m.Epoch,
                State = m.State,
                LeaseExpiresAt = m.LeaseEnd,
                LeaseValid = m.Valid,
                AppliedSeq = m.Applied,
                ServingSince = m.Serving,
                Version = m.Version,
                Protocol = m.Protocol,
                Sections = sections.GetValueOrDefault((m.Id, m.Epoch)) ?? [],
                Fences = fences.GetValueOrDefault(m.Id) ?? [],
            })],
        };
    }
}
