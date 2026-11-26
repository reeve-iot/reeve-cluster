using System.Data;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Log;

#pragma warning disable CA2007

namespace Reeve.Cluster.Owned;

internal sealed record PlacementRead(IReadOnlyList<PlacementMember> Members, IReadOnlyList<PlacedRow> Rows, IReadOnlyList<OverdueRelease> Overdue);

internal sealed record OverdueRelease(string Kind, string Key, long Token, string Owner, DateTimeOffset RequestedAt);

internal sealed partial class OwnedStore
{
    internal const string PlacerFenceSql = "SELECT reeve_cluster.assert_owner(@placer_kind, @placer_key, @placer_token, @placer_timeline)";

    internal const string PlacementMembersSql = """
        SELECT replica_id, epoch, capabilities FROM reeve_cluster.member
        WHERE state = 'active' AND lease_expires_at > clock_timestamp() AND serving_since IS NOT NULL
        """;

    internal const string PlacementRowsSql = """
        SELECT o.kind, o.key, o.owner_replica, o.owner_epoch, o.token,
               coalesce(m.epoch = o.owner_epoch AND m.state = 'active' AND m.lease_expires_at > clock_timestamp(), false)
        FROM reeve_cluster.owned o
        LEFT JOIN reeve_cluster.member m ON m.replica_id = o.owner_replica
        WHERE o.kind = ANY(@kinds)
        """;

    internal const string OverdueReleasesSql = """
        SELECT r.kind, r.key, r.token, o.owner_replica, r.requested_at
        FROM reeve_cluster.release_request r
        JOIN reeve_cluster.owned o ON o.kind = r.kind AND o.key = r.key AND o.token = r.token
        WHERE o.owner_replica IS NOT NULL AND r.requested_at + @grace < clock_timestamp()
        ORDER BY r.requested_at, r.kind, r.key
        """;

    internal const string PlacerInsertSql = """
        INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token)
        SELECT @kind, @key, @owner, @epoch, reeve_cluster.draw_token()
        WHERE EXISTS (
            SELECT 1 FROM reeve_cluster.member t
            WHERE t.replica_id = @owner AND t.epoch = @epoch AND t.state = 'active' AND t.lease_expires_at > clock_timestamp()
              AND t.serving_since IS NOT NULL AND t.capabilities @> @required AND NOT t.capabilities && @forbidden)
        ON CONFLICT (kind, key) DO NOTHING
        RETURNING token, current_setting('transaction_timeout')
        """;

    internal const string PlacerLockSql = """
        SELECT token, current_setting('transaction_timeout') FROM reeve_cluster.owned WHERE kind = @kind AND key = @key FOR NO KEY UPDATE
        """;

    internal const string PlacerTakeSql = """
        UPDATE reeve_cluster.owned o
        SET owner_replica = @owner, owner_epoch = @epoch, token = reeve_cluster.draw_token()
        WHERE o.kind = @kind AND o.key = @key AND o.token = @token
          AND o.owner_replica IS NOT DISTINCT FROM @expected_owner AND o.owner_epoch IS NOT DISTINCT FROM @expected_epoch
          AND NOT EXISTS (
            SELECT 1 FROM reeve_cluster.member m
            WHERE m.replica_id = o.owner_replica AND m.epoch = o.owner_epoch AND m.state = 'active'
              AND m.lease_expires_at > clock_timestamp())
          AND EXISTS (
            SELECT 1 FROM reeve_cluster.member t
            WHERE t.replica_id = @owner AND t.epoch = @epoch AND t.state = 'active' AND t.lease_expires_at > clock_timestamp()
              AND t.serving_since IS NOT NULL AND t.capabilities @> @required AND NOT t.capabilities && @forbidden)
        RETURNING o.token
        """;

    public async Task<PlacementRead> ReadPlacementAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken)
    {
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var batch = new NpgsqlBatch(connection)
        {
            BatchCommands =
            {
                new NpgsqlBatchCommand(PlacementMembersSql),
                new NpgsqlBatchCommand(PlacementRowsSql) { Parameters = { new NpgsqlParameter("kinds", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = kinds.ToArray() } } },
                new NpgsqlBatchCommand(OverdueReleasesSql) { Parameters = { new NpgsqlParameter("grace", NpgsqlDbType.Interval) { Value = options.Value.ReleaseGrace } } },
            },
        };
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var members = new List<PlacementMember>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            members.Add(new PlacementMember(reader.GetString(0), reader.GetInt64(1), reader.GetFieldValue<string[]>(2)));
        }

        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        var rows = new List<PlacedRow>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new OwnedRow(reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetInt64(3), reader.GetInt64(4));
            rows.Add(new PlacedRow(reader.GetString(0), reader.GetString(1), row, reader.GetBoolean(5)));
        }

        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        var overdue = new List<OverdueRelease>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            overdue.Add(new OverdueRelease(
                reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetString(3), reader.GetFieldValue<DateTimeOffset>(4)));
        }

        return new PlacementRead(members, rows, overdue);
    }

    public Task<long?> AssignAsync(IFencedOwner placer, Assignment assignment, EligibilityFilter eligibility, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        var (kind, key, expected, target) = assignment;
        var required = eligibility.Required.Append(ClusterCapabilities.ForUnit(kind)).Distinct(StringComparer.Ordinal).ToArray();
        NpgsqlBatchCommand Target(string sql, params (string Name, object Value)[] parameters)
        {
            var command = Command(sql, kind, key, [("owner", target.ReplicaId), ("epoch", target.Epoch), .. parameters]);
            command.Parameters.Add(new NpgsqlParameter("required", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = required });
            command.Parameters.Add(new NpgsqlParameter("forbidden", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = eligibility.Forbidden.ToArray() });
            return command;
        }

        if (expected is not { } row)
        {
            return RunPlacerAsync(placer, Target(PlacerInsertSql), null, cancellationToken);
        }

        var take = Target(PlacerTakeSql, ("token", row.Token));
        take.Parameters.Add(new NpgsqlParameter("expected_owner", NpgsqlDbType.Text) { Value = (object?)row.OwnerReplica ?? DBNull.Value });
        take.Parameters.Add(new NpgsqlParameter("expected_epoch", NpgsqlDbType.Bigint) { Value = (object?)row.OwnerEpoch ?? DBNull.Value });
        return RunPlacerAsync(placer, Command(PlacerLockSql, kind, key), take, cancellationToken);
    }

    public async Task<bool> RequestReleaseAsync(IFencedOwner placer, string kind, string key, long token, CancellationToken cancellationToken) =>
        await RunPlacerAsync(placer, Command(RequestSql, kind, key, ("token", token)), null, cancellationToken).ConfigureAwait(false) is not null;

    private async Task<long?> RunPlacerAsync(IFencedOwner placer, NpgsqlBatchCommand first, NpgsqlBatchCommand? then, CancellationToken cancellationToken)
    {
        var fence = placer.Start();
        await using var connection = await data.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        try
        {
            long value;
            var batch = new NpgsqlBatch(connection, transaction)
            {
                BatchCommands =
                {
                    TransactionTimeout.Statement(BoundMilliseconds()),
                    new NpgsqlBatchCommand(PlacerFenceSql)
                    {
                        Parameters =
                        {
                            new NpgsqlParameter("placer_kind", fence.Kind),
                            new NpgsqlParameter("placer_key", fence.Key),
                            new NpgsqlParameter("placer_token", fence.Token),
                            new NpgsqlParameter("placer_timeline", fence.Timeline),
                        },
                    },
                    first,
                },
            };
            await using (batch.ConfigureAwait(false))
            {
                var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        return null;
                    }

                    value = reader.GetInt64(0);
                }
            }

            if (then is not null)
            {
                var next = new NpgsqlBatch(connection, transaction) { BatchCommands = { then } };
                await using (next.ConfigureAwait(false))
                {
                    if (await next.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not long drawn)
                    {
                        return null;
                    }

                    value = drawn;
                }
            }

            await TransactionTimeout.CommitAsync(transaction, cancellationToken).ConfigureAwait(false);
            return value;
        }
        catch (PostgresException e) when (e.SqlState == FencedTransactions.OwnershipLost)
        {
            throw new OwnershipLostException($"The database refused the placer's tenure {fence.Token}: its row is no longer this tenure's, or its lease is gone.", e);
        }
        catch (PostgresException e) when (e.SqlState == FencedTransactions.TimelineChanged)
        {
            placer.OnOtherTimeline();
            throw new OwnershipLostException($"The server is on another timeline than the one the placer's replica joined on ({fence.Timeline}); the replica fences.", e);
        }
    }
}
