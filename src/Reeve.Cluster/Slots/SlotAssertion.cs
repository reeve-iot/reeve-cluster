using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Slots;

internal static class SlotAssertion
{
    internal const string AssertSql = "SELECT reeve_cluster.assert_slot($1, $2, $3, $4)";

    internal static async Task AssertAsync(
        string key, string id, Guid token, long timeline, int timeoutMilliseconds, Action onOtherTimeline, IFaultPoints points,
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var assert = new NpgsqlBatchCommand(AssertSql)
        {
            Parameters =
            {
                new NpgsqlParameter<string> { TypedValue = key },
                new NpgsqlParameter<string> { TypedValue = id },
                new NpgsqlParameter<Guid> { TypedValue = token },
                new NpgsqlParameter<long> { TypedValue = timeline },
            },
        };
        var batch = new NpgsqlBatch(connection, transaction);
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(TransactionTimeout.Statement(timeoutMilliseconds));
            batch.BatchCommands.Add(assert);
            try
            {
                await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException e) when (SlotDecisions.Refusal(e.SqlState) == SlotRefusal.SlotLost)
            {
                throw new SlotLostException($"The database refused the slot {id} of {key} of token {token}: it is not valid.", e);
            }
            catch (PostgresException e) when (SlotDecisions.Refusal(e.SqlState) == SlotRefusal.OtherTimeline)
            {
                onOtherTimeline();
                throw new SlotLostException($"The server is on another timeline than the one the replica joined on ({timeline}); the replica fences.", e);
            }
        }

        await points.ReachAsync(SlotsFaultPoints.FencedAfterAssert, cancellationToken).ConfigureAwait(false);
    }
}
