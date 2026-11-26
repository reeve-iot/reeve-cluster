using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal static class ClaimAssertion
{
    internal const string AssertSql = "SELECT reeve_cluster.assert_claim($1, $2, $3)";

    internal static async Task AssertAsync(
        string key, Guid token, long timeline, int timeoutMilliseconds, Action onOtherTimeline, IFaultPoints points,
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var assert = new NpgsqlBatchCommand(AssertSql)
        {
            Parameters =
            {
                new NpgsqlParameter<string> { TypedValue = key },
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
            catch (PostgresException e) when (e.SqlState == "RC003")
            {
                throw new ClaimLostException($"The database refused the claim {key} of token {token}: it is not valid.", e);
            }
            catch (PostgresException e) when (e.SqlState == "RC002")
            {
                onOtherTimeline();
                throw new ClaimLostException($"The server is on another timeline than the one the replica joined on ({timeline}); the replica fences.", e);
            }
        }

        await points.ReachAsync(ClaimsFaultPoints.FencedAfterAssert, cancellationToken).ConfigureAwait(false);
    }
}
