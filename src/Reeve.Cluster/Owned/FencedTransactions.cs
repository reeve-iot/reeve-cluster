using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;

namespace Reeve.Cluster.Owned;

internal readonly record struct FencedStart(string Kind, string Key, long Token, long Timeline, int TimeoutMilliseconds);

internal interface IFencedOwner
{
    FencedStart Start();

    void OnOtherTimeline();

    IFaultPoints FaultPoints { get; }
}

internal static class FencedTransactions
{
    internal const string OwnershipLost = "RC001";
    internal const string TimelineChanged = "RC002";

    internal const string AssertSql = "SELECT reeve_cluster.assert_owner($1, $2, $3, $4)";

    private static readonly TimeSpan TimeoutMargin = TimeSpan.FromMilliseconds(100);

    private static readonly ConditionalWeakTable<NpgsqlConnection, StrongBox<int>> Checked = [];

    public static IFencedOwner Owner(OwnedContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context as IFencedOwner
            ?? throw new ArgumentException("A fenced transaction needs the OwnedContext the library passed to OnAcquiredAsync.", nameof(context));
    }

    public static async Task CheckConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("A fenced transaction begins on an open connection with no transaction.");
        }

        var backend = connection.ProcessID;
        if (Checked.TryGetValue(connection, out var seen) && seen.Value == backend)
        {
            return;
        }

        var probe = new NpgsqlCommand(PhysicalConnectionCheck.ProbeSql, connection);
        await using (probe.ConfigureAwait(false))
        {
            var reader = await probe.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (PhysicalConnectionCheck.Evaluate(reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2)) is { } fault)
                {
                    throw fault;
                }
            }
        }

        Checked.AddOrUpdate(connection, new StrongBox<int>(backend));
    }

    public static async Task AssertAsync(
        IFencedOwner owner, FencedStart start, NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var assert = new NpgsqlBatchCommand(AssertSql)
        {
            Parameters =
            {
                new NpgsqlParameter<string> { TypedValue = start.Kind },
                new NpgsqlParameter<string> { TypedValue = start.Key },
                new NpgsqlParameter<long> { TypedValue = start.Token },
                new NpgsqlParameter<long> { TypedValue = start.Timeline },
            },
        };
        try
        {
            await SetBoundAndAssertAsync(connection, transaction, start.TimeoutMilliseconds, assert, cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException e) when (e.SqlState == OwnershipLost)
        {
            throw new OwnershipLostException($"The database refused {start.Kind}/{start.Key} tenure {start.Token}: the row is no longer this tenure's, or its owner's lease is gone.", e);
        }
        catch (PostgresException e) when (e.SqlState == TimelineChanged)
        {
            owner.OnOtherTimeline();
            throw new OwnershipLostException($"The server is on another timeline than the one the replica joined on ({start.Timeline}); the replica fences.", e);
        }

        await owner.FaultPoints.ReachAsync(OwnedFaultPoints.FencedAfterAssert, cancellationToken).ConfigureAwait(false);
    }

    private static async Task SetBoundAndAssertAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int timeoutMilliseconds, NpgsqlBatchCommand assert, CancellationToken cancellationToken)
    {
        var batch = new NpgsqlBatch(connection, transaction);
        await using (batch.ConfigureAwait(false))
        {
            batch.BatchCommands.Add(TransactionTimeout.Statement(timeoutMilliseconds));
            batch.BatchCommands.Add(assert);
            await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public static async Task CommitAsync(
        Func<CancellationToken, Task> commit, NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var commandTimeout = connection.CommandTimeout;
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        try
        {
            try
            {
                await commit(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (IsUnknown(e, cancellationToken, started, commandTimeout, connection))
            {
                outcome = "unknown";
                throw Unknown(e);
            }

            if (IsUnknown(null, cancellationToken, started, commandTimeout, connection))
            {
                outcome = "unknown";
                throw Unknown(null);
            }
            outcome = "committed";
        }
        finally
        {
            ClusterDiagnostics.RecordCommitDuration(Stopwatch.GetElapsedTime(started).TotalSeconds, "fenced", outcome);
        }
    }

    internal static bool IsUnknown(Exception? exception, CancellationToken cancellationToken, long started, int commandTimeout, NpgsqlConnection? connection) =>
        cancellationToken.IsCancellationRequested || TimedOut(started, commandTimeout) ||
        (exception is not null && exception is not PostgresException && connection is not null && connection.FullState != ConnectionState.Open);

    private static bool TimedOut(long started, int commandTimeout) =>
        commandTimeout > 0 && Stopwatch.GetElapsedTime(started) >= TimeSpan.FromSeconds(commandTimeout) - TimeoutMargin;

    private static CommitOutcomeUnknownException Unknown(Exception? cause)
    {
        const string Message = "The commit's outcome is unknown: it was cancelled, timed out or lost its connection before it returned, so it may be committed on the primary and not replicated.";
        return cause is null ? new CommitOutcomeUnknownException(Message) : new CommitOutcomeUnknownException(Message, cause);
    }
}
