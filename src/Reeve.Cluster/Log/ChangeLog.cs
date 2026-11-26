using System.Data;
using System.Data.Common;

namespace Reeve.Cluster.Log;

internal static class ChangeLogLock
{
    public const long Key = 0x52454556434C4F47;
}

internal readonly record struct AppendResult(long HighestSeq, string Xid);

internal sealed class ChangeLog : IChangeLog
{
    private const string AppendSql = """
        INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key)
        SELECT u.t, u.k FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS u(t, k, n) ORDER BY u.n
        RETURNING seq, xid::text
        """;

    private readonly IClusterScopeSource scopes;
    private readonly CommitRefusal refusal;
    private readonly EfTransactions efTransactions;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;

    public ChangeLog(IClusterScopeSource scopes, CommitRefusal refusal, EfTransactions efTransactions, IFaultPoints faultPoints, IModelEventSink events)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(refusal);
        ArgumentNullException.ThrowIfNull(efTransactions);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);

        this.scopes = scopes;
        this.refusal = refusal;
        this.efTransactions = efTransactions;
        this.faultPoints = faultPoints;
        this.events = events;
    }

    public async Task<long> AppendAsync(DbTransaction transaction, IReadOnlyCollection<ChangeEntry> entries, CancellationToken cancellationToken = default) =>
        (await AppendAsync(transaction, entries, scopes.CurrentWriter, cancellationToken).ConfigureAwait(false)).HighestSeq;

    internal Task<AppendResult> AppendInCurrentScopeAsync(DbTransaction transaction, IReadOnlyCollection<ChangeEntry> entries, CancellationToken cancellationToken) =>
        AppendAsync(transaction, entries, scopes.CurrentWriter, cancellationToken);

    internal async Task<AppendResult> AppendAsync(
        DbTransaction transaction, IReadOnlyCollection<ChangeEntry> entries, IClusterScopeWriter? scope, CancellationToken cancellationToken, Action? rolledBack = null)
    {
        scope = scope.UnlessEnded();

        ArgumentNullException.ThrowIfNull(transaction, nameof(transaction));
        ArgumentNullException.ThrowIfNull(entries);
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The transaction has ended: Append runs inside an open transaction, as its last statement before the commit.");

        var types = new string[entries.Count];
        var keys = new string?[entries.Count];
        var i = 0;
        foreach (var entry in entries)
        {
            if (entry is null || string.IsNullOrEmpty(entry.AggregateType))
            {
                throw new ArgumentException("Every entry names an aggregate type.", nameof(entries));
            }

            types[i] = entry.AggregateType;
            keys[i] = entry.AggregateKey;
            i++;
        }

        if (types.Length == 0)
        {
            throw new ArgumentException("Append takes at least one entry.", nameof(entries));
        }

        await faultPoints.ReachAsync(LogFaultPoints.AppendBefore, cancellationToken).ConfigureAwait(false);

        if (refusal.Refuses(scope))
        {
            var (refused, ended) = await RefuseCommitAsync(transaction, scope!, refusedAppend: true, types).ConfigureAwait(false);
            Notify(ended, rolledBack);
            throw refused;
        }

        long[] seqs;
        string xid;
        try
        {
            (seqs, xid) = await InsertAsync(connection, transaction, types, keys, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Notify(await RollBackAsync(transaction).ConfigureAwait(false), rolledBack);
            throw;
        }

        efTransactions.Holds(transaction, scope);
        foreach (var seq in seqs)
        {
            scope?.RecordSeq(seq);
        }

        events.Emit(new ModelEvent(LogActions.WAppend, new Dictionary<string, object?>
        {
            [ObservationKeys.Xid] = xid,
            [ObservationKeys.Seqs] = seqs,
            [ObservationKeys.Types] = types,
            [ObservationKeys.ScopeKind] = scope?.Kind.ToString(),
            [ObservationKeys.Request] = (scope as ClusterScope)?.Id,
            [ObservationKeys.Refused] = false,
        }));

        await faultPoints.ReachAsync(LogFaultPoints.AppendAfter, cancellationToken).ConfigureAwait(false);
        return new AppendResult(seqs[^1], xid);
    }

    internal async Task<(InvalidOperationException Refused, bool Ended)> RefuseCommitAsync(DbTransaction transaction, IClusterScopeWriter scope, bool refusedAppend, IReadOnlyCollection<string>? types = null)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(scope);
        scope.RecordRefusedCommit();
        if (refusedAppend)
        {
            events.Emit(new ModelEvent(LogActions.WAppend, new Dictionary<string, object?>
            {
                [ObservationKeys.Xid] = null,
                [ObservationKeys.Seqs] = Array.Empty<long>(),
                [ObservationKeys.Types] = types ?? [],
                [ObservationKeys.ScopeKind] = scope.Kind.ToString(),
                [ObservationKeys.Request] = (scope as ClusterScope)?.Id,
                [ObservationKeys.Refused] = true,
            }));
        }

        var ended = await RollBackAsync(transaction).ConfigureAwait(false);
        return (new InvalidOperationException(
            "The replica is fenced: fence step 2 refuses the commits of a client request that is not [AllowWhenFenced]. The transaction was rolled back."), ended);
    }

    private static async Task<(long[] Seqs, string Xid)> InsertAsync(
        DbConnection connection, DbTransaction transaction, string[] types, string?[] keys, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = AppendSql;
            command.Transaction = transaction;
            command.Parameters.Add(Parameter(command, types));
            command.Parameters.Add(Parameter(command, keys));

            var seqs = new List<long>(types.Length);
            string? xid = null;
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    seqs.Add(reader.GetInt64(0));
                    xid = reader.GetString(1);
                }
            }

            if (seqs.Count != types.Length || xid is null)
            {
                throw new InvalidOperationException($"The append returned {seqs.Count} seqs for {types.Length} entries.");
            }

            seqs.Sort();
            return ([.. seqs], xid);
        }
    }

    private static DbParameter Parameter(DbCommand command, object value)
    {
        var parameter = command.CreateParameter();
        parameter.Value = value;
        return parameter;
    }

    private static async Task<bool> RollBackAsync(DbTransaction transaction)
    {
        try
        {
            if (CannotCommit(transaction))
            {
                return true;
            }

            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is DbException or InvalidOperationException)
        {
            return CannotCommit(transaction);
        }
    }

    private static bool CannotCommit(DbTransaction transaction)
    {
        try
        {
            return transaction.Connection is not { State: ConnectionState.Open };
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    private static void Notify(bool ended, Action? rolledBack)
    {
        if (!ended || rolledBack is null)
        {
            return;
        }

        try
        {
            rolledBack();
        }
        catch (Exception)
        {
        }
    }
}
