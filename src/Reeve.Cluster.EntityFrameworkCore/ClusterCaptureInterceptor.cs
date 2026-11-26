using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Reeve.Cluster.Log;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed partial class ClusterCaptureInterceptor : ISaveChangesInterceptor, IDbTransactionInterceptor, IDisposable
{
    internal const string DiscardsName = "reeve_cluster.capture.discards";

    private static readonly Counter<long> Discards = ClusterDiagnostics.Meter.CreateCounter<long>(
        DiscardsName,
        unit: "{transaction}",
        description: "Transactions whose captured entries were discarded at a later TransactionStarted on the same DbTransaction: each ended around EF, and its changes committed without their change-log entries or rolled back unseen.");

    private readonly IClusterMapping mapping;
    private readonly IClusterScopeSource scopes;
    private readonly ChangeLog changeLog;
    private readonly CommitRefusal refusal;
    private readonly EfTransactions efTransactions;
    private readonly PostCommitWakeup postCommit;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;
    private readonly ILogger<ClusterCaptureInterceptor> logger;
    private readonly DisposalFeed disposals;

    private readonly object gate = new();

    private readonly ConditionalWeakTable<DbTransaction, Dictionary<DbContext, Pair>> pairs = new();

    private readonly ConditionalWeakTable<DbContext, SaveState> saves = new();

    public ClusterCaptureInterceptor(
        IClusterMapping mapping,
        IClusterScopeSource scopes,
        ChangeLog changeLog,
        CommitRefusal refusal,
        EfTransactions efTransactions,
        PostCommitWakeup postCommit,
        IFaultPoints faultPoints,
        IModelEventSink events,
        ILogger<ClusterCaptureInterceptor> logger)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(changeLog);
        ArgumentNullException.ThrowIfNull(refusal);
        ArgumentNullException.ThrowIfNull(efTransactions);
        ArgumentNullException.ThrowIfNull(postCommit);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(logger);

        this.mapping = mapping;
        this.scopes = scopes;
        this.changeLog = changeLog;
        this.refusal = refusal;
        this.efTransactions = efTransactions;
        this.postCommit = postCommit;
        this.faultPoints = faultPoints;
        this.events = events;
        this.logger = logger;
        disposals = new DisposalFeed(this);
    }

    public void Dispose() => disposals.Dispose();

    internal int PairCount
    {
        get
        {
            lock (gate)
            {
                return pairs.Sum(kv => kv.Value.Count);
            }
        }
    }

    public InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Saving(eventData.Context);
        return result;
    }

    public ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        Saving(eventData.Context);
        return ValueTask.FromResult(result);
    }

    public int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        Ended(eventData.Context, saved: true);
        return result;
    }

    public ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        Ended(eventData.Context, saved: true);
        return ValueTask.FromResult(result);
    }

    public void SaveChangesFailed(DbContextErrorEventData eventData) => Ended(eventData.Context, saved: false);

    public Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken cancellationToken = default)
    {
        Ended(eventData.Context, saved: false);
        return Task.CompletedTask;
    }

    private void Saving(DbContext? context)
    {
        if (context is null)
        {
            return;
        }

        if (context.ChangeTracker.CascadeDeleteTiming == CascadeTiming.OnSaveChanges || context.ChangeTracker.DeleteOrphansTiming == CascadeTiming.OnSaveChanges)
        {
            throw new InvalidOperationException(
                "The capture needs ChangeTracker.CascadeDeleteTiming and DeleteOrphansTiming to be Immediate or Never: under OnSaveChanges "
                + "EF deletes dependents after the interceptor read the changes, and their entries would be lost.");
        }

        var entries = EntryCapture.Entries(context, mapping);
        var ambient = scopes.CurrentWriter;
        var transaction = context.Database.CurrentTransaction?.GetDbTransaction();
        var commitScope = transaction is null ? ambient : ScopeForSave(context, transaction, ambient);
        if (entries.Count > 0 && HttpRequestFlow.IsInside && commitScope.UnlessEnded() is null)
        {
            var entities = string.Join(", ", context.ChangeTracker.Entries()
                .Where(entry => entry.State is (EntityState.Added or EntityState.Modified or EntityState.Deleted)
                    && mapping.Find(entry.Metadata.ClrType) is not null)
                .Select(entry => entry.Metadata.ClrType.FullName)
                .Distinct());
            var subject = entities.Length > 0
                ? $"mapped entity {entities}"
                : $"mapped aggregate type {string.Join(", ", entries.Select(entry => entry.AggregateType).Distinct())} (through an owned, many-to-many or cascade change)";
            throw RefuseSave(
                $"A write to {subject} ran inside an HTTP request outside the live cluster scope opened by UseReeveCluster(). "
                + "Nothing was written.", ClusterConfigurationReason.WriteOutsideClusterScope, emitEvent: false);
        }

        if (entries.Count > 0 || ambient is { Kind: ClusterScopeKind.ClientRequest })
        {
            context.Database.AutoTransactionBehavior = AutoTransactionBehavior.Always;
        }

        lock (gate)
        {
            saves.Remove(context);
        }

        if (entries.Count == 0)
        {
            return;
        }

        if (Transaction.Current is not null || context.Database.GetEnlistedTransaction() is not null)
        {
            throw RefuseSave(
                "A save that captures configuration changes cannot run under an ambient System.Transactions transaction "
                + "(Transaction.Current, or one the connection is enlisted in): its commit is not one the capture sees, and the "
                + "changes would commit without their change-log entries. Nothing was written.");
        }

        Pair? taker = null;
        lock (gate)
        {
            if (transaction is null)
            {
                saves.AddOrUpdate(context, new SaveState(entries));
                return;
            }

            if (TryFind(context, transaction, out var own))
            {
                taker = own.BeganThroughEf ? own : BeginningPairFor(transaction, ambient);
            }

            taker?.Entries.AddRange(entries);
        }

        if (taker is null)
        {
            throw RefuseSave(
                "A save that captures configuration changes must run without a transaction, in one its context began through EF "
                + "(Database.BeginTransaction), or in one it joined with UseTransaction that another context of the same capture "
                + "began through EF, in the scope of this save, and that has not ended. This transaction is none of these — raw, "
                + "begun by a context without the capture or in another scope, ended, or not joined since a later one began on "
                + "the connection — so its commit is not one the capture sees. Capture in the context that began the transaction, "
                + "or append with IChangeLog.Append. Nothing was written.");
        }

        efTransactions.Holds(transaction!, taker.Scope.UnlessEnded());
    }

    private IClusterScopeWriter? ScopeForSave(DbContext context, DbTransaction transaction, IClusterScopeWriter? ambient)
    {
        lock (gate)
        {
            if (TryFind(context, transaction, out var own))
            {
                if (own.BeganThroughEf)
                {
                    return own.Scope;
                }

                var beginning = BeginningPairForTransaction(transaction);
                return beginning is null ? own.Scope : beginning.Scope;
            }

            var beginningPair = BeginningPairForTransaction(transaction);
            return beginningPair is null ? ambient : beginningPair.Scope;
        }
    }

    private Pair? BeginningPairForTransaction(DbTransaction transaction) =>
        pairs.TryGetValue(transaction, out var contexts)
            ? contexts.Values.FirstOrDefault(pair => pair.BeganThroughEf)
            : null;

    private Pair? BeginningPairFor(DbTransaction transaction, IClusterScopeWriter? ambient) =>
        pairs.TryGetValue(transaction, out var contexts)
        && contexts.Values.FirstOrDefault(p => p.BeganThroughEf) is { } beginning
        && ReferenceEquals(beginning.Scope, ambient)
            ? beginning
            : null;

    private InvalidOperationException RefuseSave(string message, ClusterConfigurationReason? reason = null, bool emitEvent = true)
    {
        if (emitEvent)
        {
            events.Emit(new ModelEvent(LogActions.WSaveRefuse, new Dictionary<string, object?>
            {
                [ObservationKeys.Xid] = null,
                [ObservationKeys.Request] = (scopes.CurrentWriter as ClusterScope)?.Id,
            }));
        }
        return reason is { } named
            ? new ClusterConfigurationException(named, message)
            : new InvalidOperationException(message);
    }

    public void SaveChangesCanceled(DbContextEventData eventData) => Ended(eventData.Context, saved: false);

    public Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken cancellationToken = default)
    {
        Ended(eventData.Context, saved: false);
        return Task.CompletedTask;
    }

    private void Ended(DbContext? context, bool saved)
    {
        if (context is null)
        {
            return;
        }

        SaveState? state;
        lock (gate)
        {
            saves.Remove(context, out state);
        }

        if (state is null)
        {
            return;
        }

        if (saved && state.Transactions.Count == 0)
        {
            LogNoTransactionStarted(state.Entries.Count);
        }

        foreach (var (transaction, id) in state.Transactions)
        {
            Drop(context, transaction, id);
        }
    }

    public DbTransaction TransactionStarted(DbConnection connection, TransactionEndEventData eventData, DbTransaction result)
    {
        if (eventData.Context is not null)
        {
            Reach(CaptureFaultPoints.TransactionStarted);
        }

        Started(eventData.Context, eventData.TransactionId, result);
        return result;
    }

    public async ValueTask<DbTransaction> TransactionStartedAsync(DbConnection connection, TransactionEndEventData eventData, DbTransaction result, CancellationToken cancellationToken = default)
    {
        if (eventData.Context is not null)
        {
            await faultPoints.ReachAsync(CaptureFaultPoints.TransactionStarted, cancellationToken).ConfigureAwait(false);
        }

        Started(eventData.Context, eventData.TransactionId, result);
        return result;
    }

    private void Started(DbContext? context, Guid id, DbTransaction transaction)
    {
        if (context is null)
        {
            return;
        }

        var scope = scopes.CurrentWriter;
        var stale = 0;
        var staleEntries = 0;
        var held = false;
        lock (gate)
        {
            var contexts = pairs.GetOrCreateValue(transaction);
            foreach (var old in contexts.Values.Where(p => p.Holds))
            {
                stale++;
                staleEntries += old.Entries.Count - old.AppendedCount;
            }

            contexts.Clear();

            var pair = new Pair(id, scope, beganThroughEf: true);
            contexts[context] = pair;

            if (saves.TryGetValue(context, out var save))
            {
                pair.Entries.AddRange(save.Entries);
                save.Transactions.Add((transaction, id));
                held = true;
            }
        }

        if (stale > 0)
        {
            LogDiscardedEntries(staleEntries, stale);
            Discards.Add(stale);
        }

        efTransactions.Began(transaction);
        if (held)
        {
            efTransactions.Holds(transaction, scope.UnlessEnded());
        }
    }

    public DbTransaction TransactionUsed(DbConnection connection, TransactionEventData eventData, DbTransaction result)
    {
        if (eventData.Context is not null)
        {
            Reach(CaptureFaultPoints.TransactionUsed);
        }

        Used(eventData.Context, eventData.TransactionId, result);
        return result;
    }

    public async ValueTask<DbTransaction> TransactionUsedAsync(DbConnection connection, TransactionEventData eventData, DbTransaction result, CancellationToken cancellationToken = default)
    {
        if (eventData.Context is not null)
        {
            await faultPoints.ReachAsync(CaptureFaultPoints.TransactionUsed, cancellationToken).ConfigureAwait(false);
        }

        Used(eventData.Context, eventData.TransactionId, result);
        return result;
    }

    private void Used(DbContext? context, Guid id, DbTransaction transaction)
    {
        if (context is null)
        {
            return;
        }

        var scope = scopes.CurrentWriter;
        lock (gate)
        {
            var contexts = pairs.GetOrCreateValue(transaction);
            if (contexts.TryGetValue(context, out var own) && own.Holds)
            {
                throw new InvalidOperationException(
                    "The context holds captured configuration changes on this transaction's connection that were not appended: it "
                    + "cannot join a transaction there with UseTransaction until the transaction it captured in ends through EF "
                    + "(IDbContextTransaction.Commit, Rollback or its disposal).");
            }

            contexts[context] = new Pair(id, scope, beganThroughEf: false);
        }
    }

    public InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        CommittingAsync(transaction, eventData.Context, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return result;
    }

    public async ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        await CommittingAsync(transaction, eventData.Context, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private async ValueTask CommittingAsync(DbTransaction transaction, DbContext? context, CancellationToken cancellationToken)
    {
        Pair? mine = null;
        Pair? beginning = null;
        var foreign = false;
        ChangeEntry[]? entries = null;
        var captured = 0;
        lock (gate)
        {
            if (pairs.TryGetValue(transaction, out var contexts))
            {
                if (context is not null)
                {
                    contexts.TryGetValue(context, out mine);
                }

                foreign = contexts.Values.Any(p => !ReferenceEquals(p, mine) && p.Holds);
                beginning = contexts.Values.FirstOrDefault(p => p.BeganThroughEf);

                if (mine is { Holds: true })
                {
                    captured = mine.Entries.Count;
                    entries = [.. mine.Entries.GetRange(mine.AppendedCount, captured - mine.AppendedCount).Distinct()];
                }
            }
        }

        var own = mine is null ? scopes.CurrentWriter : mine.Scope;
        var scope = beginning is not null && refusal.Refuses(beginning.Scope) ? beginning.Scope : own;
        if (refusal.Refuses(scope))
        {
            var (refused, ended) = await changeLog.RefuseCommitAsync(transaction, scope!, refusedAppend: entries is not null).ConfigureAwait(false);

            if (!ReferenceEquals(own, scope) && refusal.Refuses(own))
            {
                own!.RecordRefusedCommit();
            }

            if (ended)
            {
                EndAllGuarded(transaction);
            }

            throw refused;
        }

        if (foreign)
        {
            throw new InvalidOperationException(context is null
                ? "A commit that names no context cannot be attributed, and a context's captured configuration changes on this transaction "
                  + "would commit without their change-log entries. Nothing was committed."
                : "Another context holds captured configuration changes on this transaction: commit it through the context that began "
                  + "it (IDbContextTransaction.Commit), which appends their change-log entries. Nothing was committed.");
        }

        if (entries is null)
        {
            return;
        }

        await faultPoints.ReachAsync(CaptureFaultPoints.CommittingBeforeAppend, cancellationToken).ConfigureAwait(false);
        var appended = await changeLog.AppendAsync(transaction, entries, mine!.Scope, cancellationToken, () => EndAllGuarded(transaction)).ConfigureAwait(false);
        lock (gate)
        {
            mine.AppendedCount = captured;
            mine.HighestSeq = appended.HighestSeq;
            mine.Xid = appended.Xid;
        }

        await faultPoints.ReachAsync(CaptureFaultPoints.CommittingAfterAppend, cancellationToken).ConfigureAwait(false);
    }

    public void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        CommittedAsync(transaction, eventData, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public async Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default) =>
        await CommittedAsync(transaction, eventData, cancellationToken).ConfigureAwait(false);

    private async ValueTask CommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken)
    {
        if (eventData.Context is null)
        {
            return;
        }

        foreach (var pair in EndAll(transaction))
        {
            if (pair.Appended)
            {
                await postCommit.AfterCommitAsync(pair.HighestSeq, pair.Xid, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData) => RolledBack(eventData.Context, transaction);

    public Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        RolledBack(eventData.Context, transaction);
        return Task.CompletedTask;
    }

    private void RolledBack(DbContext? context, DbTransaction transaction)
    {
        if (context is not null)
        {
            EndAll(transaction);
        }
    }

    private void Disposed(DbContext? context, DbTransaction? transaction, Guid id)
    {
        if (transaction is not null)
        {
            Drop(context, transaction, id);
        }
    }

    private bool TryFind(DbContext context, DbTransaction transaction, out Pair pair)
    {
        if (pairs.TryGetValue(transaction, out var contexts) && contexts.TryGetValue(context, out var found))
        {
            pair = found;
            return true;
        }

        pair = null!;
        return false;
    }

    private Pair? Drop(DbContext? context, DbTransaction transaction, Guid id)
    {
        Pair? pair = null;
        lock (gate)
        {
            if (context is not null
                && pairs.TryGetValue(transaction, out var contexts)
                && contexts.TryGetValue(context, out var found)
                && found.Id == id)
            {
                contexts.Remove(context);
                pair = found;
            }
        }

        if (pair is not null)
        {
            efTransactions.Ended(transaction);
        }

        return pair;
    }

    private List<Pair> EndAll(DbTransaction transaction)
    {
        List<Pair> ended;
        lock (gate)
        {
            if (!pairs.TryGetValue(transaction, out var contexts) || contexts.Count == 0)
            {
                return [];
            }

            ended = [.. contexts.Values];
            contexts.Clear();
        }

        efTransactions.Ended(transaction);
        return ended;
    }

    private void EndAllGuarded(DbTransaction transaction)
    {
        try
        {
            EndAll(transaction);
        }
        catch (Exception ex)
        {
            LogEndFailed(ex);
        }
    }

    private void Reach(FaultPoint point)
    {
        var reached = faultPoints.ReachAsync(point);
        if (!reached.IsCompletedSuccessfully)
        {
            reached.AsTask().GetAwaiter().GetResult();
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Discarded {Entries} captured change-log entries of {Transactions} transaction(s) on a reused DbTransaction: each ended without a commit, rollback or disposal event, so it was committed or rolled back around EF; if it committed, its changes have no change-log entries.")]
    private partial void LogDiscardedEntries(int entries, int transactions);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The capture's state of a transaction its own rollback ended could not be ended; the failure of the commit is thrown instead.")]
    private partial void LogEndFailed(Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "A save captured {Entries} change-log entries but EF began no transaction for it; they are not appended.")]
    private partial void LogNoTransactionStarted(int entries);

    private sealed class SaveState(List<ChangeEntry> entries)
    {
        public List<ChangeEntry> Entries { get; } = entries;

        public List<(DbTransaction Transaction, Guid Id)> Transactions { get; } = [];
    }

    private sealed class Pair(Guid id, IClusterScopeWriter? scope, bool beganThroughEf)
    {
        public Guid Id { get; } = id;

        public IClusterScopeWriter? Scope { get; } = scope;

        public bool BeganThroughEf { get; } = beganThroughEf;

        public List<ChangeEntry> Entries { get; } = [];

        public int AppendedCount { get; set; }

        public bool Appended => AppendedCount > 0;

        public bool Holds => Entries.Count > AppendedCount;

        public long HighestSeq { get; set; }

        public string? Xid { get; set; }
    }

    private sealed class DisposalFeed : IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>, IDisposable
    {
        private const string ListenerName = "Microsoft.EntityFrameworkCore";

        private readonly ClusterCaptureInterceptor owner;
        private readonly object feedGate = new();
        private readonly List<IDisposable> subscriptions = [];
        private IDisposable? all;
        private bool disposed;

        public DisposalFeed(ClusterCaptureInterceptor owner)
        {
            this.owner = owner;

            all = DiagnosticListener.AllListeners.Subscribe(this);
        }

        public void OnNext(DiagnosticListener listener)
        {
            if (listener.Name != ListenerName)
            {
                return;
            }

            lock (feedGate)
            {
                if (!disposed)
                {
                    subscriptions.Add(listener.Subscribe(this, static (name, _, _) => name == RelationalEventId.TransactionDisposed.Name));
                }
            }
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Key == RelationalEventId.TransactionDisposed.Name && value.Value is TransactionEventData data)
            {
                owner.Disposed(data.Context, data.Transaction, data.TransactionId);
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose()
        {
            lock (feedGate)
            {
                disposed = true;
                all?.Dispose();
                all = null;
                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }

                subscriptions.Clear();
            }
        }
    }
}
