using System.Collections.Frozen;
using System.Data.Common;

namespace Reeve.Cluster;

/// <summary>
/// A connection of the reconciler's own that imports the pass's exported snapshot, with the <c>REPEATABLE READ</c>
/// transaction that holds it, so an EF Core context can <c>UseTransaction</c> on it. Disposing it ends the transaction and
/// closes the connection.
/// </summary>
public interface ISnapshotSession : IAsyncDisposable
{
    /// <summary>The connection, on the pass's snapshot.</summary>
    DbConnection Connection { get; }

    /// <summary>The read-only <c>REPEATABLE READ</c> transaction on <see cref="Connection"/>.</summary>
    DbTransaction Transaction { get; }
}

/// <summary>
/// What an <see cref="IReconciler"/> attempt sees: the pass's snapshot — one <c>REPEATABLE READ</c>, read-only
/// transaction — and the hints that narrow what it re-reads. The library builds one per
/// attempt; the constructor is public so an application can test a reconciler without a cluster.
/// </summary>
public sealed class ReconcileContext
{
    private readonly Func<CancellationToken, Task<ISnapshotSession>> openSnapshotSession;
    private readonly FrozenDictionary<string, object> dependencies;
    private readonly string[] dependencyNames;

    /// <summary>Creates the context of one attempt of a type that declares no dependency.</summary>
    /// <param name="aggregateType">The type this attempt reconciles.</param>
    /// <param name="head">The snapshot's head <c>Hs</c>: the seq the resulting generation stands for.</param>
    /// <param name="passNumber">The number of the pass.</param>
    /// <param name="attempt">The number of this attempt within the pass; only the awaited attempt's result is published.</param>
    /// <param name="fullLoad">Whether every type is dirty in this pass (the first start or a full reload): load from scratch.</param>
    /// <param name="hints">Aggregate keys that changed since the cursor; <see langword="null"/> when the reconciler must run without hints.</param>
    /// <param name="previous">The type's derived state in the current generation; <see langword="null"/> on the first load.</param>
    /// <param name="transaction">The pass's snapshot transaction, on the applier's connection.</param>
    /// <param name="openSnapshotSession">Opens a connection of its own that imports the pass's exported snapshot; the caller disposes it.</param>
    public ReconcileContext(
        string aggregateType,
        long head,
        long passNumber,
        int attempt,
        bool fullLoad,
        IReadOnlySet<string>? hints,
        object? previous,
        DbTransaction transaction,
        Func<CancellationToken, Task<ISnapshotSession>> openSnapshotSession)
        : this(aggregateType, head, passNumber, attempt, fullLoad, hints, previous, transaction, openSnapshotSession, new Dictionary<string, object>())
    {
    }

    /// <summary>Creates the context of one attempt of a type that declares dependencies (<see cref="IReconciler.DependsOn"/>).</summary>
    /// <param name="aggregateType">The type this attempt reconciles.</param>
    /// <param name="head">The snapshot's head <c>Hs</c>: the seq the resulting generation stands for.</param>
    /// <param name="passNumber">The number of the pass.</param>
    /// <param name="attempt">The number of this attempt within the pass; only the awaited attempt's result is published.</param>
    /// <param name="fullLoad">Whether every type is dirty in this pass (the first start or a full reload): load from scratch.</param>
    /// <param name="hints">Aggregate keys that changed since the cursor; <see langword="null"/> when the reconciler must run without hints.</param>
    /// <param name="previous">The type's derived state in the current generation; <see langword="null"/> on the first load.</param>
    /// <param name="transaction">The pass's snapshot transaction, on the applier's connection.</param>
    /// <param name="openSnapshotSession">Opens a connection of its own that imports the pass's exported snapshot; the caller disposes it.</param>
    /// <param name="dependencies">The derived state of each declared dependency, by type; no value is <see langword="null"/>. The context copies it and compares the names ordinally.</param>
    public ReconcileContext(
        string aggregateType,
        long head,
        long passNumber,
        int attempt,
        bool fullLoad,
        IReadOnlySet<string>? hints,
        object? previous,
        DbTransaction transaction,
        Func<CancellationToken, Task<ISnapshotSession>> openSnapshotSession,
        IReadOnlyDictionary<string, object> dependencies)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        ArgumentNullException.ThrowIfNull(dependencies);
        foreach (var (type, state) in dependencies)
        {
            ArgumentNullException.ThrowIfNull(state, $"dependencies[{type}]");
        }

        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(openSnapshotSession);

        AggregateType = aggregateType;
        Head = head;
        PassNumber = passNumber;
        Attempt = attempt;
        FullLoad = fullLoad;
        Hints = hints;
        Previous = previous;
        Transaction = transaction;
        this.openSnapshotSession = openSnapshotSession;
        this.dependencies = dependencies.ToFrozenDictionary(StringComparer.Ordinal);
        dependencyNames = [.. this.dependencies.Keys];
    }

    /// <summary>The type this attempt reconciles.</summary>
    public string AggregateType { get; }

    /// <summary>The snapshot's head <c>Hs</c>.</summary>
    public long Head { get; }

    /// <summary>The number of the pass.</summary>
    public long PassNumber { get; }

    /// <summary>The number of this attempt within the pass.</summary>
    public int Attempt { get; }

    /// <summary>Whether this pass loads every type in full.</summary>
    public bool FullLoad { get; }

    /// <summary>
    /// Keys that changed since the cursor. Hints only narrow what a reconciler re-reads; <see langword="null"/> means
    /// none are given — a type with more than <c>HintLimit</c> keys, a type with a dependency that the pass also reconciles, and a retry after a stall run without them.
    /// </summary>
    public IReadOnlySet<string>? Hints { get; }

    /// <summary>The type's derived state in the current generation, or <see langword="null"/> before the first load.</summary>
    public object? Previous { get; }

    /// <summary>The types this attempt's type declared in <see cref="IReconciler.DependsOn"/>.</summary>
    public IReadOnlyCollection<string> Dependencies => dependencyNames;

    /// <summary>The pass's snapshot transaction. Read through it (an EF Core context bound to it, or its connection); what is read elsewhere is not covered by the configuration guarantees.</summary>
    public DbTransaction Transaction { get; }

    /// <summary>The applier's connection, on which <see cref="Transaction"/> runs.</summary>
    public DbConnection Connection =>
        Transaction.Connection ?? throw new InvalidOperationException("The snapshot transaction has no connection.");

    /// <summary>
    /// Opens a connection of its own on the same snapshot (<c>SET TRANSACTION SNAPSHOT</c>), for a reconciler that
    /// reads in parallel. Keeps the library's bounded <c>CommandTimeout</c>; the caller disposes the session.
    /// </summary>
    public Task<ISnapshotSession> OpenSnapshotSessionAsync(CancellationToken cancellationToken = default) =>
        openSnapshotSession(cancellationToken);

    /// <summary>
    /// The derived state the published generation will hold for a declared dependency: this pass's accepted result when the
    /// pass reconciles it, else the current generation's. A type that depends on another is reconciled whenever that one is,
    /// so the state read here is the one this type's own state is published beside. Only the state itself: what the
    /// dependency's <see cref="ReconcileResult.Deferred"/> work activates does not exist yet.
    /// </summary>
    /// <param name="aggregateType">A type listed in <see cref="IReconciler.DependsOn"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="aggregateType"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The type is not a declared dependency: a read outside the declaration escapes the pass's closure.</exception>
    public object GetDependency(string aggregateType)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        return dependencies.TryGetValue(aggregateType, out var state)
            ? state
            : throw new InvalidOperationException(
                $"'{aggregateType}' is not a declared dependency of '{AggregateType}'; list it in the reconciler's DependsOn.");
    }

    /// <inheritdoc cref="GetDependency(string)"/>
    /// <typeparam name="T">The type of the dependency's state.</typeparam>
    /// <exception cref="InvalidCastException">The state is not a <typeparamref name="T"/>.</exception>
    public T GetDependency<T>(string aggregateType)
        where T : class => (T)GetDependency(aggregateType);
}
