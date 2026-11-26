using System.Data.Common;

namespace Reeve.Cluster;

/// <summary>
/// The kind of an ambient scope: what the code inside it is, which decides how fencing and acknowledgement treat it
/// </summary>
/// <remarks>
/// The HTTP package begins a request's scope in the outermost middleware, before routing, when the endpoint's
/// <c>[AllowWhenFenced]</c> is not yet known. It therefore begins it as <see cref="ClientRequest"/> — the fail-closed
/// kind — and classifies it once as <see cref="ExemptRequest"/> after routing, before the handler runs and so before the
/// request's first append. Nothing in a scope's life changes a kind but that one step; the library's internal writer
/// refuses it after the first recorded seq or refused commit.
/// </remarks>
public enum ClusterScopeKind
{
    /// <summary>A client request or hub invocation that is not exempt. Fence step 2 refuses its commits; its seqs are tracked.</summary>
    ClientRequest,

    /// <summary>A request marked <c>[AllowWhenFenced]</c>: its writes commit in every phase and are not awaited; its seqs are not recorded in the tracker.</summary>
    ExemptRequest,

    /// <summary>One unit of work of an owned unit or a background service.</summary>
    UnitOfWork,

    /// <summary>Other background code with an explicit begin and end.</summary>
    Background,

    /// <summary>The applier's own execution context, which a reconciler runs in.</summary>
    Applier,
}

/// <summary>
/// The ambient record of one request, hub invocation or unit of work: the seqs it appended, a refused commit, the
/// opt-out, the pinned generation and the open transactions that hold entries
/// (its tracker). This is the read side, for application code and the HTTP filter; only the
/// library writes it, through an internal interface, so application code cannot widen what a scope exempts. Members are
/// safe for concurrent use, and the collections are snapshots.
/// </summary>
public interface IClusterScope
{
    /// <summary>What the code inside the scope is.</summary>
    ClusterScopeKind Kind { get; }

    /// <summary>A snapshot of the seqs the scope appended, each recorded at its append; empty for an exempt scope.</summary>
    IReadOnlyList<long> Seqs { get; }

    /// <summary>Whether a commit of the scope was refused by fence step 2; decides the fenced-request answer.</summary>
    bool CommitRefused { get; }

    /// <summary>Whether the scope opted out of acknowledgement with <see cref="IChangeAck.AcknowledgeAtCommit"/>.</summary>
    bool AcknowledgedAtCommit { get; }

    /// <summary>The generation the scope pinned at its read index; <see langword="null"/> before the pin.</summary>
    IGeneration? PinnedGeneration { get; }

    /// <summary>
    /// A snapshot of the transactions begun through EF that are still open and hold captured entries or an
    /// <c>Append</c>; the filter does not await a write while any is open. A
    /// transaction is listed before its seqs are recorded, so a reader takes <see cref="Seqs"/> first and this second. A
    /// transaction enters only through the EF capture, which sees it begin and end; a raw transaction the application
    /// begins is never listed, because the library cannot see how it ends (that stays the caller's rule, as for an
    /// <c>Append</c> in a transaction no context began through EF).
    /// </summary>
    IReadOnlyCollection<DbTransaction> OpenTransactions { get; }
}

/// <summary>
/// Reads the ambient scope and begins the scopes application code owns. A scope for a request or a hub invocation is
/// begun by the ASP.NET Core package, and the applier's by the applier; neither is available here, and a begin inside one
/// of those joins it, so that application code inside a client request cannot begin a scope that escapes fence step 2 or
/// the request's tracker. The one begin that does not join is the library's <c>Ask</c> dispatch: a handler runs in a scope of its
/// own, begun with the caller's execution context suppressed, so its writes are neither refused by the caller's fence step 2 nor
/// held by the caller's tracker.
/// </summary>
public interface IClusterScopeAccessor
{
    /// <summary>
    /// The scope ambient to the calling flow, or <see langword="null"/> outside every scope. A scope ends when its handle is
    /// disposed: a flow that outlives it — a task, a timer or a loop started inside it — is outside it from then on.
    /// </summary>
    IClusterScope? Current { get; }

    /// <summary>
    /// Begins a <see cref="ClusterScopeKind.UnitOfWork"/> scope, ambient until the handle is disposed. Inside a live client
    /// request's, an exempt request's or the applier's scope it begins none: the handle names that scope, which stays
    /// ambient, and disposing it ends nothing — so a write there is still the request's (fenced, tracked and awaited as the
    /// request's) or the applier's (never awaited by a reconciler).
    /// </summary>
    IClusterScopeHandle BeginUnitOfWork();

    /// <summary>
    /// Begins a <see cref="ClusterScopeKind.Background"/> scope, ambient until the handle is disposed; inside a request's or
    /// the applier's scope it joins that scope, as <see cref="BeginUnitOfWork"/> does.
    /// </summary>
    IClusterScopeHandle BeginBackground();
}

/// <summary>The explicit end of a scope begun with <see cref="IClusterScopeAccessor"/>.</summary>
public interface IClusterScopeHandle : IDisposable
{
    /// <summary>The scope this handle ends, or, for a begin that joined a request's or the applier's scope, the scope it joined (its disposal then ends nothing).</summary>
    IClusterScope Scope { get; }
}
