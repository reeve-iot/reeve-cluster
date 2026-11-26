namespace Reeve.Cluster;

/// <summary>
/// The generation for a reader outside a request: a background service, a singleton component that reacts to events,
/// deferred work that retries its activation. A request reads
/// through its pin and a unit through <see cref="OwnedContext.Configuration"/>; the local phase is
/// <see cref="IReplicaPhase"/>'s.
/// </summary>
public interface IClusterGenerations
{
    /// <summary>
    /// The newest generation the replica published, or <see langword="null"/> before the first load, with no check and no
    /// pin. On a replica that does not serve it may be older than an acknowledged write, so no action rests on it: it is for
    /// diagnostics, for waiting for the first load, and for deferred work whose <see cref="IGeneration.TryActivate"/>
    /// returned <see langword="false"/>. Inside a
    /// <see cref="IGeneration.TryActivate"/> activation, which holds the swap, it is the generation whose state of the
    /// activated type the activation found current.
    /// </summary>
    IGeneration? Current { get; }

    /// <summary>
    /// The read for an action. The caller begins one scope per action with
    /// <see cref="IClusterScopeAccessor.BeginUnitOfWork"/> or <see cref="IClusterScopeAccessor.BeginBackground"/> and calls
    /// it there; a scope held around a loop keeps its first pin for every later action. The answer depends on the ambient
    /// scope:
    /// <list type="bullet">
    /// <item><description>A unit of work or background scope that has pinned: its pin, without a check.</description></item>
    /// <item><description>A unit of work or background scope that has not pinned: runs the serving check, reads the current
    /// generation, and runs the check again; when both pass within one join (the same epoch), pins that generation into the
    /// scope and returns it; otherwise <see langword="null"/>, and a check that found fence start passed starts the fence.</description></item>
    /// <item><description>A client or exempt request's scope that has pinned, including a begin that joined it: the request's
    /// pin, without a check.</description></item>
    /// <item><description>A client or exempt request's scope that has not pinned — a hub's disconnection handler, the request that
    /// carries a hub connection while it lives: as the second case, but the generation is returned without being pinned into
    /// the request's scope, so two calls may answer two generations and the caller keeps the one it got for the
    /// action.</description></item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// <para><see langword="null"/> means the replica does not serve: the caller does not act, and tries again at its next
    /// action. In a unit's scope the pin taken here is the one <see cref="OwnedContext.Configuration"/> answers afterwards;
    /// the unit's effects still run <see cref="OwnedContext.EnsureOwner"/>.</para>
    /// <para>What a pinned reader is promised: once a write has been acknowledged, a call that then
    /// runs its checks — the second and fourth cases — and returns a generation, on any replica, returns one that includes
    /// the write, except for a type the acknowledgement names as skipped: it read the generation between two checks
    /// that passed within one join, so while the replica was serving, active and leased. An earlier pin, which the first and third
    /// cases return, has that pin's guarantee: a unit of work's or background scope's is as of when it pinned, a request's
    /// is the request's. This is not the read index: it does not wait for the head, so a write in flight may be in one
    /// replica's generation and not yet in another's. Readers outside a request see every acknowledged write, but are not promised one whole, linearizable read.</para>
    /// </remarks>
    /// <returns>The generation for the action, or <see langword="null"/> when the replica does not serve.</returns>
    /// <exception cref="InvalidOperationException">No scope is live; the ambient scope is the applier's (a reconciler reads
    /// its snapshot and <see cref="ReconcileContext.Previous"/>, so code it shares with background readers takes the
    /// generation as a parameter); or the call comes from inside an <see cref="IGeneration.TryActivate"/> activation, where a
    /// check could run fence step 0 under the swap's lock (an activation reads <see cref="Current"/>).</exception>
    IGeneration? TryPin();
}
