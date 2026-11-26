namespace Reeve.Cluster;

/// <summary>
/// The local phase for a component outside a request: it stops at a fence and starts again at <see cref="ReplicaPhase.Serving"/>
/// without polling readiness. It is not the serving check — fence start may have passed with
/// the phase still <see cref="ReplicaPhase.Serving"/> — so no action rests on it: the action pins with
/// <see cref="IClusterGenerations.TryPin"/>, whose refusal keeps a missed transition safe.
/// </summary>
public interface IReplicaPhase
{
    /// <summary>The local phase now.</summary>
    ReplicaPhase Current { get; }

    /// <summary>
    /// Completes once the phase is not <paramref name="seen"/> — at once when it already is not — with the phase it read then.
    /// Level-triggered: phases passed through between two reads are not reported, and a fence followed by a rejoin can be read
    /// as <see cref="ReplicaPhase.Serving"/> twice.
    /// </summary>
    /// <remarks>
    /// <para>When it waits, its continuations run on the thread pool, never inside a phase transition, a fence step or the
    /// token's cancellation. When <paramref name="seen"/> is already stale the task is complete on return, so an
    /// <see langword="await"/> continues on the caller's thread; a token already cancelled answers a cancelled task first.</para>
    /// <para>A wait holds a subscription to the phase until it ends: pass a token, so that a wait the caller abandons does not
    /// stay until the next transition.</para>
    /// </remarks>
    /// <param name="seen">The phase the caller last read.</param>
    /// <param name="cancellationToken">Cancels the wait; the task is then cancelled.</param>
    /// <returns>The phase read once it was not <paramref name="seen"/>.</returns>
    Task<ReplicaPhase> WaitForChangeAsync(ReplicaPhase seen, CancellationToken cancellationToken = default);
}
