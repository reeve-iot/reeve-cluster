namespace Reeve.Cluster;

/// <summary>
/// The application's code for one unit kind: what it does while this replica owns a key of the
/// kind. One instance serves every key of its kind, resolved from the container as a singleton. It does not say what happens
/// when two replicas own a key, because through fenced paths that cannot happen.
/// </summary>
public interface IOwnedUnit
{
    /// <summary>
    /// Runs the key while this replica owns it; started only while the replica is <c>Serving</c>. May return early: the key
    /// stays owned until it is lost. An exception, other than cancellation once <paramref name="stop"/> is cancelled, is
    /// logged and this method is called again for the same tenure, context and <paramref name="stop"/> after 1 second, doubling
    /// to 60 seconds, until <paramref name="stop"/> is cancelled. A failure does not release the key.
    /// </summary>
    /// <param name="context">The key and its tenure.</param>
    /// <param name="stop">Cancelled when the library begins stopping the unit: at fence step 0, on a release request, on a drain, and when the row is no longer this tenure's.</param>
    Task OnAcquiredAsync(OwnedContext context, CancellationToken stop);

    /// <summary>
    /// Started when the key is lost, while <see cref="OnAcquiredAsync"/> may still be unwinding — or may not have started yet, and
    /// then starts with its <c>stop</c> token cancelled. It is not awaited past
    /// <c>StopBudget</c>: once it returned or <c>StopBudget</c> passed the key may run on another replica — on a release request
    /// or a drain this replica releases the row then — and the key does not start again on this replica until it returned.
    /// </summary>
    /// <param name="context">The context <see cref="OnAcquiredAsync"/> received.</param>
    Task OnLostAsync(OwnedContext context);
}

/// <summary>One tenure of one key, as the unit's code sees it.</summary>
public abstract class OwnedContext
{
    /// <summary>The unit kind.</summary>
    public abstract string Kind { get; }

    /// <summary>The key; a named singleton unit has one key.</summary>
    public abstract string Key { get; }

    /// <summary>The token of this tenure: never reused, higher than every earlier tenure's of the key.</summary>
    /// <exception cref="InvalidOperationException">The kind is convergent: a convergent unit gets no token.</exception>
    public abstract long Token { get; }

    /// <summary>
    /// The current generation, pinned for one unit of work: the first read in the ambient scope pins it, and every later read in
    /// that scope answers the same one. A unit takes it per action, inside a scope it begins for that action with
    /// <see cref="IClusterScopeAccessor.BeginUnitOfWork"/>, and does not hold it across actions. A read that pins reads the
    /// current generation, then runs <see cref="EnsureOwner"/>'s checks, and pins it only when they pass; a read in a scope that has pinned answers the pin, which may be the one
    /// <see cref="IClusterGenerations.TryPin"/> took. A read that would pin is refused inside an
    /// <see cref="IGeneration.TryActivate"/> activation.
    /// </summary>
    /// <exception cref="InvalidOperationException">No scope is ambient, nothing is published yet, or a read that would pin
    /// comes from inside an <see cref="IGeneration.TryActivate"/> activation.</exception>
    /// <exception cref="OwnershipLostException">A read that would pin finds the unit being stopped, or the replica not serving.</exception>
    public abstract IGeneration Configuration { get; }

    /// <summary>
    /// Throws once the library has begun stopping this unit — fence step 0, a release request, a drain, a row no longer this
    /// tenure's — or the replica's fence start has passed, in which case the check also starts the fence. Call it
    /// immediately before every external effect.
    /// </summary>
    /// <exception cref="OwnershipLostException">The unit no longer owns the key, or the replica no longer serves.</exception>
    /// <exception cref="InvalidOperationException">The kind is convergent: a convergent unit has no tenure to check.</exception>
    public abstract void EnsureOwner();

    /// <summary>
    /// Ends the resource: the unit is stopped — <see cref="EnsureOwner"/>
    /// throws from the call on — and the row is freed with its token, as the key's tombstone, so that a re-creation takes it
    /// under a higher token. <see cref="IOwnedUnit.OnLostAsync"/> then runs as for a lost key, and the key does not start again
    /// on this replica until it returned. An end that got no answer is retried as a release, which frees the row the same way,
    /// and the key is not started again here meanwhile; if the replica fences or stops first, the end does not happen, and the
    /// row is orphaned like any other.
    /// </summary>
    /// <param name="cancellationToken">Ends the call; the end's outcome is then unknown, and the library frees the row by its release.
    /// The unit's own <c>stop</c> token, which the end cancels, does not end it; a token linked from it does.</param>
    /// <exception cref="OwnershipLostException">The unit was already being stopped, or the row was no longer this tenure's.</exception>
    /// <exception cref="InvalidOperationException">The kind is not a keyed resource (<c>AddClusterKeyedResource</c>).</exception>
    public abstract Task EndAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// The owner of a key may no longer act on it: its unit is being stopped, the
/// replica no longer serves, or the database refused the tenure. Its effects must not happen. It derives from
/// <see cref="InvalidOperationException"/>: code that catches that and means to tell them apart catches this one first.
/// </summary>
public sealed class OwnershipLostException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public OwnershipLostException()
        : base("This replica no longer owns the key.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public OwnershipLostException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and its cause.</summary>
    public OwnershipLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// A fenced transaction's commit has an unknown outcome: it was cancelled, timed
/// out or lost its connection before the call returned. It may be committed on the primary and not replicated, so it may not
/// survive a failover; no answer may rest on it. The application treats it as neither a success nor a rollback — it reads the
/// state again, or repeats an idempotent write.
/// </summary>
public sealed class CommitOutcomeUnknownException : Exception
{
    /// <summary>Creates the exception.</summary>
    public CommitOutcomeUnknownException()
        : base("The commit's outcome is unknown.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public CommitOutcomeUnknownException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and its cause.</summary>
    public CommitOutcomeUnknownException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Who takes an orphaned row of a kind over.</summary>
public enum AdoptionPolicy
{
    /// <summary>The placer assigns orphaned and free rows of the kind; units only, a keyed kind cannot have it.</summary>
    Placer,

    /// <summary>Any serving eligible member adopts an orphaned row; keyed resources only, a unit cannot have it.</summary>
    AnyEligible,

    /// <summary>An orphaned row waits for its creator; <c>Ask</c> answers <c>NoOwner</c> meanwhile. Keyed resources only, a unit cannot have it.</summary>
    None,
}

/// <summary>The declaration of a unit kind, beside its <see cref="IOwnedUnit"/>.</summary>
public sealed class UnitKindOptions
{
    /// <summary>
    /// Who takes an orphaned row over. Default <see cref="AdoptionPolicy.Placer"/>, which a unit must keep and a keyed resource
    /// cannot have: a keyed resource sets <see cref="AdoptionPolicy.AnyEligible"/> or <see cref="AdoptionPolicy.None"/>.
    /// </summary>
    public AdoptionPolicy Policy { get; set; } = AdoptionPolicy.Placer;

    /// <summary>
    /// Whether the unit is convergent: idempotent and self-correcting, reading every input from the
    /// primary as it acts. It gets no token: <see cref="OwnedContext.Token"/>, <see cref="OwnedContext.EnsureOwner"/> and fenced
    /// transactions throw <see cref="InvalidOperationException"/>. It is stopped at a fence and by a drain like any other unit, and is
    /// placed or adopted again after the rejoin, as its <see cref="Policy"/> says. Default <see langword="false"/>.
    /// </summary>
    public bool Convergent { get; set; }

    /// <summary>
    /// The members that may own a key of the kind, besides advertising <c>unit:&lt;kind&gt;</c>. The placer assigns a unit only
    /// to a member that passes, and an owner that stops passing, after <see cref="IClusterCapabilities.SetAsync"/>, stops the unit
    /// and releases its row at its next poll. A keyed resource is created and adopted only by a member that passes; an owner
    /// that stops passing keeps the key until it drains, fences, stops or crashes, or the resource is ended. Default <see cref="EligibilityFilter.Any"/>.
    /// </summary>
    public EligibilityFilter Eligibility { get; set; } = EligibilityFilter.Any;

    /// <summary>
    /// The unit's start time: from its acquisition to running. The kind's failover bound adds it to the library's part.
    /// Default zero.
    /// </summary>
    public TimeSpan StartTime { get; set; }

    /// <summary>
    /// Whether the kind keeps state in Redis behind <c>FenceRedis</c> (the Redis package): each tenure's
    /// token is then raised into the key's Redis fence before <see cref="IOwnedUnit.OnAcquiredAsync"/> starts, and refreshed after
    /// every renewal. The start waits for the install raise at most <c>FencedTransactionTimeout</c>, which the kind's failover
    /// bound adds; until a raise succeeds, the kind's fenced Redis writes are refused. A convergent kind cannot have it. Default
    /// <see langword="false"/>.
    /// </summary>
    public bool RedisFence { get; set; }
}
