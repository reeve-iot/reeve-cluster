namespace Reeve.Cluster;

/// <summary>A request as the handler sees it.</summary>
/// <param name="Kind">The key's kind for a request addressed to a key; <see langword="null"/> for a request addressed to a replica.</param>
/// <param name="Key">The key; <see langword="null"/> for a request addressed to a replica.</param>
/// <param name="Payload">The request's bytes.</param>
public sealed record AskRequest(string? Kind, string? Key, ReadOnlyMemory<byte> Payload)
{
    /// <summary>Whether the request was addressed to a replica rather than to a key's owner.</summary>
    public bool IsMemberRequest => Kind is null;

    /// <summary>
    /// The tenure the handler acts for: the context the key's unit received in <see cref="IOwnedUnit.OnAcquiredAsync"/>, for a
    /// request addressed to a key. <see langword="null"/> for a request addressed to a replica, which has no tenure. Use it for
    /// the fenced paths of the key (<see cref="OwnedContext.EnsureOwner"/>, a fenced transaction, <c>FenceRedis</c>): a handler
    /// that looks the context up in the application's state may find none, or the previous tenure's.
    /// </summary>
    public OwnedContext? Owner { get; init; }
}

/// <summary>
/// Answers the requests an <c>Ask</c> delivers. Registered with <c>AddClusterAskHandler</c> for a kind
/// or <c>AddClusterMemberAskHandler</c> for requests addressed to a replica.
/// </summary>
/// <remarks>
/// The handler runs in a scope of its own, begun with the caller's execution context suppressed, so the code that asked is
/// not its ambient scope and its writes are the handler's own: a handler that reports a configuration write awaits it before
/// answering, or returns its seq, which the caller may await as a handed-over seq. What it may do on a replica that is not
/// serving rests on the serving check every fenced path runs, not on what it pinned. A request for a key's owner carries the
/// token of the cancellation of the tenure as well: it ends when the replica stops owning the key, and the caller then receives
/// <see cref="AskOutcome.OwnerLost"/>.
/// <para>A request for a key's owner is taken only for a ready tenure: the Redis install raise of a kind that declares
/// <c>FenceRedis</c> has returned, and the unit's <see cref="IOwnedUnit.OnAcquiredAsync"/> is about to be called. The handler
/// starts beside <c>OnAcquiredAsync</c>, which it does not wait for, so application state that <c>OnAcquiredAsync</c> fills may
/// not be there yet. <see cref="AskRequest.Owner"/> is the tenure's own context, the one <c>OnAcquiredAsync</c> received: take
/// the key's fenced paths from it, not from a lookup. If the raise had not succeeded when <c>FencedTransactionTimeout</c> passed,
/// the tenure is ready all the same and <c>FenceRedis</c> refuses until the raise, or a later refresh, succeeds.</para>
/// </remarks>
public interface IAskHandler
{
    /// <summary>
    /// Answers <paramref name="request"/>. An exception thrown here is an answer that carries its message; a cancellation the
    /// handler ends with is an answer that says the handler was cancelled. A handler still running when the caller's wait ends
    /// gives the caller <see cref="AskOutcome.TakenUnanswered"/> whatever it throws then.
    /// </summary>
    /// <param name="request">The request.</param>
    /// <param name="cancellationToken">Ends when the caller gave up, or, for a key's request, when the replica stopped owning the key.</param>
    Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken);
}
