namespace Reeve.Cluster;

/// <summary>
/// How an <c>Ask</c> ended. The five outcomes are the whole answer a feature gets: it never sees
/// "not owner" and never walks replicas.
/// </summary>
public enum AskOutcome
{
    /// <summary>The target took the request and answered; a handler exception is an answer that carries the error. Not a retry question.</summary>
    Answered,

    /// <summary>Nobody took it: it was never delivered, or was withdrawn before a target took it without a Redis connection event during the call. Safe to retry.</summary>
    NotTaken,

    /// <summary>A target took it and did not answer in time, or a Redis connection event during the call leaves delivery uncertain; the effect may or may not have happened. Retry only if idempotent.</summary>
    TakenUnanswered,

    /// <summary>The owner took it and lost ownership before answering. Retry only if idempotent.</summary>
    OwnerLost,

    /// <summary>No serving owner for the key, and none appeared within the wait the caller allowed (at most the failover bound). Safe to retry.</summary>
    NoOwner,
}

/// <summary>The end of one <c>Ask</c>: its <see cref="Outcome"/>, and for <see cref="AskOutcome.Answered"/> the handler's response or its error.</summary>
public sealed record AskResult
{
    private AskResult(AskOutcome outcome, ReadOnlyMemory<byte> response, string? error)
    {
        Outcome = outcome;
        Response = response;
        Error = error;
    }

    /// <summary>How the ask ended.</summary>
    public AskOutcome Outcome { get; }

    /// <summary>The handler's response when <see cref="Outcome"/> is <see cref="AskOutcome.Answered"/> and the handler returned; empty otherwise.</summary>
    public ReadOnlyMemory<byte> Response { get; }

    /// <summary>The message of the exception the handler threw, when <see cref="Outcome"/> is <see cref="AskOutcome.Answered"/> and it threw; otherwise <see langword="null"/>.</summary>
    public string? Error { get; }

    /// <summary>Whether the request is known not to have run, so that sending it again is safe whatever it does: <see cref="AskOutcome.NotTaken"/> and <see cref="AskOutcome.NoOwner"/>.</summary>
    public bool SafeToRetry => Outcome is AskOutcome.NotTaken or AskOutcome.NoOwner;

    /// <summary>The ask ended <see cref="AskOutcome.NotTaken"/>.</summary>
    public static AskResult NotTaken { get; } = new(AskOutcome.NotTaken, default, null);

    /// <summary>The ask ended <see cref="AskOutcome.TakenUnanswered"/>.</summary>
    public static AskResult TakenUnanswered { get; } = new(AskOutcome.TakenUnanswered, default, null);

    /// <summary>The ask ended <see cref="AskOutcome.OwnerLost"/>.</summary>
    public static AskResult OwnerLost { get; } = new(AskOutcome.OwnerLost, default, null);

    /// <summary>The ask ended <see cref="AskOutcome.NoOwner"/>.</summary>
    public static AskResult NoOwner { get; } = new(AskOutcome.NoOwner, default, null);

    /// <summary>The target answered with <paramref name="response"/>.</summary>
    public static AskResult Answered(ReadOnlyMemory<byte> response) => new(AskOutcome.Answered, response, null);

    /// <summary>The target answered that its handler threw, with the exception's message.</summary>
    public static AskResult AnsweredWithError(string error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new(AskOutcome.Answered, default, error);
    }
}

/// <summary>
/// Delivers a request to the current owner of a key, or to a named replica. To the caller an
/// <c>Ask</c> is always a remote call, one that is dispatched locally included: the handler runs in a scope of its own, and
/// its writes are not the caller's (not refused by the caller's fence step 2, not in its tracker, not awaited by its filter).
/// </summary>
public interface IClusterAsk
{
    /// <summary>
    /// Delivers <paramref name="request"/> to the owner of (<paramref name="kind"/>, <paramref name="key"/>) that serves: its member
    /// row <c>active</c>, leased, of the row's epoch, with <c>serving_since</c> set. A target that no longer serves or no longer
    /// runs the key's tenure declines without taking the request. The library then waits, within the caller's wait, until the
    /// database names another owner (a target that has only not started the tenure yet is given one poll first) and delivers once
    /// more: a second decline ends <see cref="AskOutcome.NotTaken"/>, and no other owner by the end of the wait ends
    /// <see cref="AskOutcome.NoOwner"/>. The wait for an owner and the answer share <paramref name="timeout"/>; the wait for an
    /// owner is at most the failover bound of the kind as registered on the calling replica (a kind registered only elsewhere has
    /// the default bound here). Cancelling the token ends the call as the timeout does: with <see cref="AskOutcome.NotTaken"/>
    /// before a target took the request, with <see cref="AskOutcome.TakenUnanswered"/> after.
    /// </summary>
    /// <param name="kind">The kind of the owned key.</param>
    /// <param name="key">The key.</param>
    /// <param name="request">The request's bytes, handed to the handler registered for <paramref name="kind"/>.</param>
    /// <param name="timeout">How long the call may take, finite and positive.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not finite and positive.</exception>
    Task<AskResult> AskAsync(string kind, string key, ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delivers <paramref name="request"/> to the replica <paramref name="replicaId"/> in any phase, a fenced one included. The
    /// target's <c>process_id</c> is read from its member row per call, in any row state: no row, or a <c>draining</c> row (its
    /// process has exited), ends <see cref="AskOutcome.NotTaken"/> at once with nothing delivered.
    /// </summary>
    /// <param name="replicaId">The replica.</param>
    /// <param name="request">The request's bytes, handed to the member handler.</param>
    /// <param name="timeout">How long the call may take, finite and positive.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not finite and positive.</exception>
    Task<AskResult> AskMemberAsync(string replicaId, ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// As <see cref="AskMemberAsync(string, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/>, and a request the target has not
    /// taken within <paramref name="takeTimeout"/> of the call is withdrawn and ends <see cref="AskOutcome.NotTaken"/>, unless a
    /// connection to Redis failed or came back during the call, which gives <see cref="AskOutcome.TakenUnanswered"/>. A target
    /// that crashed inside its lease or lost Redis costs the caller the take bound, not the whole timeout. A request taken by then
    /// is waited on for its answer until <paramref name="timeout"/>. The withdrawal at the bound takes up to a second more. The default
    /// implementation ignores the take bound; the library's validates it.
    /// </summary>
    /// <param name="replicaId">The replica.</param>
    /// <param name="request">The request's bytes, handed to the member handler.</param>
    /// <param name="timeout">How long the call may take, finite and positive.</param>
    /// <param name="takeTimeout">How long the target has to take the request, finite and positive.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> or <paramref name="takeTimeout"/> is not finite and positive.</exception>
    Task<AskResult> AskMemberAsync(string replicaId, ReadOnlyMemory<byte> request, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken = default) =>
        AskMemberAsync(replicaId, request, timeout, cancellationToken);
}
