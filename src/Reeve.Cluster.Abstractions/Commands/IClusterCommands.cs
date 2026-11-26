namespace Reeve.Cluster;

/// <summary>What a durable command does when the owner that took it is replaced before it completed.</summary>
public enum InDoubtPolicy
{
    /// <summary>
    /// The handler is idempotent: the new owner takes the command again and runs it. Nothing counts the attempts, so a handler that
    /// kills its process on one command is run by every next owner.
    /// </summary>
    ReRun,

    /// <summary>The new owner does not run it again: the command becomes <c>in_doubt</c>.</summary>
    Complete,
}

/// <summary>The state of a durable command.</summary>
public enum DurableCommandState
{
    /// <summary>Sent, not taken.</summary>
    Pending,

    /// <summary>Taken by an owner whose handler is running, or was, when its tenure ended.</summary>
    Taken,

    /// <summary>The handler returned and the owner completed the command.</summary>
    Done,

    /// <summary>Taken by an owner that was replaced, and not run again.</summary>
    InDoubt,

    /// <summary>No owner took it in time; it never ran, and sending it again pends it again.</summary>
    Expired,
}

/// <summary>How a <see cref="IClusterCommands.SendDurableAsync"/> ended.</summary>
public enum DurableSendOutcome
{
    /// <summary>The command was written and is pending.</summary>
    Sent,

    /// <summary>A command with the same key and the same payload exists already; <see cref="DurableSendResult.State"/> is its state.</summary>
    Existing,

    /// <summary>An expired command with the same key and payload was pended again.</summary>
    Resent,

    /// <summary>A command with the same key and another payload exists: nothing was written.</summary>
    IdempotencyConflict,
}

/// <summary>The end of one <see cref="IClusterCommands.SendDurableAsync"/>.</summary>
/// <param name="Outcome">How the send ended.</param>
/// <param name="CommandId">The command's id; for <see cref="DurableSendOutcome.IdempotencyConflict"/> the id of the command that exists.</param>
/// <param name="State">The command's state as the send last saw it.</param>
public sealed record DurableSendResult(DurableSendOutcome Outcome, long CommandId, DurableCommandState State);

/// <summary>How a <see cref="IClusterCommands.WaitAsync"/> ended.</summary>
public enum DurableCommandOutcome
{
    /// <summary>The command is <c>done</c>: its handler returned, and <see cref="DurableCommandResult.Result"/> is its result.</summary>
    Completed,

    /// <summary>The command is <c>done</c> with an error: its handler threw, and <see cref="DurableCommandResult.Error"/> is the exception's message. The handler may have had effects. Like an <c>Ask</c> whose handler threw, it is an answer, and the key is free for the next command.</summary>
    Failed,

    /// <summary>The command is <c>in_doubt</c>: its owner was replaced while the handler may have run, and the kind's policy is <see cref="InDoubtPolicy.Complete"/>, so nothing ran it again. Retry only if idempotent.</summary>
    InDoubt,

    /// <summary>The command is <c>expired</c>: no owner took it within <c>CommandPendingTtl</c>. It never ran; sending it again pends it again. Safe to retry.</summary>
    NoOwner,

    /// <summary>The caller's wait ended with the command still <c>pending</c> or <c>taken</c>; it may come back later by idempotency key.</summary>
    Unfinished,

    /// <summary>No command has this kind, key and idempotency key: it was never sent, or retention deleted it.</summary>
    NotFound,
}

/// <summary>The end of one <see cref="IClusterCommands.WaitAsync"/>.</summary>
/// <param name="Outcome">How the wait ended.</param>
/// <param name="CommandId">The command's id; 0 for <see cref="DurableCommandOutcome.NotFound"/>.</param>
/// <param name="State">The command's state as the wait last saw it; <see langword="null"/> for <see cref="DurableCommandOutcome.NotFound"/>.</param>
/// <param name="Result">The handler's result for <see cref="DurableCommandOutcome.Completed"/>; empty otherwise.</param>
/// <param name="Error">The message of the exception the handler threw for <see cref="DurableCommandOutcome.Failed"/>; otherwise <see langword="null"/>.</param>
public sealed record DurableCommandResult(DurableCommandOutcome Outcome, long CommandId, DurableCommandState? State, ReadOnlyMemory<byte> Result, string? Error)
{
    /// <summary>Whether the command is known not to have run, so that sending it again is safe whatever the handler does: <see cref="DurableCommandOutcome.NoOwner"/>.</summary>
    public bool SafeToRetry => Outcome == DurableCommandOutcome.NoOwner;
}

/// <summary>
/// Durable commands: a command to a key's owner that survives a change of owner while it is younger
/// than <c>CommandPendingTtl</c>. The command is a row; the serving owner takes it, runs the handler registered with
/// <c>AddClusterCommandHandler</c> outside any transaction, and completes it under the token of the tenure that took it.
/// </summary>
public interface IClusterCommands
{
    /// <summary>
    /// Writes the command, idempotently on (<paramref name="kind"/>, <paramref name="key"/>, <paramref name="idempotencyKey"/>): the
    /// same key with the same payload returns the command that exists, an expired one pended again; the same key with another
    /// payload is <see cref="DurableSendOutcome.IdempotencyConflict"/>. The write is the library's own transaction, never the
    /// caller's. A cancel that comes before the call returns leaves the outcome unknown: send again with the same key. A cancel after
    /// the command was written (the insert's or the resend's commit) surfaces as <see cref="OperationCanceledException"/> although the write
    /// committed (the library does not report a commit it cancelled as a success); sending again with the same key is safe, since
    /// the send is idempotent. So does a statement that outlasts the library's own bound (<c>FencedTransactionTimeout + 2 s</c>), which
    /// throws <see cref="OperationCanceledException"/>.
    /// A kind with no handler registered on this replica is refused with <see cref="InvalidOperationException"/>: registration is the same on every replica.
    /// A <paramref name="key"/> or <paramref name="idempotencyKey"/> that holds U+0000 or an unpaired surrogate, which the command table cannot store,
    /// is refused with <see cref="ArgumentException"/> before any statement; the command's bytes take any value.
    /// </summary>
    /// <param name="kind">The kind of the owned key.</param>
    /// <param name="key">The key.</param>
    /// <param name="command">The command's bytes, handed to the kind's handler.</param>
    /// <param name="idempotencyKey">Names the command within (<paramref name="kind"/>, <paramref name="key"/>).</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    Task<DurableSendResult> SendDurableAsync(string kind, string key, ReadOnlyMemory<byte> command, string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits on the command's row until it is <c>done</c>, <c>in_doubt</c> or <c>expired</c>, or the wait ends: a nudge on completion
    /// wakes the caller, and a poll every <c>PlacementInterval</c> is the fallback, so a lost nudge costs a poll and nothing else. The
    /// caller may come back later by idempotency key; the wait never changes the command. Cancelling the token ends the call as the
    /// timeout does, with <see cref="DurableCommandOutcome.Unfinished"/>. A read that fails on the connection or the database (a refused connection, a failover) is read again by the poll; any other exception is thrown.
    /// <see cref="DurableCommandOutcome.Unfinished"/> can be permanent: a command taken on a key that has ended and is not created
    /// again stays <c>taken</c>, and every wait on it ends so.
    /// </summary>
    /// <param name="kind">The kind of the owned key.</param>
    /// <param name="key">The key.</param>
    /// <param name="idempotencyKey">Names the command within (<paramref name="kind"/>, <paramref name="key"/>).</param>
    /// <param name="timeout">How long the call may take, finite and positive.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not finite and positive.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> or <paramref name="idempotencyKey"/> holds U+0000 or an unpaired surrogate.</exception>
    Task<DurableCommandResult> WaitAsync(string kind, string key, string idempotencyKey, TimeSpan timeout, CancellationToken cancellationToken = default);
}

/// <summary>A command as the handler sees it.</summary>
/// <param name="Id">The command's id.</param>
/// <param name="Kind">The kind.</param>
/// <param name="Key">The key.</param>
/// <param name="IdempotencyKey">The key the sender named it with.</param>
/// <param name="Payload">The command's bytes.</param>
public sealed record DurableCommand(long Id, string Kind, string Key, string IdempotencyKey, ReadOnlyMemory<byte> Payload)
{
    /// <summary>
    /// The tenure the handler acts for: the context the key's unit received in <see cref="IOwnedUnit.OnAcquiredAsync"/>. Use it for
    /// the fenced paths of the key (<see cref="OwnedContext.EnsureOwner"/>, a fenced transaction, <c>FenceRedis</c>): a handler that
    /// looks the context up in the application's state may find none, or the previous tenure's. The library always sets it;
    /// it is <see langword="null"/> only on a command a test constructs by hand without it.
    /// </summary>
    public OwnedContext Owner { get; init; } = null!;
}

/// <summary>
/// Runs the durable commands of a kind, on the serving owner of the key, one at a time per key, oldest first. Registered with
/// <c>AddClusterCommandHandler</c>.
/// </summary>
/// <remarks>
/// The handler runs outside any transaction, in a scope of its own begun with the execution context suppressed, as an
/// <c>Ask</c> handler does. The library starts it only after a check that the replica still serves the key, and it starts no
/// other command of the key on this replica while a handler of the key runs, one that outlived its tenure included. Its token is
/// cancelled when the tenure stops or the host stops; a handler that ignores it can run on beside the next owner's handler of
/// the key on another replica; so can a handler that honours it but takes longer to stop than the release of the key, the
/// placement and the next owner's start take, since the release does not wait for the handler. The next owner's handler can
/// therefore overlap the old one for as long as the old one runs on. Only effects overlap: the old completion is refused.
/// A handler that returns completes the command with its result. A handler that throws completes it too, <c>done</c> with the
/// exception's message as its error (made storable: U+0000 and unpaired surrogates replaced, cut to 4096 characters, and
/// "The handler threw {type}." when there is no message): the caller's wait ends <see cref="DurableCommandOutcome.Failed"/>, the key
/// goes on with the next command, and the command is not run again once its error is recorded, so a handler that may have had effects
/// before it threw is not repeated. If the tenure stops between the throw and the commit, the command is classified as any other
/// command a stop left <c>taken</c>. A cancel that
/// comes from the tenure's stop or the host's stop is not an error of the command: when the tenure stops, a return, a throw or a
/// cancel after the stop was requested leaves the command <c>taken</c> (a throw after the stop may be the stop's own effect and is
/// not told from it), and the next tenure classifies it by the kind's in-doubt policy. A handler can run again after the owner is
/// replaced (<see cref="InDoubtPolicy.ReRun"/>), and in rare failovers even under <see cref="InDoubtPolicy.Complete"/>. Under
/// <see cref="InDoubtPolicy.ReRun"/> every next owner runs a command again that its predecessor left <c>taken</c>, and nothing counts
/// the attempts: a handler that kills its process on one command can take the replicas down in turn. A key taken
/// on acquisition may be handed a command beside the unit's <c>OnAcquiredAsync</c>, which the handler does not wait for, but only
/// once the tenure is ready: the Redis install raise of a kind that declares <c>FenceRedis</c> has returned. <see cref="DurableCommand.Owner"/>
/// is the tenure's own context, the one <c>OnAcquiredAsync</c> received: take the key's fenced paths from it, not from a lookup in
/// state that <c>OnAcquiredAsync</c> fills. If the raise had not succeeded when <c>FencedTransactionTimeout</c> passed, the tenure is
/// ready all the same and <c>FenceRedis</c> refuses until the raise, or a later refresh, succeeds. A throw is final, so a handler
/// that cannot proceed yet (such a refusal, or state that <c>OnAcquiredAsync</c> has not filled) waits, bounded by its token, rather
/// than throwing.
/// </remarks>
public interface IDurableCommandHandler
{
    /// <summary>Runs <paramref name="command"/> and returns its result.</summary>
    /// <param name="command">The command.</param>
    /// <param name="cancellationToken">Ends when the replica stops owning the key.</param>
    Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken);
}
