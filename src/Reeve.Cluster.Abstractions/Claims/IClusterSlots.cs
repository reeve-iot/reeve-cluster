namespace Reeve.Cluster;

/// <summary>
/// Held slots on PostgreSQL: at most <c>limit</c> concurrent holdings of a key. A
/// holder names its slot with an id of its own choosing and keeps it by renewing it; the library runs no renewal. Only a serving
/// replica acquires. The effect of a holder runs in a slot-fenced transaction (<c>BeginSlotFencedTransactionAsync</c>), whose
/// begin the database refuses when the slot is no longer valid. A slot's validity does not depend on its member's lease: a draining
/// member's slot stays valid until its holder releases it or its renewal lapses, and a fenced member's slots end at fence step 3.
/// Slot keys are a namespace of their own: a slot and a claim under one key are unrelated.
/// </summary>
public interface IClusterSlots
{
    /// <summary>
    /// Acquires a slot of <paramref name="key"/> under <paramref name="id"/> for <paramref name="ttl"/>. A valid slot of the key
    /// and id is extended and answered with its token without comparing the limit; otherwise the key's slots that are not valid are
    /// deleted and a new slot is granted while fewer than <paramref name="limit"/> slots of the key are left.
    /// </summary>
    /// <param name="key">The key; non-empty.</param>
    /// <param name="id">The holder's name for the slot; non-empty. A re-acquire with the id of a valid slot answers that slot, whoever acquired it.</param>
    /// <param name="limit">The most slots of the key that may be valid at once, on this call; at least 1.</param>
    /// <param name="ttl">How long the slot is valid unless renewed, on the database's clock; more than <c>SafetyMargin</c>.</param>
    /// <param name="cancellationToken">Ends the call; a cancel by this token throws <see cref="OperationCanceledException"/>.</param>
    /// <returns><see cref="SlotAcquireResult.Acquired"/>, <see cref="SlotAcquireResult.Full"/>, <see cref="SlotAcquireResult.NotServing"/> or <see cref="SlotAcquireResult.Unknown"/>.</returns>
    /// <exception cref="ArgumentException">The key or the id is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> is less than 1, or <paramref name="ttl"/> is not more than <c>SafetyMargin</c>.</exception>
    /// <exception cref="InvalidOperationException">This process has an open slot-fenced transaction on a slot of the key.</exception>
    Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extends the slot of <paramref name="token"/> to now + <paramref name="ttl"/> if it is valid. It compares no limit and runs the
    /// slot holder check instead of the serving check, so a draining replica renews.
    /// </summary>
    /// <param name="key">The key; non-empty.</param>
    /// <param name="id">The id the slot was acquired under; non-empty.</param>
    /// <param name="token">The token <see cref="SlotAcquireResult.Acquired"/> carried.</param>
    /// <param name="ttl">The new time to live, from now on the database's clock; more than <c>SafetyMargin</c>.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <returns>The outcome; after <see cref="SlotRenewResult.Ended"/> or <see cref="SlotRenewResult.NotServing"/> the holder's tenure is over.</returns>
    /// <exception cref="ArgumentException">The key or the id is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is not more than <c>SafetyMargin</c>.</exception>
    Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the slot of <paramref name="key"/> and <paramref name="id"/>. With a token it deletes only that token's slot; without
    /// one it deletes whatever slot the id has. It runs in every phase and checks no serving. A release that fails leaves the slot to
    /// its time to live.
    /// </summary>
    /// <param name="key">The key; non-empty.</param>
    /// <param name="id">The id; non-empty.</param>
    /// <param name="token">The token of the slot to end, or <see langword="null"/> for the id's slot whatever its token.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <returns>Whether a slot was deleted.</returns>
    /// <exception cref="ArgumentException">The key or the id is empty.</exception>
    /// <exception cref="InvalidOperationException">This process holds an open slot-fenced transaction on the slot.</exception>
    /// <exception cref="CommitOutcomeUnknownException">The release's database outcome is unknown.</exception>
    Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the holders of the valid slots of <paramref name="key"/>, ordered by id in ordinal order. A slot is read while its time
    /// to live has not passed on the database's clock and its member row, under the epoch of the grant, is <c>active</c> or
    /// <c>draining</c>: the validity <c>AcquireAsync</c>, <c>RenewAsync</c> and the slot-fenced transaction judge. The read is one
    /// statement that takes no lock and grants nothing, so it runs in every phase with no serving check. Its answer is a snapshot: a
    /// release, an expiry or a fence can end a slot it read, and an acquire or a late renewal can make a slot valid that it did not
    /// read. An empty answer, or a missing id, means no holder to address at that instant, not that the key is free. For a key
    /// always acquired with a <c>limit</c> of 1 it answers at most one holder.
    /// </summary>
    /// <param name="key">The key; non-empty.</param>
    /// <param name="cancellationToken">Ends the call; a cancel by this token throws <see cref="OperationCanceledException"/>.</param>
    /// <returns>The holders, ordered by <see cref="SlotHolder.Id"/>; empty when the key has no valid slot.</returns>
    /// <exception cref="ArgumentException">The key is empty.</exception>
    /// <exception cref="NotSupportedException">The implementation reads no holders; the library's PostgreSQL implementation does.</exception>
    Task<IReadOnlyList<SlotHolder>> ReadHoldersAsync(string key, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This IClusterSlots implementation does not read slot holders.");
}

/// <summary>
/// The holder of one valid slot, as <see cref="IClusterSlots.ReadHoldersAsync"/> read it. <see cref="ReplicaId"/> addresses the
/// holder's replica, for example with <c>IClusterAsk.AskMemberAsync</c>; <see cref="Epoch"/> names the member's tenure the slot was
/// granted in, which a rejoin of that replica replaces. A holder whose member is draining is read while its slot is valid, and a
/// member ask to that replica ends <see cref="AskOutcome.NotTaken"/>.
/// </summary>
public sealed record SlotHolder
{
    /// <summary>The id the slot was acquired under.</summary>
    public required string Id { get; init; }

    /// <summary>The replica whose process holds the slot.</summary>
    public required string ReplicaId { get; init; }

    /// <summary>The epoch of the member row the slot was granted under.</summary>
    public required long Epoch { get; init; }
}

/// <summary>
/// What <see cref="IClusterSlots.AcquireAsync"/> answered. Only <see cref="Acquired"/> is a grant: <see cref="IsAcquired"/> and
/// <see cref="TryGetToken"/> tell it from the three answers that are not.
/// </summary>
public abstract record SlotAcquireResult
{
    private protected SlotAcquireResult()
    {
    }

    /// <summary>Whether the caller holds the slot: this is <see cref="Acquired"/>.</summary>
    public bool IsAcquired => this is Acquired;

    /// <summary>The token of an acquired slot; <see langword="false"/> for every other answer.</summary>
    /// <param name="token">The token <see cref="Acquired"/> carries, or <see cref="Guid.Empty"/>.</param>
    public bool TryGetToken(out Guid token)
    {
        token = this is Acquired acquired ? acquired.Token : Guid.Empty;
        return this is Acquired;
    }

    /// <summary>The caller holds the slot under <see cref="Token"/> until its time to live passes, and keeps it by renewing it.</summary>
    /// <param name="Token">The random token of the slot.</param>
    public sealed record Acquired(Guid Token) : SlotAcquireResult;

    /// <summary>
    /// <c>limit</c> slots of the key are counted; nothing was granted. On PostgreSQL they are the rows left after the invalid ones were deleted,
    /// or the id's own row could not be extended; on Redis, the members of the key's set at the grant's <c>EXEC</c>, ended ones not yet removed included.
    /// </summary>
    public sealed record Full : SlotAcquireResult;

    /// <summary>The replica is not serving, or the store answered on another timeline than the replica joined on; nothing was granted.</summary>
    public sealed record NotServing : SlotAcquireResult;

    /// <summary>
    /// The grant is not known, so it is not a grant. On PostgreSQL the library's bound cancelled the commit before it returned; on
    /// Redis, Redis did not answer and the policy is <c>Refuse</c>. The
    /// slot may exist, ends at its time to live unless renewed, and on PostgreSQL a retry with the same id answers it. On Redis a retry
    /// answers it only when the grant's id key was written; otherwise it grants again and is counted against the limit until the first member ends.
    /// </summary>
    public sealed record Unknown : SlotAcquireResult;
}

/// <summary>What <see cref="IClusterSlots.RenewAsync"/> answered.</summary>
public enum SlotRenewResult
{
    /// <summary>The slot was valid and is extended.</summary>
    Renewed,

    /// <summary>No valid slot of the key and id holds the token, or the token is not in this process's registry; the holder's tenure is over.</summary>
    Ended,

    /// <summary>The slot holder check refused, or the store answered on another timeline; nothing was extended and the holder's tenure is over.</summary>
    NotServing,

    /// <summary>
    /// The renewal is not known; the slot may have been extended. On PostgreSQL the library's bound cancelled the commit before it
    /// returned; on Redis, Redis did not answer.
    /// </summary>
    Unknown,
}

/// <summary>
/// The database refused a slot: the slot of the token is no longer valid, the
/// slot holder check refused, or the replica is on another timeline than the one it joined on (which also fences the replica). The
/// effect must not happen. It derives from <see cref="InvalidOperationException"/>: code that catches that and means to tell them
/// apart catches this one first.
/// </summary>
public sealed class SlotLostException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public SlotLostException()
        : base("The slot is no longer valid.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public SlotLostException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and its cause.</summary>
    public SlotLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
