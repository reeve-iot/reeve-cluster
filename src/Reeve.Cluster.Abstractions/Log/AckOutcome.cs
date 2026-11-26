namespace Reeve.Cluster;

/// <summary>
/// The answer of <see cref="IChangeAck.WaitAppliedAsync"/>: one of <see cref="Acknowledged"/>,
/// <see cref="Unconfirmed"/> and <see cref="NotAwaited"/>. The arguments are reasons, not phases. Which phase and
/// caller give which answer is decided by the library; this type only names the answers.
/// </summary>
public abstract record AckOutcome
{
    private AckOutcome()
    {
    }

    /// <summary>
    /// The local applier and every member of the wait set applied the write, and the record committed. Names
    /// the aggregate types a skip may cover for it, when any.
    /// </summary>
    /// <param name="Skipped">The skipped types the acknowledgement names; empty when none.</param>
    public sealed record Acknowledged(IReadOnlyList<string> Skipped) : AckOutcome
    {
        /// <summary>An acknowledgement over no skipped type.</summary>
        public static Acknowledged None { get; } = new([]);

        /// <inheritdoc/>
        public bool Equals(Acknowledged? other) =>
            other is not null && Skipped.SequenceEqual(other.Skipped, StringComparer.Ordinal);

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (var type in Skipped)
            {
                hash.Add(type, StringComparer.Ordinal);
            }

            return hash.ToHashCode();
        }
    }

    /// <summary>
    /// The write is committed and propagating (or, for a caller that rolled back after its append, recorded), and its
    /// acknowledgement was not given. The caller decides what to do; in HTTP it is a <c>202 write_unconfirmed</c>.
    /// </summary>
    /// <param name="Reason">Why the wait ended without an acknowledgement.</param>
    public sealed record Unconfirmed(UnconfirmedReason Reason) : AckOutcome;

    /// <summary>The wait was not run, and the caller is told why at once.</summary>
    /// <param name="Reason">Why nothing was awaited.</param>
    public sealed record NotAwaited(NotAwaitedReason Reason) : AckOutcome;
}

/// <summary>Why an <see cref="AckOutcome.Unconfirmed"/> answer was given.</summary>
public enum UnconfirmedReason
{
    /// <summary>The replica fenced while the call waited.</summary>
    Fenced,

    /// <summary>Drain step 4 came first.</summary>
    Draining,

    /// <summary><c>AckTimeout</c> passed first.</summary>
    Timeout,
}

/// <summary>Why an <see cref="AckOutcome.NotAwaited"/> answer was given.</summary>
public enum NotAwaitedReason
{
    /// <summary>The caller is the applier's own execution context (a reconciler).</summary>
    Applier,

    /// <summary>The scope called <see cref="IChangeAck.AcknowledgeAtCommit"/>.</summary>
    AtCommit,

    /// <summary>The replica has not served yet: <c>Starting</c> to <c>Activated</c>.</summary>
    Joining,

    /// <summary>The replica is draining and the caller is not a request (anything but a running request: a unit's <c>OnLostAsync</c>, background code).</summary>
    Draining,

    /// <summary>The replica is <c>Fencing</c> or <c>Fenced</c> and the caller is not a client request.</summary>
    Fenced,
}
