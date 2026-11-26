namespace Reeve.Cluster;

/// <summary>
/// Acknowledgement of configuration writes: waits until every replica that serves traffic has applied a write, so
/// that no request served afterwards sees the previous state. Must not be resolved by a migration-only run, which
/// has no lease.
/// </summary>
public interface IChangeAck
{
    /// <summary>
    /// The seqs the current scope appended, from its tracker. Empty outside a scope, and in an
    /// exempt request's scope, whose seqs are not recorded: an explicit wait there takes the seq
    /// <see cref="IChangeLog.AppendAsync"/> returned, or one handed over. The HTTP filter reads this list.
    /// </summary>
    IReadOnlyList<long> Seqs { get; }

    /// <summary>
    /// Marks the current scope so that the entries it commits are not awaited: the HTTP filter lets the response
    /// through at once, and <see cref="WaitAppliedAsync"/> for a seq its tracker holds answers
    /// <see cref="NotAwaitedReason.AtCommit"/>. The entries still propagate, and the call covers the scope's earlier
    /// entries too. Never for a write that revokes or disables something.
    /// </summary>
    void AcknowledgeAtCommit();

    /// <summary>
    /// Waits for the write that <paramref name="seq"/> belongs to — any seq of it will do — to be applied by the local
    /// applier and every member of the wait set, then records the acknowledgement and answers. The answer depends on the
    /// phase and the caller (see <see cref="AckOutcome"/>) and the wait is bounded by <c>AckTimeout</c>. Awaited inside the transaction
    /// that wrote it, the wait cannot complete: call it after the transaction ended.
    /// </summary>
    /// <param name="seq">A seq of the write.</param>
    /// <param name="cancellationToken">Cancels the wait; the answer is then an <see cref="OperationCanceledException"/>, not an outcome.</param>
    Task<AckOutcome> WaitAppliedAsync(long seq, CancellationToken cancellationToken = default);
}
