using System.Data.Common;

namespace Reeve.Cluster;

/// <summary>One configuration change to log: the aggregate type, and the key within it that narrows what a reconciler re-reads.</summary>
/// <param name="AggregateType">The mapping's unit of change.</param>
/// <param name="AggregateKey">The key within the type, used as a hint; <see langword="null"/> when the change has none.</param>
public sealed record ChangeEntry(string AggregateType, string? AggregateKey)
{
    /// <summary>An entry with no key: a reconciler re-reads the whole type.</summary>
    public ChangeEntry(string aggregateType)
        : this(aggregateType, null)
    {
    }
}

/// <summary>
/// Appends configuration entries to the change log for a write the capture interceptor cannot see (raw SQL).
/// </summary>
public interface IChangeLog
{
    /// <summary>
    /// Appends <paramref name="entries"/> in <paramref name="transaction"/>, at once, so the caller calls it as the
    /// last statement before commit. Throws without a transaction, and — for a non-exempt client request — after fence
    /// step 2, recording the refusal in the scope's tracker. Records the seqs in the ambient scope's tracker as soon
    /// as the append returns, whether or not the transaction then commits.
    /// </summary>
    /// <remarks>
    /// When fence step 2 refuses the append, or its statement fails or is cancelled, the transaction has been rolled back by
    /// the time the exception reaches the caller and cannot be used again: a <c>catch</c> that rolls back again gets the
    /// provider's "transaction has completed" error instead of this one.
    /// </remarks>
    /// <returns>The highest seq it appended; awaiting it awaits the whole write.</returns>
    Task<long> AppendAsync(DbTransaction transaction, IReadOnlyCollection<ChangeEntry> entries, CancellationToken cancellationToken = default);
}
