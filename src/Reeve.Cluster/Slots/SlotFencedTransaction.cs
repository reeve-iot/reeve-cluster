using System.Data;
using Npgsql;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Slots;

namespace Reeve.Cluster;

/// <summary>
/// A slot-fenced transaction on an application's Npgsql connection, begun with
/// <see cref="SlotFencedTransactionExtensions.BeginSlotFencedTransactionAsync"/>. It holds the slot's row <c>FOR KEY SHARE</c>
/// until it ends, so it cannot commit after the slot's row was released or deleted. Commit through
/// <see cref="CommitAsync"/>: a commit through <see cref="Transaction"/> directly does not report an unknown outcome.
/// End scopes with this wrapper's commit, rollback or disposal. A caller ending the transaction through <see cref="Transaction"/>
/// directly must dispose this wrapper before another slot operation on the same key.
/// </summary>
public sealed class SlotFencedTransaction : IAsyncDisposable, IDisposable
{
    private readonly IDisposable scope;

    internal SlotFencedTransaction(NpgsqlConnection connection, NpgsqlTransaction transaction, IDisposable scope)
    {
        Connection = connection;
        Transaction = transaction;
        this.scope = scope;
    }

    /// <summary>The connection the transaction runs on.</summary>
    public NpgsqlConnection Connection { get; }

    /// <summary>The transaction, for commands that name it.</summary>
    public NpgsqlTransaction Transaction { get; }

    /// <summary>Commits the transaction; a cancel that comes before the call returns makes its outcome unknown.</summary>
    /// <param name="cancellationToken">Cancels the commit.</param>
    /// <exception cref="CommitOutcomeUnknownException">The commit may or may not have happened, and may not survive a failover.</exception>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await FencedTransactions.CommitAsync(Transaction.CommitAsync, Connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>Rolls the transaction back.</summary>
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await Transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>Ends the transaction, rolling it back unless it was committed.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await Transaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>Ends the transaction, rolling it back unless it was committed.</summary>
    public void Dispose()
    {
        try
        {
            Transaction.Dispose();
        }
        finally
        {
            scope.Dispose();
        }
    }
}

/// <summary>Begins slot-fenced transactions on Npgsql connections.</summary>
public static class SlotFencedTransactionExtensions
{
    /// <summary>
    /// Begins the slot-fenced transaction of <paramref name="token"/> on the slot of <paramref name="key"/> and <paramref name="id"/>. In
    /// order it runs the slot holder check, which passes for a draining replica; checks, once per connection object and the backend it
    /// is open on, that the connection's <c>transaction_timeout</c> and <c>statement_timeout</c> are 0; begins the transaction at <paramref name="isolationLevel"/>;
    /// and sends <c>SET LOCAL transaction_timeout = FencedTransactionTimeout</c> and <c>assert_slot()</c>, with the timeline the replica
    /// joined on, in one round trip.
    /// </summary>
    /// <param name="slots">The library's slots.</param>
    /// <param name="key">The slot's key.</param>
    /// <param name="id">The id the slot was acquired under.</param>
    /// <param name="token">The token <see cref="SlotAcquireResult.Acquired"/> carried.</param>
    /// <param name="connection">An open connection to the cluster's database, with no transaction. Its <c>transaction_timeout</c> and <c>statement_timeout</c> must be 0.</param>
    /// <param name="isolationLevel">The application's isolation level; <see cref="IsolationLevel.Unspecified"/> is Npgsql's default, <c>READ COMMITTED</c>.</param>
    /// <param name="cancellationToken">Cancels the begin.</param>
    /// <remarks>Under <c>REPEATABLE READ</c> or <c>SERIALIZABLE</c>, an assertion that waited for a release or an acquire's delete can throw PostgreSQL <c>40001</c>, as-is.</remarks>
    /// <exception cref="SlotLostException">The slot holder check refused, or the database refused the slot (<c>RC004</c>) or the timeline (<c>RC002</c>, which also fences the replica).</exception>
    /// <exception cref="ClusterConfigurationException">The connection's <c>transaction_timeout</c> or <c>statement_timeout</c> is not 0, it reached a standby, or the server is older than PostgreSQL 18.</exception>
    /// <exception cref="ArgumentException"><paramref name="slots"/> is not the library's, or the key or the id is empty.</exception>
    /// <exception cref="InvalidOperationException">The connection is not open, or it has a transaction.</exception>
    public static Task<SlotFencedTransaction> BeginSlotFencedTransactionAsync(
        this IClusterSlots slots, string key, string id, Guid token, NpgsqlConnection connection,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(slots);
        return slots is ClusterSlots own
            ? own.BeginFencedTransactionAsync(key, id, token, connection, isolationLevel, cancellationToken)
            : throw new ArgumentException("A slot-fenced transaction needs the IClusterSlots the library registered.", nameof(slots));
    }
}
