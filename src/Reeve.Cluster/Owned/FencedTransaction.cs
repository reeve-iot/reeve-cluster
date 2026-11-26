using System.Data;
using Npgsql;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster;

/// <summary>
/// A fenced transaction on an application's Npgsql connection, begun with
/// <see cref="FencedTransactionExtensions.BeginFencedTransactionAsync(OwnedContext, NpgsqlConnection, IsolationLevel, CancellationToken)"/>.
/// It holds the key's <c>owned</c> row <c>FOR SHARE</c> until it ends, so it cannot commit after the row changed owner, and
/// <c>FencedTransactionTimeout</c> ends it, counted from its first statement. Run the business statements on
/// <see cref="Connection"/>, then commit through <see cref="CommitAsync"/>: a commit through <see cref="Transaction"/> directly
/// does not report an unknown outcome.
/// </summary>
public sealed class FencedTransaction : IAsyncDisposable, IDisposable
{
    internal FencedTransaction(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        Connection = connection;
        Transaction = transaction;
    }

    /// <summary>The connection the transaction runs on.</summary>
    public NpgsqlConnection Connection { get; }

    /// <summary>The transaction, for commands that name it.</summary>
    public NpgsqlTransaction Transaction { get; }

    /// <summary>Commits the transaction.</summary>
    /// <param name="cancellationToken">
    /// Cancels the commit. A cancel that comes before the call returns makes its outcome unknown, whatever the server answered: a
    /// cancel during the wait for the synchronous standby leaves the commit visible on the primary and not replicated.
    /// </param>
    /// <exception cref="CommitOutcomeUnknownException">The commit may or may not have happened, and may not survive a failover.</exception>
    public Task CommitAsync(CancellationToken cancellationToken = default) =>
        FencedTransactions.CommitAsync(Transaction.CommitAsync, Connection, cancellationToken);

    /// <summary>Rolls the transaction back.</summary>
    public Task RollbackAsync(CancellationToken cancellationToken = default) => Transaction.RollbackAsync(cancellationToken);

    /// <summary>Ends the transaction, rolling it back unless it was committed.</summary>
    public ValueTask DisposeAsync() => Transaction.DisposeAsync();

    /// <summary>Ends the transaction, rolling it back unless it was committed.</summary>
    public void Dispose() => Transaction.Dispose();
}

/// <summary>Begins fenced transactions on Npgsql connections (the EF Core helper is in Reeve.Cluster.EntityFrameworkCore).</summary>
public static class FencedTransactionExtensions
{
    /// <summary>
    /// Begins a fenced transaction for <paramref name="context"/>'s tenure on <paramref name="connection"/>. In order it runs
    /// <see cref="OwnedContext.EnsureOwner"/>; checks, once per connection object and the backend it is open on, that the connection's
    /// <c>transaction_timeout</c> and <c>statement_timeout</c> are 0; begins the transaction at <paramref name="isolationLevel"/>; and sends
    /// <c>SET LOCAL transaction_timeout = FencedTransactionTimeout</c> and <c>assert_owner()</c>, with the timeline the replica
    /// joined on, in one round trip.
    /// </summary>
    /// <param name="context">The context the library passed to <see cref="IOwnedUnit.OnAcquiredAsync"/>.</param>
    /// <param name="connection">An open connection to the cluster's database, with no transaction. Its <c>transaction_timeout</c> and <c>statement_timeout</c> must be 0 for the role, the database and the session.</param>
    /// <param name="isolationLevel">The application's isolation level; <see cref="IsolationLevel.Unspecified"/> is Npgsql's default, <c>READ COMMITTED</c>.</param>
    /// <param name="cancellationToken">Cancels the begin.</param>
    /// <exception cref="OwnershipLostException">The unit is being stopped, the replica does not serve, or the database refused the tenure (<c>RC001</c>) or the timeline (<c>RC002</c>, which also fences the replica).</exception>
    /// <exception cref="ClusterConfigurationException">The connection's <c>transaction_timeout</c> or <c>statement_timeout</c> is not 0, it reached a standby, or the server is older than PostgreSQL 18.</exception>
    /// <exception cref="ArgumentException"><paramref name="context"/> is not one the library passed to a unit.</exception>
    /// <exception cref="InvalidOperationException">The kind is convergent, the connection is not open, or it has a transaction.</exception>
    /// <exception cref="PostgresException">Any other error of the first round trip, such as a <c>40001</c> under <c>REPEATABLE READ</c> or <c>SERIALIZABLE</c> after a change of owner; the application retries it as a serialization failure.</exception>
    public static async Task<FencedTransaction> BeginFencedTransactionAsync(
        this OwnedContext context, NpgsqlConnection connection, IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        var owner = FencedTransactions.Owner(context);
        ArgumentNullException.ThrowIfNull(connection);

        var start = owner.Start();
        await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        try
        {
            await FencedTransactions.AssertAsync(owner, start, connection, transaction, cancellationToken).ConfigureAwait(false);
            return new FencedTransaction(connection, transaction);
        }
        catch
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
