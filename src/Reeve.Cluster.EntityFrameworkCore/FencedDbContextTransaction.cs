using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster;

/// <summary>
/// A fenced transaction begun through EF Core on a <see cref="DbContext"/>, with
/// <see cref="FencedDbContextExtensions.BeginFencedTransactionAsync"/>. It holds the key's <c>owned</c> row <c>FOR SHARE</c> until it
/// ends, so every statement of the context in it is covered, its inserts included, and <c>FencedTransactionTimeout</c> ends
/// it, counted from its first statement. Commit through <see cref="CommitAsync"/>: it commits through EF, where the capture appends
/// the context's entries last, and reports an unknown outcome; a commit through <see cref="Transaction"/> does not report one.
/// </summary>
public sealed class FencedDbContextTransaction : IAsyncDisposable, IDisposable
{
    private readonly DbContext context;
    private readonly NpgsqlConnection connection;
    private int disposed;

    internal FencedDbContextTransaction(DbContext context, NpgsqlConnection connection, IDbContextTransaction transaction)
    {
        this.context = context;
        this.connection = connection;
        Transaction = transaction;
    }

    /// <summary>The context's transaction.</summary>
    public IDbContextTransaction Transaction { get; }

    /// <summary>Commits the transaction through EF.</summary>
    /// <param name="cancellationToken">
    /// Cancels the commit. A cancel that comes before the call returns makes its outcome unknown, whatever the server answered: a
    /// cancel during the wait for the synchronous standby leaves the commit visible on the primary and not replicated.
    /// </param>
    /// <exception cref="CommitOutcomeUnknownException">The commit may or may not have happened, and may not survive a failover.</exception>
    public Task CommitAsync(CancellationToken cancellationToken = default) =>
        FencedTransactions.CommitAsync(Transaction.CommitAsync, connection, cancellationToken);

    /// <summary>Rolls the transaction back.</summary>
    public Task RollbackAsync(CancellationToken cancellationToken = default) => Transaction.RollbackAsync(cancellationToken);

    /// <summary>Ends the transaction, rolling it back unless it was committed, and closes the connection the helper opened.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        await Transaction.DisposeAsync().ConfigureAwait(false);
        await context.Database.CloseConnectionAsync().ConfigureAwait(false);
    }

    /// <summary>Ends the transaction, rolling it back unless it was committed, and closes the connection the helper opened.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        Transaction.Dispose();
        context.Database.CloseConnection();
    }
}

/// <summary>The EF Core helper of fenced transactions.</summary>
public static class FencedDbContextExtensions
{
    /// <summary>
    /// Begins a fenced transaction for <paramref name="context"/>'s tenure through <paramref name="db"/>. In order it runs
    /// <see cref="OwnedContext.EnsureOwner"/>; opens the context's connection and checks, once per connection object and the backend it is open on, that its
    /// <c>transaction_timeout</c> and <c>statement_timeout</c> are 0; begins the transaction through EF at <paramref name="isolationLevel"/>; and sends
    /// <c>SET LOCAL transaction_timeout = FencedTransactionTimeout</c> and <c>assert_owner()</c>, with the timeline the replica
    /// joined on, in one round trip. The context must use Npgsql, and no execution strategy that retries.
    /// </summary>
    /// <param name="context">The context the library passed to <see cref="IOwnedUnit.OnAcquiredAsync"/>.</param>
    /// <param name="db">A context on the cluster's database, with no transaction.</param>
    /// <param name="isolationLevel">The application's isolation level; <see cref="IsolationLevel.Unspecified"/> is Npgsql's default, <c>READ COMMITTED</c>.</param>
    /// <param name="cancellationToken">Cancels the begin.</param>
    /// <exception cref="OwnershipLostException">The unit is being stopped, the replica does not serve, or the database refused the tenure (<c>RC001</c>) or the timeline (<c>RC002</c>, which also fences the replica).</exception>
    /// <exception cref="ClusterConfigurationException">The connection's <c>transaction_timeout</c> or <c>statement_timeout</c> is not 0, it reached a standby, or the server is older than PostgreSQL 18.</exception>
    /// <exception cref="ArgumentException"><paramref name="context"/> is not one the library passed to a unit.</exception>
    /// <exception cref="InvalidOperationException">The kind is convergent, the context is not on Npgsql, or it has a transaction.</exception>
    /// <exception cref="PostgresException">Any other error of the first round trip, such as a <c>40001</c> under <c>REPEATABLE READ</c> or <c>SERIALIZABLE</c> after a change of owner; the application retries it as a serialization failure.</exception>
    public static async Task<FencedDbContextTransaction> BeginFencedTransactionAsync(
        this OwnedContext context, DbContext db, IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        var owner = FencedTransactions.Owner(context);
        ArgumentNullException.ThrowIfNull(db);

        var start = owner.Start();
        await db.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = db.Database.GetDbConnection() as NpgsqlConnection
                ?? throw new InvalidOperationException("A fenced transaction needs a context on Npgsql.");
            await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            var transaction = await db.Database.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            try
            {
                await FencedTransactions.AssertAsync(
                    owner, start, connection, (NpgsqlTransaction)transaction.GetDbTransaction(), cancellationToken).ConfigureAwait(false);
                return new FencedDbContextTransaction(db, connection, transaction);
            }
            catch
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            await db.Database.CloseConnectionAsync().ConfigureAwait(false);
            throw;
        }
    }
}
