using System.Data;
using Npgsql;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster;

/// <summary>
/// A claim-fenced transaction on an application's Npgsql connection, begun with
/// <see cref="ClaimFencedTransactionExtensions.BeginClaimFencedTransactionAsync"/>. It holds the key's claim row <c>FOR SHARE</c>
/// until it ends, so it cannot commit after the claim was taken over, released or deleted. Commit through
/// <see cref="CommitAsync"/>: a commit through <see cref="Transaction"/> directly does not report an unknown outcome.
/// End scopes with this wrapper's commit, rollback or disposal. A caller ending the transaction through <see cref="Transaction"/>
/// directly must dispose this wrapper before another claim operation on the same key.
/// </summary>
public sealed class ClaimFencedTransaction : IAsyncDisposable, IDisposable
{
    private readonly IDisposable scope;

    internal ClaimFencedTransaction(NpgsqlConnection connection, NpgsqlTransaction transaction, IDisposable scope)
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

/// <summary>Begins claim-fenced transactions on Npgsql connections.</summary>
public static class ClaimFencedTransactionExtensions
{
    /// <summary>
    /// Begins the claim-fenced transaction of <paramref name="token"/> on <paramref name="key"/>. In order it runs the serving check;
    /// checks, once per connection object and the backend it is open on, that the connection's <c>transaction_timeout</c> and <c>statement_timeout</c> are 0;
    /// begins the transaction at <paramref name="isolationLevel"/>; and sends <c>SET LOCAL transaction_timeout =
    /// FencedTransactionTimeout</c> and <c>assert_claim()</c>, with the timeline the replica joined on, in one round trip.
    /// </summary>
    /// <param name="claims">The library's claims.</param>
    /// <param name="key">The claim's key.</param>
    /// <param name="token">The token <see cref="ClaimResult.Won"/> carried.</param>
    /// <param name="connection">An open connection to the cluster's database, with no transaction. Its <c>transaction_timeout</c> and <c>statement_timeout</c> must be 0.</param>
    /// <param name="isolationLevel">The application's isolation level; <see cref="IsolationLevel.Unspecified"/> is Npgsql's default, <c>READ COMMITTED</c>.</param>
    /// <param name="cancellationToken">Cancels the begin.</param>
    /// <remarks>Under <c>REPEATABLE READ</c> or <c>SERIALIZABLE</c>, an assertion that waited for a takeover can throw PostgreSQL <c>40001</c>, as-is.</remarks>
    /// <exception cref="ClaimLostException">The replica does not serve, or the database refused the claim (<c>RC003</c>) or the timeline (<c>RC002</c>, which also fences the replica).</exception>
    /// <exception cref="ClusterConfigurationException">The connection's <c>transaction_timeout</c> or <c>statement_timeout</c> is not 0, it reached a standby, or the server is older than PostgreSQL 18.</exception>
    /// <exception cref="ArgumentException"><paramref name="claims"/> is not the library's.</exception>
    /// <exception cref="ArgumentException">The key starts with <c>reeve.once:</c>, which belongs to <see cref="IClusterOnce"/>, or with <c>reeve.flight:</c>, which belongs to <see cref="IClusterSingleFlight"/>.</exception>
    /// <exception cref="InvalidOperationException">The connection is not open, or it has a transaction.</exception>
    public static Task<ClaimFencedTransaction> BeginClaimFencedTransactionAsync(
        this IClusterClaims claims, string key, Guid token, NpgsqlConnection connection,
        IsolationLevel isolationLevel = IsolationLevel.Unspecified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claims);
        return claims is ClusterClaims own
            ? own.BeginFencedTransactionAsync(key, token, connection, isolationLevel, cancellationToken)
            : throw new ArgumentException("A claim-fenced transaction needs the IClusterClaims the library registered.", nameof(claims));
    }
}
