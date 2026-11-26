using System.Data;
using System.Transactions;
using IsolationLevel = System.Data.IsolationLevel;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal sealed partial class ClusterClaims : IClusterClaims
{
    private readonly IClaimRows rows;
    private readonly IClaimGate gate;
    private readonly IOptions<ClusterOptions> options;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly ILogger<ClusterClaims> logger;
    private readonly ClaimScopes scopes;

    public ClusterClaims(
        IClaimRows rows, IClaimGate gate, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<ClusterClaims> logger)
        : this(rows, gate, options, time, points, logger, new ClaimScopes())
    {
    }

    public ClusterClaims(
        IClaimRows rows, IClaimGate gate, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<ClusterClaims> logger,
        ClaimScopes scopes)
    {
        this.rows = rows;
        this.gate = gate;
        this.options = options;
        this.time = time;
        this.points = points;
        this.logger = logger;
        this.scopes = scopes;
    }

    public async Task<ClaimResult> ClaimAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ClaimKeys.ThrowIfReserved(key);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);
        if (scopes.IsOpen(key))
        {
            throw new InvalidOperationException(
                $"The claim {key} has an open claim-fenced transaction in this process: a grant on another connection would wait for it. End the transaction first.");
        }

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        while (true)
        {
            await points.ReachAsync(ClaimsFaultPoints.GrantBeforeInsert, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } seat)
            {
                return new ClaimResult.NotServing();
            }

            var inserted = await BoundedAsync(bound => rows.InsertAsync(key, seat.ReplicaId, seat.Epoch, ttl, bound), cancellationToken).ConfigureAwait(false);
            if (inserted.Value is not null || inserted.Cancelled)
            {
                return await AnsweredAsync(key, inserted.Value, inserted.Cancelled, cancellationToken).ConfigureAwait(false);
            }

            await points.ReachAsync(ClaimsFaultPoints.GrantBeforeRead, cancellationToken).ConfigureAwait(false);
            var read = await BoundedAsync(bound => rows.ReadAsync(key, bound), cancellationToken).ConfigureAwait(false);
            if (read.Cancelled)
            {
                return new ClaimResult.Unknown();
            }

            switch (ClaimDecisions.AfterConflict(read.Value))
            {
                case ConflictAction.InsertAgain:
                    continue;
                case ConflictAction.Lost:
                    return new ClaimResult.Lost(read.Value!.Value.HolderReplica);
            }

            var seen = read.Value!.Value;
            await points.ReachAsync(ClaimsFaultPoints.GrantBeforeTakeover, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } takingSeat)
            {
                return new ClaimResult.NotServing();
            }

            var taken = await BoundedAsync(
                bound => rows.TakeoverAsync(key, seen.Token, takingSeat.ReplicaId, takingSeat.Epoch, ttl, bound), cancellationToken).ConfigureAwait(false);
            return taken.Value is null && !taken.Cancelled
                ? new ClaimResult.Lost(seen.HolderReplica)
                : await AnsweredAsync(key, taken.Value, taken.Cancelled, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ClaimKeys.ThrowIfReserved(key);
        if (scopes.IsOpen(key))
        {
            throw new InvalidOperationException(
                $"The claim {key} has an open claim-fenced transaction in this process: a release on another connection would wait for it, which waits for the release. End the transaction first.");
        }

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await points.ReachAsync(ClaimsFaultPoints.ReleaseBeforeStatement, cancellationToken).ConfigureAwait(false);
        var released = await BoundedAsync(bound => rows.ReleaseAsync(key, token, bound), cancellationToken).ConfigureAwait(false);
        return released.Cancelled
            ? throw new CommitOutcomeUnknownException($"The release of the claim {key} was cancelled by the library's bound before it returned: its outcome is unknown.")
            : released.Value;
    }

    public async Task<ClaimFencedTransaction> BeginFencedTransactionAsync(
        string key, Guid token, NpgsqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ClaimKeys.ThrowIfReserved(key);
        ArgumentNullException.ThrowIfNull(connection);

        var seat = gate.CheckServing()
            ?? throw new ClaimLostException($"The replica does not serve: the claim {key} may not be acted on.");
        var milliseconds = RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout).Milliseconds
            ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
        await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        var scope = scopes.Open(key);
        try
        {
            await ClaimAssertion.AssertAsync(
                key, token, seat.Timeline, milliseconds, () => gate.OnOtherTimeline(seat.Epoch), points, connection, transaction, cancellationToken).ConfigureAwait(false);
            return new ClaimFencedTransaction(connection, transaction, scope);
        }
        catch
        {
            scope.Dispose();
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task<ClaimResult> AnsweredAsync(string key, Guid? token, bool cancelled, CancellationToken cancellationToken)
    {
        var answer = token is { } drawn ? ClaimDecisions.GrantAnswer(drawn, cancelled) : new ClaimResult.Unknown();
        if (answer is ClaimResult.Won)
        {
            await points.ReachAsync(ClaimsFaultPoints.GrantAfterCommit, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            LogUnknown(logger, key);
        }

        return answer;
    }

    private Task<(T? Value, bool Cancelled)> BoundedAsync<T>(Func<CancellationToken, Task<T>> statement, CancellationToken cancellationToken) =>
        StatementBound.RunAsync(statement, options, time, cancellationToken);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The grant of claim {Key} was cancelled by the library's bound before it returned; it is not a grant.")]
    private static partial void LogUnknown(ILogger logger, string key);
}
