using System.Data;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;
using IsolationLevel = System.Data.IsolationLevel;

namespace Reeve.Cluster.Slots;

internal interface ISlotObserver
{
    void Acquired(SlotAcquireResult result);

    void Renewed(SlotRenewResult result);
}

internal sealed class NoSlotObserver : ISlotObserver
{
    public static NoSlotObserver Instance { get; } = new();

    public void Acquired(SlotAcquireResult result)
    {
    }

    public void Renewed(SlotRenewResult result)
    {
    }
}

internal sealed partial class ClusterSlots(
    ISlotRows rows, ISlotGate gate, SlotRegistry registry, SlotScopes scopes, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points,
    ILogger<ClusterSlots> logger, ISlotObserver observer) : IClusterSlots
{
    private readonly ISlotObserver slotObserver = observer;

    public async Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ThrowIfTtlTooShort(ttl);
        if (scopes.IsKeyOpen(key))
        {
            throw new InvalidOperationException(
                $"The key {key} has an open slot-fenced transaction in this process: an acquire on another connection would wait for it. End the transaction first.");
        }

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await points.ReachAsync(SlotsFaultPoints.AcquireBeforeStatement, cancellationToken).ConfigureAwait(false);
        if (gate.CheckServing() is not { } seat)
        {
            return Observed(new SlotAcquireResult.NotServing());
        }

        var started = time.GetTimestamp();
        try
        {
            var granted = await BoundedAsync(
                bound => rows.AcquireAsync(
                    key, id, seat.ReplicaId, seat.Epoch, seat.Timeline, limit, ttl, afterDelete => points.ReachAsync(SlotsFaultPoints.AcquireAfterDelete, afterDelete), bound),
                cancellationToken).ConfigureAwait(false);
            var answer = SlotDecisions.AcquireAnswer(granted.Value, granted.Cancelled);
            if (answer is not SlotAcquireResult.Acquired acquired)
            {
                if (answer is SlotAcquireResult.Unknown)
                {
                    LogAcquireUnknown(logger, key);
                }

                return Observed(answer);
            }

            registry.Record(key, id, acquired.Token, seat.Epoch, started, ttl);
            await points.ReachAsync(SlotsFaultPoints.AcquireAfterCommit, cancellationToken).ConfigureAwait(false);
            return Observed(answer);
        }
        catch (PostgresException e) when (SlotDecisions.Refusal(e.SqlState) == SlotRefusal.OtherTimeline)
        {
            gate.OnOtherTimeline(seat.Epoch);
            return Observed(new SlotAcquireResult.NotServing());
        }
    }

    public async Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        ThrowIfTtlTooShort(ttl);

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await points.ReachAsync(SlotsFaultPoints.RenewBeforeStatement, cancellationToken).ConfigureAwait(false);
        if (registry.TryGet(key, id, token) is not { } entry)
        {
            return Observed(SlotRenewResult.Ended);
        }

        if (gate.CheckHolder(entry.Epoch) is not { } seat)
        {
            registry.Remove(key, id, token);
            return Observed(SlotRenewResult.NotServing);
        }

        var started = time.GetTimestamp();
        try
        {
            var renewed = await BoundedAsync(bound => rows.RenewAsync(key, id, token, seat.Timeline, ttl, bound), cancellationToken).ConfigureAwait(false);
            var answer = SlotDecisions.RenewAnswer(renewed.Value, renewed.Cancelled);
            switch (answer)
            {
                case SlotRenewResult.Renewed:
                    registry.Extend(key, id, token, started, ttl);
                    break;
                case SlotRenewResult.Ended:
                    registry.Remove(key, id, token);
                    break;
                default:
                    LogRenewUnknown(logger, key);
                    break;
            }

            return Observed(answer);
        }
        catch (PostgresException e) when (SlotDecisions.Refusal(e.SqlState) == SlotRefusal.OtherTimeline)
        {
            gate.OnOtherTimeline(seat.Epoch);
            registry.Remove(key, id, token);
            return Observed(SlotRenewResult.NotServing);
        }
    }

    public async Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken = default)
    {
        ThrowIfInvalid(key, id);
        if (scopes.IsOpen(key, id))
        {
            throw new InvalidOperationException(
                $"The slot {id} of {key} has an open slot-fenced transaction in this process: a release on another connection would wait for it, which waits for the release. End the transaction first.");
        }

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        try
        {
            await points.ReachAsync(SlotsFaultPoints.ReleaseBeforeStatement, cancellationToken).ConfigureAwait(false);
            var released = await BoundedAsync(bound => rows.ReleaseAsync(key, id, token, bound), cancellationToken).ConfigureAwait(false);
            return released.Cancelled
                ? throw new CommitOutcomeUnknownException($"The release of the slot {id} of {key} was cancelled by the library's bound before it returned: its outcome is unknown.")
                : released.Value;
        }
        finally
        {
            registry.Remove(key, id, token);
        }
    }

    public async Task<IReadOnlyList<SlotHolder>> ReadHoldersAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return await rows.ReadHoldersAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async Task<SlotFencedTransaction> BeginFencedTransactionAsync(
        string key, string id, Guid token, NpgsqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        ThrowIfInvalid(key, id);
        ArgumentNullException.ThrowIfNull(connection);

        var seat = registry.TryGet(key, id, token) is { } entry ? gate.CheckHolder(entry.Epoch) : null;
        if (seat is null)
        {
            throw new SlotLostException($"The slot {id} of {key} may not be acted on: it is not held by this process, or the slot holder check refused.");
        }

        var milliseconds = RecordTimeout.FromRemaining(options.Value.FencedTransactionTimeout).Milliseconds
            ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
        await FencedTransactions.CheckConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
        var scope = scopes.Open(key, id);
        try
        {
            await SlotAssertion.AssertAsync(
                key, id, token, seat.Value.Timeline, milliseconds, () => gate.OnOtherTimeline(seat.Value.Epoch), points, connection, transaction, cancellationToken)
                .ConfigureAwait(false);
            return new SlotFencedTransaction(connection, transaction, scope);
        }
        catch
        {
            scope.Dispose();
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static void ThrowIfInvalid(string key, string id)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(id);
    }

    private void ThrowIfTtlTooShort(TimeSpan ttl) => ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, options.Value.SafetyMargin);

    private SlotAcquireResult Observed(SlotAcquireResult result)
    {
        slotObserver.Acquired(result);
        return result;
    }

    private SlotRenewResult Observed(SlotRenewResult result)
    {
        slotObserver.Renewed(result);
        return result;
    }

    private Task<(T? Value, bool Cancelled)> BoundedAsync<T>(Func<CancellationToken, Task<T>> statement, CancellationToken cancellationToken) =>
        StatementBound.RunAsync(statement, options, time, cancellationToken);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The acquire of a slot of {Key} was cancelled by the library's bound before it returned; it is not a grant, and a retry with the same id answers the slot if it exists.")]
    private static partial void LogAcquireUnknown(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The renewal of a slot of {Key} was cancelled by the library's bound before it returned; the slot may or may not be extended.")]
    private static partial void LogRenewUnknown(ILogger logger, string key);
}
