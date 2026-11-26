using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal sealed partial class ClusterOnce(
    IOnceRows rows, IClaimGate gate, OnceEffects effects, IServiceScopeFactory scopes, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<ClusterOnce> logger)
    : IClusterOnce
{
    public async Task<OnceResult> RunAsync(string key, string effect, OnceMode mode, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(effect);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);
        if (!Enum.IsDefined(mode))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        if (!effects.TryGet(effect, out var body))
        {
            throw new ArgumentException($"No once effect is registered as '{effect}' on this replica.", nameof(effect));
        }

        var stored = OnceKeys.Store(key);
        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        while (true)
        {
            await points.ReachAsync(ClaimsFaultPoints.OnceBeforeInsert, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } seat)
            {
                return new OnceResult.NotServing();
            }

            var inserted = await Bounded(bound => rows.InsertAsync(stored, mode, effect, seat.ReplicaId, seat.Epoch, ttl, bound), cancellationToken).ConfigureAwait(false);
            if (inserted.Value is not null || inserted.Cancelled)
            {
                if (inserted.Value is not { } granted || !OnceDecisions.MayStart(inserted.Cancelled))
                {
                    LogUnknown(logger, key);
                    return new OnceResult.Unknown();
                }

                await points.ReachAsync(ClaimsFaultPoints.OnceAfterGrant, cancellationToken).ConfigureAwait(false);
                return await RunGrantedAsync(key, stored, granted, body, cancellationToken).ConfigureAwait(false);
            }

            await points.ReachAsync(ClaimsFaultPoints.OnceBeforeRead, cancellationToken).ConfigureAwait(false);
            var read = await Bounded(bound => rows.ReadAsync(stored, bound), cancellationToken).ConfigureAwait(false);
            if (read.Cancelled)
            {
                LogInDoubt(logger, key);
                return new OnceResult.InDoubt();
            }

            switch (OnceDecisions.AfterConflict(read.Value))
            {
                case OnceConflictAction.InsertAgain:
                    continue;
                case OnceConflictAction.AlreadyCompleted:
                    return new OnceResult.AlreadyCompleted();
                case OnceConflictAction.Running:
                    return new OnceResult.Running(read.Value!.Value.HolderReplica);
                default:
                    LogInDoubt(logger, key);
                    return new OnceResult.InDoubt();
            }
        }
    }

    private async Task<OnceResult> RunGrantedAsync(
        string key, string stored, Guid token, Func<IServiceProvider, string, CancellationToken, Task> body, CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await body(scope.ServiceProvider, key, cancellationToken).ConfigureAwait(false);
        }
        await points.ReachAsync(ClaimsFaultPoints.OnceBeforeComplete, cancellationToken).ConfigureAwait(false);
        try
        {
            var marked = await Bounded(bound => rows.CompleteAsync(stored, token, bound), cancellationToken).ConfigureAwait(false);
            return OnceDecisions.AfterCompletion(marked.Value, marked.Cancelled);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            LogCompletionFailed(logger, e, key);
            return new OnceResult.CompletionUnknown();
        }
    }

    private Task<(T? Value, bool Cancelled)> Bounded<T>(Func<CancellationToken, Task<T>> statement, CancellationToken cancellationToken) =>
        StatementBound.RunAsync(statement, options, time, cancellationToken);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The grant of the once-run {Key} was cancelled by the library's bound before it returned; the effect was not started here.")]
    private static partial void LogUnknown(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The once-run of {Key} could not be resolved from the conflicting record; this call did not start the effect.")]
    private static partial void LogInDoubt(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The completion of the once-run of {Key} failed; its outcome is unknown.")]
    private static partial void LogCompletionFailed(ILogger logger, Exception exception, string key);
}
