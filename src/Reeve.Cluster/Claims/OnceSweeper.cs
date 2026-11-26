using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal sealed partial class OnceSweeper(
    IOnceRows rows, IClaimGate gate, OnceEffects effects, IServiceScopeFactory scopes, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<OnceSweeper> logger,
    int passLimit = OnceSweeper.PassLimit) : IOwnedUnit
{
    public const string Kind = "reeve.once-sweeper";

    internal const int PassLimit = 1000;

    public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await PassAsync(stop).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException && !stop.IsCancellationRequested)
            {
                LogPassFailed(logger, exception);
            }

            await Task.Delay(options.Value.RetentionInterval, time, stop).ConfigureAwait(false);
        }
    }

    public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

    internal async Task<int> PassAsync(CancellationToken cancellationToken)
    {
        var names = effects.Names;
        if (names.Count == 0)
        {
            return 0;
        }

        var listed = await StatementBound.RunAsync(bound => rows.ListRetakeableAsync(names, passLimit, bound), options, time, cancellationToken).ConfigureAwait(false);
        if (listed.Cancelled || listed.Value is not { } candidates)
        {
            return 0;
        }

        var ran = 0;
        foreach (var candidate in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!OnceKeys.IsReserved(candidate.Key) || !effects.TryGet(candidate.Effect, out var body))
            {
                continue;
            }

            await points.ReachAsync(ClaimsFaultPoints.RetakeBeforeStatement, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } seat)
            {
                return ran;
            }

            var retaken = await StatementBound.RunAsync(
                bound => rows.RetakeAsync(candidate.Key, candidate.Token, seat.ReplicaId, seat.Epoch, bound), options, time, cancellationToken).ConfigureAwait(false);
            switch (OnceDecisions.AfterRetake(retaken.Value, retaken.Cancelled))
            {
                case RetakeAction.Skip:
                    continue;
                case RetakeAction.NotRun:
                    LogNotRun(logger, candidate.Key);
                    continue;
            }

            await points.ReachAsync(ClaimsFaultPoints.RetakeAfterCommit, cancellationToken).ConfigureAwait(false);
            var key = OnceKeys.Application(candidate.Key);
            try
            {
                var scope = scopes.CreateAsyncScope();
                await using (scope.ConfigureAwait(false))
                {
                    await body(scope.ServiceProvider, key, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                LogEffectFailed(logger, e, key);
                continue;
            }

            ran++;
            await CompleteAsync(candidate.Key, retaken.Value!.Value, cancellationToken).ConfigureAwait(false);
        }

        return ran;
    }

    private async Task CompleteAsync(string stored, Guid token, CancellationToken cancellationToken)
    {
        try
        {
            var marked = await StatementBound.RunAsync(bound => rows.CompleteAsync(stored, token, bound), options, time, cancellationToken).ConfigureAwait(false);
            if (!OnceDecisions.AfterCompletion(marked.Value, marked.Cancelled).IsCompleted)
            {
                LogNotCompleted(logger, stored);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            LogNotCompletedFailed(logger, e, stored);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "A pass of the once sweeper failed; the next pass reads again.")]
    private static partial void LogPassFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The re-take of {Key} was cancelled by the library's bound before it returned; the effect is not run.")]
    private static partial void LogNotRun(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The repeated effect of the once-run {Key} failed; the record stays for the next sweep.")]
    private static partial void LogEffectFailed(ILogger logger, Exception exception, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The completion of the repeated once-run {Key} was refused or unanswered; the record stays for the next sweep.")]
    private static partial void LogNotCompleted(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The completion of the repeated once-run {Key} failed; the record stays for the next sweep.")]
    private static partial void LogNotCompletedFailed(ILogger logger, Exception exception, string key);
}
