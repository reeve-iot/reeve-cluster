using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;

namespace Reeve.Cluster.Claims;

internal sealed partial class ClusterSingleFlight(
    IFlightRows rows, IClaimGate gate, ClaimScopes scopes, IOptions<ClusterOptions> options, TimeProvider time, IFaultPoints points, ILogger<ClusterSingleFlight> logger)
    : IClusterSingleFlight
{
    internal static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    public async Task<SingleFlightResult> RunAsync(
        string key, Func<CancellationToken, Task<byte[]>> compute, TimeSpan claimTtl, TimeSpan resultTtl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(compute);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(claimTtl, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(resultTtl, TimeSpan.Zero);
        var stored = FlightKeys.Store(key);
        ThrowIfScopeOpen(stored);

        using var outsideCaller = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        while (true)
        {
            await points.ReachAsync(ClaimsFaultPoints.FlightBeforeInsert, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } seat)
            {
                return new SingleFlightResult.NotServing();
            }

            var inserted = await BoundedAsync(bound => rows.InsertAsync(stored, seat.ReplicaId, seat.Epoch, claimTtl, bound), cancellationToken).ConfigureAwait(false);
            if (inserted.Value is not null || inserted.Cancelled)
            {
                return await ComputeAsync(key, stored, inserted.Value, inserted.Cancelled, compute, resultTtl, cancellationToken).ConfigureAwait(false);
            }

            await points.ReachAsync(ClaimsFaultPoints.FlightBeforeRead, cancellationToken).ConfigureAwait(false);
            var read = await BoundedAsync(bound => rows.ReadAsync(stored, bound), cancellationToken).ConfigureAwait(false);
            if (read.Cancelled)
            {
                LogUnknown(logger, key);
                return new SingleFlightResult.Unknown();
            }

            switch (FlightDecisions.AfterConflict(read.Value))
            {
                case FlightAction.InsertAgain:
                    continue;
                case FlightAction.UseResult:
                    return new SingleFlightResult.Waited(read.Value!.Value.Result!);
                case FlightAction.Wait:
                    await Task.Delay(PollInterval, time, cancellationToken).ConfigureAwait(false);
                    continue;
            }

            var seen = read.Value!.Value;
            await points.ReachAsync(ClaimsFaultPoints.FlightBeforeTakeover, cancellationToken).ConfigureAwait(false);
            if (gate.CheckServing() is not { } takingSeat)
            {
                return new SingleFlightResult.NotServing();
            }

            var taken = await BoundedAsync(
                bound => rows.TakeoverAsync(stored, seen.Token, takingSeat.ReplicaId, takingSeat.Epoch, claimTtl, bound), cancellationToken).ConfigureAwait(false);
            if (taken.Value is not null || taken.Cancelled)
            {
                return await ComputeAsync(key, stored, taken.Value, taken.Cancelled, compute, resultTtl, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<SingleFlightResult> ComputeAsync(
        string key, string stored, Guid? token, bool cancelled, Func<CancellationToken, Task<byte[]>> compute, TimeSpan resultTtl, CancellationToken cancellationToken)
    {
        if (token is not { } granted || !FlightDecisions.MayCompute(cancelled))
        {
            LogUnknown(logger, key);
            return new SingleFlightResult.Unknown();
        }

        await points.ReachAsync(ClaimsFaultPoints.FlightAfterGrant, cancellationToken).ConfigureAwait(false);
        byte[] value;
        try
        {
            value = await compute(cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"The computation of {key} returned null.");
        }
        catch
        {
            await ReleaseAsync(key, stored, granted).ConfigureAwait(false);
            throw;
        }

        await points.ReachAsync(ClaimsFaultPoints.FlightBeforeResult, cancellationToken).ConfigureAwait(false);
        if (gate.CheckServing() is not { } seat)
        {
            return new SingleFlightResult.ComputedRefused(value);
        }

        try
        {
            var written = await BoundedAsync(bound => rows.WriteResultAsync(stored, granted, seat.Timeline, value, resultTtl, bound), cancellationToken).ConfigureAwait(false);
            return FlightDecisions.AfterResultWrite(written.Value, written.Cancelled) switch
            {
                ResultAction.Stored => new SingleFlightResult.Computed(value),
                ResultAction.Refused => new SingleFlightResult.ComputedRefused(value),
                _ => LoggedUnknown(key, value),
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PostgresException e) when (ClaimDecisions.Refusal(e.SqlState) == DatabaseRefusal.OtherTimeline)
        {
            gate.OnOtherTimeline(seat.Epoch);
            return new SingleFlightResult.ComputedRefused(value);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            LogResultFailed(logger, e, key);
            return new SingleFlightResult.ComputedUnknown(value);
        }
    }

    private SingleFlightResult LoggedUnknown(string key, byte[] value)
    {
        LogResultUnknown(logger, key);
        return new SingleFlightResult.ComputedUnknown(value);
    }

    private async Task ReleaseAsync(string key, string stored, Guid token)
    {
        try
        {
            await BoundedAsync(bound => rows.ReleaseAsync(stored, token, bound), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            LogReleaseFailed(logger, e, key);
        }
    }

    private void ThrowIfScopeOpen(string stored)
    {
        if (scopes.IsOpen(stored))
        {
            throw new InvalidOperationException(
                $"The claim {stored} has an open claim-fenced transaction in this process: a result write on another connection would wait for it. End the transaction first.");
        }
    }

    private Task<(T? Value, bool Cancelled)> BoundedAsync<T>(Func<CancellationToken, Task<T>> statement, CancellationToken cancellationToken) =>
        StatementBound.RunAsync(statement, options, time, cancellationToken);

    [LoggerMessage(Level = LogLevel.Warning, Message = "A grant, takeover or read of the single-flight {Key} was cancelled by the library's bound before it returned; no computation was started here.")]
    private static partial void LogUnknown(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The result write of the single-flight {Key} was cancelled by the library's bound before it returned; the result may or may not be stored.")]
    private static partial void LogResultUnknown(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The result write of the single-flight {Key} failed; the result may or may not be stored.")]
    private static partial void LogResultFailed(ILogger logger, Exception exception, string key);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The release of the claim of the failed single-flight {Key} failed; the claim ends with its time to live.")]
    private static partial void LogReleaseFailed(ILogger logger, Exception exception, string key);
}
