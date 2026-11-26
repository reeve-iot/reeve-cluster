using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Commands;

internal readonly record struct CommandSweepRun(int Expired, int Deleted);

internal sealed partial class CommandSweep(
    ICommandRows rows, IWakeupChannel wakeups, IOptions<ClusterOptions> options, TimeProvider time, ILogger<CommandSweep> logger, int passLimit = CommandSweep.PassLimit) : IOwnedUnit
{
    public const string Kind = "reeve.command-sweep";

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

    internal async Task<CommandSweepRun> PassAsync(CancellationToken cancellationToken)
    {
        var expired = 0;
        var nudges = new HashSet<(string Kind, string Key)>();
        try
        {
            var listed = await StatementBound.RunAsync(bound => rows.ListExpirableAsync(passLimit, bound), options, time, cancellationToken).ConfigureAwait(false);
            if (!listed.Cancelled && listed.Value is { } candidates)
            {
                foreach (var candidate in candidates)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        var done = await StatementBound.RunAsync(bound => rows.ExpireAsync(candidate.Id, bound), options, time, cancellationToken).ConfigureAwait(false);
                        if (!done.Cancelled && done.Value)
                        {
                            expired++;
                            nudges.Add((candidate.Kind, candidate.Key));
                        }
                    }
                    catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
                    {
                        LogExpireFailed(logger, exception, candidate.Id);
                    }
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            LogListFailed(logger, exception);
        }

        foreach (var (kind, key) in nudges)
        {
            await NudgeWaitersAsync(kind, key, cancellationToken).ConfigureAwait(false);
        }

        var deleted = 0;
        try
        {
            var retained = await StatementBound.RunAsync(bound => rows.DeleteRetainedAsync(passLimit, bound), options, time, cancellationToken).ConfigureAwait(false);
            if (!retained.Cancelled)
            {
                deleted = retained.Value;
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            LogRetentionFailed(logger, exception);
        }

        if (expired > 0 || deleted > 0)
        {
            LogPass(logger, expired, deleted);
        }

        return new CommandSweepRun(expired, deleted);
    }

    private async ValueTask NudgeWaitersAsync(string kind, string key, CancellationToken cancellationToken)
    {
        try
        {
            await wakeups.PublishAsync(Wakeup.Wakeup.Nudge(WakeupKind.CommandNudge, CommandNudges.Completed(kind, key)), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            LogNudgeFailed(logger, exception, kind, key);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "A pass of the command sweep failed; the next pass reads again.")]
    private static partial void LogPassFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The command sweep could not list the pending commands past the ttl; retention goes on and the next pass reads again.")]
    private static partial void LogListFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The command sweep could not expire command {Id}; the rest of the pass goes on and the next pass reads it again.")]
    private static partial void LogExpireFailed(ILogger logger, Exception exception, long id);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The command sweep could not delete the finished commands past the retention; the next pass tries again.")]
    private static partial void LogRetentionFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "The command sweep expired {Expired} pending commands and deleted {Deleted} finished ones.")]
    private static partial void LogPass(ILogger logger, int expired, int deleted);

    [LoggerMessage(Level = LogLevel.Debug, Message = "The completion nudge for {Kind}/{Key} was not published; the waiting callers poll.")]
    private static partial void LogNudgeFailed(ILogger logger, Exception exception, string kind, string key);
}
