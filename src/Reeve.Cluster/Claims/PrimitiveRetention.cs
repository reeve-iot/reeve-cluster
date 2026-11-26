using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal sealed partial class PrimitiveRetention(
    IRetentionRows rows, IOptions<ClusterOptions> options, TimeProvider time, ILogger<PrimitiveRetention> logger, int passLimit = PrimitiveRetention.PassLimit) : IOwnedUnit
{
    public const string Kind = "reeve.primitive-retention";

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
        var deleted = 0;
        deleted += await HalfAsync("claim", async token => Plain(await rows.DeleteClaimsAsync(passLimit, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        deleted += await HalfAsync("once", async token => Plain(await rows.DeleteOnceAsync(passLimit, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        deleted += await HalfAsync("flight", async token => Plain(await rows.DeleteFlightsAsync(passLimit, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        deleted += await HalfAsync("counter", async token => Plain(await rows.DeleteCountersAsync(passLimit, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        deleted += await HalfAsync("slot", async token => Plain(await rows.DeleteSlotsAsync(passLimit, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        deleted += await HalfAsync("budget", async token =>
        {
            var retained = await rows.DeleteBudgetsAsync(passLimit, token).ConfigureAwait(false);
            return new Run(retained.Windows, retained.Ids, retained.Windows >= passLimit || retained.Ids >= passLimit * RetentionStore.IdsPerWindow);
        }, cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    private TimeSpan HalfBudget => options.Value.RetentionInterval / 12;

    private Run Plain(int count) => new(count, 0, count >= passLimit);

    private readonly record struct Run(int Rows, int Ids, bool Full);

    private async Task<int> HalfAsync(string record, Func<CancellationToken, Task<Run>> half, CancellationToken cancellationToken)
    {
        var started = time.GetTimestamp();
        var total = 0;
        try
        {
            while (true)
            {
                var done = await StatementBound.RunAsync(half, options, time, cancellationToken).ConfigureAwait(false);
                if (done.Cancelled)
                {
                    return total;
                }

                var run = done.Value;
                if (run.Ids > 0)
                {
                    ClusterDiagnostics.RetentionDeleted.Add(run.Ids, new KeyValuePair<string, object?>("record", "budget_id"));
                }

                if (run.Rows > 0)
                {
                    ClusterDiagnostics.RetentionDeleted.Add(run.Rows, new KeyValuePair<string, object?>("record", record));
                    LogDeleted(logger, run.Rows, record);
                    total += run.Rows;
                }

                if (!run.Full || time.GetElapsedTime(started) >= HalfBudget)
                {
                    return total;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            LogHalfFailed(logger, exception, record);
            return total;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "A pass of primitive retention failed; the next pass reads again.")]
    private static partial void LogPassFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Primitive retention could not delete the {Record} rows; the other records and the next pass go on.")]
    private static partial void LogHalfFailed(ILogger logger, Exception exception, string record);

    [LoggerMessage(Level = LogLevel.Information, Message = "Primitive retention deleted {Count} {Record} rows.")]
    private static partial void LogDeleted(ILogger logger, int count, string record);
}
