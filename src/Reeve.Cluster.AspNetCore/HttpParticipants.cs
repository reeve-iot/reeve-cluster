using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.AspNetCore;

internal sealed partial class HttpFenceParticipant(RequestTracker tracker, ILogger<HttpFenceParticipant> logger) : IFenceParticipant
{
    public FenceStep Step => FenceStep.CloseTraffic;

    public ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken)
    {
        LogClosed(logger, tracker.Running);
        return ValueTask.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Closed to client traffic at the fence; {Running} requests and invocations still running.")]
    private static partial void LogClosed(ILogger logger, int running);
}

internal sealed partial class HttpDrainCloseParticipant(RequestTracker tracker, ILogger<HttpDrainCloseParticipant> logger) : IDrainParticipant
{
    public DrainStep Step => DrainStep.CloseTraffic;

    public ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken)
    {
        LogClosed(logger, tracker.CloseAtDrain());
        return ValueTask.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Closed to client traffic at the drain; waiting for {Running} requests and invocations.")]
    private static partial void LogClosed(ILogger logger, int running);
}

internal sealed partial class HttpDrainAwaitParticipant(
    RequestTracker tracker, ClusterConnections connections, IOptions<ClusterOptions> options, TimeProvider time, ILogger<HttpDrainAwaitParticipant> logger)
    : IDrainParticipant
{
    public DrainStep Step => DrainStep.AwaitRequests;

    public async ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken)
    {
        var running = tracker.RunningAtMark();
        if (running.Count > 0)
        {
            var bound = options.Value.ProxyReadTimeout ?? TimeSpan.Zero;
            try
            {
                await Task.WhenAll(running).WaitAsync(bound, time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                LogOverran(logger, running.Count(t => !t.IsCompleted), bound);
            }
        }

        if (connections.AbortAll() is var aborted and > 0)
        {
            LogAborted(logger, aborted);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Aborted {Aborted} connections that stayed open at drain step 4.")]
    private static partial void LogAborted(ILogger logger, int aborted);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Running} requests were still running at ProxyReadTimeout {Bound} of the drain; they are cut with the process.")]
    private static partial void LogOverran(ILogger logger, int running, TimeSpan bound);
}
