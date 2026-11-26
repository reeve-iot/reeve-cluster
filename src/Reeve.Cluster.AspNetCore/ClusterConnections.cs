using System.Collections.Concurrent;
using Microsoft.AspNetCore.Http;

namespace Reeve.Cluster.AspNetCore;

/// <summary>
/// The connections that stay open past a request — hub connections and raw WebSockets — which fence step 4 aborts through
/// <see cref="AspNetCoreFenceSink"/> and drain step 4 aborts once the running requests have ended. The package tracks hub connections and WebSocket upgrades itself; an
/// application that holds another kind open, such as a server-sent-events or other streaming response, tracks it with
/// <see cref="Track(HttpContext)"/> for the life of the response.
/// </summary>
public sealed class ClusterConnections
{
    private readonly ConcurrentDictionary<object, Action> open = new();

    /// <summary>The number of connections tracked now.</summary>
    public int Count => open.Count;

    /// <summary>
    /// Tracks the connection of <paramref name="context"/> until the returned handle is disposed; fence step 4 and drain step 4 abort it
    /// with <see cref="HttpContext.Abort"/>. From this call the request no longer counts as running for the drain, which aborts it
    /// instead of waiting for it.
    /// </summary>
    public IDisposable Track(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Items.TryGetValue(RequestTracker.TicketKey, out var ticket))
        {
            context.Items.Remove(RequestTracker.TicketKey);
            (ticket as IDisposable)?.Dispose();
        }

        return Track(context, context.Abort);
    }

    /// <summary>Aborts every connection tracked now, each as it was registered, and forgets them. Returns how many.</summary>
    public int AbortAll()
    {
        var aborted = 0;
        foreach (var (key, abort) in open.ToArray())
        {
            if (open.TryRemove(key, out _))
            {
                aborted++;
                try
                {
                    abort();
                }
                catch (Exception)
                {
                }
            }
        }

        return aborted;
    }

    internal IDisposable Track(object key, Action abort)
    {
        open[key] = abort;
        return new Tracked(this, key);
    }

    internal void Release(object key) => open.TryRemove(key, out _);

    private sealed class Tracked(ClusterConnections owner, object key) : IDisposable
    {
        public void Dispose() => owner.Release(key);
    }
}

/// <summary>
/// Fence step 4 for ASP.NET Core: aborts the hub connections and raw WebSockets the package tracks, and any connection the
/// application tracked through <see cref="ClusterConnections.Track(HttpContext)"/>. Name it as the sink port,
/// <c>new ClusterPorts(reconcilers, typeof(AspNetCoreFenceSink))</c>, or call <see cref="ClusterConnections.AbortAll"/> from the
/// application's own sink.
/// </summary>
/// <remarks>
/// A hub connection on WebSockets or server-sent events is aborted at its transport, so a SignalR client with automatic reconnect reconnects by itself (the
/// balancer then sends it to a replica that serves). A hub connection on long polling is aborted with <c>HubCallerContext.Abort()</c>:
/// SignalR sends the client a close message that does not allow automatic reconnection, and the client application's own code restarts the connection.
/// A raw WebSocket is aborted at the transport.
/// </remarks>
public sealed partial class AspNetCoreFenceSink(ClusterConnections connections, Microsoft.Extensions.Logging.ILogger<AspNetCoreFenceSink> logger) : IFenceSink
{
    /// <inheritdoc/>
    public ValueTask AbortConnectionsAsync(CancellationToken cancellationToken)
    {
        LogAborted(logger, connections.AbortAll());
        return ValueTask.CompletedTask;
    }

    [Microsoft.Extensions.Logging.LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Information, Message = "Fence step 4 aborted {Connections} open connections.")]
    private static partial void LogAborted(Microsoft.Extensions.Logging.ILogger logger, int connections);
}
