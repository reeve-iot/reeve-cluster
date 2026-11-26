using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Http.Connections.Features;
using Microsoft.AspNetCore.SignalR;
using Reeve.Cluster.Log;

namespace Reeve.Cluster.AspNetCore;

internal sealed partial class ClusterHubFilter(
    IClusterScopeSource scopes, Admission admission, RequestTracker tracker, AcknowledgementStep step, ClusterConnections connections,
    Microsoft.Extensions.Logging.ILogger<ClusterHubFilter> logger) : IHubFilter
{
    public async ValueTask<object?> InvokeMethodAsync(HubInvocationContext invocationContext, Func<HubInvocationContext, ValueTask<object?>> next)
    {
        ArgumentNullException.ThrowIfNull(invocationContext);
        ArgumentNullException.ThrowIfNull(next);
        var hub = invocationContext.Hub.GetType();
        var method = invocationContext.HubMethod;

        using var running = tracker.Enter();
        using var handle = scopes.Begin(ClusterScopeKind.ClientRequest);
        var endpoint = invocationContext.Context.GetHttpContext()?.GetEndpoint();
        var marked = EndpointRules.IsInvocationExempt(hub, method) || EndpointRules.IsMarked(endpoint);
        await AdmitAsync(
            marked,
            marked,
            EndpointRules.InvocationSkipsReadIndex(hub, method) || EndpointRules.SkipsReadIndex(endpoint),
            invocationContext.Context.ConnectionAborted).ConfigureAwait(false);
        var scope = scopes.CurrentWriter!;
        var aborted = invocationContext.Context.ConnectionAborted;
        object? result;
        try
        {
            result = await next(invocationContext).ConfigureAwait(false);
        }
        catch (Exception exception) when (scope.CommitRefused)
        {
            LogRefusedException(logger, invocationContext.HubMethodName, exception);
            if (ToException(await step.RunAsync(scope, aborted).ConfigureAwait(false)) is { } refused)
            {
                throw refused;
            }

            throw;
        }

        return ToException(await step.RunAsync(scope, aborted).ConfigureAwait(false)) is { } unanswered ? throw unanswered : result;
    }

    private static HubException? ToException(Verdict verdict) => verdict.Kind switch
    {
        VerdictKind.WriteRefused => WriteRefusedException(),
        VerdictKind.Unconfirmed => WriteUnconfirmedException(),
        _ => null,
    };

    private static HubException WriteUnconfirmedException()
    {
        HttpDiagnostics.WriteUnconfirmed.Add(1);
        return new HubException(RefusalResponse.WriteUnconfirmed);
    }

    private static HubException WriteRefusedException()
    {
        HttpDiagnostics.WriteRefused.Add(1);
        return new HubException(RefusalResponse.WriteRefused);
    }

    public async Task OnConnectedAsync(HubLifetimeContext context, Func<HubLifetimeContext, Task> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        var hub = context.Hub.GetType();
        var endpoint = context.Context.GetHttpContext()?.GetEndpoint();
        using var running = tracker.Enter();
        using var handle = scopes.Begin(ClusterScopeKind.ClientRequest);
        var marked = hub.IsDefined(typeof(AllowWhenFencedAttribute), inherit: true) || EndpointRules.IsMarked(endpoint);

        connections.Track(context.Context.ConnectionId, () => AbortConnection(context.Context));
        try
        {
            await AdmitAsync(marked, EndpointRules.IsConnectionExempt(hub) || marked, skipReadIndex: true, context.Context.ConnectionAborted).ConfigureAwait(false);
            await next(context).ConfigureAwait(false);
        }
        catch
        {
            connections.Release(context.Context.ConnectionId);
            throw;
        }
    }

    private static void AbortConnection(HubCallerContext connection)
    {
        if (connection.Features.Get<IHttpTransportFeature>()?.TransportType is HttpTransportType.WebSockets or HttpTransportType.ServerSentEvents
            && connection.GetHttpContext() is { } http)
        {
            http.Abort();
        }
        else
        {
            connection.Abort();
        }
    }

    public async Task OnDisconnectedAsync(HubLifetimeContext context, Exception? exception, Func<HubLifetimeContext, Exception?, Task> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        connections.Release(context.Context.ConnectionId);
        var endpoint = context.Context.GetHttpContext()?.GetEndpoint();
        using var handle = scopes.Begin(ClusterScopeKind.ClientRequest);
        if (context.Hub.GetType().IsDefined(typeof(AllowWhenFencedAttribute), inherit: true) || EndpointRules.IsMarked(endpoint))
        {
            scopes.CurrentWriter!.ClassifyAsExempt();
        }

        await next(context, exception).ConfigureAwait(false);
    }

    private async ValueTask AdmitAsync(bool marked, bool admittedWhenNotServing, bool skipReadIndex, CancellationToken cancellationToken)
    {
        var scope = scopes.CurrentWriter!;
        if (marked)
        {
            scope.ClassifyAsExempt();
        }

        var admitted = await admission.AdmitAsync(scope, admittedWhenNotServing, skipReadIndex, cancellationToken).ConfigureAwait(false);
        if (admitted.Refusal is { } refusal)
        {
            throw new HubException(RefusalResponse.CodeOf(refusal));
        }
    }

    [Microsoft.Extensions.Logging.LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Warning, Message = "The hub method {Method} failed after fence step 2 refused a commit of the invocation; answered by the fenced-request rule instead.")]
    private static partial void LogRefusedException(Microsoft.Extensions.Logging.ILogger logger, string method, Exception exception);
}
