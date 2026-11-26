using Microsoft.AspNetCore.Http;
using Reeve.Cluster.Log;

namespace Reeve.Cluster.AspNetCore;

internal sealed class FenceMiddleware(
    RequestDelegate next, IClusterScopeSource scopes, Admission admission, RequestTracker tracker, ErrorEnvelope envelope, HttpAnswers answers,
    ClusterConnections connections)
{
    public async Task InvokeAsync(HttpContext context)
    {
        var endpoint = context.GetEndpoint();
        var exempt = EndpointRules.IsAdmittedWhenNotServing(endpoint);
        var scope = scopes.CurrentWriter
            ?? throw new InvalidOperationException("No cluster scope: UseReeveCluster() opens it, and it must run before the endpoints.");

        if (EndpointRules.IsMarked(endpoint))
        {
            scope.ClassifyAsExempt();
        }

        if (EndpointRules.HubTypeOf(endpoint) is not null)
        {
            var connection = await admission.AdmitConnectionAsync(exempt, context.RequestAborted).ConfigureAwait(false);
            if (connection.Refusal is { } refused)
            {
                await envelope.WriteRefusalAsync(context, refused).ConfigureAwait(false);
                return;
            }

            await next(context).ConfigureAwait(false);
            return;
        }

        var socket = IsWebSocketUpgrade(context.Request);
        using var ticket = socket ? null : tracker.Enter();
        using var tracked = socket ? connections.Track(context) : null;
        if (ticket is not null)
        {
            context.Items[RequestTracker.TicketKey] = ticket;
        }

        var admitted = await admission.AdmitAsync(scope, exempt, EndpointRules.SkipsReadIndex(endpoint), context.RequestAborted).ConfigureAwait(false);
        if (admitted.Refusal is { } refusal)
        {
            await envelope.WriteRefusalAsync(context, refusal).ConfigureAwait(false);
            return;
        }

        if (socket)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        context.Response.OnStarting(static state => ((Backstop)state).RunAsync(), new Backstop(answers, context, scope));
        try
        {
            await next(context).ConfigureAwait(false);
        }
        catch (Exception exception) when (scope.CommitRefused && !context.Response.HasStarted)
        {
            if (!await answers.AnswerRefusedAsync(context, scope, exception).ConfigureAwait(false))
            {
                throw;
            }

            return;
        }

        if (!context.Response.HasStarted)
        {
            await answers.BackstopAsync(context, scope).ConfigureAwait(false);
        }
    }

    private sealed class Backstop(HttpAnswers answers, HttpContext context, IClusterScope scope)
    {
        public Task RunAsync() => answers.BackstopAsync(context, scope);
    }

    private static bool IsWebSocketUpgrade(HttpRequest request) =>
        request.Headers.Upgrade.Any(v => v is not null && v.Contains("websocket", StringComparison.OrdinalIgnoreCase));
}
