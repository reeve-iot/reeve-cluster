using Microsoft.AspNetCore.Http;

namespace Reeve.Cluster.AspNetCore;

internal sealed class ClusterScopeMiddleware(RequestDelegate next, Log.IClusterScopeSource scopes)
{
    public async Task InvokeAsync(HttpContext context)
    {
        ResponseHeaderSnapshot.Capture(context);
        using var requestFlow = HttpRequestFlow.Enter();
        using var handle = scopes.Begin(ClusterScopeKind.ClientRequest);
        await next(context).ConfigureAwait(false);
    }
}
