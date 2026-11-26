using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Reeve.Cluster.AspNetCore;

namespace Microsoft.AspNetCore.Builder;

/// <summary>The middleware and the endpoint conventions of the ASP.NET Core package.</summary>
public static class ReeveClusterEndpointExtensions
{
    /// <summary>
    /// Adds the cluster scope middleware, routing, and the fence middleware, in that order: the scope spans everything after it, so
    /// call this first in the pipeline.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>On a <c>WebApplication</c>, routing here is the application's routing: a later <c>UseRouting()</c> keeps the match. On a
    /// plain <c>IApplicationBuilder</c> every <c>UseRouting()</c> has a route builder of its own and <c>UseEndpoints</c> registers into
    /// the last one, so call <c>UseRouting()</c> before this or not at all; a routing after it matches nothing here and every request
    /// is refused as one that is not exempt.</item>
    /// <item>Middleware after this one sees only requests the replica admits: <c>UseCors</c> after it leaves the <c>503</c> without CORS
    /// headers, and static files, health checks mapped as middleware and remote-authentication callbacks have no endpoint to mark, so
    /// map them as endpoints and mark them. A readiness or liveness endpoint is an endpoint like any other: mark it
    /// <see cref="AllowWhenFencedAttribute"/> and <see cref="SkipReadIndexAttribute"/>, or a probe is answered by this middleware
    /// and, on a serving replica, waits on the read index. In a <c>WebApplication</c>, authentication and authorization are no longer
    /// added implicitly once routing is: call them after this.</item>
    /// <item>An endpoint that short-circuits (<c>ShortCircuit()</c>) is answered by the routing middleware before the fence middleware:
    /// it is not refused and pins nothing, so it must not read derived state.</item>
    /// <item>The acknowledgement: <c>AddReeveClusterAspNetCore</c> adds a global MVC result filter and a hub filter that await a write before
    /// the response starts; a minimal-API endpoint or group that writes adds <see cref="AcknowledgeWrites{TBuilder}"/> for the error envelope as its
    /// body; without it a backstop in the fence middleware still awaits the write and gives the response the rule's status and headers, and
    /// the body stays the endpoint's. Put an application's <c>UseExceptionHandler</c> before or after this: an exception that follows a
    /// refused commit is answered by the fenced-request rule either way. Fence step 4 aborts open connections
    /// through the application's <see cref="Reeve.Cluster.IFenceSink"/>: name <see cref="AspNetCoreFenceSink"/> as the sink port. A raw WebSocket needs
    /// no help; a streaming or server-sent-events response is tracked by the application with <see cref="ClusterConnections.Track(HttpContext)"/>.</item>
    /// <item>Register <c>AddReeveClusterAspNetCore</c> before any application <c>IHubFilter</c>: filters run in registration order, outermost
    /// first, and one registered earlier runs outside each invocation's scope and pin.</item>
    /// </list>
    /// </remarks>
    /// <param name="app">The application builder.</param>
    public static IApplicationBuilder UseReeveCluster(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.UseMiddleware<ClusterScopeMiddleware>();
        app.UseRouting();
        return app.UseMiddleware<FenceMiddleware>();
    }

    /// <summary>
    /// Marks the endpoint, or every endpoint of a <c>MapGroup</c>, as served while the replica is not <c>Serving</c>: the minimal-API
    /// form of <see cref="AllowWhenFencedAttribute"/>.
    /// </summary>
    public static TBuilder AllowWhenFenced<TBuilder>(this TBuilder builder)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Add(endpoint => endpoint.Metadata.Add(new AllowWhenFencedAttribute()));
        return builder;
    }

    /// <summary>Turns the read index's wait off for the endpoint, or every endpoint of a <c>MapGroup</c>: the minimal-API form of <see cref="SkipReadIndexAttribute"/>.</summary>
    public static TBuilder SkipReadIndex<TBuilder>(this TBuilder builder)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Add(endpoint => endpoint.Metadata.Add(new SkipReadIndexAttribute()));
        return builder;
    }

    /// <summary>
    /// Adds the acknowledgement to a minimal-API endpoint, or every endpoint of a <c>MapGroup</c>: the form of the MVC result filter
    /// for an endpoint that writes. It awaits the write before the response starts and answers <c>202 write_unconfirmed</c> or
    /// <c>503 write_refused</c> where the fenced-request rule says so, with the application's error envelope as the body. An endpoint
    /// without it still gets the rule's status and headers from the fence middleware, and keeps its own body.
    /// </summary>
    public static TBuilder AcknowledgeWrites<TBuilder>(this TBuilder builder)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddEndpointFilter(new AcknowledgementEndpointFilter());
        return builder;
    }
}
