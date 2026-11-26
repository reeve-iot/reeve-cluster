using System.Text.Json;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Microsoft.AspNetCore.Builder;

/// <summary>
/// The skip endpoint ("HTTP integration"): an operator's open and close of a skip over
/// <see cref="IChangeSkip"/>, reachable when every replica is fenced.
/// </summary>
public static class ReeveClusterSkipEndpoints
{
    /// <summary>The code of the open's refusal: the range would start at or below <c>acked_seq</c>.</summary>
    public const string CoversAcknowledged = "skip_covers_acknowledged";

    /// <summary>The code of a close of an id no skip has.</summary>
    public const string NotFound = "skip_not_found";

    /// <summary>The code of a request whose body the endpoint cannot take.</summary>
    public const string Invalid = "skip_request_invalid";

    /// <summary>The code of a skip request refused because the group has neither authorization metadata nor a fallback policy.</summary>
    public const string Unprotected = "skip_endpoint_unprotected";

    /// <summary>
    /// Maps the skip endpoint under <paramref name="pattern"/>, marked <see cref="AllowWhenFencedAttribute"/> and
    /// <see cref="SkipReadIndexAttribute"/>: it reads no derived state, so it answers on a fenced replica and on one whose cursor is
    /// behind without waiting for the applier. Add the application's authorization to the returned group
    /// (<c>RequireAuthorization(...)</c> or <c>AllowAnonymous()</c>); a request with neither endpoint metadata nor a fallback policy answers
    /// <c>403 skip_endpoint_unprotected</c> without writing a skip. A custom policy provider's fallback is not inspected, and a fallback
    /// policy protects the route only when <c>UseAuthorization()</c> runs; protect the group with endpoint metadata.
    /// <list type="bullet">
    /// <item><c>POST {pattern}</c> with <c>{ "aggregateType", "fromSeq", "reason", "openedBy"? }</c> opens a skip: <c>201</c> with
    /// <c>{ "skipId" }</c>; <c>409 skip_covers_acknowledged</c> with <c>ackedSeq</c> when <c>fromSeq</c> is at or below <c>acked_seq</c>,
    /// and nothing was written.</item>
    /// <item><c>POST {pattern}/{id}/close</c> with a JSON body <c>{ "closedBy"? }</c>, <c>{}</c> at least, closes it: <c>200</c> with
    /// <c>{ "untilSeq" }</c>, the repair entry's seq; a close of a closed skip answers the same seq again; <c>404 skip_not_found</c> for
    /// an unknown id. A missing or non-JSON body is <c>400 skip_request_invalid</c>, so a cross-site form cannot close a skip.</item>
    /// <item>On an authenticated request the operator is the authenticated user's name, and <c>openedBy</c> and <c>closedBy</c> in the
    /// body are ignored; only an unauthenticated request names its operator in the body. With no name the answer is <c>400
    /// skip_request_invalid</c>, as for a body the endpoint cannot read.</item>
    /// </list>
    /// The errors are written through the package's error envelope (<see cref="ReeveClusterHttpOptions.WriteError"/>). As an exempt
    /// request the close's repair entry commits in every phase and is not awaited.
    /// </summary>
    /// <param name="endpoints">The route builder.</param>
    /// <param name="pattern">The group's route; <c>/reeve/skips</c> by default.</param>
    /// <returns>The group, for the application's authorization and other conventions.</returns>
    public static RouteGroupBuilder MapReeveClusterSkips(this IEndpointRouteBuilder endpoints, string pattern = "/reeve/skips")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(pattern);
        var group = endpoints.MapGroup(pattern);
        group.AllowWhenFenced().SkipReadIndex();
        group.MapPost(string.Empty, OpenAsync);
        group.MapPost("/{id:long}/close", CloseAsync);
        return group;
    }

    private static async Task OpenAsync(HttpContext context)
    {
        if (IsUnprotected(context))
        {
            await RefuseUnprotectedAsync(context).ConfigureAwait(false);
            return;
        }

        var body = await ReadAsync<SkipOpenBody>(context).ConfigureAwait(false);
        var openedBy = Who(body?.OpenedBy, context);
        if (body is null || string.IsNullOrEmpty(body.AggregateType) || body.FromSeq is not > 0 || body.Reason is null || openedBy is null)
        {
            await WriteAsync(context, new ClusterHttpError(
                StatusCodes.Status400BadRequest, Invalid,
                "A skip needs aggregateType, a positive fromSeq, a reason, and openedBy or an authenticated user.")).ConfigureAwait(false);
            return;
        }

        var skips = context.RequestServices.GetRequiredService<IChangeSkip>();
        var result = await skips.OpenAsync(new SkipRequest(body.AggregateType, body.FromSeq.Value, body.Reason, openedBy), context.RequestAborted)
            .ConfigureAwait(false);
        switch (result)
        {
            case SkipOpenResult.Opened opened:
                await Results.Json(new SkipOpenedBody(opened.SkipId), ClusterHttpJson.Info<SkipOpenedBody>(context), statusCode: StatusCodes.Status201Created).ExecuteAsync(context).ConfigureAwait(false);
                return;
            case SkipOpenResult.Refused refused:
                await WriteAsync(context, new ClusterHttpError(
                    StatusCodes.Status409Conflict, CoversAcknowledged,
                    $"A skip must start above acked_seq {refused.AckedSeq}: a write at or below it was acknowledged without naming a skip. Nothing was written.")
                {
                    Extensions = new Dictionary<string, object?> { ["ackedSeq"] = refused.AckedSeq },
                }).ConfigureAwait(false);
                return;
        }
    }

    private static async Task CloseAsync(HttpContext context, long id)
    {
        if (IsUnprotected(context))
        {
            await RefuseUnprotectedAsync(context).ConfigureAwait(false);
            return;
        }

        var body = await ReadAsync<SkipCloseBody>(context).ConfigureAwait(false);
        var closedBy = Who(body?.ClosedBy, context);
        if (body is null || closedBy is null)
        {
            await WriteAsync(context, new ClusterHttpError(
                StatusCodes.Status400BadRequest, Invalid, "A close needs a JSON body ({} at least), and closedBy or an authenticated user.")).ConfigureAwait(false);
            return;
        }

        long untilSeq;
        try
        {
            untilSeq = await context.RequestServices.GetRequiredService<IChangeSkip>().CloseAsync(id, closedBy, context.RequestAborted).ConfigureAwait(false);
        }
        catch (KeyNotFoundException)
        {
            await WriteAsync(context, new ClusterHttpError(StatusCodes.Status404NotFound, NotFound, $"No skip has the id {id}.")).ConfigureAwait(false);
            return;
        }

        await Results.Json(new SkipClosedBody(untilSeq), ClusterHttpJson.Info<SkipClosedBody>(context)).ExecuteAsync(context).ConfigureAwait(false);
    }

    private static string? Who(string? named, HttpContext context) =>
        context.User.Identity is { IsAuthenticated: true, Name: { Length: > 0 } user } ? user
        : !string.IsNullOrEmpty(named) ? named
        : null;

    private static async Task<T?> ReadAsync<T>(HttpContext context)
        where T : class
    {
        try
        {
            return await context.Request.ReadFromJsonAsync<T>(context.RequestAborted).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or BadHttpRequestException)
        {
            return null;
        }
    }

    private static Task WriteAsync(HttpContext context, ClusterHttpError error) =>
        context.RequestServices.GetRequiredService<ErrorEnvelope>().WriteReplaceAsync(context, error);

    private static bool IsUnprotected(HttpContext context)
    {
        var metadata = context.GetEndpoint()?.Metadata;
        if (metadata is null)
        {
            return true;
        }

        if (metadata.GetMetadata<IAuthorizeData>() is not null || metadata.GetMetadata<AuthorizationPolicy>() is not null ||
            metadata.GetMetadata<IAuthorizationRequirementData>() is not null || metadata.GetMetadata<IAllowAnonymous>() is not null)
        {
            return false;
        }

        return context.RequestServices.GetService<IOptions<AuthorizationOptions>>()?.Value.FallbackPolicy is null;
    }

    private static async Task RefuseUnprotectedAsync(HttpContext context)
    {
        var route = context.GetEndpoint()?.DisplayName ?? context.Request.Path.Value;
        context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(ReeveClusterSkipEndpoints).FullName!)
            .LogWarning("The skip endpoint {Route} has no authorization metadata or fallback policy; add RequireAuthorization or AllowAnonymous to the group.", route);
        await WriteAsync(context, new ClusterHttpError(
            StatusCodes.Status403Forbidden, Unprotected,
            "Add RequireAuthorization or AllowAnonymous to the skip endpoint group.")).ConfigureAwait(false);
    }
}
