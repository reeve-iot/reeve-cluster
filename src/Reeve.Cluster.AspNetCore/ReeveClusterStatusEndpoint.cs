using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Microsoft.AspNetCore.Builder;

/// <summary>
/// The status endpoint: the aggregated read of every member's row, sections and fence
/// history, answered in every phase once the replica has loaded its first generation. Its body is <see cref="IClusterStatus.ReadAsync"/>'s
/// result in the envelope below; the typed read is the same registry for code.
/// </summary>
public static class ReeveClusterStatusEndpoint
{
    /// <summary>The code of the answer when the aggregated read could not be made: the database did not answer within <c>ActionTimeBudget</c>, or failed.</summary>
    public const string Unavailable = "status_unavailable";

    private static readonly Func<HttpContext, Task> Handler = ReadAsync;

    /// <summary>
    /// Maps <c>GET {pattern}</c>, marked <see cref="AllowWhenFencedAttribute"/> and <see cref="SkipReadIndexAttribute"/>: it reads no
    /// derived state, so it answers on a fenced replica and on one whose cursor is behind without waiting for the applier. Like every
    /// endpoint it is refused (<c>replica_not_serving</c>) until the replica has loaded its first generation, and behind an ingress that
    /// routes only to ready replicas a fully fenced fleet reaches it by port-forward only (as the skip endpoint).
    /// <para>
    /// <b>Protect it.</b> The endpoint adds no authorization, and the body names operators (who opened a skip or moved a floor), skip
    /// reasons, application unit keys and exact versions: call <c>RequireAuthorization(...)</c> on the returned builder, as for
    /// <c>MapReeveClusterSkips</c>, and keep the sign-in endpoint exempt so an operator can still reach it on a fenced fleet.
    /// </para>
    /// <list type="bullet">
    /// <item><c>200</c> with <c>{ "readAt", "members": [ { "replicaId", "epoch", "state", "leaseExpiresAt", "leaseValid", "appliedSeq",
    /// "servingSince", "version", "protocol", "sections": [ { "name", "ageSeconds", "values" } ], "fences": [ { "epoch", "trigger",
    /// "fencedAt", "spanSeconds" } ] } ] }</c>, in camel case whatever the application's own JSON options say. The envelope is stable;
    /// the keys inside a section's <c>values</c> are diagnostic and may change in a minor release. A time or a duration inside
    /// <c>values</c> is as of the member's write: subtract <c>ageSeconds</c> to read it now. A member's sections are there while its lease
    /// is valid and for <c>LeaseTtl</c> after it ended, each with the age it has on the database's clock; <c>fences</c> is its recorded
    /// fence history, newest first. The enumerated strings (a member's <c>state</c>, a fence's <c>trigger</c>, the <c>subscriptions</c> of the
    /// <c>ask</c> section) may gain values in a minor release: a consumer treats a value it does not know as unknown.</item>
    /// <item><c>503 status_unavailable</c> when the read did not complete within <c>ActionTimeBudget</c> or failed, written through the
    /// package's error envelope (<see cref="ReeveClusterHttpOptions.WriteError"/>); the log carries the cause.</item>
    /// </list>
    /// </summary>
    /// <param name="endpoints">The route builder.</param>
    /// <param name="pattern">The route; <c>/reeve/status</c> by default.</param>
    /// <returns>The endpoint's builder, for the application's authorization and other conventions.</returns>
    public static RouteHandlerBuilder MapReeveClusterStatus(this IEndpointRouteBuilder endpoints, string pattern = "/reeve/status")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(pattern);
        return endpoints.MapGet(pattern, Handler).AllowWhenFenced().SkipReadIndex();
    }

    private static async Task ReadAsync(HttpContext context)
    {
        var services = context.RequestServices;
        ClusterStatusRead read;
        try
        {
            read = await services.GetRequiredService<IClusterStatus>().ReadAsync(context.RequestAborted).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException && !context.RequestAborted.IsCancellationRequested)
        {
            services.GetRequiredService<ILoggerFactory>().CreateLogger("Reeve.Cluster.Status").LogWarning(ex, "The status could not be read.");
            await services.GetRequiredService<ErrorEnvelope>().WriteReplaceAsync(context, new ClusterHttpError(
                StatusCodes.Status503ServiceUnavailable, Unavailable,
                "The status could not be read: the database did not answer within ActionTimeBudget, or failed.")).ConfigureAwait(false);
            return;
        }

        await Results.Json(StatusBody.Of(read), ClusterStatusJsonContext.Default.StatusBody).ExecuteAsync(context).ConfigureAwait(false);
    }
}
