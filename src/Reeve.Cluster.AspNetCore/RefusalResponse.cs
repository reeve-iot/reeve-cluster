using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Reeve.Cluster.AspNetCore;

internal static class RefusalResponse
{
    public const string NotServing = "replica_not_serving";
    public const string Fenced = "replica_fenced";
    public const string Behind = "replica_behind";
    public const string WriteUnconfirmed = "write_unconfirmed";

    public const string WriteRefused = "write_refused";

    public const string WriteRefusedDetail =
        "A fence refused the request's commit: no configuration write took effect, but the handler ran. Retry only an idempotent method, or an action whose other effects are idempotent.";

    public static string CodeOf(Refusal refusal) => refusal switch
    {
        Refusal.NotServing => NotServing,
        Refusal.Fenced => Fenced,
        _ => Behind,
    };

    public static string DetailOf(Refusal refusal) => refusal switch
    {
        Refusal.NotServing => "This replica is not serving; nothing ran. Retry.",
        Refusal.Fenced => "This replica is fenced; nothing ran. Retry.",
        _ => "This replica has not caught up with the configuration; the request did not run. Retry.",
    };
}

internal sealed class ErrorEnvelope(IOptions<ReeveClusterHttpOptions> options)
{
    public async Task WriteRefusalAsync(HttpContext context, Refusal refusal)
    {
        await WriteReplaceAsync(context, new ClusterHttpError(StatusCodes.Status503ServiceUnavailable, RefusalResponse.CodeOf(refusal), RefusalResponse.DetailOf(refusal))).ConfigureAwait(false);
    }

    public async Task WriteReplaceAsync(HttpContext context, ClusterHttpError error)
    {
        var response = context.Response;
        if (context.Items.Remove(PreparedErrorKey))
        {
            ClearBody(response);
            ResetEnvelopeHeaders(response, options.Value.HeaderPrefix, clearOwnedHeaders: false);
        }
        else
        {
            ResetResponse(context, options.Value.HeaderPrefix);
        }

        response.StatusCode = error.StatusCode;
        if (error.StatusCode == StatusCodes.Status503ServiceUnavailable)
        {
            response.Headers.RetryAfter = "1";
        }

        if (options.Value.WriteError is { } write)
        {
            await write(context, error).ConfigureAwait(false);
            return;
        }

        var problem = new ProblemDetails { Status = error.StatusCode, Title = error.ErrorCode, Detail = error.Detail };
        problem.Extensions["errorCode"] = error.ErrorCode;
        foreach (var (name, value) in error.Extensions)
        {
            problem.Extensions[name] = value;
        }

        await response.WriteAsJsonAsync(problem, ClusterHttpJson.Info<ProblemDetails>(context), contentType: "application/problem+json", context.RequestAborted).ConfigureAwait(false);
    }

    internal static readonly object PreparedErrorKey = new();

    internal static void ResetResponse(HttpContext context, string headerPrefix)
    {
        var response = context.Response;
        if (response.HasStarted)
        {
            throw new InvalidOperationException("The response has already started.");
        }

        response.Clear();
        ResponseHeaderSnapshot.Restore(context);
        ResetEnvelopeHeaders(response, headerPrefix, clearOwnedHeaders: true);
    }

    private static void ResetEnvelopeHeaders(HttpResponse response, string headerPrefix, bool clearOwnedHeaders)
    {
        response.Headers.Remove("Content-Type");
        response.Headers.Remove("Content-Length");
        response.Headers.Remove("Retry-After");
        if (clearOwnedHeaders)
        {
            foreach (var name in response.Headers.Keys.Where(name => name.StartsWith(headerPrefix, StringComparison.OrdinalIgnoreCase)).ToArray())
            {
                response.Headers.Remove(name);
            }
        }
        ClearBody(response);
    }

    private static void ClearBody(HttpResponse response)
    {
        if (response.Body.CanSeek)
        {
            response.Body.SetLength(0);
            response.Body.Position = 0;
        }
    }
}
