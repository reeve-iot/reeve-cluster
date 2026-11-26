using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http.Timeouts;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;

namespace Reeve.Cluster.AspNetCore;

internal sealed partial class HttpAnswers(
    IOptions<ReeveClusterHttpOptions> options, ErrorEnvelope envelope, AcknowledgementStep step, ILogger<HttpAnswers> logger)
{
    public enum AnswerKind { Replace, Amend }
    public const string Unconfirmed = "write_unconfirmed";

    private const string UnconfirmedDetail =
        "The write is committed on this replica and its acknowledgement was not given. Read before you resend: a failover can still remove a commit whose wait for the standby ended early.";

    public ClusterHttpError? Apply(HttpContext context, Verdict verdict, int? handlerStatus, AnswerKind kind)
    {
        var response = context.Response;
        var success = handlerStatus is null or < StatusCodes.Status400BadRequest;
        var prefix = options.Value;
        switch (verdict.Kind)
        {
            case VerdictKind.Acknowledged:
                response.Headers[prefix.HeaderName("Skipped")] = string.Join(", ", verdict.Skipped);
                return null;

            case VerdictKind.AtCommit:
                SetSeq(response, verdict);
                response.Headers[prefix.HeaderName("Replication")] = "at-commit";
                return null;

            case VerdictKind.WriteRefused when success:
                HttpDiagnostics.WriteRefused.Add(1);
                if (kind == AnswerKind.Replace)
                    PrepareErrorResponse(context);
                Replace(response);
                return new ClusterHttpError(StatusCodes.Status503ServiceUnavailable, RefusalResponse.WriteRefused, RefusalResponse.WriteRefusedDetail);

            case VerdictKind.Unconfirmed:
                if (success)
                {
                    if (kind == AnswerKind.Replace)
                    {
                        PrepareErrorResponse(context);
                    }
                }
                SetSeq(response, verdict);
                if (success)
                {
                    HttpDiagnostics.WriteUnconfirmed.Add(1);
                    Replace(response);
                    return new ClusterHttpError(StatusCodes.Status202Accepted, Unconfirmed, UnconfirmedDetail);
                }

                response.Headers[prefix.HeaderName("Replication")] = "unconfirmed";
                return null;

            default:
                return null;
        }
    }

    public Task WriteReplaceAsync(HttpContext context, ClusterHttpError error) => envelope.WriteReplaceAsync(context, error);

    public static bool TryClaim(HttpContext context) => context.Items.TryAdd(HandledKey, true);

    public static void DisableRequestTimeout(HttpContext context)
    {
        try
        {
            context.Features.Get<IHttpRequestTimeoutFeature>()?.DisableTimeout();
        }
        catch (Exception exception) when (exception is ObjectDisposedException or InvalidOperationException)
        {
        }
    }

    public async ValueTask<bool> AnswerRefusedAsync(HttpContext context, IClusterScope? scope, Exception exception)
    {
        if (scope is not { Kind: ClusterScopeKind.ClientRequest, CommitRefused: true } || context.Response.HasStarted)
        {
            return false;
        }

        LogRefusedException(logger, context.Features.Get<Microsoft.AspNetCore.Diagnostics.IExceptionHandlerPathFeature>()?.Path ?? context.Request.Path.Value, exception);
        var verdict = FencedRequestRule.Plan(TrackerView.Of(scope)).Settled!.Value;
        context.Items[HandledKey] = true;
        if (Apply(context, verdict, handlerStatus: null, kind: AnswerKind.Replace) is { } error)
        {
            await envelope.WriteReplaceAsync(context, error).ConfigureAwait(false);
        }

        return true;
    }

    public async Task BackstopAsync(HttpContext context, IClusterScope scope)
    {
        if (!TryClaim(context))
        {
            return;
        }

        ApplyAtStart(context, await step.RunAsync(scope, context.RequestAborted, () => DisableRequestTimeout(context)).ConfigureAwait(false), now: true);
    }

    public void ApplyAtStart(HttpContext context, Verdict verdict, bool now)
    {
        if (verdict.Kind == VerdictKind.Pass)
        {
            return;
        }

        if (!now)
        {
            context.Response.OnStarting(static state => { var (answers, http, v) = ((HttpAnswers, HttpContext, Verdict))state; answers.ApplyAtStart(http, v, now: true); return Task.CompletedTask; }, (this, context, verdict));
            return;
        }

        if (Apply(context, verdict, context.Response.StatusCode, kind: AnswerKind.Amend) is { } error)
        {
            context.Response.StatusCode = error.StatusCode;
            if (error.StatusCode == StatusCodes.Status503ServiceUnavailable)
            {
                context.Response.Headers.RetryAfter = "1";
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The request to {Path} failed after fence step 2 refused a commit of it; answered by the fenced-request rule instead.")]
    private static partial void LogRefusedException(ILogger logger, string? path, Exception exception);

    private void SetSeq(HttpResponse response, Verdict verdict)
    {
        if (verdict.Seq is { } seq)
        {
            response.Headers[options.Value.HeaderName("Seq")] = seq.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    private void PrepareErrorResponse(HttpContext context)
    {
        ErrorEnvelope.ResetResponse(context, options.Value.HeaderPrefix);
        context.Items[ErrorEnvelope.PreparedErrorKey] = true;
    }

    private static void Replace(HttpResponse response)
    {
        response.Headers.Remove(HeaderNames.Location);
        response.Headers.Remove(HeaderNames.ContentLocation);
        response.Headers.Remove(HeaderNames.ContentLength);
        response.Headers.Remove(HeaderNames.ETag);
    }

    public static readonly object HandledKey = new();
}

internal sealed class AcknowledgementResultFilter(AcknowledgementStep step, HttpAnswers answers, IClusterScopeAccessor scopes) : IAsyncAlwaysRunResultFilter
{
    public async Task OnResultExecutionAsync(ResultExecutingContext context, ResultExecutionDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        var http = context.HttpContext;
        if (scopes.Current is { } scope && HttpAnswers.TryClaim(http))
        {
            var verdict = await step.RunAsync(scope, http.RequestAborted, () => HttpAnswers.DisableRequestTimeout(http)).ConfigureAwait(false);
            if (HandlerStatus.Of(context.Result, http.Response.StatusCode) is { } status)
            {
                if (answers.Apply(http, verdict, status, kind: HttpAnswers.AnswerKind.Replace) is { } error)
                {
                    context.Result = new ErrorResult(answers, error);
                }
            }
            else
            {
                answers.ApplyAtStart(http, verdict, now: false);
            }
        }

        await next().ConfigureAwait(false);
    }

    private sealed class ErrorResult(HttpAnswers answers, ClusterHttpError error) : IActionResult
    {
        public Task ExecuteResultAsync(ActionContext context) => answers.WriteReplaceAsync(context.HttpContext, error);
    }
}

internal sealed class AcknowledgementEndpointFilter : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        var http = context.HttpContext;
        var result = await next(context).ConfigureAwait(false);
        var services = http.RequestServices;
        if (services.GetRequiredService<IClusterScopeAccessor>().Current is not { } scope || !HttpAnswers.TryClaim(http))
        {
            return result;
        }

        var answers = services.GetRequiredService<HttpAnswers>();
        var verdict = await services.GetRequiredService<AcknowledgementStep>().RunAsync(
            scope, http.RequestAborted, () => HttpAnswers.DisableRequestTimeout(http)).ConfigureAwait(false);
        if (HandlerStatus.Of(result, http.Response.StatusCode) is not { } status)
        {
            answers.ApplyAtStart(http, verdict, now: false);
            return result;
        }

        return answers.Apply(http, verdict, status, kind: HttpAnswers.AnswerKind.Replace) is { } error ? new ErrorHttpResult(answers, error) : result;
    }

    private sealed class ErrorHttpResult(HttpAnswers answers, ClusterHttpError error) : IResult
    {
        public Task ExecuteAsync(HttpContext httpContext) => answers.WriteReplaceAsync(httpContext, error);
    }
}

internal static class HandlerStatus
{
    public static int? Of(IActionResult? result, int responseStatus) =>
        result is IStatusCodeActionResult status ? status.StatusCode ?? responseStatus : null;

    public static int? Of(object? result, int responseStatus)
    {
        while (result is INestedHttpResult nested)
        {
            result = nested.Result;
        }

        return result is IStatusCodeHttpResult status ? status.StatusCode ?? responseStatus : null;
    }
}

internal sealed class RefusedRequestExceptionHandler(IClusterScopeAccessor scopes, HttpAnswers answers) : Microsoft.AspNetCore.Diagnostics.IExceptionHandler
{
    public ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        return answers.AnswerRefusedAsync(httpContext, scopes.Current, exception);
    }
}
