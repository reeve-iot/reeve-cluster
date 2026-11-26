using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Timeouts;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class AcknowledgementFilterTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly HttpRig rig = HttpRig.Create();

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    private async Task ServeAsync()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
    }

    private Task<HttpResponseMessage> PostAsync(string path) => rig.Client.PostAsync(path, null);

    private static async Task<(string? Code, string? Detail)> ProblemAsync(HttpResponseMessage response)
    {
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;
        return (body.GetProperty("errorCode").GetString(), body.TryGetProperty("detail", out var d) ? d.GetString() : null);
    }

    private static string? Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? string.Join("|", values) : null;

    private static void AssertNoReeveHeaders(HttpResponseMessage response) =>
        Assert.DoesNotContain(response.Headers, h => h.Key.StartsWith("Reeve-", StringComparison.Ordinal));

    [Theory]
    [InlineData("/mvc/write")]
    [InlineData("/min/write")]
    public async Task A_request_with_two_commits_is_awaited_once_by_its_highest_seq_and_an_acknowledgement_lets_it_through(string path)
    {
        await ServeAsync();

        var response = await PostAsync(path + "?seqs=3,9,5&status=200");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal([9L], rig.Ack.Waits);
        AssertNoReeveHeaders(response);
    }

    [Fact]
    public async Task A_request_that_wrote_nothing_waits_for_nothing()
    {
        await ServeAsync();

        var response = await PostAsync("/mvc/write");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Empty(rig.Ack.Waits);
        AssertNoReeveHeaders(response);
    }

    [Theory]
    [InlineData("/min/write?seqs=4")]
    [InlineData("/mvc/write?seqs=4")]
    public async Task An_acknowledged_write_disables_the_request_timeout_before_waiting(string path)
    {
        var feature = new RecordingRequestTimeoutFeature();
        await using var timeoutRig = HttpRig.Create(beforeFence: app => app.Use(next => context =>
        {
            context.Features.Set<IHttpRequestTimeoutFeature>(feature);
            return next(context);
        }));
        await timeoutRig.PublishAsync(5);
        timeoutRig.ToServing();

        var response = await timeoutRig.Client.PostAsync(path, null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(feature.Disabled);
        Assert.Equal(1, feature.DisableCalls);
    }

    [Fact]
    public async Task A_settled_write_and_a_read_keep_the_request_timeout()
    {
        var feature = new RecordingRequestTimeoutFeature();
        await using var timeoutRig = HttpRig.Create(beforeFence: app => app.Use(next => context =>
        {
            context.Features.Set<IHttpRequestTimeoutFeature>(feature);
            return next(context);
        }));
        await timeoutRig.PublishAsync(5);
        timeoutRig.ToServing();

        var write = await timeoutRig.Client.PostAsync("/min/write?seqs=4&optout", null);
        var read = await timeoutRig.Client.GetAsync("/min/plain");

        Assert.Equal(HttpStatusCode.OK, write.StatusCode);
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal(0, feature.DisableCalls);
    }

    private sealed class RecordingRequestTimeoutFeature : IHttpRequestTimeoutFeature
    {
        public TimeSpan? RequestTimeout { get; set; }
        public CancellationToken RequestTimeoutToken => CancellationToken.None;
        public bool HasStarted { get; private set; }
        public bool Disabled { get; private set; }
        public int DisableCalls { get; private set; }

        public void DisableTimeout()
        {
            Disabled = true;
            DisableCalls++;
            HasStarted = true;
        }
    }

    [Fact]
    public async Task An_acknowledgement_over_skipped_types_says_so_in_a_header()
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Acknowledged(["a", "b"]);

        var response = await PostAsync("/mvc/write?seqs=4");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("a, b", Header(response, "Reeve-Skipped"));
        Assert.Null(Header(response, "Reeve-Seq"));
    }

    [Theory]
    [InlineData("/mvc/write", UnconfirmedReason.Timeout)]
    [InlineData("/mvc/write", UnconfirmedReason.Fenced)]
    [InlineData("/mvc/write", UnconfirmedReason.Draining)]
    [InlineData("/min/write", UnconfirmedReason.Timeout)]
    public async Task An_unconfirmed_write_turns_a_2xx_into_202_write_unconfirmed_with_the_seq_and_a_body_that_says_to_read_first(string path, UnconfirmedReason reason)
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(reason);

        var response = await PostAsync(path + "?seqs=4,7");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Null(response.Headers.RetryAfter);
        Assert.Equal("7", Header(response, "Reeve-Seq"));
        var (code, detail) = await ProblemAsync(response);
        Assert.Equal("write_unconfirmed", code);
        Assert.Contains("Read before you resend", detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_202_loses_the_location_the_action_would_have_returned()
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var response = await PostAsync("/mvc/write-created?seqs=4");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Null(response.Headers.Location);
    }

    [Fact]
    public async Task A_non_2xx_response_after_an_unconfirmed_commit_keeps_its_status_and_gains_the_seq_and_the_unconfirmed_header()
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var response = await PostAsync("/mvc/write?seqs=4&status=409");

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("4", Header(response, "Reeve-Seq"));
        Assert.Equal("unconfirmed", Header(response, "Reeve-Replication"));
        Assert.Equal("body", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task A_request_that_opted_out_is_let_through_at_once_with_its_own_status_the_seq_and_at_commit()
    {
        await ServeAsync();

        var response = await PostAsync("/mvc/write?seqs=4&optout&status=201");

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal("4", Header(response, "Reeve-Seq"));
        Assert.Equal("at-commit", Header(response, "Reeve-Replication"));
        Assert.Empty(rig.Ack.Waits);
    }

    [Theory]
    [InlineData("/mvc/write")]
    [InlineData("/min/write")]
    public async Task A_handler_that_swallowed_the_refusal_still_answers_503_write_refused_when_nothing_was_appended(string path)
    {
        await ServeAsync();

        var response = await PostAsync(path + "?refuse&status=200");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(1, response.Headers.RetryAfter?.Delta?.TotalSeconds);
        var (code, detail) = await ProblemAsync(response);
        Assert.Equal("write_refused", code);
        Assert.Contains("the handler ran", detail, StringComparison.Ordinal);
        Assert.Empty(rig.Ack.Waits);
        AssertNoReeveHeaders(response);
    }

    [Theory]
    [InlineData("/mvc/write")]
    [InlineData("/min/write")]
    public async Task A_write_refused_at_admission_answers_503_replica_fenced(string path)
    {
        await ServeAsync();
        rig.ToFenced();

        var response = await PostAsync(path + "?refuse&status=200");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(1, response.Headers.RetryAfter?.Delta?.TotalSeconds);
        Assert.Equal("replica_fenced", (await ProblemAsync(response)).Code);
    }

    [Theory]
    [InlineData("/mvc/write?refuse&status=302")]
    [InlineData("/mvc/write?refuse&status=204")]
    public async Task Any_success_status_of_a_refused_request_is_replaced_not_only_a_200(string path)
    {
        await ServeAsync();

        var response = await PostAsync(path);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task A_refused_request_whose_handler_answered_an_error_keeps_the_handlers_status()
    {
        await ServeAsync();

        var response = await PostAsync("/mvc/write?refuse&status=409");

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Theory]
    [InlineData("/mvc/write")]
    [InlineData("/min/write")]
    public async Task A_request_fenced_between_two_commits_answers_202_without_waiting(string path)
    {
        await ServeAsync();

        var response = await PostAsync(path + "?seqs=3&refuse");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("3", Header(response, "Reeve-Seq"));
        Assert.Equal("write_unconfirmed", (await ProblemAsync(response)).Code);
        Assert.Empty(rig.Ack.Waits);
    }

    [Fact]
    public async Task An_opted_out_request_fenced_between_commits_answers_202()
    {
        await ServeAsync();

        var response = await PostAsync("/mvc/write?seqs=3&refuse&optout");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Null(Header(response, "Reeve-Replication"));
    }

    [Fact]
    public async Task An_exception_after_a_refused_commit_is_answered_by_the_rule_and_not_as_a_500()
    {
        await ServeAsync();

        var fenced = await PostAsync("/mvc/write-throw?refuse");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, fenced.StatusCode);
        Assert.Equal("write_refused", (await ProblemAsync(fenced)).Code);
        Assert.Equal(1, fenced.Headers.RetryAfter?.Delta?.TotalSeconds);
        Assert.False(fenced.Headers.Contains("X-Handler"), "what the handler set before it failed is not sent");
        var unconfirmed = await PostAsync("/mvc/write-throw?seqs=6&refuse");
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal("6", Header(unconfirmed, "Reeve-Seq"));
    }

    [Fact]
    public async Task An_exception_without_a_refused_commit_is_not_the_rules()
    {
        await ServeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => PostAsync("/mvc/write-throw?seqs=6"));
    }

    [Fact]
    public async Task An_exempt_request_records_no_seq_and_is_answered_as_it_is()
    {
        await ServeAsync();

        var response = await PostAsync("/mvc/write-exempt?seqs=3&status=200");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Empty(rig.Ack.Waits);
        AssertNoReeveHeaders(response);
    }

    [Fact]
    public async Task The_filter_does_not_wait_inside_an_open_transaction_and_answers_as_unconfirmed()
    {
        await ServeAsync();

        var withSeq = await PostAsync("/mvc/write?seqs=4&opentx");
        var noSeq = await PostAsync("/mvc/write?opentx");

        Assert.Equal(HttpStatusCode.Accepted, withSeq.StatusCode);
        Assert.Equal("4", Header(withSeq, "Reeve-Seq"));
        Assert.Equal(HttpStatusCode.Accepted, noSeq.StatusCode);
        Assert.Null(Header(noSeq, "Reeve-Seq"));
        Assert.Empty(rig.Ack.Waits);
    }

    [Fact]
    public async Task The_wait_ends_before_the_response_starts_and_the_two_fault_points_are_reached_in_order()
    {
        await ServeAsync();
        rig.Ack.Hold();
        var response = rig.Client.SendAsync(new HttpRequestMessage(HttpMethod.Post, "/mvc/write?seqs=4"), HttpCompletionOption.ResponseHeadersRead);

        await rig.Ack.Entered.Task.WaitAsync(Wait);

        await Task.Delay(100);
        Assert.False(response.IsCompleted, "the response does not start while the wait is held");

        rig.Ack.Release();
        using var done = await response.WaitAsync(Wait);
        Assert.Equal(HttpStatusCode.OK, done.StatusCode);
        var reached = rig.Points.Reached.Where(p => p.StartsWith("http.before-", StringComparison.Ordinal)).ToArray();
        Assert.Equal(["http.before-ack-await", "http.before-response-starts"], reached);
    }

    [Fact]
    public async Task The_error_envelope_hook_writes_the_body_of_every_error_answer_and_the_header_prefix_is_the_applications()
    {
        await using var hosted = HttpRig.Create(
            configureHttp: o =>
            {
                o.HeaderPrefix = "X-Cluster-";
                o.WriteError = (context, error) =>
                    context.Response.WriteAsync($"envelope:{error.StatusCode}:{error.ErrorCode}", CancellationToken.None);
            });
        await hosted.PublishAsync(5);
        hosted.ToServing();
        hosted.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var unconfirmed = await hosted.Client.PostAsync("/mvc/write?seqs=4", null);
        hosted.ToFenced();
        var refused = await hosted.Client.PostAsync("/mvc/write", null);

        Assert.Equal("envelope:202:write_unconfirmed", await unconfirmed.Content.ReadAsStringAsync());
        Assert.Equal("4", Header(unconfirmed, "X-Cluster-Seq"));
        Assert.Equal("envelope:503:replica_fenced", await refused.Content.ReadAsStringAsync());
        Assert.Equal(1, refused.Headers.RetryAfter?.Delta?.TotalSeconds);
    }

    [Fact]
    public async Task A_minimal_endpoint_without_the_filter_gets_the_rules_status_and_headers_from_the_backstop()
    {
        await ServeAsync();

        var swallowed = await PostAsync("/min/write-bare?refuse&status=200");
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);
        var unconfirmed = await PostAsync("/min/write-bare?seqs=3");
        var conflict = await PostAsync("/min/write-bare?seqs=4&status=409");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, swallowed.StatusCode);
        Assert.Equal(1, swallowed.Headers.RetryAfter?.Delta?.TotalSeconds);
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal("3", Header(unconfirmed, "Reeve-Seq"));
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.Equal("4", Header(conflict, "Reeve-Seq"));
        Assert.Equal("unconfirmed", Header(conflict, "Reeve-Replication"));
        Assert.Equal([3L, 4L], rig.Ack.Waits);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task The_backstop_awaits_the_write_as_the_response_starts_and_keeps_the_handlers_body(bool kestrel)
    {
        await using var host = kestrel ? HttpRig.CreateKestrel() : HttpRig.Create();
        await host.PublishAsync(5);
        host.ToServing();
        host.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var unconfirmed = await host.Client.PostAsync("/min/write-bare-body?seqs=3,5", null);
        var refused = await host.Client.PostAsync("/min/write-bare-body?refuse", null);

        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal("5", Header(unconfirmed, "Reeve-Seq"));
        Assert.Equal("handler body", await unconfirmed.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Equal([5L], host.Ack.Waits);
    }

    [Fact]
    public async Task A_minimal_union_result_keeps_its_non_success_status_after_an_unconfirmed_commit_and_after_a_swallowed_refusal()
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var unconfirmed = await PostAsync("/min/union?seqs=4&status=409");
        var refused = await PostAsync("/min/union?refuse&status=409");
        var ok = await PostAsync("/min/union?seqs=4");

        Assert.Equal(HttpStatusCode.Conflict, unconfirmed.StatusCode);
        Assert.Equal("unconfirmed", Header(unconfirmed, "Reeve-Replication"));
        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, ok.StatusCode);
    }

    [Theory]
    [InlineData("/mvc/write-empty")]
    [InlineData("/mvc/write-iresult")]
    public async Task A_result_that_names_no_status_is_judged_by_the_status_the_response_has_when_it_starts(string path)
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var conflict = await PostAsync(path + "?seqs=4&status=409");
        var ok = await PostAsync(path + "?seqs=4&status=200");
        var refused = await PostAsync(path + "?refuse&status=200");

        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.Equal("4", Header(conflict, "Reeve-Seq"));
        Assert.Equal("unconfirmed", Header(conflict, "Reeve-Replication"));
        Assert.Equal(HttpStatusCode.Accepted, ok.StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
    }

    [Fact]
    public async Task A_redirect_after_an_unconfirmed_commit_becomes_202()
    {
        await ServeAsync();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var response = await PostAsync("/mvc/write-redirect?seqs=4");

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Null(response.Headers.Location);
    }

    [Fact]
    public async Task AcknowledgeWrites_on_a_group_and_on_its_endpoint_waits_once()
    {
        await ServeAsync();

        var response = await PostAsync("/min/group/write?seqs=4");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal([4L], rig.Ack.Waits);
    }

    [Fact]
    public async Task An_exception_handler_inside_UseReeveCluster_does_not_turn_the_refusals_exception_into_a_500()
    {
        await using var inside = HttpRig.Create(afterFence: app => app.UseExceptionHandler(o => o.Run(context =>
        {
            context.Response.StatusCode = 500;
            return context.Response.WriteAsync("error page");
        })));
        await inside.PublishAsync(5);
        inside.ToServing();

        var fenced = await inside.Client.PostAsync("/mvc/write-throw?refuse", null);
        var unconfirmed = await inside.Client.PostAsync("/mvc/write-throw?seqs=6&refuse", null);
        var failure = await inside.Client.PostAsync("/mvc/write-throw?seqs=6", null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, fenced.StatusCode);
        Assert.Equal("write_refused", (await ProblemAsync(fenced)).Code);
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal("6", Header(unconfirmed, "Reeve-Seq"));
        Assert.Equal(HttpStatusCode.InternalServerError, failure.StatusCode);
        Assert.Equal("error page", await failure.Content.ReadAsStringAsync());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task An_application_exception_handler_does_not_see_a_refused_commits_exception_and_sees_every_other(bool registeredBefore)
    {
        var handler = new RecordingExceptionHandler();
        void Register(IServiceCollection services) => services.AddSingleton<IExceptionHandler>(handler);
        await using var hosted = HttpRig.Create(
            before: registeredBefore ? Register : null,
            configure: registeredBefore ? null : Register,
            afterFence: app => app.UseExceptionHandler(o => o.Run(context => context.Response.WriteAsync("error page"))));
        await hosted.PublishAsync(5);
        hosted.ToServing();

        var refused = await hosted.Client.PostAsync("/mvc/write-throw?refuse", null);
        var unconfirmed = await hosted.Client.PostAsync("/mvc/write-throw?seqs=6&refuse", null);
        var failure = await hosted.Client.PostAsync("/mvc/write-throw", null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Equal("write_refused", (await ProblemAsync(refused)).Code);
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal(HttpStatusCode.InternalServerError, failure.StatusCode);
        Assert.Equal("application handler", await failure.Content.ReadAsStringAsync());
        Assert.Equal(["/mvc/write-throw"], handler.Seen);
    }

    private sealed class RecordingExceptionHandler : IExceptionHandler
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> seen = new();

        public string[] Seen => [.. seen];

        public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
        {
            seen.Enqueue(httpContext.Request.Path.Value!);
            httpContext.Response.StatusCode = StatusCodes.Status500InternalServerError;
            await httpContext.Response.WriteAsync("application handler", cancellationToken);
            return true;
        }
    }
}
