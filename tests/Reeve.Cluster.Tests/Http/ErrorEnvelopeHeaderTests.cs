using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class ErrorEnvelopeHeaderTests
{
    private static Action<IApplicationBuilder> EarlierHeaders => app => app.Use(async (context, next) =>
    {
        context.Response.Headers["X-Content-Type-Options"] = "nosniff";
        context.Response.Headers["Access-Control-Allow-Origin"] = "https://client.example";
        context.Response.Headers["Content-Type"] = "text/plain";
        context.Response.Headers["Content-Length"] = "999";
        context.Response.Headers["Retry-After"] = "99";
        context.Response.Headers["Reeve-Seq"] = "stale";
        await next();
    });

    [Fact]
    public async Task Refusal_write_throws_when_the_response_has_started()
    {
        var context = new DefaultHttpContext();
        context.Features.Set<IHttpResponseFeature>(new StartedResponseFeature());
        var envelope = new ErrorEnvelope(Options.Create(new ReeveClusterHttpOptions()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => envelope.WriteRefusalAsync(context, Refusal.NotServing));
    }

    [Fact]
    public async Task Admission_refusal_preserves_prior_headers_and_resets_envelope_headers()
    {
        await using var rig = HttpRig.Create(beforeFence: EarlierHeaders);

        var response = await rig.GetAsync("/min/plain");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("https://client.example", response.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("1", response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.DoesNotContain(response.Headers, header => header.Key == "Reeve-Seq");
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("replica_not_serving", JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Write_refused_preserves_prior_headers_and_resets_envelope_headers()
    {
        await using var rig = HttpRig.Create(beforeFence: EarlierHeaders);
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.Client.PostAsync("/min/write?refuse&status=200", null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("https://client.example", response.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("1", response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.DoesNotContain(response.Headers, header => header.Key == "Reeve-Seq");
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("write_refused", JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Write_unconfirmed_preserves_prior_headers_and_writes_its_seq()
    {
        await using var rig = HttpRig.Create(beforeFence: EarlierHeaders);
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var response = await rig.Client.PostAsync("/min/write?seqs=6&status=200", null);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("https://client.example", response.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("6", response.Headers.GetValues("Reeve-Seq").Single());
        Assert.Equal("write_unconfirmed", JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Exception_refusal_preserves_prior_headers()
    {
        await using var rig = HttpRig.Create(beforeFence: EarlierHeaders);
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.Client.PostAsync("/mvc/write-throw?refuse", null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("https://client.example", response.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("write_refused", JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Backstop_amends_status_and_seq_without_changing_handler_or_late_middleware_headers()
    {
        await using var rig = HttpRig.Create(afterFence: app => app.Use(async (context, next) =>
        {
            context.Response.Headers["Content-Encoding"] = "br";
            await next();
        }));
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);

        var response = await rig.Client.PostAsync("/min/write-bare-body?seqs=6", null);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal("6", response.Headers.GetValues("Reeve-Seq").Single());
        Assert.Equal("br", response.Content.Headers.ContentEncoding.Single());
        Assert.Equal("text/plain", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("handler body", await response.Content.ReadAsStringAsync());
    }

    private sealed class StartedResponseFeature : IHttpResponseFeature
    {
        public int StatusCode { get; set; } = 200;
        public string? ReasonPhrase { get; set; }
        public IHeaderDictionary Headers { get; set; } = new HeaderDictionary();
        public Stream Body { get; set; } = Stream.Null;
        public bool HasStarted => true;
        public void OnStarting(Func<object, Task> callback, object state) { }
        public void OnCompleted(Func<object, Task> callback, object state) { }
    }
}
