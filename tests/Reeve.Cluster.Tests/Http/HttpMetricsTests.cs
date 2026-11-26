using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Xunit;

namespace Unit.Http;

[Collection(Unit.Diagnostics.MetricsCollection.Name)]
public sealed class HttpMetricsTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Admission_refusals_count_not_serving_and_fenced_reasons_but_not_admitted_requests()
    {
        using var recorder = new Unit.Diagnostics.InstrumentRecorder(Reeve.Cluster.AspNetCore.HttpDiagnostics.AdmissionRefusals);
        await using (var notServing = HttpRig.Create())
        {
            await notServing.PublishAsync(5);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, (await RequestAsync(token => notServing.Client.GetAsync("/min/plain", token))).StatusCode);
        }
        await using (var fenced = HttpRig.Create())
        {
            await fenced.PublishAsync(5);
            fenced.ToServing();
            fenced.ToFenced();
            Assert.Equal(HttpStatusCode.ServiceUnavailable, (await RequestAsync(token => fenced.Client.GetAsync("/min/plain", token))).StatusCode);
        }
        await using (var serving = HttpRig.Create())
        {
            await serving.PublishAsync(5);
            serving.ToServing();
            Assert.Equal(HttpStatusCode.OK, (await RequestAsync(token => serving.Client.GetAsync("/min/plain", token))).StatusCode);
        }

        var results = recorder.Measurements();
        Assert.Equal(2, results.Count);
        Assert.All(results, m => Assert.Equal(1, m.Value));
        Assert.Equal("replica_not_serving", results[0].Tags["reason"]);
        Assert.Equal("replica_fenced", results[1].Tags["reason"]);
    }

    [Fact]
    public async Task Admission_counts_behind_and_hub_connection_and_invocation_refusals()
    {
        using var recorder = new Unit.Diagnostics.InstrumentRecorder(Reeve.Cluster.AspNetCore.HttpDiagnostics.AdmissionRefusals);
        await using (var behind = HttpRig.Create())
        {
            await behind.PublishAsync(3, alsoHead: false);
            behind.Store.Head = 9;
            behind.ToServing();
            var request = RequestAsync(token => behind.Client.GetAsync("/min/plain", token));
            await behind.ExpireReadIndexAsync();
            Assert.Equal(HttpStatusCode.ServiceUnavailable, (await request.WaitAsync(Wait)).StatusCode);
        }
        await using (var connectionRefusal = HttpRig.Create())
        {
            await connectionRefusal.PublishAsync(5);
            await Assert.ThrowsAnyAsync<Exception>(() => connectionRefusal.ConnectHubAsync("/hub/closed").WaitAsync(Wait));
        }
        await using (var invocationRefusal = HttpRig.Create())
        {
            await invocationRefusal.PublishAsync(5);
            invocationRefusal.ToServing();
            await using var hub = await invocationRefusal.ConnectHubAsync("/hub/probe").WaitAsync(Wait);
            invocationRefusal.ToFenced();
            var completion = await hub.InvokeAsync("Plain").WaitAsync(Wait);
            Assert.Contains("replica_fenced", completion, StringComparison.Ordinal);
        }
        var measurements = recorder.Measurements();
        Assert.Equal(3, measurements.Count);
        Assert.Equal(new[] { "replica_behind", "replica_not_serving", "replica_fenced" }, measurements.Select(m => (string)m.Tags["reason"]!).ToArray());
    }

    [Fact]
    public async Task Write_refused_counts_http_answer_and_hub_exception_once_but_not_acknowledged_write()
    {
        using var recorder = new Unit.Diagnostics.InstrumentRecorder(Reeve.Cluster.AspNetCore.HttpDiagnostics.WriteRefused);
        await using var rig = HttpRig.Create(services => services.AddSingleton<Reeve.Cluster.IFenceSink, Reeve.Cluster.AspNetCore.AspNetCoreFenceSink>());
        await rig.PublishAsync(5);
        rig.ToServing();

        var acknowledged = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?seqs=1", null, token));
        Assert.Equal(HttpStatusCode.OK, acknowledged.StatusCode);
        Assert.Empty(recorder.Measurements());

        var refused = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?refuse", null, token));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Single(recorder.Measurements());
        Assert.Equal(1, Assert.Single(recorder.Measurements()).Value);

        var failedHandler = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?refuse&status=500", null, token));
        Assert.Equal(HttpStatusCode.InternalServerError, failedHandler.StatusCode);
        Assert.Single(recorder.Measurements());

        var backstop = await RequestAsync(token => rig.Client.PostAsync("/mvc/write-throw?refuse", null, token));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, backstop.StatusCode);
        Assert.Equal(2, recorder.Measurements().Count);

        await using var hub = await rig.ConnectHubAsync("/hub/write").WaitAsync(Wait);
        var completion = await hub.InvokeAsync("Write", "refuse").WaitAsync(Wait);
        Assert.Contains("write_refused", completion, StringComparison.Ordinal);
        Assert.Equal(3, recorder.Measurements().Count);
        Assert.All(recorder.Measurements(), m => Assert.Equal(1, m.Value));
    }

    [Fact]
    public async Task Write_unconfirmed_counts_the_202_and_the_hub_exception_but_not_an_acknowledged_write_or_a_failed_handler()
    {
        using var recorder = new Unit.Diagnostics.InstrumentRecorder(Reeve.Cluster.AspNetCore.HttpDiagnostics.WriteUnconfirmed);
        await using var rig = HttpRig.Create(services => services.AddSingleton<Reeve.Cluster.IFenceSink, Reeve.Cluster.AspNetCore.AspNetCoreFenceSink>());
        await rig.PublishAsync(5);
        rig.ToServing();

        var acknowledged = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?seqs=1", null, token));
        Assert.Equal(HttpStatusCode.OK, acknowledged.StatusCode);
        Assert.Empty(recorder.Measurements());

        rig.Ack.Outcome = _ => new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout);
        var unconfirmed = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?seqs=2", null, token));
        Assert.Equal(HttpStatusCode.Accepted, unconfirmed.StatusCode);
        Assert.Equal(1, Assert.Single(recorder.Measurements()).Value);

        var failedHandler = await RequestAsync(token => rig.Client.PostAsync("/mvc/write?seqs=3&status=409", null, token));
        Assert.Equal(HttpStatusCode.Conflict, failedHandler.StatusCode);
        Assert.Single(recorder.Measurements());

        await using var hub = await rig.ConnectHubAsync("/hub/write").WaitAsync(Wait);
        var completion = await hub.InvokeAsync("Write", "seqs=4").WaitAsync(Wait);
        Assert.Contains("write_unconfirmed", completion, StringComparison.Ordinal);
        Assert.Equal(2, recorder.Measurements().Count);
        Assert.All(recorder.Measurements(), m => Assert.Equal(1, m.Value));
    }

    [Fact]
    public void Drain_awaited_requests_reports_the_requests_running_at_step_1_that_have_not_ended()
    {
        using var recorder = new Unit.Diagnostics.InstrumentRecorder(Reeve.Cluster.AspNetCore.HttpDiagnostics.DrainAwaitedRequests);
        var tracker = new Reeve.Cluster.AspNetCore.RequestTracker();
        var tickets = Enumerable.Range(0, 11).Select(_ => tracker.Enter()).ToList();
        Assert.DoesNotContain(11, Awaited(recorder));

        Assert.Equal(11, tracker.CloseAtDrain());
        var after = tracker.Enter();
        Assert.Contains(11, Awaited(recorder));

        tickets[0].Dispose();
        Assert.Contains(10, Awaited(recorder));
        Assert.DoesNotContain(11, Awaited(recorder));

        after.Dispose();
        Assert.Contains(10, Awaited(recorder));
        foreach (var ticket in tickets.Skip(1))
            ticket.Dispose();
        Assert.Contains(0, Awaited(recorder));
        GC.KeepAlive(tracker);
    }

    private static List<int> Awaited(Unit.Diagnostics.InstrumentRecorder recorder)
    {
        var before = recorder.Measurements().Count;
        recorder.RecordObservable();
        return [.. recorder.Measurements().Skip(before).Select(m => (int)m.Value)];
    }

    private static async Task<HttpResponseMessage> RequestAsync(Func<CancellationToken, Task<HttpResponseMessage>> send)
    {
        using var bound = new CancellationTokenSource(Wait);
        return await send(bound.Token).WaitAsync(Wait);
    }
}
