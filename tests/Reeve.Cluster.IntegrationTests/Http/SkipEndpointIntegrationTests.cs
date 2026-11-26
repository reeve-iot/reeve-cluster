using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Int.Log;

namespace Int.Http;

public sealed class SkipEndpointIntegrationTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public SkipEndpointIntegrationTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            await item.DisposeAsync();
        }
    }

    [Fact]
    public async Task Skip_endpoint_an_open_above_acked_seq_answers_201_with_the_skip_id_and_stores_the_request()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var from = await host.Rig.WriteAsync(("a", "k"));

        var response = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = "a", fromSeq = from, reason = "poison", openedBy = "ops" });

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var id = (await JsonAsync(response)).GetProperty("skipId").GetInt64();
        Assert.Equal(
            $"a|{from}|poison|ops|",
            await host.Rig.Log.ScalarAsync<string>($"SELECT concat_ws('|', aggregate_type, from_seq, reason, opened_by, coalesce(until_seq::text, '')) FROM reeve_cluster.change_skip WHERE id = {id}"));
    }

    [Fact]
    public async Task I30_an_open_at_or_below_acked_seq_answers_409_skip_covers_acknowledged_with_ackedSeq_and_writes_nothing()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        await host.Rig.WriteAsync(("a", "k1"));
        var acked = await host.Rig.WriteAsync(("a", "k2"));
        await host.Rig.ExecuteAsync($"UPDATE reeve_cluster.change_ack SET acked_seq = {acked}");

        foreach (var from in new[] { acked, acked - 1 })
        {
            var response = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = "b", fromSeq = from, reason = "poison", openedBy = "ops" });

            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            var body = await JsonAsync(response);
            Assert.Equal("skip_covers_acknowledged", body.GetProperty("errorCode").GetString());
            Assert.Equal(acked, body.GetProperty("ackedSeq").GetInt64());
        }

        Assert.Equal(0L, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_skip"));
    }

    [Fact]
    public async Task The_409_goes_through_the_applications_error_envelope_with_ackedSeq_among_its_extensions()
    {
        ClusterHttpError? seen = null;
        var host = await HostAsync(ReplicaPhase.Serving, writeError: async (context, error) =>
        {
            seen = error;
            context.Response.ContentType = "application/vnd.app+json";
            await context.Response.WriteAsync($"{{\"code\":\"{error.ErrorCode}\"}}");
        });
        await host.Rig.ExecuteAsync("UPDATE reeve_cluster.change_ack SET acked_seq = 7");

        var response = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = "a", fromSeq = 3, reason = "r", openedBy = "ops" });

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("application/vnd.app+json", response.Content.Headers.ContentType?.MediaType);
        Assert.NotNull(seen);
        Assert.Equal(StatusCodes.Status409Conflict, seen.StatusCode);
        Assert.Equal(ReeveClusterSkipEndpoints.CoversAcknowledged, seen.ErrorCode);
        Assert.Equal(7L, seen.Extensions["ackedSeq"]);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"aggregateType":"a","reason":"r","openedBy":"ops"}""")]
    [InlineData("""{"aggregateType":"a","fromSeq":0,"reason":"r","openedBy":"ops"}""")]
    [InlineData("""{"aggregateType":"","fromSeq":5,"reason":"r","openedBy":"ops"}""")]
    [InlineData("""{"aggregateType":"a","fromSeq":5,"openedBy":"ops"}""")]
    [InlineData("""{"aggregateType":"a","fromSeq":5,"reason":"r"}""")]
    public async Task A_body_the_endpoint_cannot_take_answers_400_skip_request_invalid_and_writes_nothing(string body)
    {
        var host = await HostAsync(ReplicaPhase.Serving);

        var response = await host.Client.PostAsync("/skips", new StringContent(body, System.Text.Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("skip_request_invalid", (await JsonAsync(response)).GetProperty("errorCode").GetString());
        Assert.Equal(0L, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_skip"));
    }

    [Fact]
    public async Task A_close_answers_200_with_untilSeq_the_repair_entry_again_on_a_retry_404_for_an_unknown_id_and_400_without_a_JSON_body()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var from = await host.Rig.WriteAsync(("a", "k"));
        var id = await OpenAsync(host, "a", from);

        var form = await host.Client.PostAsync($"/skips/{id}/close", new StringContent("", System.Text.Encoding.UTF8, "application/x-www-form-urlencoded"));
        Assert.Equal(HttpStatusCode.BadRequest, form.StatusCode);
        Assert.Equal("skip_request_invalid", (await JsonAsync(form)).GetProperty("errorCode").GetString());
        Assert.True(await host.Rig.Log.ScalarAsync<bool>($"SELECT until_seq IS NULL FROM reeve_cluster.change_skip WHERE id = {id}"));

        var closed = await host.Client.PostAsJsonAsync($"/skips/{id}/close", new { closedBy = "ops" });
        Assert.Equal(HttpStatusCode.OK, closed.StatusCode);
        var until = (await JsonAsync(closed)).GetProperty("untilSeq").GetInt64();
        Assert.Equal((await host.Rig.HeadAsync()).Seq, until);
        Assert.Equal("ops", await host.Rig.Log.ScalarAsync<string>($"SELECT closed_by FROM reeve_cluster.change_skip WHERE id = {id}"));

        var again = await host.Client.PostAsJsonAsync($"/skips/{id}/close", new { closedBy = "ops" });
        Assert.Equal(until, (await JsonAsync(again)).GetProperty("untilSeq").GetInt64());

        var unknown = await host.Client.PostAsJsonAsync($"/skips/{id + 100}/close", new { closedBy = "ops" });
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
        Assert.Equal("skip_not_found", (await JsonAsync(unknown)).GetProperty("errorCode").GetString());

        var nobody = await host.Client.PostAsync($"/skips/{id}/close", null);
        Assert.Equal(HttpStatusCode.BadRequest, nobody.StatusCode);
    }

    [Fact]
    public async Task Reaching_it_the_skip_endpoint_answers_within_one_round_trip_on_a_fenced_replica_whose_cursor_is_behind_and_its_writes_commit()
    {
        var host = await HostAsync(ReplicaPhase.Serving);
        var rig = host.Rig;
        var before = rig.Store.Current!.Head;
        var from = await rig.WriteAsync(("a", "poison"));
        Assert.True(from > before, "the log is ahead of the replica's cursor");

        await host.FenceAsync();

        var opened = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = "a", fromSeq = from, reason = "poison", openedBy = "ops" });
        Assert.Equal(HttpStatusCode.Created, opened.StatusCode);

        Assert.DoesNotContain(rig.Events.All, e => e.Action == LogActions.QStart);

        var id = (await JsonAsync(opened)).GetProperty("skipId").GetInt64();
        var closed = await host.Client.PostAsJsonAsync($"/skips/{id}/close", new { closedBy = "ops" });
        Assert.Equal(HttpStatusCode.OK, closed.StatusCode);
        Assert.Equal((await JsonAsync(closed)).GetProperty("untilSeq").GetInt64(), (await rig.HeadAsync()).Seq);
        Assert.Equal(before, rig.Store.Current!.Head);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
    }

    [Fact]
    public async Task On_a_serving_replica_whose_cursor_is_behind_the_skip_endpoint_does_not_wait_on_the_read_index()
    {
        var host = await HostAsync(ReplicaPhase.Serving, readIndexTimeout: TimeSpan.FromSeconds(3));
        var from = await host.Rig.WriteAsync(("a", "poison"));

        var response = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = "a", fromSeq = from, reason = "poison", openedBy = "ops" });

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.DoesNotContain(host.Rig.Events.All, e => e.Action == LogActions.QStart);
        Assert.True(host.Rig.Store.Current!.Head < from, "the replica had not applied the write");
    }

    private static async Task<long> OpenAsync(HttpHostRig host, string type, long from)
    {
        var response = await host.Client.PostAsJsonAsync("/skips", new { aggregateType = type, fromSeq = from, reason = "poison", openedBy = "ops" });
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        return (await JsonAsync(response)).GetProperty("skipId").GetInt64();
    }

    private static async Task<JsonElement> JsonAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

    private async Task<HttpHostRig> HostAsync(
        ReplicaPhase phase, TimeSpan? readIndexTimeout = null, Func<HttpContext, ClusterHttpError, Task>? writeError = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, o => o.ReadIndexTimeout = readIndexTimeout ?? TimeSpan.FromSeconds(10));
        owned.Add(rig);
        await rig.LoadAsync();
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(phase, rig.Gate.Phase);
        var index = new ReadIndex(
            rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), TimeProvider.System,
            NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        var host = new HttpHostRig(rig, index, writeError: writeError);
        owned.Add(host);
        return host;
    }

    private sealed class Disposing(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
