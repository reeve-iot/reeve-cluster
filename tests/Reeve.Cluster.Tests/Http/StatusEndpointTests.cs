using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.Status;
using Unit.Status;

namespace Unit.Http;

public sealed class StatusEndpointTests : IAsyncDisposable
{
    private readonly FakeStatusStore store = new();
    private readonly HttpRig rig;

    public StatusEndpointTests()
    {
        store.OnRead = () => Task.FromResult(Read());
        rig = HttpRig.Create(
            configure: services => services.AddSingleton<IStatusStore>(store),
            afterFence: app => ((IEndpointRouteBuilder)app).MapReeveClusterStatus("/status"));
    }

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    private static ClusterStatusRead Read()
    {
        using var phase = JsonDocument.Parse("""{"phase":"Fenced","epoch":7}""");
        return new ClusterStatusRead
        {
            ReadAt = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
            Members =
            [
                new ClusterMemberStatus
                {
                    ReplicaId = "r1",
                    Epoch = 7,
                    State = "fenced",
                    LeaseExpiresAt = new DateTimeOffset(2026, 1, 2, 3, 4, 0, TimeSpan.Zero),
                    LeaseValid = false,
                    AppliedSeq = 12,
                    ServingSince = null,
                    Version = "1.0.0",
                    Protocol = 3,
                    Sections = [new ClusterStatusSection { Name = "phase", Values = phase.RootElement.Clone(), Age = TimeSpan.FromSeconds(1.5) }],
                    Fences = [new ClusterFenceRecord { Epoch = 6, Trigger = "applier-stall", FencedAt = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero), Span = TimeSpan.FromSeconds(40) }],
                },
            ],
        };
    }

    [Theory]
    [InlineData("Loading")]
    [InlineData("Serving")]
    [InlineData("Draining")]
    [InlineData("Fenced")]
    public async Task The_status_endpoint_is_exempt_and_marked_for_a_fenced_replica(string phase)
    {
        await rig.PublishAsync(5);
        switch (phase)
        {
            case "Loading": rig.ToLoading(); break;
            case "Serving": rig.ToServing(); break;
            case "Draining": rig.ToServing(); rig.ToDraining(); break;
            default: rig.ToServing(); rig.ToFenced(); break;
        }

        var response = await rig.GetAsync("/status");
        var text = await response.Content.ReadAsStringAsync();
        Assert.True(response.StatusCode == HttpStatusCode.OK, text);
        var body = JsonDocument.Parse(text).RootElement;
        var member = body.GetProperty("members")[0];
        Assert.Equal(("r1", 7, "fenced", false), (member.GetProperty("replicaId").GetString(), member.GetProperty("epoch").GetInt32(), member.GetProperty("state").GetString(), member.GetProperty("leaseValid").GetBoolean()));
        var section = member.GetProperty("sections")[0];
        Assert.Equal(("phase", 1.5, "Fenced"), (section.GetProperty("name").GetString(), section.GetProperty("ageSeconds").GetDouble(), section.GetProperty("values").GetProperty("phase").GetString()));
        var fence = member.GetProperty("fences")[0];
        Assert.Equal(("applier-stall", 40.0), (fence.GetProperty("trigger").GetString(), fence.GetProperty("spanSeconds").GetDouble()));
    }

    [Fact]
    public async Task The_body_equals_the_typed_read()
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.GetAsync("/status");
        var text = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(
            """{"readAt":"2026-01-02T03:04:05+00:00","members":[{"replicaId":"r1","epoch":7,"state":"fenced","leaseExpiresAt":"2026-01-02T03:04:00+00:00","leaseValid":false,"appliedSeq":12,"servingSince":null,"version":"1.0.0","protocol":3,"sections":[{"name":"phase","ageSeconds":1.5,"values":{"phase":"Fenced","epoch":7}}],"fences":[{"epoch":6,"trigger":"applier-stall","fencedAt":"2026-01-01T00:00:00+00:00","spanSeconds":40}]}]}""",
            text);

        var typed = await new Reeve.Cluster.Status.ClusterStatus(store, Microsoft.Extensions.Options.Options.Create(new ClusterOptions()), TimeProvider.System).ReadAsync();
        var body = JsonDocument.Parse(text).RootElement;
        Assert.Equal(typed.ReadAt, body.GetProperty("readAt").GetDateTimeOffset());
        var (member, json) = (Assert.Single(typed.Members), body.GetProperty("members")[0]);
        Assert.Equal(
            (member.ReplicaId, member.Epoch, member.State, member.LeaseExpiresAt, member.LeaseValid, member.AppliedSeq, member.ServingSince, member.Version, member.Protocol),
            (json.GetProperty("replicaId").GetString(), json.GetProperty("epoch").GetInt64(), json.GetProperty("state").GetString(), json.GetProperty("leaseExpiresAt").GetDateTimeOffset(),
                json.GetProperty("leaseValid").GetBoolean(), json.GetProperty("appliedSeq").GetInt64(), (DateTimeOffset?)null, json.GetProperty("version").GetString(), json.GetProperty("protocol").GetInt32()));
        Assert.Equal(member.Sections[0].Age.TotalSeconds, json.GetProperty("sections")[0].GetProperty("ageSeconds").GetDouble());
        Assert.Equal(member.Fences[0].Span.TotalSeconds, json.GetProperty("fences")[0].GetProperty("spanSeconds").GetDouble());
    }

    [Fact]
    public async Task An_application_section_is_in_the_body_beside_the_library_sections()
    {
        using var queue = JsonDocument.Parse("""{"queued":3}""");
        var read = Read();
        var member = read.Members[0] with { Sections = [.. read.Members[0].Sections, new ClusterStatusSection { Name = "queue-depth", Values = queue.RootElement.Clone(), Age = TimeSpan.FromSeconds(0.5) }] };
        store.OnRead = () => Task.FromResult(read with { Members = [member] });
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.GetAsync("/status");
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

        var sections = body.GetProperty("members")[0].GetProperty("sections");
        Assert.Equal(["phase", "queue-depth"], sections.EnumerateArray().Select(x => x.GetProperty("name").GetString()));
        Assert.Equal((0.5, 3), (sections[1].GetProperty("ageSeconds").GetDouble(), sections[1].GetProperty("values").GetProperty("queued").GetInt32()));
    }

    [Fact]
    public async Task A_replica_that_is_not_serving_still_refuses_an_endpoint_that_is_not_exempt_while_the_status_answers()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.ToFenced();

        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await rig.GetAsync("/min/plain")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await rig.GetAsync("/status")).StatusCode);
        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task A_failed_read_answers_503_status_unavailable()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        store.OnRead = () => throw new InvalidOperationException("the database is down");

        var response = await rig.GetAsync("/status");
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(ReeveClusterStatusEndpoint.Unavailable, body.GetProperty("errorCode").GetString());
        Assert.DoesNotContain("InvalidOperationException", body.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_read_that_outlasts_ActionTimeBudget_answers_503_status_unavailable()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        await using var bounded = HttpRig.Create(
            configure: services =>
            {
                services.AddSingleton<IStatusStore>(store);
                services.AddSingleton<TimeProvider>(clock);
            },
            afterFence: app => ((IEndpointRouteBuilder)app).MapReeveClusterStatus("/status"));
        await bounded.PublishAsync(5);
        bounded.ToServing();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.OnReadBound = async token =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return Read();
        };

        var request = bounded.GetAsync("/status");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.False(request.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(31));
        var response = await request.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(ReeveClusterStatusEndpoint.Unavailable, JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Before_the_first_generation_the_status_is_refused_like_every_endpoint()
    {
        var response = await rig.GetAsync("/status");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("replica_not_serving", JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task The_envelope_keeps_its_names_whatever_the_applications_naming_policy()
    {
        await using var snake = HttpRig.Create(
            configure: services =>
            {
                services.AddSingleton<IStatusStore>(store);
                services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower);
            },
            afterFence: app => ((IEndpointRouteBuilder)app).MapReeveClusterStatus("/status"));
        await snake.PublishAsync(5);
        snake.ToServing();

        var body = JsonDocument.Parse(await (await snake.GetAsync("/status")).Content.ReadAsStringAsync()).RootElement;

        Assert.True(body.TryGetProperty("readAt", out _));
        Assert.True(body.GetProperty("members")[0].TryGetProperty("replicaId", out _));
    }
}
