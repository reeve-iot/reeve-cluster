using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Http;

public sealed class FenceMiddlewareTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly HttpRig rig = HttpRig.Create();

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    private static async Task<(HttpStatusCode Status, string? Code, string? RetryAfter)> RefusalAsync(HttpResponseMessage response)
    {
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var code = body.RootElement.TryGetProperty("errorCode", out var c) ? c.GetString() : null;
        return (response.StatusCode, code, response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    private static async Task<Seen> SeenAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<Seen>())!;
    }

    [Theory]
    [InlineData("/mvc/plain")]
    [InlineData("/min/plain")]
    public async Task A_serving_replica_admits_a_request_in_a_client_request_scope_and_pins_the_generation_the_index_found(string path)
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        var seen = await SeenAsync(await rig.GetAsync(path));

        Assert.Equal(new Seen("ClientRequest", 5), seen);
        Assert.Equal(1, rig.Store.Reads);
    }

    public static TheoryData<string, ReplicaPhase> NotServing()
    {
        var data = new TheoryData<string, ReplicaPhase>();
        foreach (var path in new[] { "/mvc/plain", "/min/plain" })
        {
            foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Draining })
            {
                data.Add(path, phase);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(NotServing))]
    public async Task A_request_that_is_not_exempt_answers_replica_not_serving_with_retry_after_one_in_every_phase_that_is_not_serving(string path, ReplicaPhase phase)
    {
        await rig.PublishAsync(5);
        Reach(phase);

        var answer = await RefusalAsync(await rig.GetAsync(path));

        Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_not_serving", "1"), answer);
        Assert.Equal(0, rig.Store.Reads);
    }

    [Theory]
    [InlineData("/mvc/plain", ReplicaPhase.Fencing)]
    [InlineData("/mvc/plain", ReplicaPhase.Fenced)]
    [InlineData("/min/plain", ReplicaPhase.Fencing)]
    [InlineData("/min/plain", ReplicaPhase.Fenced)]
    public async Task A_request_that_is_not_exempt_answers_replica_fenced_on_a_fencing_or_fenced_replica(string path, ReplicaPhase phase)
    {
        await rig.PublishAsync(5);
        Reach(phase);

        var answer = await RefusalAsync(await rig.GetAsync(path));

        Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_fenced", "1"), answer);
    }

    [Fact]
    public async Task A_fence_start_that_passed_during_a_pause_is_found_by_the_check_and_the_request_is_refused_as_fenced()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.PassFenceStart();

        var answer = await RefusalAsync(await rig.GetAsync("/mvc/plain"));

        Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_fenced", "1"), answer);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
    }

    [Theory]
    [InlineData("/mvc/exempt", ReplicaPhase.CatchingUp)]
    [InlineData("/mvc/exempt", ReplicaPhase.Activated)]
    [InlineData("/mvc/exempt", ReplicaPhase.Draining)]
    [InlineData("/mvc/exempt", ReplicaPhase.Fencing)]
    [InlineData("/mvc/exempt", ReplicaPhase.Fenced)]
    [InlineData("/mvc/class-exempt", ReplicaPhase.Fenced)]
    [InlineData("/min/exempt", ReplicaPhase.CatchingUp)]
    [InlineData("/min/exempt", ReplicaPhase.Fenced)]
    [InlineData("/min/attribute", ReplicaPhase.Fenced)]
    public async Task An_exempt_request_runs_on_a_replica_that_is_not_serving_pinning_the_generation_it_has_without_reading_the_head(string path, ReplicaPhase phase)
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        Reach(phase);

        var seen = await SeenAsync(await rig.GetAsync(path));

        Assert.Equal(new Seen("ExemptRequest", 3), seen);

        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task CatchingUp_does_not_wait_for_the_head_while_an_exempt_request_on_a_serving_replica_does()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToCatchingUp();
        Assert.Equal(new Seen("ExemptRequest", 3), await SeenAsync(await rig.GetAsync("/mvc/exempt")));
        Assert.Equal(0, rig.Store.Reads);

        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        var behind = rig.GetAsync("/mvc/exempt");
        await rig.ExpireReadIndexAsync();
        var answer = await RefusalAsync(await behind.WaitAsync(Wait));

        Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_behind", "1"), answer);
        Assert.True(rig.Store.Reads > 0);
    }

    [Fact]
    public async Task No_request_is_served_before_the_first_generation_exempt_ones_included()
    {
        rig.ToLoading();

        foreach (var path in new[] { "/mvc/plain", "/mvc/exempt", "/min/exempt" })
        {
            Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_not_serving", "1"), await RefusalAsync(await rig.GetAsync(path)));
        }

        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task A_request_that_arrives_while_the_generation_is_behind_waits_for_it_and_is_served_when_it_publishes()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var request = rig.GetAsync("/mvc/plain");
        await rig.ReadsAsync(1);
        Assert.False(request.IsCompleted, "the request waits for the generation that stands for the head");
        await rig.PublishAsync(9, alsoHead: false);
        await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Applied, 9));

        Assert.Equal(new Seen("ClientRequest", 9), await SeenAsync(await request.WaitAsync(Wait)));
    }

    [Fact]
    public async Task A_replica_that_does_not_catch_up_within_ReadIndexTimeout_answers_replica_behind()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var request = rig.GetAsync("/mvc/plain");
        await rig.ExpireReadIndexAsync();
        var answer = await RefusalAsync(await request.WaitAsync(Wait));

        Assert.Equal((HttpStatusCode.ServiceUnavailable, "replica_behind", "1"), answer);
    }

    [Theory]
    [InlineData("/mvc/skip")]
    [InlineData("/min/off/plain")]
    public async Task A_group_that_turned_the_wait_off_pins_the_current_generation_without_reading_the_head(string path)
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var seen = await SeenAsync(await rig.GetAsync(path));

        Assert.Equal(new Seen("ClientRequest", 3), seen);
        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task A_group_that_turned_the_wait_off_is_still_refused_when_the_replica_is_not_serving_unless_it_is_exempt()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.ToActivated();

        Assert.Equal("replica_not_serving", (await RefusalAsync(await rig.GetAsync("/mvc/skip"))).Code);
        Assert.Equal("replica_not_serving", (await RefusalAsync(await rig.GetAsync("/min/off/plain"))).Code);
        Assert.Equal(new Seen("ExemptRequest", 3), await SeenAsync(await rig.GetAsync("/mvc/exempt-skip")));
        Assert.Equal(new Seen("ExemptRequest", 3), await SeenAsync(await rig.GetAsync("/min/off/exempt")));
    }

    [Theory]
    [InlineData("/mvc/exempt")]
    [InlineData("/min/exempt")]
    public async Task An_exempt_request_on_a_serving_replica_waits_on_the_read_index_like_any_request(string path)
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var request = rig.GetAsync(path);
        await rig.ExpireReadIndexAsync();
        var answer = await RefusalAsync(await request.WaitAsync(Wait));

        Assert.Equal("replica_behind", answer.Code);
        Assert.True(rig.Store.Reads > 0);
    }

    [Theory]
    [InlineData("/mvc/exempt-skip")]
    [InlineData("/min/off/exempt")]
    public async Task An_exempt_request_on_a_serving_replica_does_not_wait_where_its_group_turned_the_wait_off(string path)
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var seen = await SeenAsync(await rig.GetAsync(path));

        Assert.Equal(new Seen("ExemptRequest", 3), seen);
        Assert.Equal(0, rig.Store.Reads);
    }

    [Fact]
    public async Task An_exempt_request_that_waits_when_the_replica_leaves_Serving_runs_unpinned_by_the_wait_like_one_on_a_fenced_replica()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var request = rig.GetAsync("/mvc/exempt");
        await rig.ReadsAsync(1);
        rig.ToFencing();
        await rig.NotifyAppliedAsync(3);

        Assert.Equal(new Seen("ExemptRequest", 3), await SeenAsync(await request.WaitAsync(Wait)));
    }

    [Fact]
    public async Task A_request_that_is_not_exempt_and_waits_when_the_replica_leaves_Serving_is_refused_as_fenced()
    {
        await rig.PublishAsync(3, alsoHead: false);
        rig.Store.Head = 9;
        rig.ToServing();

        var request = rig.GetAsync("/mvc/plain");
        await rig.ReadsAsync(1);
        rig.ToFencing();
        await rig.NotifyAppliedAsync(3);

        Assert.Equal("replica_fenced", (await RefusalAsync(await request.WaitAsync(Wait))).Code);
    }

    [Fact]
    public async Task The_fault_point_between_the_serving_check_and_the_handler_is_reached_by_an_admitted_request_and_not_by_a_refused_one()
    {
        await rig.PublishAsync(5);
        rig.ToServing();
        rig.Points.Hold(HttpFaultPoints.BetweenServingCheckAndHandler);

        var request = rig.GetAsync("/mvc/plain");
        await rig.Points.Arrived.WaitAsync(Wait);
        Assert.False(request.IsCompleted);
        rig.Points.Release();
        await SeenAsync(await request.WaitAsync(Wait));

        rig.ToFencing();
        var before = rig.Points.Reached.Count(p => p == HttpFaultPoints.BetweenServingCheckAndHandler.Name);
        await rig.GetAsync("/mvc/plain");
        Assert.Equal(before, rig.Points.Reached.Count(p => p == HttpFaultPoints.BetweenServingCheckAndHandler.Name));
    }

    [Fact]
    public async Task Each_request_has_a_scope_of_its_own_that_ends_with_it()
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        await SeenAsync(await rig.GetAsync("/min/capture"));
        await SeenAsync(await rig.GetAsync("/min/capture"));

        var scopes = rig.Captured.ToArray();
        Assert.Equal(2, scopes.Length);
        Assert.NotSame(scopes[0], scopes[1]);
        Assert.All(scopes, s => Assert.True(((Reeve.Cluster.Log.ClusterScope)s).Ended));
        Assert.Null(rig.Scopes.Current);
    }

    private void Reach(ReplicaPhase phase)
    {
        switch (phase)
        {
            case ReplicaPhase.Loading: rig.ToLoading(); break;
            case ReplicaPhase.CatchingUp: rig.ToCatchingUp(); break;
            case ReplicaPhase.Activated: rig.ToActivated(); break;
            case ReplicaPhase.Draining: rig.ToServing(); rig.ToDraining(); break;
            case ReplicaPhase.Fencing: rig.ToServing(); rig.ToFencing(); break;
            case ReplicaPhase.Fenced: rig.ToServing(); rig.ToFenced(); break;
            default: throw new ArgumentOutOfRangeException(nameof(phase));
        }

        Assert.Equal(phase, rig.Gate.Phase);
    }
}
