using System.Diagnostics;
using System.Net;
using Harness.SelfTests;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Slice;

[Collection(StackCollection.Name)]
public sealed class SliceDrainTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan ProxyRead = TimeSpan.FromSeconds(120);

    [Fact]
    public async Task A_write_in_flight_at_the_hosts_stop_is_acknowledged_and_the_draining_write_comes_after_its_answer()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = rig.Client(r1).PutAsync("items/widget/h1?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Contains(LogActions.AckBegin)), "r1's acknowledgement wait to begin");
        var answeredBeforeStop = put.IsCompleted;

        var clock = Stopwatch.StartNew();
        var stop = r1.StopAsync();
        var reachedDraining = true;
        try
        {
            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Draining);
        }
        catch (TimeoutException)
        {
            reachedDraining = false;
        }

        var drainingAt = clock.Elapsed;
        int? readStatus = null;
        string? readCode = null;
        ObservedMember? during = null;
        var stopDoneBeforeRelease = false;
        if (reachedDraining)
        {
            using var read = await rig.Client(r1).GetAsync("items/widget/h1");
            readStatus = (int)read.StatusCode;
            readCode = await SliceRig.ErrorCodeAsync(read);
            during = await rig.Run.Observer.MemberAsync("r1");
            stopDoneBeforeRelease = await Task.WhenAny(stop, Task.Delay(TimeSpan.FromMilliseconds(1500))) == stop;
        }

        var putDoneBeforeRelease = put.IsCompleted;
        pass.Release();
        using var response = await put.WaitAsync(Limit);
        var answeredAt = clock.Elapsed;
        await stop.WaitAsync(Limit);
        var stoppedAt = clock.Elapsed;
        var after = await rig.Run.Observer.MemberAsync("r1");

        var r1Events = rig.Run.Events.Of("r1");
        var drainActions = r1Events.Where(e => e.Action is MembershipActions.Drain1 or MembershipActions.Drain23 or MembershipActions.Drain4).Select(e => e.Action).ToList();
        var ackDone = r1Events.LastOrDefault(e => e.Action == LogActions.AckDone)?.Sequence;
        var drain4 = r1Events.FirstOrDefault(e => e.Action == MembershipActions.Drain4)?.Sequence;
        var ackTimeouts = r1Events.Count(e => e.Action == LogActions.AckTimeout);

        var clock2 = Stopwatch.StartNew();
        using var put2 = await rig.Client(r2).PutAsync("items/widget/h1b?value=two", null);
        var put2Elapsed = clock2.Elapsed;
        var put2Set = rig.Run.Events.WaitSets().LastOrDefault(w => w.Replica == "r2" && w.Action == LogActions.AckBegin);

        var line = FormattableString.Invariant(
            $"answered before stop {answeredBeforeStop}; Draining reached {reachedDraining} at {drainingAt.TotalMilliseconds:F0} ms; read on r1 during the drain {readStatus} {readCode}; row during {during?.State} serving_since {(during?.ServingSince is null ? "null" : "set")}; stop done before release {stopDoneBeforeRelease}; put done before release {putDoneBeforeRelease}; put {(int)response.StatusCode} at {answeredAt.TotalMilliseconds:F0} ms; stop returned at {stoppedAt.TotalMilliseconds:F0} ms; row after {after?.State}; r1 drain events [{string.Join(",", drainActions)}]; AckDone seq {ackDone}, Drain4 seq {drain4}, AckTimeouts {ackTimeouts}; r2 write after the stop {(int)put2.StatusCode} in {put2Elapsed.TotalMilliseconds:F0} ms, wait set [{(put2Set is null ? "-" : string.Join(",", put2Set.Members.Order()))}]");
        output.WriteLine(line);

        Assert.False(answeredBeforeStop, line);
        Assert.True(reachedDraining, line);
        Assert.Equal(503, readStatus);
        Assert.Equal("replica_not_serving", readCode);
        Assert.Equal(MemberRowState.Active, during!.State);
        Assert.Null(during.ServingSince);
        Assert.False(stopDoneBeforeRelease, line);
        Assert.False(putDoneBeforeRelease, line);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(MemberRowState.Draining, after!.State);
        Assert.Equal([MembershipActions.Drain1, MembershipActions.Drain23, MembershipActions.Drain23, MembershipActions.Drain4], drainActions);
        Assert.True(ackDone < drain4, line);
        Assert.Equal(HttpStatusCode.OK, put2.StatusCode);
        Assert.DoesNotContain("r1", put2Set!.Members);
    }

    [Fact]
    public async Task A_tracked_event_stream_open_at_the_stop_does_not_hold_the_drain_for_ProxyReadTimeout()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StreamSpec());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);

        using var request = new HttpRequestMessage(HttpMethod.Get, "stream");
        using var response = await rig.Client(r1).SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
        var first = await reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("data: open", first);

        var (drainingAt, exitedAt, stoppedAt, stopError) = await StopAndTimeAsync(r1);
        var row = await rig.Run.Observer.MemberAsync("r1");
        var line = FormattableString.Invariant(
            $"one tracked event stream open at the stop; ProxyReadTimeout {ProxyRead.TotalMilliseconds:F0} ms; Draining at {drainingAt?.TotalMilliseconds:F0} ms, Exited at {exitedAt?.TotalMilliseconds:F0} ms, stop returned at {stoppedAt.TotalMilliseconds:F0} ms (error {stopError}); row after {row?.State}");
        output.WriteLine(line);
        Assert.Equal(MemberRowState.Draining, row!.State);
        Assert.Equal("none", stopError);
        Assert.True(exitedAt < ProxyRead / 2, line);

        try
        {
            while (await reader.ReadLineAsync().WaitAsync(Limit) is not null)
            {
            }
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public async Task With_no_stream_open_the_same_replica_drains_at_once()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", StreamSpec());
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);

        var (drainingAt, exitedAt, stoppedAt, stopError) = await StopAndTimeAsync(r1);
        var row = await rig.Run.Observer.MemberAsync("r1");
        var line = FormattableString.Invariant(
            $"no stream open at the stop; Draining at {drainingAt?.TotalMilliseconds:F0} ms, Exited at {exitedAt?.TotalMilliseconds:F0} ms, stop returned at {stoppedAt.TotalMilliseconds:F0} ms (error {stopError}); row after {row?.State}");
        output.WriteLine(line);
        Assert.Equal(MemberRowState.Draining, row!.State);
        Assert.Equal("none", stopError);
        Assert.True(exitedAt < ProxyRead / 2, line);
    }

    [Fact]
    public async Task While_r1_drains_its_applier_runs_and_a_write_on_r2_counts_it_and_is_acknowledged()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();

        using var step4 = r1.Faults.HoldNext(MembershipFaultPoints.DrainBeforeStep4);
        var stop = r1.StopAsync();
        await step4.WaitReachedAsync(timeout: Limit);
        var phase = SliceRig.PhaseOf(r1);

        var clock = Stopwatch.StartNew();
        using var put = await rig.Client(r2).PutAsync("items/widget/h3?value=one", null);
        var elapsed = clock.Elapsed;
        long? seq = put.StatusCode == HttpStatusCode.OK ? (await SliceRig.ItemAsync(put)).Seq : null;
        var set = rig.Run.Events.WaitSets().LastOrDefault(w => w.Replica == "r2" && w.Action == LogActions.AckBegin);
        var during = await rig.Run.Observer.MemberAsync("r1");

        step4.Release();
        await stop.WaitAsync(Limit);
        var after = await rig.Run.Observer.MemberAsync("r1");
        var line = FormattableString.Invariant(
            $"r1 phase at the hold {phase}; write on r2 {(int)put.StatusCode} in {elapsed.TotalMilliseconds:F0} ms, seq {seq}; r2's wait set [{(set is null ? "-" : string.Join(",", set.Members.Order()))}]; r1 row during {during?.State}, applied {during?.AppliedSeq}; r1 row after {after?.State}");
        output.WriteLine(line);

        Assert.Equal(ReplicaPhase.Draining, phase);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.Contains("r1", set!.Members);
        Assert.True(during!.AppliedSeq >= seq, line);
        Assert.Equal(MemberRowState.Active, during.State);
        Assert.Equal(MemberRowState.Draining, after!.State);
    }

    private static async Task<(TimeSpan? DrainingAt, TimeSpan? ExitedAt, TimeSpan StoppedAt, string StopError)> StopAndTimeAsync(Replica replica)
    {
        var clock = Stopwatch.StartNew();
        var stop = replica.StopAsync();
        TimeSpan? drainingAt = null;
        TimeSpan? exitedAt = null;
        while (!stop.IsCompleted && clock.Elapsed < Limit)
        {
            var phase = SliceRig.PhaseOf(replica);
            if (drainingAt is null && phase == ReplicaPhase.Draining)
            {
                drainingAt = clock.Elapsed;
            }

            if (exitedAt is null && phase == ReplicaPhase.Exited)
            {
                exitedAt = clock.Elapsed;
            }

            await Task.WhenAny(stop, Task.Delay(20));
        }

        var stopError = "none";
        try
        {
            await stop.WaitAsync(Limit);
        }
        catch (Exception exception) when (exception is not TimeoutException)
        {
            stopError = exception.GetType().Name;
        }

        var stoppedAt = clock.Elapsed;
        if (exitedAt is null && SliceRig.PhaseOf(replica) == ReplicaPhase.Exited)
        {
            exitedAt = stoppedAt;
        }

        return (drainingAt, exitedAt, stoppedAt, stopError);
    }

    private static ReplicaSpec StreamSpec()
    {
        var inner = SliceApp.Spec(configure: o =>
        {
            o.ReadIndexTimeout = TimeSpan.FromSeconds(2);
            o.ActionTimeBudget = TimeSpan.FromSeconds(1);
            o.ProxyReadTimeout = ProxyRead;
        });
        return new ReplicaSpec
        {
            ClockRate = inner.ClockRate,
            Profile = inner.Profile,
            Configure = inner.Configure,
            Reconcilers = inner.Reconcilers,
            FenceSink = inner.FenceSink,
            Redis = inner.Redis,
            ConfigureServices = inner.ConfigureServices,
            ConfigureWebApp = app =>
            {
                inner.ConfigureWebApp!(app);
                app.MapGet("/stream", async (HttpContext http) =>
                {
                    var connections = http.RequestServices.GetRequiredService<Reeve.Cluster.AspNetCore.ClusterConnections>();
                    using var tracked = connections.Track(http);
                    http.Response.ContentType = "text/event-stream";
                    await http.Response.WriteAsync("data: open\n\n", http.RequestAborted);
                    await http.Response.Body.FlushAsync(http.RequestAborted);
                    try
                    {
                        await Task.Delay(Timeout.Infinite, http.RequestAborted);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }).SkipReadIndex();
            },
        };
    }
}
