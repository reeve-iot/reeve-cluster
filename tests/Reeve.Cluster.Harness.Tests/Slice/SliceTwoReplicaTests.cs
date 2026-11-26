using System.Diagnostics;
using System.Net;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Slice;

[Collection(StackCollection.Name)]
public sealed class SliceTwoReplicaTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static void LongPolls(ClusterOptions o) => o.ApplyPollInterval = o.AckPollInterval = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_write_waits_for_the_peers_cursor_and_is_acknowledged_with_its_record_and_the_peer_then_reads_it()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        var http1 = rig.Client(r1);

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = http1.PutAsync("items/widget/a?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        var begin = await WaitSetOfAsync(rig, "r1", LogActions.AckBegin);
        Assert.Equal(["r1", "r2"], begin.Members.Order());
        Assert.False(await Task.WhenAny(put, Task.Delay(TimeSpan.FromSeconds(3))) == put, "the write was answered while the peer's applier was held");
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq < begin.AwaitedSeq);

        pass.Release();
        using var response = await put.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var seq = (await SliceRig.ItemAsync(response)).Seq!.Value;
        Assert.Equal(begin.AwaitedSeq, seq);
        var done = await WaitSetOfAsync(rig, "r1", LogActions.AckDone);
        Assert.Equal(["r1", "r2"], done.Members.Order());
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq >= seq);
        Assert.True(await AckedSeqAsync(rig) >= seq);

        Assert.Equal("one", (await SliceRig.ItemAsync(await rig.Client(r2).GetAsync("items/widget/a"))).Value);
    }

    [Fact]
    public async Task A_peer_fenced_during_a_wait_leaves_the_wait_set_and_the_wait_ends_Acknowledged()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        var http1 = rig.Client(r1);

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = http1.PutAsync("items/widget/a?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        var begin = await WaitSetOfAsync(rig, "r1", LogActions.AckBegin);
        Assert.Contains("r2", begin.Members);
        Assert.False(put.IsCompleted);

        HarnessOperations.ReportFatalFault(r2, "the peer fences during the wait");
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Phases.Path("r2").Contains(ReplicaPhase.Fenced)), "r2's fence");
        using var response = await put.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var done = await WaitSetOfAsync(rig, "r1", LogActions.AckDone);
        Assert.Equal(begin.AwaitedSeq, done.AwaitedSeq);
        Assert.Equal(["r1"], done.Members);
        Assert.Contains(await rig.Run.Observer.FenceHistoryAsync("r2"), h => h.Epoch == 2);

        pass.Release();
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving);
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.Epoch >= 3);
        Assert.Equal("one", (await SliceRig.ItemAsync(await rig.Client(r2).GetAsync("items/widget/a"))).Value);
    }

    [Fact]
    public async Task The_wait_ends_on_the_peers_applied_notice_over_Redis_long_before_the_polls()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);

        var (r1, r2) = await rig.StartPairAsync(SliceApp.Spec(configure: LongPolls));
        Assert.Equal("RedisWakeupChannel", r2.Services.GetRequiredService<Reeve.Cluster.Wakeup.IWakeupChannel>().GetType().Name);

        var started = Stopwatch.StartNew();
        using var put = await rig.Client(r1).PutAsync("items/widget/a?value=one", null);

        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(20), $"The write took {started.Elapsed}; the polls are 30 s.");
        var seq = (await SliceRig.ItemAsync(put)).Seq!.Value;
        Assert.Equal(["r1", "r2"], (await WaitSetOfAsync(rig, "r1", LogActions.AckDone)).Members.Order());
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq >= seq);
    }

    [Fact]
    public async Task With_Redis_cut_the_wait_ends_by_poll_within_AckPollInterval()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var ackPoll = TimeSpan.FromSeconds(3);
        var (r1, r2) = await rig.StartPairAsync(SliceApp.Spec(configure: o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(500);
            o.AckPollInterval = ackPoll;
        }));

        await r1.Link!.Redis.CutAsync();
        await r2.Link!.Redis.CutAsync();

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = rig.Client(r1).PutAsync("items/widget/a?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        var begin = await WaitSetOfAsync(rig, "r1", LogActions.AckBegin);
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.MemberAsync("r1"))!.AppliedSeq >= begin.AwaitedSeq, "r1's own cursor to reach the write");
        Assert.False(put.IsCompleted);

        var released = Stopwatch.StartNew();
        pass.Release();
        using var response = await put.WaitAsync(Limit);
        output.WriteLine($"the wait ended {released.Elapsed.TotalSeconds:0.00} s after r2 was released; AckPollInterval {ackPoll.TotalSeconds:0} s");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(["r1", "r2"], (await WaitSetOfAsync(rig, "r1", LogActions.AckDone)).Members.Order());
    }

    [Fact]
    public async Task Opt_out_a_PUT_under_AcknowledgeAtCommit_answers_at_once_while_the_peers_applier_is_stalled_and_the_next_request_on_the_peer_waits_for_the_write()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        using var put = await rig.Client(r1).PutAsync("optout/widget/a?value=one", null);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        await pass.WaitReachedAsync(timeout: Limit);
        Assert.Equal("at-commit", Assert.Single(put.Headers.GetValues("Reeve-Replication")));
        var seq = (await SliceRig.ItemAsync(put)).Seq!.Value;
        Assert.Equal(seq.ToString(System.Globalization.CultureInfo.InvariantCulture), Assert.Single(put.Headers.GetValues("Reeve-Seq")));

        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq < seq);
        var get = rig.Client(r2).GetAsync("items/widget/a");
        Assert.False(await Task.WhenAny(get, Task.Delay(TimeSpan.FromSeconds(2))) == get, "the read on the peer was answered before the peer applied the write");
        Assert.DoesNotContain("get widget/a", r2.Services.GetRequiredService<SliceCalls>().All);

        pass.Release();
        using var read = await get.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal("one", (await SliceRig.ItemAsync(read)).Value);
    }

    [Fact]
    public async Task Acknowledgement_bound_a_PUT_whose_acknowledgement_outlives_AckTimeout_answers_202_write_unconfirmed_within_the_bound()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        var bound = r1.Options.AckTimeout;

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var started = Stopwatch.StartNew();
        var request = rig.Client(r1).PutAsync("items/widget/a?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        using var put = await request.WaitAsync(Limit);

        Assert.Equal(HttpStatusCode.Accepted, put.StatusCode);
        Assert.Equal("write_unconfirmed", await SliceRig.ErrorCodeAsync(put));
        Assert.True(put.Headers.Contains("Reeve-Seq"));
        Assert.True(started.Elapsed >= bound - TimeSpan.FromMilliseconds(500), $"The answer came after {started.Elapsed}, before AckTimeout {bound}.");
        Assert.True(started.Elapsed < bound + TimeSpan.FromSeconds(30), $"The answer came after {started.Elapsed}, past AckTimeout {bound}.");
        Assert.Contains(rig.Run.Events.Of("r1"), e => e.Action == LogActions.AckTimeout);

        pass.Release();
        Assert.Equal("one", await ReadUntilAsync(rig, r2, "items/widget/a"));
    }

    [Fact]
    public async Task Replay_M1_N2_a_replica_that_activated_behind_an_acknowledged_write_serves_nothing_until_its_cursor_reaches_the_head_it_activated_at()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1");
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var http1 = rig.Client(r1);

        using (var seed = await http1.PutAsync("items/widget/seed?value=s", null))
        {
            Assert.Equal(HttpStatusCode.OK, seed.StatusCode);
        }

        var r2 = await rig.PrepareAsync("r2");
        using var activation = r2.Faults.HoldNext(MembershipFaultPoints.ActivateBeforeLock);
        using var passes = r2.Faults.Hold(LogFaultPoints.PassBegin, skip: 1);
        await r2.StartAsync();
        await activation.WaitReachedAsync(timeout: Limit);

        using var beforeAck = r1.Faults.HoldNext(HttpFaultPoints.BeforeAckAwait);
        var put = http1.PutAsync("items/widget/n?value=n", null);
        await beforeAck.WaitReachedAsync(timeout: Limit);
        activation.Release();
        await SliceRig.UntilAsync(() => Task.FromResult(SliceRig.PhaseOf(r2) is ReplicaPhase.Activated or ReplicaPhase.Serving), "r2's activation");
        Assert.Equal(ReplicaPhase.Activated, SliceRig.PhaseOf(r2));
        var row = (await rig.Run.Observer.MemberAsync("r2"))!;
        Assert.Equal(MemberRowState.Active, row.State);

        beforeAck.Release();
        using var response = await put.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var seq = (await SliceRig.ItemAsync(response)).Seq!.Value;
        Assert.Equal(seq, row.ActivatedAtSeq);
        var done = rig.Run.Events.WaitSets().Single(w => w.Replica == "r1" && w.Action == LogActions.AckDone && w.AwaitedSeq == seq);
        Assert.Equal(["r1"], done.Members);
        Assert.True((await rig.Run.Observer.MemberAsync("r2"))!.AppliedSeq < seq);

        Assert.Equal(ReplicaPhase.Activated, SliceRig.PhaseOf(r2));
        var http2 = rig.Client(r2);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await http2.GetAsync("ready")).StatusCode);
        using var refused = await http2.GetAsync("items/widget/n");
        Assert.Equal("replica_not_serving", await SliceRig.ErrorCodeAsync(refused));
        Assert.Empty(r2.Services.GetRequiredService<SliceCalls>().All);

        passes.Dispose();
        await SliceRig.WaitForPhaseAsync(r2, ReplicaPhase.Serving);
        Assert.Equal("n", (await SliceRig.ItemAsync(await http2.GetAsync("items/widget/n"))).Value);
    }

    [Fact]
    public async Task Replay_M1_A2_a_late_attempt_released_after_its_retrys_generation_was_published_and_acknowledged_publishes_nothing_and_activates_nothing()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, _) = await rig.StartPairAsync(SliceApp.Spec(configure: o => o.ApplyStallLimit = TimeSpan.FromSeconds(30)));
        var http1 = rig.Client(r1);
        var activations = r1.Services.GetRequiredService<SliceActivations>();

        using var late = r1.Faults.HoldNext(LogFaultPoints.BeforeReconcilerReturns);
        var put1 = http1.PutAsync("items/widget/a?value=v1", null);
        await late.WaitReachedAsync(timeout: Limit);
        using var retry = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Contains(LogActions.AttemptHang)), "the attempt to time out");
        await retry.WaitReachedAsync(timeout: Limit);
        var baseline = activations.All.Count;

        long seq2;
        await using (var write = await HarnessOperations.StartWrite(r1))
        {
            await using (var command = new NpgsqlCommand("UPDATE slice_item SET value = 'v2' WHERE type = 'widget' AND id = 'a'", write.Transaction.Connection, write.Transaction))
            {
                await command.ExecuteNonQueryAsync();
            }

            seq2 = await write.AppendAsync([new ChangeEntry(SliceApp.Widget, "a")]);
            await write.CommitAsync();
        }

        Task<AckOutcome> ack2;
        using (r1.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground())
        {
            ack2 = r1.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq2);
        }

        retry.Release();
        using var response1 = await put1.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response1.StatusCode);
        Assert.IsType<AckOutcome.Acknowledged>(await ack2.WaitAsync(Limit));
        Assert.Equal("v2", (await SliceRig.ItemAsync(await http1.GetAsync("items/widget/a"))).Value);
        Assert.Equal([$"widget@{seq2}: a=v2"], activations.All.Skip(baseline));
        var published = rig.Run.Events.Publications().Count(p => p.Replica == "r1");

        late.Release();
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Contains(LogActions.LateReturn)), "the late return");

        using var put3 = await http1.PutAsync("items/widget/a?value=v3", null);
        Assert.Equal(HttpStatusCode.OK, put3.StatusCode);
        var seq3 = (await SliceRig.ItemAsync(put3)).Seq!.Value;
        var after = rig.Run.Events.Publications().Where(p => p.Replica == "r1").Skip(published).ToList();
        Assert.Equal([seq3], after.Select(p => p.Head));
        Assert.Equal([$"widget@{seq2}: a=v2", $"widget@{seq3}: a=v3"], activations.All.Skip(baseline));
        Assert.Equal("v3", (await SliceRig.ItemAsync(await http1.GetAsync("items/widget/a"))).Value);

        var actions = rig.Run.Events.Actions("r1");
        var hang = actions.ToList().IndexOf(LogActions.AttemptHang);
        var lateReturn = actions.ToList().IndexOf(LogActions.LateReturn);
        var retryOk = actions.Select((a, i) => (a, i)).First(x => x.a == LogActions.AttemptOK && x.i > hang).i;
        var publish = actions.Select((a, i) => (a, i)).First(x => x.a == LogActions.Publish && x.i > retryOk).i;
        Assert.True(hang < retryOk && retryOk < publish && publish < lateReturn, $"the history is out of order: {string.Join(" ", actions)}");
    }

    [Fact]
    public async Task A_fence_closes_the_hubs_connection_and_the_client_reconnects_to_the_other_replica_which_serves_what_was_acknowledged()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        using (var put = await rig.Client(r1).PutAsync("items/widget/a?value=one", null))
        {
            Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        }

        await using var hub1 = await rig.ConnectHubAsync(r1);
        Assert.Contains("\"value\":\"one\"", await hub1.InvokeAsync("Get", "widget", "a"), StringComparison.Ordinal);

        using var rejoin = r1.Faults.HoldNext(MembershipFaultPoints.FenceBeforeStep6);
        await HarnessOperations.OperatorDelete(rig.Run, "r1");
        await rejoin.WaitReachedAsync(timeout: Limit);
        Assert.True(await hub1.ClosedWithinAsync(TimeSpan.FromSeconds(30)));
        using var refused = await rig.Client(r1).GetAsync("items/widget/a");
        Assert.Contains(await SliceRig.ErrorCodeAsync(refused), new[] { "replica_fenced", "replica_not_serving" });
        rejoin.Release();

        await using var hub2 = await rig.ConnectHubAsync(r2);
        Assert.Contains("\"value\":\"one\"", await hub2.InvokeAsync("Get", "widget", "a"), StringComparison.Ordinal);
        Assert.Contains("\"value\":\"two\"", await hub2.InvokeAsync("Put", "widget", "b", "two"), StringComparison.Ordinal);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal("two", (await SliceRig.ItemAsync(await rig.Client(r1).GetAsync("items/widget/b"))).Value);
    }

    [Fact]
    public async Task A_background_TryPin_on_the_peer_after_the_acknowledgement_returns_a_generation_that_includes_the_write()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();

        for (var write = 1; write <= 3; write++)
        {
            var (writer, peer) = write % 2 == 1 ? (r1, r2) : (r2, r1);
            var begins = BeginsOf(rig, writer.Id).Count;
            using var pass = peer.Faults.HoldNext(LogFaultPoints.PassBegin);
            var put = rig.Client(writer).PutAsync($"items/widget/a?value=v{write}", null);
            await pass.WaitReachedAsync(timeout: Limit);

            CountedWaitSet? begin = null;
            await SliceRig.UntilAsync(
                () => Task.FromResult((begin = BeginsOf(rig, writer.Id).Skip(begins).FirstOrDefault()) is not null),
                $"the wait of write {write} on {writer.Id}");
            Assert.Contains(peer.Id, begin!.Members);
            Assert.True((await rig.Run.Observer.MemberAsync(peer.Id))!.AppliedSeq < begin.AwaitedSeq);
            pass.Release();
            using var response = await put.WaitAsync(Limit);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var seq = (await SliceRig.ItemAsync(response)).Seq!.Value;

            using (peer.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground())
            {
                var generation = peer.Services.GetRequiredService<IClusterGenerations>().TryPin();
                Assert.NotNull(generation);
                Assert.True(generation.Head >= seq, $"{peer.Id} pinned head {generation.Head} below the acknowledged seq {seq}");
                Assert.Equal($"v{write}", generation.Get<Dictionary<string, string>>(SliceApp.Widget)["a"]);
            }
        }
    }

    private static List<CountedWaitSet> BeginsOf(SliceRig rig, string replica) =>
        [.. rig.Run.Events.WaitSets().Where(w => w.Replica == replica && w.Action == LogActions.AckBegin)];

    private static async Task<string?> ReadUntilAsync(SliceRig rig, Replica replica, string path)
    {
        using var response = await rig.Client(replica).GetAsync(path).WaitAsync(Limit);
        return (await SliceRig.ItemAsync(response)).Value;
    }

    private static async Task<CountedWaitSet> WaitSetOfAsync(SliceRig rig, string replica, string action)
    {
        CountedWaitSet? found = null;
        await SliceRig.UntilAsync(
            () =>
            {
                found = rig.Run.Events.WaitSets().FirstOrDefault(w => w.Replica == replica && w.Action == action);
                return Task.FromResult(found is not null);
            },
            $"{action} of a wait on {replica}");
        return found!;
    }

    private static async Task<long> AckedSeqAsync(SliceRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT acked_seq FROM reeve_cluster.change_ack", connection);
        return (long)(await command.ExecuteScalarAsync() ?? 0L);
    }
}
