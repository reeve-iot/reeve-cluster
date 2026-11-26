using System.Net;
using Harness.Slice;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Failover;

[Collection("harness-failover")]
public sealed class FailoverTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = FailoverRig.Limit;

    [Fact]
    public async Task The_check_warns_against_an_asynchronous_standby_and_passes_with_a_synchronous_one_under_FIRST_and_ANY()
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine, StandbyMode.Async);
        var r1 = await rig.StartAsync("r1");

        await WarningAsync(rig, r1, warned: true);

        await rig.Stack.Pair.SetModeAsync(StandbyMode.SyncFirst);
        await WarningAsync(rig, r1, warned: false);

        await rig.Stack.Pair.SetModeAsync(StandbyMode.SyncAny);
        Assert.Equal(["standby1 streaming quorum"], await rig.Stack.Pair.ReplicationStateAsync());
        await WarningAsync(rig, r1, warned: false);

        await rig.Stack.Pair.SetModeAsync(StandbyMode.Async);
        await WarningAsync(rig, r1, warned: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task I32_a_failover_leaves_no_stale_service_every_replica_fences_on_the_new_timeline_and_serves_what_was_acknowledged(bool quorum)
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine, quorum ? StandbyMode.SyncAny : StandbyMode.SyncFirst);
        var (r1, r2) = await rig.StartPairAsync();
        var acknowledged = await rig.PutAsync(r1, "kept", "1");
        var epochs = await EpochsAsync(rig, r1, r2);
        Assert.Equal(1, await rig.TimelineAsync());

        var before = rig.Run.Sequence.Next();
        var onTheTimeline = await MoveAsync(rig, r1, rig.Stack.FailOverAsync);
        await WaitForRejoinAsync(rig, epochs, r1, r2);
        if (!onTheTimeline)
        {
            return;
        }

        Assert.Equal(2, await rig.TimelineAsync());
        foreach (var replica in new[] { r1, r2 })
        {
            AssertFencedOnceOnTheTimeline(rig, replica, before);
            Assert.Equal("1", FailoverApp.StateOf(replica)["s:kept"]);
            Assert.True(FailoverApp.HeadOf(replica) >= acknowledged);
            Assert.Equal(2, JoinedTimeline(replica));
        }

        using var read = await rig.Client(r2).GetAsync("items/kept");
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal("1", (await SliceRig.ItemAsync(read)).Value);
        await rig.PutAsync(r2, "after", "2");
        Assert.Equal("2", FailoverApp.StateOf(r1)["s:after"]);
    }

    [Fact]
    public async Task I32_a_promotion_that_lost_a_suffix_fences_the_fleet_on_the_new_timeline_and_no_generation_after_the_rejoin_holds_a_lost_entry()
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        await rig.PutAsync(r1, "kept", "1");
        var epochs = await EpochsAsync(rig, r1, r2);

        var lostHead = await LoseSuffixAsync(rig, r1, r2);
        Assert.Contains(FailoverApp.StateOf(r2).Keys, k => k.StartsWith("s:lost", StringComparison.Ordinal));
        Assert.Equal("lost-captured", FailoverApp.StateOf(r1)["c:1"]);

        var before = rig.Run.Sequence.Next();
        var onTheTimeline = await MoveAsync(rig, r1, rig.Stack.FailOverAsync);

        var reads = new[] { r1, r2 }.Select(async replica =>
        {
            using var response = await rig.Client(replica).GetAsync("items/lost0");
            return (replica.Id, response.StatusCode, Body: await response.Content.ReadAsStringAsync());
        }).ToList();

        Assert.Equal(0, await rig.ScalarAsync("SELECT count(*) FROM slice_item WHERE value = 'lost'"));
        Assert.Equal(0, await rig.ScalarAsync("SELECT count(*) FROM harness_captured"));
        Assert.Equal(0, await rig.ScalarAsync("SELECT count(*) FROM reeve_cluster.change_skip"));
        Assert.True(await rig.HeadAsync() < lostHead);

        await WaitForRejoinAsync(rig, epochs, r1, r2);

        foreach (var replica in new[] { r1, r2 })
        {
            if (onTheTimeline)
            {
                AssertFencedOnceOnTheTimeline(rig, replica, before);
            }

            AssertLoadedInFullAfter(rig, replica, before, epochs[replica.Id]);
            var state = FailoverApp.StateOf(replica);
            Assert.Equal(["s:kept"], state.Keys.ToArray());
        }

        foreach (var read in reads)
        {
            var (id, status, body) = await read;
            Assert.False(status == HttpStatusCode.OK && body.Contains("lost", StringComparison.Ordinal), $"{id} served a lost row after the promotion");
        }

        var reused = await rig.PutAsync(r1, "fresh", "x");
        Assert.True(reused <= lostHead, $"the new timeline's first seq {reused} is not one the suffix drew (up to {lostHead}): the history did not reuse a seq");
        foreach (var replica in new[] { r1, r2 })
        {
            Assert.Equal("x", FailoverApp.StateOf(replica)["s:fresh"]);
            Assert.DoesNotContain(FailoverApp.StateOf(replica).Keys, k => k.StartsWith("s:lost", StringComparison.Ordinal) || k.StartsWith("c:", StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData("first", true)]
    [InlineData("any", true)]
    [InlineData("async", false)]
    public async Task G1_an_acknowledged_write_survives_a_failover_after_a_lagging_then_cut_stream_only_with_a_synchronous_standby(string mode, bool survives)
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine, mode switch { "first" => StandbyMode.SyncFirst, "any" => StandbyMode.SyncAny, _ => StandbyMode.Async });
        var (r1, _) = await rig.StartPairAsync();
        _ = await rig.Stack.ReplicationLink.LatencyAsync(TimeSpan.FromMilliseconds(survives ? 1500 : 60_000), TimeSpan.Zero, Direction.Downstream);

        var watch = System.Diagnostics.Stopwatch.StartNew();
        await rig.PutAsync(r1, "kept", "1");
        if (survives)
        {
            Assert.True(watch.Elapsed > TimeSpan.FromSeconds(1), $"the acknowledgement of a write returned in {watch.Elapsed} though the standby lagged by 1.5 s");
        }

        await rig.Stack.ReplicationLink.CutAsync();
        await rig.Stack.FailOverAsync();

        Assert.Equal(survives ? 1 : 0, await rig.ScalarAsync("SELECT count(*) FROM slice_item WHERE id = 'kept'"));
    }

    [Fact]
    public async Task I32_a_reused_seq_at_a_lost_cursor_is_never_counted_as_applied_and_the_rewind_check_reloads_in_full()
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine);
        var spec = FailoverApp.Spec(
            profile: FailoverRig.Profile,
            configure: o => o.ApplyPollInterval = TimeSpan.FromSeconds(1));
        var (r1, r2) = await rig.StartPairAsync(spec);
        await rig.PutAsync(r1, "kept", "1");
        foreach (var replica in new[] { r1, r2 })
        {
            ((LocalPhaseGate)replica.Services.GetRequiredService<IPhaseGate>()).TimelineCheckDisabled = true;
        }

        var lostHead = await LoseSuffixAsync(rig, r1, r2);

        using var hold1 = r1.Faults.Hold(LogFaultPoints.PassBegin);
        using var hold2 = r2.Faults.Hold(LogFaultPoints.PassBegin);
        var before = rig.Run.Sequence.Next();
        if (!await MoveAsync(rig, r1, rig.Stack.FailOverAsync))
        {
            return;
        }

        Assert.True(await rig.HeadAsync() < lostHead);
        var first = await rig.AppendOtherTypeAsync(r1, 1);
        Assert.True(first <= lostHead, $"the new timeline's first seq {first} is not one the suffix drew (up to {lostHead})");
        var top = first < lostHead ? await rig.AppendOtherTypeAsync(r1, (int)(lostHead - first)) : first;
        Assert.Equal(lostHead, top);
        var generations = new[] { r1, r2 }.ToDictionary(r => r.Id, FailoverApp.GenerationIdOf);

        await hold1.WaitReachedAsync(timeout: Limit);
        await hold2.WaitReachedAsync(timeout: Limit);
        using var ackScope = r1.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        using var timeout = new CancellationTokenSource(Limit);
        var ack = r1.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(top, timeout.Token);
        var read = rig.Client(r2).GetAsync("items/kept");

        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.False(ack.IsCompleted, "a write at a reused seq was answered while no replica had applied it");
        Assert.False(read.IsCompleted, "a replica's read index served from a cursor that names a lost entry");

        hold1.Release();
        hold2.Release();
        foreach (var replica in new[] { r1, r2 })
        {
            await SliceRig.UntilAsync(() => Task.FromResult(FailoverApp.GenerationIdOf(replica) != generations[replica.Id]), $"{replica.Id} to publish after the release", Limit);
            Assert.Equal(["s:kept"], FailoverApp.StateOf(replica).Keys.ToArray());
            Assert.Equal(top, FailoverApp.HeadOf(replica));
            Assert.Contains(
                rig.Run.Events.Of(replica.Id),
                e => e.Sequence > before && e.Action == LogActions.PassBegin && (bool)e.State!["full"]! && !(bool)e.State["reload"]!);
        }

        Assert.IsType<AckOutcome.Acknowledged>(await ack.WaitAsync(Limit));
        using var served = await read.WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, served.StatusCode);
    }

    [Fact]
    public async Task With_the_timeline_check_turned_off_the_appliers_rewind_check_alone_starts_a_full_reload_at_a_poll()
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine);
        var spec = FailoverApp.Spec(
            profile: FailoverRig.Profile,
            configure: o => o.ApplyPollInterval = TimeSpan.FromSeconds(1));
        var (r1, r2) = await rig.StartPairAsync(spec);
        await rig.PutAsync(r1, "kept", "1");
        var epochs = await EpochsAsync(rig, r1, r2);
        foreach (var replica in new[] { r1, r2 })
        {
            ((LocalPhaseGate)replica.Services.GetRequiredService<IPhaseGate>()).TimelineCheckDisabled = true;
        }

        await LoseSuffixAsync(rig, r1, r2);
        var before = rig.Run.Sequence.Next();

        if (!await MoveAsync(rig, r1, rig.Stack.FailOverAsync))
        {
            return;
        }

        foreach (var replica in new[] { r1, r2 })
        {
            await SliceRig.UntilAsync(
                () => Task.FromResult(ReloadsAfter(rig, replica, before).Count > 0),
                $"{replica.Id} to reload in full at a poll",
                Limit);
            await SliceRig.UntilAsync(async () => FailoverApp.HeadOf(replica) == await rig.HeadAsync(), $"{replica.Id} to publish the reload", Limit);
            Assert.Equal(["s:kept"], FailoverApp.StateOf(replica).Keys.ToArray());
            Assert.Equal(1, JoinedTimeline(replica));
            Assert.Equal(epochs[replica.Id], (await rig.Run.Observer.MemberAsync(replica.Id))!.Epoch);
            Assert.DoesNotContain(rig.Run.Phases.Of(replica.Id), c => c.Sequence > before && c.To == ReplicaPhase.Fencing);
        }

    }

    [Fact]
    public async Task A_switchover_fences_the_fleet_once_and_it_rejoins_with_the_old_primary_streaming_again()
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync();
        var acknowledged = await rig.PutAsync(r1, "kept", "1");
        var epochs = await EpochsAsync(rig, r1, r2);

        var before = rig.Run.Sequence.Next();
        var onTheTimeline = await MoveAsync(rig, r1, rig.Stack.SwitchOverAsync);
        await WaitForRejoinAsync(rig, epochs, r1, r2);
        if (!onTheTimeline)
        {
            return;
        }

        Assert.Equal(2, await rig.TimelineAsync());
        Assert.Equal(["standby1 streaming sync"], await rig.Stack.Pair.ReplicationStateAsync());
        foreach (var replica in new[] { r1, r2 })
        {
            AssertFencedOnceOnTheTimeline(rig, replica, before);
            Assert.True(FailoverApp.HeadOf(replica) >= acknowledged);
            await WarningAsync(rig, replica, warned: false);
        }

        await rig.PutAsync(r2, "after", "2");
        await SliceRig.UntilAsync(
            async () => await rig.Stack.Pair.QueryAsync(rig.Stack.Pair.Standby!, "SELECT coalesce(max(value), '') FROM slice_item WHERE id = 'after'", database: rig.Run.Database!.Name) == "2",
            "the old primary to replay the write",
            Limit);

        foreach (var replica in new[] { r1, r2 })
        {
            var rejoined = rig.Run.Phases.Of(replica.Id).Last(c => c.Sequence > before && c.To == ReplicaPhase.Serving).Sequence;
            await SliceRig.UntilAsync(
                () => Task.FromResult(rig.Run.Events.Of(replica.Id).Count(e => e.Sequence > rejoined && e.Action == MembershipActions.RenewExec && Convert.ToInt64(e.State!["rows"], System.Globalization.CultureInfo.InvariantCulture) == 1) >= 2),
                $"{replica.Id} to renew twice after its rejoin",
                Limit);
            Assert.Equal(1, rig.Run.Phases.Of(replica.Id).Count(c => c.Sequence > before && c.To == ReplicaPhase.Fencing));
        }
    }

    internal static async Task<bool> MoveAsync(
        FailoverRig rig,
        Replica sample,
        Func<CancellationToken, Task<TimeSpan>> move,
        (Reeve.Cluster.Harness.Timing.StallSpan Stalls, System.Diagnostics.Stopwatch Outage)? since = null)
    {
        var (stalls, outage) = since ?? (rig.Run.Stalls.Begin(), System.Diagnostics.Stopwatch.StartNew());
        await move(CancellationToken.None);
        var options = sample.Options;
        var rides = options.LeaseTtl - options.SafetyMargin - options.StopBudget - options.RenewInterval - options.RenewTimeout;
        if (outage.Elapsed <= rides || !stalls.Covers(outage.Elapsed - rides))
        {
            return true;
        }

        Reeve.Cluster.Harness.Timing.Degraded.Report(rig.Run.Log, $"the outage from the cut to the restore took {outage.Elapsed.TotalSeconds:0.#} s, more than the {rides.TotalSeconds:0.#} s the fleet is sure to ride out, and {stalls.Describe()} accounts for the overrun: the replicas may fence by lease before the timeline, which was not asserted");
        return false;
    }

    private static async Task<long> LoseSuffixAsync(FailoverRig rig, Replica r1, Replica r2)
    {
        await rig.Stack.ReplicationLink.CutAsync();
        await rig.WaitStandbyGoneAsync();
        await using var cancelling = rig.CancelSyncWaits();

        await rig.LostCapturedWriteAsync(r1, 1, "lost-captured");
        var appended = await rig.LostAppendAsync(r2, "lost", 40);

        foreach (var replica in new[] { r1, r2 })
        {
            await SliceRig.UntilAsync(() => Task.FromResult(FailoverApp.HeadOf(replica) >= appended), $"{replica.Id} to apply the lost Append", Limit);
        }

        var skip = await rig.LostSkipOpenAsync(r1, appended + 1);
        var covered = await rig.LostAppendAsync(r2, "covered", 1);
        Assert.True(skip > 0 && covered > appended);

        foreach (var replica in new[] { r1, r2 })
        {
            await SliceRig.UntilAsync(() => Task.FromResult(FailoverApp.HeadOf(replica) >= covered), $"{replica.Id} to apply the lost entries", Limit);
            Assert.DoesNotContain(FailoverApp.StateOf(replica).Keys, k => k.StartsWith("s:covered", StringComparison.Ordinal));
        }

        return covered;
    }

    private static async Task WarningAsync(FailoverRig rig, Replica replica, bool warned)
    {
        var monitor = replica.Services.GetRequiredService<ReplicationMonitor>();
        await SliceRig.UntilAsync(
            async () => (await monitor.CollectAsync(CancellationToken.None)).Values["warning"] is string == warned,
            warned ? "the check to warn" : "the check to pass",
            Limit);
        Assert.DoesNotContain(replica.Services.GetRequiredService<IPhaseGate>().Readiness.Current, r => r.Code.StartsWith("replication.", StringComparison.Ordinal));
        using var response = await rig.Client(replica).GetAsync("ready");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    internal static async Task<Dictionary<string, long>> EpochsAsync(FailoverRig rig, params Replica[] replicas)
    {
        var epochs = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var replica in replicas)
        {
            epochs[replica.Id] = (await rig.Run.Observer.MemberAsync(replica.Id))!.Epoch;
        }

        return epochs;
    }

    internal static async Task WaitForRejoinAsync(FailoverRig rig, Dictionary<string, long> before, params Replica[] replicas)
    {
        foreach (var replica in replicas)
        {
            await SliceRig.UntilAsync(
                async () => await rig.Run.Observer.MemberAsync(replica.Id) is { State: MemberRowState.Active } member
                    && member.Epoch > before[replica.Id]
                    && SliceRig.PhaseOf(replica) == ReplicaPhase.Serving,
                $"{replica.Id} to rejoin",
                Limit);
        }
    }

    internal static long JoinedTimeline(Replica replica) => ((LocalPhaseGate)replica.Services.GetRequiredService<IPhaseGate>()).JoinedTimeline!.Value;

    internal static void AssertFencedOnceOnTheTimeline(FailoverRig rig, Replica replica, long after)
    {
        var path = rig.Run.Phases.Of(replica.Id).Where(c => c.Sequence > after).Select(c => c.To).ToList();
        Assert.Equal(ReplicaPhase.Fencing, path[0]);
        Assert.Equal(ReplicaPhase.Serving, path[^1]);
        Assert.Equal(1, path.Count(p => p == ReplicaPhase.Fencing));
        Assert.Contains(
            rig.Run.Events.Of(replica.Id),
            e => e.Sequence > after
                && (e.Action == MembershipActions.TimelineFence || (e.Action == MembershipActions.RenewAnswer && Equals(e.State!["res"], "other"))));
    }

    internal static void AssertLoadedInFullAfter(FailoverRig rig, Replica replica, long after, long previousEpoch)
    {
        var passes = rig.Run.Events.Snapshot()
            .Where(e => e.Replica == replica.Id && e.Sequence > after && e.Action == LogActions.PassBegin && Convert.ToInt64(e.State!["epoch"], System.Globalization.CultureInfo.InvariantCulture) > previousEpoch)
            .ToList();
        Assert.True((bool)passes[0].State!["full"]!, "the first pass of the new epoch is a load");
    }

    private static List<RecordedEvent> ReloadsAfter(FailoverRig rig, Replica replica, long after) =>
        [.. rig.Run.Events.Snapshot().Where(e => e.Replica == replica.Id && e.Sequence > after && e.Action == LogActions.PassBegin && (bool)e.State!["full"]! && !(bool)e.State["reload"]!)];
}
