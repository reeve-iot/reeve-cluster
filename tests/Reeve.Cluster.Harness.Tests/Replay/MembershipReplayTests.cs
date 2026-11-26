using System.Net.Http.Json;
using Harness.SelfTests;
using Harness.Slice;
using Harness.Stall;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Harness.Timing;
using Npgsql;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Replay.Membership;

[Collection(StackCollection.Name)]
public sealed class MembershipReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static void TightStall(ClusterOptions o) => o.ApplyStallLimit = TimeSpan.FromSeconds(2);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_M1_A6_after_the_gate_raised_the_floor_a_protocol_1_replica_joining_is_refused_activation_and_stays_joining(int replicas)
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var peers = await ReplayFleet.StartAsync(rig, replicas, ReplayApp.Spec(protocol: 2));

        var gate = peers[0].Services.GetRequiredService<Reeve.Cluster.IClusterVersionGate>();
        Assert.True(await gate.RequireProtocolAsync(2).WaitAsync(Limit), "the raise answered false with no member below protocol 2");
        rig.Run.RecordEnvironment(null, EnvironmentActions.FloorRaise, ("floor", 2));
        Assert.Equal(2, await ProtocolFloorAsync(rig.Run));
        Assert.True(await peers[^1].Services.GetRequiredService<Reeve.Cluster.IClusterVersionGate>().RequireProtocolAsync(2).WaitAsync(Limit));

        var old = await rig.StartAsync("old", ReplayApp.Spec());

        await SliceRig.UntilAsync(
            () => Task.FromResult(old.Services.GetRequiredService<IPhaseGate>().Readiness.Current.Any(r => r.Code == ActivationRules.ProtocolFloorReason)),
            "the refusal by the protocol floor",
            TimeSpan.FromSeconds(20));

        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(ReplicaPhase.CatchingUp, SliceRig.PhaseOf(old));
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync(old.Id))!.State);
        Assert.DoesNotContain(rig.Run.Phases.Path(old.Id), p => p is ReplicaPhase.Activated or ReplicaPhase.Serving);
        Assert.DoesNotContain(MembershipActions.Activate, rig.Run.Events.Actions(old.Id));

        Assert.True(await gate.RequireProtocolAsync(2).WaitAsync(Limit));
        Assert.Equal(2, await ProtocolFloorAsync(rig.Run));

        foreach (var peer in peers)
        {
            Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(peer));
            using var response = await rig.Client(peer).PutAsync("items/a?value=1", null);
            Assert.True(response.IsSuccessStatusCode, $"a write on {peer.Id} answered {(int)response.StatusCode}");
        }
    }

    [Fact]
    public async Task Replay_M1_A6_a_raise_is_refused_while_a_protocol_1_member_holds_a_lease_and_that_member_goes_on_serving()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var old = (await ReplayFleet.StartAsync(rig, 1))[0];
        var newer = await rig.StartAsync("newer", ReplayApp.Spec(protocol: 2));
        await SliceRig.WaitForPhaseAsync(newer, ReplicaPhase.Serving, Limit);

        var gate = newer.Services.GetRequiredService<Reeve.Cluster.IClusterVersionGate>();
        var floorBefore = await ProtocolFloorAsync(rig.Run);
        Assert.False(await gate.RequireProtocolAsync(2).WaitAsync(Limit), "the raise answered true with a leased member that speaks 1");
        Assert.Equal(floorBefore, await ProtocolFloorAsync(rig.Run));

        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(old));
        Assert.Equal(MemberRowState.Active, (await rig.Run.Observer.MemberAsync(old.Id))!.State);
        foreach (var replica in new[] { old, newer })
        {
            using var response = await rig.Client(replica).PutAsync("items/a?value=1", null);
            Assert.True(response.IsSuccessStatusCode, $"a write on {replica.Id} answered {(int)response.StatusCode}");
        }
    }

    private static async Task<int> ProtocolFloorAsync(HarnessRun run)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT protocol_floor FROM reeve_cluster.cluster", connection);
        return (int)(await command.ExecuteScalarAsync())!;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_M1_N5_a_zero_row_renewal_fences_the_replica_within_one_renewal_of_the_delete(int replicas)
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var fleet = await ReplayFleet.StartAsync(rig, replicas);
        var r1 = fleet[0];
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Successful(rig.Run, r1).Count >= 1), "a renewal on record");

        Assert.Equal(1, await HarnessOperations.OperatorDelete(rig.Run, "r1"));
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Phases.Path("r1").Contains(ReplicaPhase.Fencing)), "r1 to fence", Limit);
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.FenceHistoryAsync("r1")).Count > 0, "the fence's record", Limit);

        Assert.Contains(MembershipActions.RenewZeroL, rig.Run.Events.Actions("r1"));
        Assert.Equal("zero-row-renewal", (await rig.Run.Observer.FenceHistoryAsync("r1"))[0].Trigger);

        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving, Limit);
        Assert.True((await rig.Run.Observer.MemberAsync("r1"))!.Epoch > 1, "the rejoin keeps its epoch");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_M1_D3_after_an_operators_delete_a_write_is_acknowledged_while_the_deleted_replica_serves_until_its_next_renewal(int replicas)
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var fleet = await ReplayFleet.StartAsync(rig, replicas);
        var r1 = fleet[0];
        var writer = fleet[^1];

        using var held = replicas == 2 ? r1.Faults.HoldNext(LogFaultPoints.PassBegin) : null;
        using var renewal = r1.Faults.Hold(MembershipFaultPoints.RenewSend);
        await renewal.WaitReachedAsync(timeout: Limit);
        Assert.Equal(1, await HarnessOperations.OperatorDelete(rig.Run, "r1"));

        var write = await rig.Client(writer).PutAsync("items/a?value=1", null);
        Assert.True(write.IsSuccessStatusCode, $"the write answered {(int)write.StatusCode}");
        var seq = (await SliceRig.ItemAsync(write)).Seq!.Value;

        Assert.DoesNotContain("r1", await rig.Run.Observer.WaitSetAsync(seq));
        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r1));
        if (replicas == 2)
        {
            Assert.True(ReplayState.HeadOf(r1) < seq, "r1 applied the write: the replay needs it behind");
        }

        renewal.Release();
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Phases.Path("r1").Contains(ReplicaPhase.Fencing)), "r1 to fence", Limit);
        await SliceRig.UntilAsync(async () => (await rig.Run.Observer.FenceHistoryAsync("r1")).Count > 0, "the fence's record", Limit);
        var path = rig.Run.Phases.Path("r1").ToList();
        Assert.Equal(ReplicaPhase.Fencing, path[path.LastIndexOf(ReplicaPhase.Serving) + 1]);
        Assert.Equal("zero-row-renewal", (await rig.Run.Observer.FenceHistoryAsync("r1"))[0].Trigger);
        held?.Release();
    }

    [Fact]
    public async Task Replay_M1_D1_with_clock_errors_at_their_bound_the_deadline_passes_the_old_clause_and_fence_start_still_precedes_the_expiry()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        const double rate = 0.85;
        var profile = TestProfile.Options("x", "x", profile: null);
        var lease = profile.LeaseTtl.TotalSeconds;
        var fenceStartOffset = (profile.LeaseTtl - profile.SafetyMargin - profile.StopBudget).TotalSeconds;
        Assert.True(fenceStartOffset / rate < lease, "the rate must keep the lease bound under the current rule");
        Assert.True((lease - profile.SafetyMargin.TotalSeconds) / rate > lease, "the rate must break the lease bound for a fence start without StopBudget");

        var r1 = await rig.StartAsync("r1", ReplayApp.Spec(clockRate: rate));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var stalls = rig.Run.Stalls.Begin();
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Successful(rig.Run, r1).Count >= 2), "two renewals on record");

        await r1.Link!.CutAsync();
        var (members, _) = await rig.Run.Observer.MembersAsync();
        var expiry = members.Single().LeaseExpiresAt;
        var before = Renewals.Successful(rig.Run, r1)[^1];

        await SliceRig.UntilAsync(() => Task.FromResult(Fencing(rig, r1) is not null), "the fence to start", Limit);

        var offset = Renewals.Seconds(r1, Fencing(rig, r1)!.Timestamp) - Renewals.Seconds(r1, before.T0);
        var onTime = new Window("fence start at rate 0.85", TimeSpan.FromSeconds(fenceStartOffset - 0.4), TimeSpan.FromSeconds(fenceStartOffset + 0.8))
            .AssertContains(TimeSpan.FromSeconds(offset), stalls, output.WriteLine);
        if (!onTime)
        {
            return;
        }

        var readAt = r1.Clock.GetTimestamp();
        var databaseNow = (await rig.Run.Observer.MembersAsync()).DatabaseNow;
        var sinceFence = TimeSpan.FromSeconds((readAt - Fencing(rig, r1)!.Timestamp) / (double)r1.Clock.TimestampFrequency / r1.Clock.Rate);
        var databaseNowAtFence = databaseNow - sinceFence;
        Assert.True(databaseNowAtFence < expiry, $"the fence was at {databaseNowAtFence:O} on the database's clock, after the database's expiry {expiry:O}");
        var deadline = databaseNowAtFence!.Value + profile.StopBudget / rate;
        Assert.True(deadline > expiry - profile.SafetyMargin, "the local deadline is not after the expiry less SafetyMargin: the replay does not reach the old clause");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_M1_S6_a_local_fault_fences_once_per_poison_entry_and_the_rejoin_stays_in_Loading(int replicas)
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var fleet = await ReplayFleet.StartAsync(rig, replicas, ReplayApp.Spec(TightStall));
        var poisoned = fleet[^1];
        HarnessOperations.ReconcilerThrow(poisoned, ReplayApp.Widget, () => new InvalidOperationException("poison"), times: 100_000);

        await ReplayWrites.CommitAsync(fleet[0], "poison");

        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(poisoned.Id).Contains(MembershipActions.FenceStall)), "the stall's fence", Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions(poisoned.Id).Contains(MembershipActions.Rejoin)), "the rejoin", Limit);
        await SliceRig.WaitForPhaseAsync(poisoned, ReplicaPhase.Loading, Limit);

        var window = 3 * (poisoned.Options.ReconcileTimeout + poisoned.Options.ApplyStallLimit) + poisoned.Options.RejoinBackoff;
        var from = poisoned.Clock.GetTimestamp();
        await SliceRig.UntilAsync(() => Task.FromResult(poisoned.Clock.GetElapsedTime(from) >= window), "three stall limits and a RejoinBackoff on the replica's clock", window + Limit);
        var actions = rig.Run.Events.Actions(poisoned.Id);
        Assert.Equal(1, actions.Count(a => a == MembershipActions.FenceStall));
        Assert.Equal(ReplicaPhase.Loading, SliceRig.PhaseOf(poisoned));
        Assert.Equal(MemberRowState.Joining, (await rig.Run.Observer.MemberAsync(poisoned.Id))!.State);
        var afterFence = rig.Run.Phases.Path(poisoned.Id).SkipWhile(p => p != ReplicaPhase.Fenced).ToList();
        Assert.DoesNotContain(afterFence, p => p is ReplicaPhase.Activated or ReplicaPhase.Serving);

        HarnessOperations.FixFault(rig.Run);
        await SliceRig.WaitForPhaseAsync(poisoned, ReplicaPhase.Serving, Limit);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_M1_R1_the_skip_endpoint_is_reached_while_every_replica_is_fenced_and_the_fleet_serves_after_it(int replicas)
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var fleet = await ReplayFleet.StartAsync(rig, replicas, ReplayApp.Spec(TightStall));
        foreach (var replica in fleet)
        {
            HarnessOperations.ReconcilerThrow(replica, ReplayApp.Widget, () => new InvalidOperationException("poison"), times: 100_000);
        }

        var seq = await ReplayWrites.CommitAsync(fleet[0], "poison");
        foreach (var replica in fleet)
        {
            await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Phases.Path(replica.Id).Contains(ReplicaPhase.Fencing)), $"{replica.Id} to fence", Limit);
        }

        Assert.All(fleet, r => Assert.NotEqual(ReplicaPhase.Serving, SliceRig.PhaseOf(r)));
        using var response = await rig.Client(fleet[0]).PostAsJsonAsync("skips", new { aggregateType = ReplayApp.Widget, fromSeq = seq, reason = "poison", openedBy = "ops" });
        Assert.Equal(System.Net.HttpStatusCode.Created, response.StatusCode);

        foreach (var replica in fleet)
        {
            await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Serving, Limit);
        }

        using var read = await rig.Client(fleet[0]).GetAsync("ping");
        Assert.Equal(System.Net.HttpStatusCode.OK, read.StatusCode);
    }

    [Fact]
    public async Task Replay_SIMP_5_a_restore_under_running_replicas_rewinds_the_epoch_sequence_and_each_live_holder_fences_on_its_next_renewal()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);

        await using var backup = await HarnessOperations.TakeBackup(rig.Run);
        var fleet = await ReplayFleet.StartAsync(rig, 2);
        var held = new Dictionary<string, long>();
        foreach (var replica in fleet)
        {
            held[replica.Id] = (await rig.Run.Observer.MemberAsync(replica.Id))!.Epoch;
        }

        Assert.Equal(2, held.Values.Distinct().Count());
        using (var write = await rig.Client(fleet[0]).PutAsync("items/kept?value=1", null))
        {
            Assert.True(write.IsSuccessStatusCode);
        }

        var before = rig.Run.Sequence.Next();
        await HarnessOperations.RestoreDatabase(rig.Run, backup);
        Assert.Empty((await rig.Run.Observer.MembersAsync()).Members);

        foreach (var replica in fleet)
        {
            await SliceRig.UntilAsync(
                () => Task.FromResult(rig.Run.Events.Of(replica.Id).Any(e => e.Sequence > before && e.Action == MembershipActions.RenewZeroL)),
                $"{replica.Id} to meet zero rows",
                Limit);
            var execs = rig.Run.Events.Of(replica.Id).Where(e => e.Sequence > before && e.Action == MembershipActions.RenewExec).ToList();
            Assert.NotEmpty(execs);
            Assert.All(execs, e => Assert.Equal(0L, Convert.ToInt64(e.State!["rows"], System.Globalization.CultureInfo.InvariantCulture)));
        }

        var drawn = new HashSet<long>();
        var exited = new List<Replica>();
        foreach (var replica in fleet)
        {
            await SliceRig.UntilAsync(
                () => Task.FromResult(SliceRig.PhaseOf(replica) is ReplicaPhase.Exited or ReplicaPhase.Serving && rig.Run.Phases.Path(replica.Id).Count(p => p == ReplicaPhase.Fencing) == 1),
                $"{replica.Id} to exit or serve",
                Limit);
            var epoch = (await rig.Run.Observer.MemberAsync(replica.Id))!.Epoch;
            Assert.True(drawn.Add(epoch), "two processes were given one epoch");
            Assert.Equal(epoch <= held[replica.Id], SliceRig.PhaseOf(replica) == ReplicaPhase.Exited);
            if (SliceRig.PhaseOf(replica) == ReplicaPhase.Exited)
            {
                exited.Add(replica);
            }
        }

        Assert.True(drawn.SetEquals(held.Values), $"the drawn epochs {string.Join(",", drawn)} are not the ones the sequence handed out before ({string.Join(",", held.Values)})");
        Assert.NotEmpty(exited);

        var restarted = new List<Replica>();
        foreach (var replica in exited)
        {
            await replica.StopAsync();
            var again = await rig.StartAsync(replica.Id, ReplayApp.Spec());
            await SliceRig.WaitForPhaseAsync(again, ReplicaPhase.Serving);
            restarted.Add(again);
        }

        foreach (var replica in restarted)
        {
            Assert.DoesNotContain((await rig.Run.Observer.MemberAsync(replica.Id))!.Epoch, drawn);
        }
    }

    private static PhaseChange? Fencing(SliceRig rig, Replica replica) =>
        rig.Run.Phases.Of(replica.Id).FirstOrDefault(c => c.To == ReplicaPhase.Fencing);
}

internal static class ReplayFleet
{
    public static async Task<IReadOnlyList<Replica>> StartAsync(SliceRig rig, int replicas, ReplicaSpec? spec = null)
    {
        var fleet = new List<Replica>();
        for (var i = 1; i <= replicas; i++)
        {
            var replica = await rig.StartAsync($"r{i}", spec ?? ReplayApp.Spec());
            await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Serving);
            fleet.Add(replica);
        }

        return fleet;
    }
}

internal static class ReplayWrites
{
    public static async Task<long> CommitAsync(Replica replica, string id)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        var item = await SliceApp.WriteAsync(replica.Services.GetRequiredService<Reeve.Cluster.IChangeLog>(), replica.Options, ReplayApp.Widget, id, "v", CancellationToken.None);
        return item.Seq!.Value;
    }
}

internal static class ReplayState
{
    public static long HeadOf(Replica replica) =>
        replica.Services.GetRequiredService<Reeve.Cluster.Log.GenerationStore>().Current!.Head;
}
