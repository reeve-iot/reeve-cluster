using System.Diagnostics;
using System.Net;
using Harness.SelfTests;
using Harness.Slice;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Replay.Membership;

[Collection(StackCollection.Name)]
public sealed class OneReplicaTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_write_in_flight_at_the_stop_of_the_only_replica_is_acknowledged_by_its_own_cursor_and_the_draining_write_comes_after()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = (await ReplayFleet.StartAsync(rig, 1))[0];

        using var pass = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = rig.Client(r1).PutAsync("items/h1?value=one", null);
        await pass.WaitReachedAsync(timeout: Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Actions("r1").Contains(LogActions.AckBegin)), "the acknowledgement wait to begin");
        Assert.False(put.IsCompleted, "the write was answered before its own applier applied it");

        var stop = r1.StopAsync();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Draining);
        using (var read = await rig.Client(r1).GetAsync("ping"))
        {
            Assert.Equal(HttpStatusCode.ServiceUnavailable, read.StatusCode);
            Assert.Equal("replica_not_serving", await SliceRig.ErrorCodeAsync(read));
        }

        var during = (await rig.Run.Observer.MemberAsync("r1"))!;
        Assert.Equal(MemberRowState.Active, during.State);
        Assert.Null(during.ServingSince);
        Assert.False(stop.IsCompleted, "the stop finished while the write was in flight");

        pass.Release();
        using var response = await put.WaitAsync(Limit);
        await stop.WaitAsync(Limit);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(MemberRowState.Draining, (await rig.Run.Observer.MemberAsync("r1"))!.State);
        var events = rig.Run.Events.Of("r1");
        Assert.Equal(
            [MembershipActions.Drain1, MembershipActions.Drain23, MembershipActions.Drain23, MembershipActions.Drain4],
            events.Where(e => e.Action is MembershipActions.Drain1 or MembershipActions.Drain23 or MembershipActions.Drain4).Select(e => e.Action));
        Assert.True(events.Last(e => e.Action == LogActions.AckDone).Sequence < events.First(e => e.Action == MembershipActions.Drain4).Sequence);
    }

    [Fact]
    public async Task After_an_offline_restore_the_only_replica_loads_in_full_from_the_restored_head_and_draws_its_epoch_from_the_restored_sequence()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = (await ReplayFleet.StartAsync(rig, 1))[0];
        var atBackup = await PutAsync(rig, r1, "before");
        await r1.StopAsync();
        var epochAtBackup = await ScalarAsync(rig, "SELECT last_value FROM reeve_cluster.member_epoch");
        await using var backup = await HarnessOperations.TakeBackup(rig.Run);

        var r1b = await rig.StartAsync("r1", ReplayApp.Spec());
        await SliceRig.WaitForPhaseAsync(r1b, ReplicaPhase.Serving);
        var afterBackup = await PutAsync(rig, r1b, "after");
        Assert.True(afterBackup > atBackup);
        Assert.True(await ScalarAsync(rig, "SELECT last_value FROM reeve_cluster.member_epoch") > epochAtBackup);
        await r1b.StopAsync();

        await HarnessOperations.RestoreDatabase(rig.Run, backup);
        var restored = rig.Run.Sequence.Next();
        Assert.Equal(atBackup, await ScalarAsync(rig, "SELECT coalesce(max(seq), 0) FROM reeve_cluster.change_log"));

        var r1c = await rig.StartAsync("r1", ReplayApp.Spec());
        await SliceRig.WaitForPhaseAsync(r1c, ReplicaPhase.Serving);
        var first = rig.Run.Events.Snapshot().First(e => e.Replica == "r1" && e.Sequence > restored && e.Action == LogActions.PassBegin);
        Assert.True((bool)first.State!["full"]!, "the first pass after the restore is a load");
        Assert.Equal(atBackup, ReplayState.HeadOf(r1c));
        Assert.Equal(epochAtBackup + 1, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);

        Assert.True(await PutAsync(rig, r1c, "again") > atBackup);
    }

    private static async Task<long> PutAsync(SliceRig rig, Replica replica, string id)
    {
        using var response = await rig.Client(replica).PutAsync($"items/{id}?value=1", null);
        response.EnsureSuccessStatusCode();
        return (await SliceRig.ItemAsync(response)).Seq!.Value;
    }

    private static async Task<long> ScalarAsync(SliceRig rig, string sql)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }
}
