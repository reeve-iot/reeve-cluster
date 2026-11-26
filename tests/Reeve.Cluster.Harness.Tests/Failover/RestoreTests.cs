using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Failover;

[Collection(StackCollection.Name)]
public sealed class RestoreTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = FailoverRig.Limit;

    [Fact]
    public async Task After_an_offline_restore_below_their_cursors_every_replica_loads_in_full_epochs_come_from_the_restored_sequence_and_the_skip_rows_rewind()
    {
        await using var rig = await FailoverRig.OnAsync(stack.Stack, output.WriteLine);
        var spec = FailoverApp.Spec();

        var (r1, r2) = await rig.StartPairAsync(spec);
        var atBackup = await rig.PutAsync(r1, "before", "1");
        var wasOpenAtBackup = await OpenSkipAsync(r1, "ledger", atBackup + 1);
        await r1.StopAsync();
        await r2.StopAsync();
        var epochAtBackup = await rig.ScalarAsync("SELECT last_value FROM reeve_cluster.member_epoch");
        await using var backup = await HarnessOperations.TakeBackup(rig.Run);

        var (r1b, r2b) = await rig.StartPairAsync(spec);
        var afterBackup = await rig.PutAsync(r1b, "after", "2");
        Assert.True(await rig.ScalarAsync("SELECT last_value FROM reeve_cluster.member_epoch") > epochAtBackup);
        await r1b.Services.GetRequiredService<IChangeSkip>().CloseAsync(wasOpenAtBackup, "ops");
        var openedAfterBackup = await OpenSkipAsync(r1b, "ledger2", afterBackup + 1);
        var cursorBeforeRestore = FailoverApp.HeadOf(r2b);
        Assert.True(cursorBeforeRestore > atBackup);
        await r1b.StopAsync();
        await r2b.StopAsync();

        var planted = $"{{{rig.Run.RedisKeyPrefix}owned:planted";
        var foreign = "{other-run:owned:planted";
        await using (var redis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(rig.Stack.ObserverRedisConnectionString))
        {
            await redis.GetDatabase().StringSetAsync(planted, "1");
            await redis.GetDatabase().StringSetAsync(foreign, "1");
        }

        await HarnessOperations.RestoreDatabase(rig.Run, backup);
        await using (var redis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(rig.Stack.ObserverRedisConnectionString))
        {
            Assert.False(await redis.GetDatabase().KeyExistsAsync(planted), "the restore left a library Redis key of the run");
            Assert.True(await redis.GetDatabase().KeyExistsAsync(foreign), "the restore cleared a key of another run");
            await redis.GetDatabase().KeyDeleteAsync(foreign);
        }

        var restored = rig.Run.Sequence.Next();

        Assert.Equal(1, await rig.ScalarAsync($"SELECT count(*) FROM reeve_cluster.change_skip WHERE id = {wasOpenAtBackup} AND closed_at IS NULL"));
        Assert.Equal(0, await rig.ScalarAsync($"SELECT count(*) FROM reeve_cluster.change_skip WHERE id = {openedAfterBackup}"));
        Assert.Equal(atBackup, await rig.HeadAsync());
        Assert.Equal(epochAtBackup, await rig.ScalarAsync("SELECT last_value FROM reeve_cluster.member_epoch"));

        var r1c = await rig.StartAsync("r1", spec);
        var r2c = await rig.StartAsync("r2", spec);
        foreach (var replica in new[] { r1c, r2c })
        {
            var first = rig.Run.Events.Snapshot()
                .First(e => e.Replica == replica.Id && e.Sequence > restored && e.Action == LogActions.PassBegin);
            Assert.True((bool)first.State!["full"]!, $"{replica.Id}'s first pass after the restore is a load");
            Assert.Equal(["s:before"], FailoverApp.StateOf(replica).Keys.ToArray());
            Assert.Equal(atBackup, FailoverApp.HeadOf(replica));
        }

        Assert.Equal(epochAtBackup + 1, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
        Assert.Equal(epochAtBackup + 2, (await rig.Run.Observer.MemberAsync("r2"))!.Epoch);

        await rig.PutAsync(r1c, "again", "3");
        Assert.Equal("3", FailoverApp.StateOf(r2c)["s:again"]);
    }

    private static async Task<long> OpenSkipAsync(Replica replica, string type, long from) =>
        Assert.IsType<SkipOpenResult.Opened>(await replica.Services.GetRequiredService<IChangeSkip>().OpenAsync(new SkipRequest(type, from, "poison", "ops"))).SkipId;
}
