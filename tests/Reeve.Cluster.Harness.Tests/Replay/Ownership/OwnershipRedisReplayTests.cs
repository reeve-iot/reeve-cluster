using System.Collections.Concurrent;
using System.Diagnostics;
using Harness.Owned;
using Harness.Placer;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using StackExchange.Redis;
using Xunit.Abstractions;
using OwnedWriter = Harness.Owned.OwnedWriter;

namespace Harness.Replay.Ownership;

[Collection(StackCollection.Name)]
public sealed class OwnershipRedisReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = "fenced";
    private static readonly EligibilityFilter Hosting = EligibilityFilter.Any.Require(PlacerRig.WidgetHost);

    [Fact]
    public async Task Replay_M2_A4_a_delayed_fenced_write_is_refused_once_the_fence_key_has_expired_with_the_lease()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100)));
                services.AddClusterUnit<OwnedWriter>(Kind, o => o.RedisFence = true);
            },
            redis: true);
        var r1 = await rig.StartAsync("r1", spec);
        var first = await rig.Log.StartAsync(s => s.Kind == Kind, "tenure t1 on r1");
        var database = await rig.RedisAsync();
        var tag = first.Context.RedisHashTag();
        var fence = tag + ":fence";
        var highWater = tag + ":hw";
        var list = OwnedWriter.RedisLogKey(tag);
        await OwnedRig.UntilAsync(async () => await TokenAsync(database, fence) == first.Token, "the install raise");

        var ttl = await database.KeyTimeToLiveAsync(fence);
        Assert.True(await database.KeyExistsAsync(highWater));
        Assert.Null(await database.KeyTimeToLiveAsync(highWater));

        using var write = DelayedWrite.Start(first.Context, list);
        await write.EnteredAsync();
        using var noRenewals = r1.Faults.Hold(MembershipFaultPoints.RenewSend);
        await noRenewals.WaitReachedAsync(timeout: OwnedRig.Limit);
        using var step5 = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep5);
        r1.Services.GetRequiredService<LocalPhaseGate>().RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await step5.WaitReachedAsync(timeout: OwnedRig.Limit);

        var expiry = Stopwatch.StartNew();
        var bound = (ttl ?? r1.Options.LeaseTtl) + TimeSpan.FromSeconds(10);
        while (await database.KeyExistsAsync(fence) && expiry.Elapsed < bound)
        {
            await Task.Delay(50);
        }

        await OwnedRig.UntilAsync(async () => await rig.ScalarAsync("SELECT lease_expires_at <= clock_timestamp() FROM reeve_cluster.member WHERE replica_id = 'r1'") == 1, "the lease to run out");
        Assert.True(await database.KeyExistsAsync(highWater));

        write.Release();
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => write.Result);
        Assert.Equal(0, await database.ListLengthAsync(list));
        Assert.False(await database.KeyExistsAsync(fence), "the fence key did not expire with the lease");
        Assert.True(
            ttl is { } remaining && remaining > TimeSpan.Zero && remaining <= r1.Options.LeaseTtl,
            $"the fence key's TTL was {ttl?.ToString() ?? "none"}, not within (0, LeaseTtl = {r1.Options.LeaseTtl}]");
    }

    [Fact]
    public async Task Replay_M2_A4_residual_a_stale_write_lands_after_the_lease_and_before_a_successor_installs_and_is_refused_after()
    {
        await using var staged = await StageAsync();
        var (rig, r1, r2, first, database) = (staged.Rig, staged.Owner, staged.Successor, staged.First, staged.Redis);
        var list = OwnedWriter.RedisLogKey(staged.Tag);

        using var landing = DelayedWrite.Start(first.Context, list);
        using var refused = DelayedWrite.Start(first.Context, list);
        await landing.EnteredAsync();
        await refused.EnteredAsync();

        using var noRenewals = r1.Faults.Hold(MembershipFaultPoints.RenewSend);
        await noRenewals.WaitReachedAsync(timeout: OwnedRig.Limit);
        await EndLeaseAsync(rig, "r1");
        Assert.Equal(1, await rig.ScalarAsync("SELECT lease_expires_at <= clock_timestamp() FROM reeve_cluster.member WHERE replica_id = 'r1'"));
        Assert.Equal(first.Token, await TokenAsync(database, staged.Fence));
        Assert.DoesNotContain(rig.Log.Starts, s => s is { Kind: Kind, Replica: "r2" });

        landing.Release();
        Assert.True(await landing.Result.WaitAsync(OwnedRig.Limit), "the stale write, sent after the lease ended and before a successor installed, did not land");
        Assert.Equal([first.Token], await ListAsync(database, list));

        await PlacerRig.HostWidgetsAsync(r2);
        var second = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "tenure t2 on r2");
        Assert.True(second.Token > first.Token);
        await OwnedRig.UntilAsync(async () => await TokenAsync(database, staged.Fence) == second.Token, "the successor's fence key");
        refused.Release();
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => refused.Result);

        Assert.True(await second.Context.FenceRedisAsync(t => _ = t.ListRightPushAsync(list, second.Token)));
        var entries = await ListAsync(database, list);
        Assert.Equal([first.Token, second.Token], entries);
        OwnedRig.AssertNoInterleave(entries, "the Redis list in the order Redis took it");
    }

    [Fact]
    public async Task Replay_M2_D1_a_stale_raise_finds_the_high_water_key_above_it_and_does_not_re_install_after_the_successors_key_expired()
    {
        await using var staged = await StageAsync();
        var (rig, r1, r2, first, database) = (staged.Rig, staged.Owner, staged.Successor, staged.First, staged.Redis);
        var list = OwnedWriter.RedisLogKey(staged.Tag);

        using var slowPoll = r1.Faults.Hold(OwnedFaultPoints.PollBeforeRead);

        using var staleRaise = r1.Faults.HoldNext(OwnedFaultPoints.RedisBeforeExec);
        await staleRaise.WaitReachedAsync(timeout: OwnedRig.Limit);
        using var noRenewals = r1.Faults.Hold(MembershipFaultPoints.RenewSend);
        await noRenewals.WaitReachedAsync(timeout: OwnedRig.Limit);
        using var staleWrite = DelayedWrite.Start(first.Context, list);
        await staleWrite.EnteredAsync();

        await EndLeaseAsync(rig, "r1");
        await PlacerRig.HostWidgetsAsync(r2);
        var second = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "tenure t2 on r2");
        Assert.True(second.Token > first.Token);
        await OwnedRig.UntilAsync(async () => await TokenAsync(database, staged.Fence) == second.Token && await TokenAsync(database, staged.HighWater) == second.Token, "the successor's install");
        using var successorQuiet = r2.Faults.Hold(MembershipFaultPoints.RenewSend);
        await successorQuiet.WaitReachedAsync(timeout: OwnedRig.Limit);
        await database.KeyExpireAsync(staged.Fence, TimeSpan.FromMilliseconds(1));
        await OwnedRig.UntilAsync(async () => !await database.KeyExistsAsync(staged.Fence), "the successor's fence key to expire");
        Assert.Equal(second.Token, await TokenAsync(database, staged.HighWater));

        Assert.True(staleRaise.Release());
        await OwnedRig.UntilAsync(
            async () => staged.Logged("[r1]", $"holds a token above tenure {first.Token}:") || await TokenAsync(database, staged.Fence) == first.Token,
            "the stale raise to decide");
        Assert.NotEqual(first.Token, await TokenAsync(database, staged.Fence));
        Assert.Equal(second.Token, await TokenAsync(database, staged.HighWater));
        Assert.True(staged.Logged("[r1]", $"holds a token above tenure {first.Token}:"), "the stale raise was not refused");

        staleWrite.Release();
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => staleWrite.Result);
        Assert.Equal(0, await database.ListLengthAsync(list));
    }

    [Fact]
    public async Task Replay_M2_F2_fence_step_5_leaves_the_successors_fence_key()
    {
        await using var staged = await StageAsync();
        var (rig, r1, r2, first, database) = (staged.Rig, staged.Owner, staged.Successor, staged.First, staged.Redis);
        var list = OwnedWriter.RedisLogKey(staged.Tag);

        using var step5 = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep5);
        r1.Services.GetRequiredService<LocalPhaseGate>().RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await step5.WaitReachedAsync(timeout: OwnedRig.Limit);
        await EndLeaseAsync(rig, "r1");

        await PlacerRig.HostWidgetsAsync(r2);
        var second = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "tenure t2 on r2");
        Assert.True(second.Token > first.Token);
        await OwnedRig.UntilAsync(async () => await TokenAsync(database, staged.Fence) == second.Token, "the successor's fence key");

        using var successorQuiet = r2.Faults.Hold(MembershipFaultPoints.RenewSend);
        await successorQuiet.WaitReachedAsync(timeout: OwnedRig.Limit);

        Assert.True(step5.Release());
        await OwnedRig.UntilAsync(() => Task.FromResult(staged.Logged("[r1]", "Fence step 5", $"tenure {first.Token}.")), "fence step 5's delete");
        Assert.True(staged.Logged("[r1]", "Fence step 5 left the Redis fence key", $"tenure {first.Token}."), "step 5 deleted a fence key that was not the stale tenure's");
        Assert.Equal(second.Token, await TokenAsync(database, staged.Fence));
        Assert.Equal(second.Token, await TokenAsync(database, staged.HighWater));
        Assert.True(await second.Context.FenceRedisAsync(t => _ = t.ListRightPushAsync(list, second.Token)), "the successor's fenced write did not land");
        Assert.Equal([second.Token], await ListAsync(database, list));
    }

    private async Task<Staged> StageAsync()
    {
        var lines = new ConcurrentQueue<string>();
        var rig = await OwnedRig.CreateAsync(
            stack,
            line =>
            {
                output.WriteLine(line);
                lines.Enqueue(line);
            });
        try
        {
            var spec = rig.Spec(
                services =>
                {
                    services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100)));
                    services.AddClusterUnit<OwnedWriter>(Kind, o =>
                    {
                        o.RedisFence = true;
                        o.Eligibility = Hosting;
                    });
                },
                configure: o => o.StabilisationWindow = TimeSpan.FromMinutes(10),
                redis: true,
                profile: o =>
                {
                    TestProfile.ApplyStallTolerant(o);
                    o.LeaseTtl = TimeSpan.FromMinutes(10);
                });
            var successor = await rig.StartAsync("r2", spec);
            var owner = await rig.StartAsync("r1", spec);
            await PlacerRig.HostWidgetsAsync(owner);
            var first = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r1" }, "tenure t1 on r1");
            var database = await rig.RedisAsync();
            var tag = first.Context.RedisHashTag();
            await OwnedRig.UntilAsync(
                async () => await TokenAsync(database, tag + ":fence") == first.Token && await TokenAsync(database, tag + ":hw") == first.Token,
                "the install raise");
            return new Staged(rig, lines, owner, successor, first, database, tag);
        }
        catch
        {
            await rig.DisposeAsync();
            throw;
        }
    }

    private static Task EndLeaseAsync(OwnedRig rig, string replica) =>
        rig.ExecuteAsync($"UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE replica_id = '{replica}'");

    private static async Task<long?> TokenAsync(IDatabase database, string key)
    {
        var value = await database.StringGetAsync(key);
        return value.HasValue ? (long)value : null;
    }

    private static async Task<long[]> ListAsync(IDatabase database, string key) => [.. (await database.ListRangeAsync(key)).Select(v => (long)v)];

    private sealed class Staged(OwnedRig rig, ConcurrentQueue<string> lines, Replica owner, Replica successor, OwnedStart first, IDatabase redis, string tag) : IAsyncDisposable
    {
        public OwnedRig Rig => rig;

        public Replica Owner => owner;

        public Replica Successor => successor;

        public OwnedStart First => first;

        public IDatabase Redis => redis;

        public string Tag => tag;

        public string Fence => tag + ":fence";

        public string HighWater => tag + ":hw";

        public bool Logged(params string[] parts) => lines.Any(line => parts.All(line.Contains));

        public ValueTask DisposeAsync() => rig.DisposeAsync();
    }

    private sealed class DelayedWrite : IDisposable
    {
        private readonly ManualResetEventSlim entered = new();
        private readonly ManualResetEventSlim proceed = new();

        private DelayedWrite(OwnedContext context, string list) =>
            Result = Task.Run(() => context.FenceRedisAsync(transaction =>
            {
                entered.Set();
                proceed.Wait(OwnedRig.Limit * 2);
                _ = transaction.ListRightPushAsync(list, context.Token);
            }));

        public Task<bool> Result { get; }

        public static DelayedWrite Start(OwnedContext context, string list) => new(context, list);

        public Task EnteredAsync() => OwnedRig.UntilAsync(() => Task.FromResult(entered.IsSet), "the write to pass its owner check");

        public void Release() => proceed.Set();

        public void Dispose() => proceed.Set();
    }
}
