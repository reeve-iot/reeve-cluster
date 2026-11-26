using Int.Owned;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Fence;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class FenceRedisTests : IClassFixture<RedisFixture>, IAsyncLifetime
{
    private const string Kind = "consumer";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LongTtl = TimeSpan.FromMinutes(5);

    private readonly RedisFixture redis;
    private readonly string prefix = $"t{Guid.NewGuid():N}:";
    private ConnectionMultiplexer direct = null!;
    private ConnectionMultiplexer proxied = null!;
    private MultiHoldProxy proxy = null!;

    public FenceRedisTests(RedisFixture redis) => this.redis = redis;

    private IDatabase Db => direct.GetDatabase();

    public async Task InitializeAsync()
    {
        direct = await redis.ConnectAsync();
        proxy = new MultiHoldProxy(redis.ConnectionString);

        var options = ConfigurationOptions.Parse(proxy.Endpoint);
        options.AsyncTimeout = (int)Wait.TotalMilliseconds;
        options.SyncTimeout = (int)Wait.TotalMilliseconds;
        proxied = await ConnectionMultiplexer.ConnectAsync(options);
    }

    public async Task DisposeAsync()
    {
        await proxy.DisposeAsync();
        await proxied.DisposeAsync();
        await direct.DisposeAsync();
    }

    private TenureFence Fence(long token, string key = "k", TimeSpan? ttl = null, IDatabase? database = null, IFaultPoints? points = null) =>
        new(database ?? Db, new FencedOwner(Kind, key, token, timeline: 1), points ?? NoFaultPoints.Instance, prefix, ttl ?? LongTtl);

    private static FencedOwner Owner(string key, long token) => new(Kind, key, token, timeline: 1);

    private TenureFence Fence(OwnedContext owner, IDatabase? database = null, IFaultPoints? points = null) =>
        new(database ?? Db, owner, points ?? NoFaultPoints.Instance, prefix, LongTtl);

    private static void Set(ITransaction transaction, RedisKey key, string value) => _ = transaction.StringSetAsync(key, value);

    private string State(string key = "k") => TenureFence.HashTagOf(prefix, Kind, key) + ":position";

    private async Task AwaitGoneAsync(RedisKey key)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (await Db.KeyExistsAsync(key))
        {
            await Task.Delay(20, bound.Token);
        }
    }

    [Fact]
    public async Task The_raise_installs_both_keys_under_one_hash_tag_the_fence_key_with_its_ttl_and_the_high_water_key_without()
    {
        var fence = Fence(7);

        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));

        Assert.Equal($"{{{prefix}owned:{Kind}:k}}:fence", (string?)fence.FenceKey);
        Assert.Equal($"{{{prefix}owned:{Kind}:k}}:hw", (string?)fence.HighWaterKey);
        Assert.Equal(fence.HashTag + ":fence", (string?)fence.FenceKey);
        Assert.Equal(7, (long)await Db.StringGetAsync(fence.FenceKey));
        Assert.Equal(7, (long)await Db.StringGetAsync(fence.HighWaterKey));
        var ttl = await Db.KeyTimeToLiveAsync(fence.FenceKey);
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value, TimeSpan.FromMinutes(4), LongTtl);
        Assert.Null(await Db.KeyTimeToLiveAsync(fence.HighWaterKey));
        Assert.True(fence.Raised);
    }

    [Fact]
    public async Task A_fenced_write_is_refused_until_the_raise_succeeded_and_lands_after_it()
    {
        var fence = Fence(3);

        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => fence.WriteAsync(t => Set(t, State(), "a"), CancellationToken.None));
        Assert.False(await Db.KeyExistsAsync(State()));

        await fence.RaiseAsync(CancellationToken.None);
        Assert.True(await fence.WriteAsync(t => Set(t, State(), "b"), CancellationToken.None));
        Assert.Equal("b", (string?)await Db.StringGetAsync(State()));
    }

    [Fact]
    public async Task A_check_that_refuses_sends_neither_the_raise_nor_the_write()
    {
        var owner = new FencedOwner(Kind, "k", 5, timeline: 1);
        var fence = new TenureFence(Db, owner, NoFaultPoints.Instance, prefix, LongTtl);
        await fence.RaiseAsync(CancellationToken.None);
        await Db.KeyDeleteAsync([fence.FenceKey, fence.HighWaterKey]);
        owner.Refusal = new OwnershipLostException("stopping");

        await Assert.ThrowsAsync<OwnershipLostException>(() => fence.RaiseAsync(CancellationToken.None));
        await Assert.ThrowsAsync<OwnershipLostException>(() => fence.WriteAsync(t => Set(t, State(), "x"), CancellationToken.None));

        Assert.False(await Db.KeyExistsAsync(fence.FenceKey));
        Assert.False(await Db.KeyExistsAsync(fence.HighWaterKey));
        Assert.False(await Db.KeyExistsAsync(State()));
    }

    [Fact]
    public async Task A_stale_raise_is_refused_and_lowers_neither_key_also_after_the_successors_fence_key_was_deleted_or_expired()
    {
        var stale = Fence(4);
        await stale.RaiseAsync(CancellationToken.None);
        Assert.True(stale.Raised);
        var successor = Fence(9);
        await successor.RaiseAsync(CancellationToken.None);

        Assert.Equal(RaiseOutcome.Refused, await stale.RaiseAsync(CancellationToken.None));
        Assert.False(stale.Raised);
        Assert.Equal(9, (long)await Db.StringGetAsync(successor.FenceKey));

        await Db.KeyExpireAsync(successor.FenceKey, TimeSpan.FromMilliseconds(1));
        await AwaitGoneAsync(successor.FenceKey);
        Assert.Equal(RaiseOutcome.Refused, await stale.RaiseAsync(CancellationToken.None));
        Assert.False(await Db.KeyExistsAsync(successor.FenceKey));

        await successor.RaiseAsync(CancellationToken.None);
        await Db.KeyDeleteAsync(successor.FenceKey);
        Assert.Equal(RaiseOutcome.Refused, await stale.RaiseAsync(CancellationToken.None));
        Assert.False(await Db.KeyExistsAsync(successor.FenceKey));
        Assert.Equal(9, (long)await Db.StringGetAsync(successor.HighWaterKey));
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => stale.WriteAsync(t => Set(t, State(), "stale"), CancellationToken.None));
    }

    [Fact]
    public async Task The_owner_installs_its_own_absent_fence_key_again_while_no_higher_token_is_recorded()
    {
        var fence = Fence(6);
        await fence.RaiseAsync(CancellationToken.None);
        await Db.KeyDeleteAsync(fence.FenceKey);
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => fence.WriteAsync(t => Set(t, State(), "a"), CancellationToken.None));

        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        Assert.True(await fence.WriteAsync(t => Set(t, State(), "b"), CancellationToken.None));
    }

    [Fact]
    public async Task An_EXEC_held_on_the_wire_past_the_successors_install_is_refused_also_when_the_successors_fence_key_was_deleted()
    {
        foreach (var deleteAfterInstall in new[] { false, true })
        {
            var key = $"k{deleteAfterInstall}";
            var owner = Fence(10, key, database: proxied.GetDatabase());
            await owner.RaiseAsync(CancellationToken.None);
            var hold = proxy.HoldNextMulti();
            var write = owner.WriteAsync(t => Set(t, State(key), "old"), CancellationToken.None);
            await hold.Held.WaitAsync(Wait);

            var successor = Fence(11, key);
            Assert.Equal(RaiseOutcome.Installed, await successor.RaiseAsync(CancellationToken.None));
            if (deleteAfterInstall)
            {
                await Db.KeyDeleteAsync(successor.FenceKey);
            }

            hold.Release();
            await Assert.ThrowsAsync<RedisFenceRefusedException>(() => write.WaitAsync(Wait));
            Assert.False(await Db.KeyExistsAsync(State(key)));
        }
    }

    [Fact]
    public async Task A_raise_held_on_the_wire_past_the_successors_install_is_refused_on_its_next_read()
    {
        var owner = Fence(20, database: proxied.GetDatabase());
        await owner.RaiseAsync(CancellationToken.None);
        var hold = proxy.HoldNextMulti();
        var refresh = owner.RaiseAsync(CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        await Fence(21).RaiseAsync(CancellationToken.None);
        hold.Release();

        Assert.Equal(RaiseOutcome.Refused, await refresh.WaitAsync(Wait));
        Assert.Equal(21, (long)await Db.StringGetAsync(owner.FenceKey));
        Assert.Equal(21, (long)await Db.StringGetAsync(owner.HighWaterKey));
    }

    [Fact]
    public async Task An_EXEC_held_across_the_lease_end_with_no_successor_lands_within_the_fence_keys_ttl()
    {
        var owner = Fence(30, database: proxied.GetDatabase());
        await owner.RaiseAsync(CancellationToken.None);
        var hold = proxy.HoldNextMulti();
        var write = owner.WriteAsync(t => Set(t, State(), "late"), CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        hold.Release();

        Assert.True(await write.WaitAsync(Wait));
        Assert.Equal("late", (string?)await Db.StringGetAsync(State()));
    }

    [Fact]
    public async Task An_EXEC_held_past_the_fence_keys_ttl_fails()
    {
        var owner = Fence(40, ttl: TimeSpan.FromSeconds(5), database: proxied.GetDatabase());
        await owner.RaiseAsync(CancellationToken.None);
        var hold = proxy.HoldNextMulti();
        var write = owner.WriteAsync(t => Set(t, State(), "late"), CancellationToken.None);
        Assert.Same(hold.Held, await Task.WhenAny(hold.Held, write).WaitAsync(Wait));

        await AwaitGoneAsync(owner.FenceKey);
        hold.Release();

        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => write.WaitAsync(Wait));
        Assert.False(await Db.KeyExistsAsync(State()));
    }

    [Fact]
    public async Task A_write_whose_EXEC_was_rejected_for_a_watched_key_set_to_the_same_token_is_sent_again_and_commits_once()
    {
        var owner = Fence(50, database: proxied.GetDatabase());
        await owner.RaiseAsync(CancellationToken.None);
        var counter = State() + ":n";
        var built = 0;
        var hold = proxy.HoldNextMulti();
        var write = owner.WriteAsync(
            t =>
            {
                built++;
                _ = t.StringIncrementAsync(counter);
            },
            CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        Assert.Equal(RaiseOutcome.Installed, await Fence(50).RaiseAsync(CancellationToken.None));
        hold.Release();

        Assert.True(await write.WaitAsync(Wait));
        Assert.Equal(2, built);
        Assert.Equal(1, (long)await Db.StringGetAsync(counter));
    }

    [Fact]
    public async Task A_condition_of_the_callers_that_fails_returns_false_and_is_not_a_refusal()
    {
        var owner = Fence(60);
        await owner.RaiseAsync(CancellationToken.None);

        var built = 0;
        var committed = await owner.WriteAsync(
            t =>
            {
                built++;
                t.AddCondition(Condition.KeyExists(State() + ":absent"));
                Set(t, State(), "x");
            },
            CancellationToken.None);

        Assert.False(committed);
        Assert.Equal(1, built);
        Assert.False(await Db.KeyExistsAsync(State()));
    }

    [Fact]
    public async Task Racing_raises_of_many_tenures_never_lower_the_high_water_key_and_leave_the_highest_token()
    {
        var fences = Enumerable.Range(1, 8).Select(t => Fence(t, "race")).ToArray();
        using var done = new CancellationTokenSource();
        var lowered = false;
        var watcher = Task.Run(async () =>
        {
            long seen = 0;
            while (!done.IsCancellationRequested)
            {
                var value = await Db.StringGetAsync(fences[0].HighWaterKey);
                if (!value.IsNull)
                {
                    lowered |= (long)value < seen;
                    seen = Math.Max(seen, (long)value);
                }
            }
        });

        await Task.WhenAll(fences.SelectMany(f => Enumerable.Range(0, 30).Select(_ => Task.Run(() => f.RaiseAsync(CancellationToken.None)))));
        await done.CancelAsync();
        await watcher;

        Assert.False(lowered);
        Assert.Equal(8, (long)await Db.StringGetAsync(fences[0].HighWaterKey));
        Assert.Equal(8, (long)await Db.StringGetAsync(fences[0].FenceKey));
        Assert.True(fences[7].Raised);
    }

    [Fact]
    public async Task A_raise_held_after_its_read_across_a_successors_install_and_the_deletion_of_its_fence_key_is_refused_and_lowers_nothing()
    {
        var points = new PointHold(OwnedFaultPoints.RedisBeforeExec.Name);
        var stale = Fence(Owner("rp1", 4), points: points);
        Assert.Equal(RaiseOutcome.Installed, await stale.RaiseAsync(CancellationToken.None));
        await Db.KeyDeleteAsync(stale.FenceKey);

        points.Arm();
        var raise = stale.RaiseAsync(CancellationToken.None);
        await points.Reached.WaitAsync(Wait);

        Assert.Equal(RaiseOutcome.Installed, await Fence(Owner("rp1", 9)).RaiseAsync(CancellationToken.None));
        await Db.KeyDeleteAsync(stale.FenceKey);
        points.Release();

        Assert.Equal(RaiseOutcome.Refused, await raise.WaitAsync(Wait));
        Assert.False(await Db.KeyExistsAsync(stale.FenceKey));
        Assert.Equal(9, (long)await Db.StringGetAsync(stale.HighWaterKey));
        Assert.False(stale.Raised);
    }

    [Fact]
    public async Task A_fenced_write_aborted_by_a_watched_key_change_is_checked_again_before_it_is_sent_again()
    {
        var owner = Owner("rp2", 70);
        var fence = Fence(owner, proxied.GetDatabase());
        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        var built = 0;
        var hold = proxy.HoldNextMulti();
        var write = fence.WriteAsync(
            t =>
            {
                built++;
                _ = t.StringSetAsync(State("rp2"), "after-stop");
            },
            CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        Assert.Equal(RaiseOutcome.Installed, await Fence(Owner("rp2", 70)).RaiseAsync(CancellationToken.None));
        owner.Refusal = new OwnershipLostException("stopped while its write was on the wire");
        hold.Release();

        await Assert.ThrowsAsync<OwnershipLostException>(() => write.WaitAsync(Wait));
        Assert.Equal(1, built);
        Assert.False(await Db.KeyExistsAsync(State("rp2")));
    }

    [Fact]
    public async Task A_raise_aborted_by_a_watched_key_change_is_checked_again_before_it_reads_again()
    {
        var owner = Owner("rp3", 80);
        var fence = Fence(owner, proxied.GetDatabase());
        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        var hold = proxy.HoldNextMulti();
        var refresh = fence.RaiseAsync(CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        await Db.KeyDeleteAsync(fence.FenceKey);
        owner.Refusal = new OwnershipLostException("stopped while its refresh was on the wire");
        hold.Release();

        await Assert.ThrowsAsync<OwnershipLostException>(() => refresh.WaitAsync(Wait));
        Assert.False(await Db.KeyExistsAsync(fence.FenceKey));
        Assert.Equal(80, (long)await Db.StringGetAsync(fence.HighWaterKey));
    }

    [Fact]
    public async Task The_step_5_delete_removes_only_the_tenures_own_fence_key_and_the_high_water_key_refuses_a_stale_raise_after_it()
    {
        var stale = Fence(4);
        Assert.Equal(RaiseOutcome.Installed, await stale.RaiseAsync(CancellationToken.None));
        Assert.True(await stale.DeleteOwnAsync(CancellationToken.None));
        Assert.False(stale.Raised);
        Assert.False(await Db.KeyExistsAsync(stale.FenceKey));
        Assert.Equal(4, (long)await Db.StringGetAsync(stale.HighWaterKey));

        var successor = Fence(9);
        Assert.Equal(RaiseOutcome.Installed, await successor.RaiseAsync(CancellationToken.None));
        Assert.False(await stale.DeleteOwnAsync(CancellationToken.None));
        Assert.Equal(9, (long)await Db.StringGetAsync(successor.FenceKey));

        Assert.True(await successor.DeleteOwnAsync(CancellationToken.None));
        Assert.Equal(RaiseOutcome.Refused, await stale.RaiseAsync(CancellationToken.None));
        Assert.False(await Db.KeyExistsAsync(successor.FenceKey));
        Assert.Equal(9, (long)await Db.StringGetAsync(successor.HighWaterKey));
    }

    [Fact]
    public async Task A_step_5_delete_held_on_the_wire_past_the_successors_install_leaves_the_successors_fence_key()
    {
        var stale = Fence(4, database: proxied.GetDatabase());
        Assert.Equal(RaiseOutcome.Installed, await stale.RaiseAsync(CancellationToken.None));
        var hold = proxy.HoldNextMulti();
        var delete = stale.DeleteOwnAsync(CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        var successor = Fence(9);
        Assert.Equal(RaiseOutcome.Installed, await successor.RaiseAsync(CancellationToken.None));
        hold.Release();

        Assert.False(await delete.WaitAsync(Wait));
        Assert.Equal(9, (long)await Db.StringGetAsync(successor.FenceKey));
    }

    [Fact]
    public async Task After_the_restore_clearing_a_new_owners_lower_token_raises()
    {
        Assert.Equal(RaiseOutcome.Installed, await Fence(9).RaiseAsync(CancellationToken.None));
        Assert.Equal(RaiseOutcome.Installed, await Fence(5, key: "other").RaiseAsync(CancellationToken.None));
        var rewound = Fence(3);
        Assert.Equal(RaiseOutcome.Refused, await rewound.RaiseAsync(CancellationToken.None));

        Assert.Equal(4, await ClusterRedisRestore.ClearOwnedKeysAsync(direct, prefix));

        Assert.Equal(RaiseOutcome.Installed, await rewound.RaiseAsync(CancellationToken.None));
        Assert.Equal(3, (long)await Db.StringGetAsync(rewound.HighWaterKey));
        Assert.False(await Db.KeyExistsAsync(Fence(5, key: "other").HighWaterKey));
    }

    [Theory]
    [InlineData("*:", "zz:")]
    [InlineData("[ab]:", "a:")]
    [InlineData("?:", "x:")]
    [InlineData("\\x:", "x:")]
    public async Task The_restore_clearing_matches_a_prefixs_glob_characters_literally(string mineSuffix, string theirsSuffix)
    {
        var stem = prefix[..^1];
        var mine = new TenureFence(Db, Owner("k", 2), NoFaultPoints.Instance, stem + mineSuffix, LongTtl);
        var theirs = new TenureFence(Db, Owner("k", 2), NoFaultPoints.Instance, stem + theirsSuffix, LongTtl);
        Assert.Equal(RaiseOutcome.Installed, await mine.RaiseAsync(CancellationToken.None));
        Assert.Equal(RaiseOutcome.Installed, await theirs.RaiseAsync(CancellationToken.None));

        Assert.Equal(2, await ClusterRedisRestore.ClearOwnedKeysAsync(direct, stem + mineSuffix));

        Assert.False(await Db.KeyExistsAsync(mine.FenceKey));
        Assert.False(await Db.KeyExistsAsync(mine.HighWaterKey));
        Assert.True(await Db.KeyExistsAsync(theirs.FenceKey));
        Assert.True(await Db.KeyExistsAsync(theirs.HighWaterKey));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task The_restore_clearing_refuses_and_deletes_nothing_unless_every_server_is_connected(bool oneReachable)
    {
        var fence = Fence(3, key: "partial");
        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        var options = ConfigurationOptions.Parse(oneReachable ? redis.ConnectionString : "127.0.0.1:1");
        if (oneReachable)
        {
            options.EndPoints.Add("127.0.0.1", 1);
        }

        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 1000;
        await using var partial = await ConnectionMultiplexer.ConnectAsync(options);
        if (oneReachable)
        {
            Assert.Contains(partial.GetServers(), s => s.IsConnected);
        }

        Assert.Contains(partial.GetServers(), s => !s.IsConnected);

        await Assert.ThrowsAsync<InvalidOperationException>(() => ClusterRedisRestore.ClearOwnedKeysAsync(partial, prefix));
        Assert.True(await Db.KeyExistsAsync(fence.HighWaterKey));
    }

    [Fact]
    public async Task A_step_5_delete_whose_EXEC_was_rejected_for_a_watched_key_set_to_the_same_token_is_sent_again_and_deletes()
    {
        var fence = Fence(4, key: "again", database: proxied.GetDatabase());
        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        var hold = proxy.HoldNextMulti();
        var delete = fence.DeleteOwnAsync(CancellationToken.None);
        await hold.Held.WaitAsync(Wait);

        Assert.Equal(RaiseOutcome.Installed, await Fence(4, key: "again").RaiseAsync(CancellationToken.None));
        hold.Release();

        Assert.True(await delete.WaitAsync(Wait));
        Assert.False(await Db.KeyExistsAsync(fence.FenceKey));
        Assert.Equal(4, (long)await Db.StringGetAsync(fence.HighWaterKey));
    }

    [Fact]
    public async Task The_registry_deletes_at_step_5_the_fence_key_the_install_raise_set_and_leaves_the_high_water_key()
    {
        using var gate = new LocalPhaseGate(Options.Create(new ClusterOptions()), TimeProvider.System, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        using var fences = new RedisTenureFences(
            direct,
            gate,
            NoFaultPoints.Instance,
            Options.Create(new ClusterRedisOptions { KeyPrefix = prefix }),
            Options.Create(new ClusterOptions()),
            TimeProvider.System,
            NullLogger<RedisTenureFences>.Instance);
        var owner = Owner("registry", 6);
        await fences.OnAcquiringAsync(owner, CancellationToken.None);
        var fence = RedisTenureFences.Of(owner)!;
        Assert.Equal(6, (long)await Db.StringGetAsync(fence.FenceKey));

        await fences.OnFencedAsync(owner, CancellationToken.None).WaitAsync(Wait);

        Assert.False(await Db.KeyExistsAsync(fence.FenceKey));
        Assert.Equal(6, (long)await Db.StringGetAsync(fence.HighWaterKey));
        Assert.False(fence.Raised);
    }

    [Fact]
    public async Task OnFencedAsync_returns_when_its_token_ends_while_the_step_5_EXEC_is_held_on_the_wire()
    {
        var options = ConfigurationOptions.Parse(proxy.Endpoint);
        options.AsyncTimeout = (int)TimeSpan.FromSeconds(120).TotalMilliseconds;
        options.SyncTimeout = (int)TimeSpan.FromSeconds(120).TotalMilliseconds;
        await using var slow = await ConnectionMultiplexer.ConnectAsync(options);
        using var gate = new LocalPhaseGate(Options.Create(new ClusterOptions()), TimeProvider.System, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        using var fences = new RedisTenureFences(
            slow,
            gate,
            NoFaultPoints.Instance,
            Options.Create(new ClusterRedisOptions { KeyPrefix = prefix }),
            Options.Create(new ClusterOptions()),
            TimeProvider.System,
            NullLogger<RedisTenureFences>.Instance);
        var owner = Owner("pinned", 7);
        await fences.OnAcquiringAsync(owner, CancellationToken.None);
        var hold = proxy.HoldNextMulti();
        try
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var fenced = fences.OnFencedAsync(owner, stop.Token);
            await hold.Held.WaitAsync(Wait);
            await fenced.WaitAsync(Wait);
        }
        finally
        {
            hold.Release();
        }
    }
}
