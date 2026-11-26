using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

public sealed class RedisSlotLifecycleTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly RedisFixture redis;

    public RedisSlotLifecycleTests(RedisFixture redis) => this.redis = redis;

    [Fact]
    public async Task A_renewal_moves_the_score_and_the_id_key_s_ttl_forward()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, TimeSpan.FromSeconds(10))).Token;
        var before = (await rig.Direct.SortedSetScoreAsync(rig.SlotHeldKey("k"), token.ToString("N")))!.Value;

        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", token, Minute));

        var after = (await rig.Direct.SortedSetScoreAsync(rig.SlotHeldKey("k"), token.ToString("N")))!.Value;
        Assert.InRange(after - before, 45_000, 55_000);
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.SlotIdKey("k", "i")))!.Value.TotalSeconds, 50, 60);
    }

    [Fact]
    public async Task A_lapsed_slot_s_renewal_is_refused_adds_no_member_and_ends_the_tenure()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        await rig.Direct.SortedSetAddAsync(rig.SlotHeldKey("k"), token.ToString("N"), 1);

        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", token, Minute));

        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotHeldKey("k")));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", token, Minute));
    }

    [Fact]
    public async Task The_renewal_transaction_does_not_bring_back_a_member_whose_ttl_passed_and_leaves_no_member()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await replica.Slots(OnRedisUnavailable.Refuse).AcquireAsync("k", "i", 1, TimeSpan.FromMilliseconds(400))).Token;
        await Task.Delay(700);

        var store = replica.SlotStore();
        var now = await store.TimeAsync("k", CancellationToken.None);
        Assert.False(await store.RenewAsync("k", "i", token, now, Minute, CancellationToken.None));

        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotHeldKey("k")));
    }

    [Fact]
    public async Task A_stale_tokens_renewal_release_and_effect_are_refused_and_touch_no_other_tenure()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Slots(OnRedisUnavailable.Refuse);
        var stale = Assert.IsType<SlotAcquireResult.Acquired>(await a.AcquireAsync("k", "i", 1, Minute)).Token;
        await rig.Direct.SortedSetRemoveAsync(rig.SlotHeldKey("k"), stale.ToString("N"));
        var current = Assert.IsType<SlotAcquireResult.Acquired>(await b.AcquireAsync("k", "j", 1, Minute)).Token;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";

        Assert.Equal(RedisEffectResult.Refused, await a.ExecuteAsync("k", "i", stale, t => _ = t.StringSetAsync(state, "stale")));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", stale, Minute));
        Assert.False(await a.ReleaseAsync("k", "i", stale));

        Assert.False(await rig.Direct.KeyExistsAsync(state));
        Assert.Equal(1, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
        Assert.Equal(SlotRenewResult.Renewed, await b.RenewAsync("k", "j", current, Minute));
    }

    [Fact]
    public async Task An_effect_under_a_slot_the_set_no_longer_holds_is_refused_and_writes_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";
        await rig.Direct.SortedSetRemoveAsync(rig.SlotHeldKey("k"), token.ToString("N"));

        Assert.Equal(RedisEffectResult.Refused, await slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "late")));
        Assert.False(await rig.Direct.KeyExistsAsync(state));
    }

    [Fact]
    public async Task An_effect_under_a_held_slot_commits_its_commands()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";

        Assert.Equal(RedisEffectResult.Committed, await slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "done")));
        Assert.Equal("done", (string?)await rig.Direct.StringGetAsync(state));
    }

    [Fact]
    public async Task An_effect_delayed_on_the_wire_past_the_removal_of_its_member_fails_and_writes_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";
        var hold = rig.Hold.HoldNextMulti();

        var effect = slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "late"));
        await hold.Held.WaitAsync(Wait);
        await rig.Direct.SortedSetRemoveAsync(rig.SlotHeldKey("k"), token.ToString("N"));
        hold.Release();

        Assert.Equal(RedisEffectResult.Refused, await effect.WaitAsync(Wait));
        Assert.False(await rig.Direct.KeyExistsAsync(state));
    }

    [Fact]
    public async Task An_effect_delayed_on_the_wire_past_a_renewal_of_its_slot_is_refused_and_a_retry_commits()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";
        var member = token.ToString("N");
        var hold = rig.Hold.HoldNextMulti();

        var effect = slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "first"));
        await hold.Held.WaitAsync(Wait);
        var before = (await rig.Direct.SortedSetScoreAsync(rig.SlotHeldKey("k"), member))!.Value;
        _ = await rig.Direct.SortedSetAddAsync(rig.SlotHeldKey("k"), member, before + 1_000, SortedSetWhen.Exists);
        Assert.Equal(before + 1_000, (await rig.Direct.SortedSetScoreAsync(rig.SlotHeldKey("k"), member))!.Value);
        hold.Release();

        Assert.Equal(RedisEffectResult.Refused, await effect.WaitAsync(Wait));
        Assert.False(await rig.Direct.KeyExistsAsync(state));
        Assert.Equal(RedisEffectResult.Committed, await slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "retry")));
        Assert.Equal("retry", (string?)await rig.Direct.StringGetAsync(state));
    }

    [Fact]
    public async Task A_release_by_token_ends_the_slot_and_frees_the_key_and_a_release_by_id_ends_whatever_the_id_names()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var first = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;

        Assert.True(await slots.ReleaseAsync("k", "i", first));
        Assert.False(await slots.ReleaseAsync("k", "i", first));
        var second = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        Assert.NotEqual(first, second);

        Assert.True(await slots.ReleaseAsync("k", "i", null));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotHeldKey("k")));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", second, Minute));
    }

    [Fact]
    public async Task A_late_release_of_an_earlier_tenure_s_token_ends_nothing_of_the_current_one()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var first = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        Assert.True(await slots.ReleaseAsync("k", "i", first));
        var second = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;

        Assert.False(await slots.ReleaseAsync("k", "i", first));

        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", second, Minute));
    }

    [Fact]
    public async Task A_fenced_holders_renewal_and_effect_are_refused_by_the_holder_check_and_its_slot_ends_at_its_ttl()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        var other = (await rig.AddReplicaAsync("b")).Slots(OnRedisUnavailable.Refuse);
        var slots = replica.Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, TimeSpan.FromSeconds(2))).Token;
        replica.Holder = false;
        var state = RedisPrimitiveKeys.SlotHashTag(rig.Prefix, "k") + ":state";

        Assert.Equal(SlotRenewResult.NotServing, await slots.RenewAsync("k", "i", token, Minute));
        Assert.Equal(RedisEffectResult.NotServing, await slots.ExecuteAsync("k", "i", token, t => _ = t.StringSetAsync(state, "x")));

        Assert.IsType<SlotAcquireResult.Full>(await other.AcquireAsync("k", "peer", 1, Minute));
        await Poll.UntilAsync(async () => (await other.AcquireAsync("k", "peer", 1, Minute)).IsAcquired, "the fenced holder's slot ends at its time to live");
        Assert.False(await rig.Direct.KeyExistsAsync(state));
    }

    [Fact]
    public async Task A_draining_replica_whose_serving_check_refuses_still_renews_and_releases()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        var slots = replica.Slots(OnRedisUnavailable.Refuse);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        replica.Serving = false;

        Assert.IsType<SlotAcquireResult.NotServing>(await slots.AcquireAsync("k", "j", 2, Minute));
        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", token, Minute));
        Assert.True(await slots.ReleaseAsync("k", "i", token));
    }
}
