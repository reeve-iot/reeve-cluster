using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

public sealed class RedisSlotAcquireTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly RedisFixture redis;

    public RedisSlotAcquireTests(RedisFixture redis) => this.redis = redis;

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task I35_concurrent_acquires_from_several_replicas_admit_at_least_one_and_never_more_than_the_limit(int limit)
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var clients = new List<RedisSlots>();
        foreach (var id in new[] { "a", "b", "c" })
        {
            clients.Add((await rig.AddReplicaAsync(id)).Slots(OnRedisUnavailable.Refuse));
        }

        for (var round = 0; round < 5; round++)
        {
            var key = $"k{limit}-{round}";
            var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(i => clients[i % 3].AcquireAsync(key, $"id{i}", limit, Minute)));

            var admitted = results.OfType<SlotAcquireResult.Acquired>().ToList();
            Assert.InRange(admitted.Count, 1, limit);
            Assert.Equal(12 - admitted.Count, results.OfType<SlotAcquireResult.Full>().Count());
            var members = (await rig.Direct.SortedSetRangeByRankAsync(rig.SlotHeldKey(key))).Select(m => (string)m!).Order();
            Assert.Equal(admitted.Select(a => a.Token.ToString("N")).Order(), members);
        }
    }

    [Fact]
    public async Task Sequential_acquires_fill_the_limit_and_the_next_is_Full_without_an_id_key_or_a_member()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);

        Assert.True((await slots.AcquireAsync("k", "i1", 2, Minute)).IsAcquired);
        Assert.True((await slots.AcquireAsync("k", "i2", 2, Minute)).IsAcquired);
        Assert.IsType<SlotAcquireResult.Full>(await slots.AcquireAsync("k", "i3", 2, Minute));

        Assert.Equal(2, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotIdKey("k", "i3")));
        Assert.True(await rig.Direct.KeyExistsAsync(rig.SlotIdKey("k", "i1")));
    }

    [Fact]
    public async Task The_set_scores_the_token_by_its_expiry_on_Redis_s_clock_and_the_id_key_names_it_with_the_slot_s_ttl()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);

        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;

        var time = (RedisResult[])(await rig.Direct.ExecuteAsync("TIME"))!;
        var now = (long)time[0] * 1000 + (long)time[1] / 1000;
        var score = await rig.Direct.SortedSetScoreAsync(rig.SlotHeldKey("k"), token.ToString("N"));
        Assert.InRange(score!.Value - now, 55_000, 61_000);
        Assert.Equal(token.ToString("N"), (string?)await rig.Direct.StringGetAsync(rig.SlotIdKey("k", "i")));
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.SlotIdKey("k", "i")))!.Value.TotalSeconds, 50, 60);
        Assert.Null(await rig.Direct.KeyTimeToLiveAsync(rig.SlotHeldKey("k")));
    }

    [Fact]
    public async Task A_re_acquire_with_the_id_of_a_valid_slot_answers_the_same_token_compares_no_limit_and_adds_no_member()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var first = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute));

        var again = await slots.AcquireAsync("k", "i", 1, Minute);

        Assert.Equal(first, again);
        Assert.Equal(1, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
    }

    [Fact]
    public async Task A_re_acquire_by_another_replica_answers_the_slot_whoever_acquired_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Slots(OnRedisUnavailable.Refuse);
        var first = Assert.IsType<SlotAcquireResult.Acquired>(await a.AcquireAsync("k", "i", 1, Minute));

        Assert.Equal(first, await b.AcquireAsync("k", "i", 1, Minute));
        Assert.IsType<SlotAcquireResult.Full>(await b.AcquireAsync("k", "other", 1, Minute));
    }

    [Fact]
    public async Task A_lowered_limit_ends_no_slot_refuses_new_grants_and_still_renews_and_re_acquires_the_held_ones()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var tokens = new List<Guid>();
        foreach (var id in new[] { "i1", "i2", "i3" })
        {
            tokens.Add(Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", id, 3, Minute)).Token);
        }

        Assert.IsType<SlotAcquireResult.Full>(await slots.AcquireAsync("k", "i4", 1, Minute));
        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i1", tokens[0], Minute));
        Assert.Equal(new SlotAcquireResult.Acquired(tokens[2]), await slots.AcquireAsync("k", "i3", 1, Minute));
        Assert.Equal(3, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
    }

    [Fact]
    public async Task A_crashed_holders_slot_frees_at_its_ttl_and_the_next_acquire_prunes_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Slots(OnRedisUnavailable.Refuse);
        Assert.True((await a.AcquireAsync("k", "crashed", 1, TimeSpan.FromSeconds(2))).IsAcquired);
        Assert.IsType<SlotAcquireResult.Full>(await b.AcquireAsync("k", "next", 1, Minute));

        SlotAcquireResult? later = null;
        await Poll.UntilAsync(async () => (later = await b.AcquireAsync("k", "next", 1, Minute)).IsAcquired, "the crashed holder's slot ends at its time to live");

        Assert.True(later!.IsAcquired);
        Assert.Equal(1, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
    }

    [Fact]
    public async Task C5_a_refused_grant_whose_ZREM_fails_leaves_its_member_counted_and_the_same_id_is_Full_again_never_the_refused_member()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var holder = await rig.AddReplicaAsync("a");
        var loser = await rig.AddReplicaAsync("b");
        var held = Assert.IsType<SlotAcquireResult.Acquired>(await holder.Slots(OnRedisUnavailable.Refuse).AcquireAsync("k", "holder", 1, Minute));
        var failing = loser.Slots(OnRedisUnavailable.Refuse, new RefuseFails(loser.SlotStore()));

        Assert.IsType<SlotAcquireResult.Full>(await failing.AcquireAsync("k", "loser", 1, Minute));
        Assert.Equal(2, await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotIdKey("k", "loser")));

        Assert.True(await holder.Slots(OnRedisUnavailable.Refuse).ReleaseAsync("k", "holder", held.Token));
        Assert.IsType<SlotAcquireResult.Full>(await failing.AcquireAsync("k", "loser", 1, Minute));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotIdKey("k", "loser")));
        Assert.IsType<SlotAcquireResult.Full>(await loser.Slots(OnRedisUnavailable.Refuse).AcquireAsync("k", "loser", 1, Minute));
    }

    private sealed class RefuseFails(IRedisSlotRows inner) : IRedisSlotRows
    {
        public Task<long> TimeAsync(string key, CancellationToken cancellationToken) => inner.TimeAsync(key, cancellationToken);

        public Task<RedisSlotRead> ReadAsync(string key, string id, CancellationToken cancellationToken) => inner.ReadAsync(key, id, cancellationToken);

        public Task<bool> RenewAsync(string key, string id, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken) =>
            inner.RenewAsync(key, id, token, now, ttl, cancellationToken);

        public Task<long> GrantAsync(string key, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken) =>
            inner.GrantAsync(key, token, now, ttl, cancellationToken);

        public Task AdmitAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken) =>
            inner.AdmitAsync(key, id, token, ttl, cancellationToken);

        public Task RefuseAsync(string key, Guid token, CancellationToken cancellationToken) =>
            Task.FromException(new RedisConnectionException(ConnectionFailureType.SocketFailure, CommandFlags.None, "the connection is cut", null, CommandStatus.Unknown));

        public Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken) =>
            inner.ReleaseAsync(key, id, token, cancellationToken);

        public Task<bool> ExecuteAsync(string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken) =>
            inner.ExecuteAsync(key, token, queue, cancellationToken);
    }
}
