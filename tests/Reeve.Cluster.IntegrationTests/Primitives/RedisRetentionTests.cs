using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisRetentionTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture redis;

    public RedisRetentionTests(RedisFixture redis) => this.redis = redis;

    [Fact]
    public async Task Claim_and_flight_keys_carry_a_ttl_and_a_flight_ends_with_its_result_key_alone()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        _ = Assert.IsType<ClaimResult.Won>(await replica.Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", TimeSpan.FromMinutes(1)));
        _ = await replica.Flight(OnRedisUnavailable.Refuse).RunAsync("f", _ => Task.FromResult(new byte[] { 1 }), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2));

        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.ClaimTokenKey("k")))!.Value.TotalSeconds, 50, 60);
        Assert.False(await rig.Direct.KeyExistsAsync(rig.FlightTokenKey("f")));
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.FlightResultKey("f")))!.Value.TotalSeconds, 110, 120);
    }

    [Fact]
    public async Task Budget_sets_expire_at_the_end_of_their_window()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var budget = (await rig.AddReplicaAsync("a")).Budget(OnRedisUnavailable.Refuse);
        _ = await budget.ConsumeAsync("k", "i", 5, TimeSpan.FromHours(1));

        var server = rig.Direct.Multiplexer.GetServers()[0];
        var keys = server.Keys(pattern: "{" + rig.Prefix + "budget:k}:*").ToArray();

        var set = Assert.Single(keys);
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(set))!.Value.TotalSeconds, 1, 3601);
    }

    [Fact]
    public async Task A_counter_has_no_expiry_unless_the_retention_is_set()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var counter = new RedisCounter(new RedisCounterStore(rig.Direct, rig.Prefix));

        Assert.Equal(new CounterResult.Incremented(1), await counter.IncrementAsync("c"));

        Assert.Null(await rig.Direct.KeyTimeToLiveAsync(CounterKey(rig, "c")));
    }

    [Fact]
    public async Task A_counter_with_the_retention_set_expires_idle_and_every_increment_renews_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var counter = new RedisCounter(new RedisCounterStore(rig.Direct, rig.Prefix, TimeSpan.FromMinutes(10)));

        Assert.Equal(new CounterResult.Incremented(1), await counter.IncrementAsync("c"));
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(CounterKey(rig, "c")))!.Value.TotalSeconds, 590, 600);
        _ = await rig.Direct.KeyExpireAsync(CounterKey(rig, "c"), TimeSpan.FromSeconds(5));
        Assert.Equal(new CounterResult.Incremented(2), await counter.IncrementAsync("c"));

        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(CounterKey(rig, "c")))!.Value.TotalSeconds, 590, 600);
    }

    [Fact]
    public async Task An_increment_without_the_retention_leaves_the_ttl_of_the_counter_as_it_was()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var with = new RedisCounter(new RedisCounterStore(rig.Direct, rig.Prefix, TimeSpan.FromMinutes(10)));
        var without = new RedisCounter(new RedisCounterStore(rig.Direct, rig.Prefix));
        _ = await with.IncrementAsync("c");
        _ = await rig.Direct.KeyExpireAsync(CounterKey(rig, "c"), TimeSpan.FromSeconds(30));

        Assert.Equal(new CounterResult.Incremented(2), await without.IncrementAsync("c"));

        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(CounterKey(rig, "c")))!.Value.TotalSeconds, 1, 30);
    }

    [Fact]
    public async Task A_counter_resolved_from_the_container_takes_the_retention_of_the_cluster_options()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var services = new ServiceCollection();
        _ = services.AddSingleton<IConnectionMultiplexer>(rig.Direct.Multiplexer);
        _ = services.AddSingleton<IOptions<ClusterRedisOptions>>(Options.Create(new ClusterRedisOptions { KeyPrefix = rig.Prefix }));
        _ = services.AddSingleton<IOptions<ClusterOptions>>(Options.Create(new ClusterOptions { CounterRetention = TimeSpan.FromMinutes(10) }));
        _ = services.AddReeveClusterRedisCounter();
        await using var provider = services.BuildServiceProvider();

        Assert.Equal(new CounterResult.Incremented(1), await provider.GetRequiredService<IRedisCounter>().IncrementAsync("c"));

        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(CounterKey(rig, "c")))!.Value.TotalSeconds, 590, 600);
    }

    [Fact]
    public async Task A_slot_s_id_key_has_the_slot_s_ttl_and_the_held_set_has_none_by_design()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var slots = (await rig.AddReplicaAsync("a")).Slots(OnRedisUnavailable.Refuse);
        _ = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, TimeSpan.FromMinutes(1)));

        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.SlotIdKey("k", "i")))!.Value.TotalSeconds, 50, 60);
        Assert.Null(await rig.Direct.KeyTimeToLiveAsync(rig.SlotHeldKey("k")));
    }

    private static string CounterKey(RedisPrimitivesRig rig, string key) => RedisPrimitiveKeys.CounterHashTag(rig.Prefix, key) + ":value";
}
