using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisClaimTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly RedisFixture redis;

    public RedisClaimTests(RedisFixture redis) => this.redis = redis;

    [Fact]
    public async Task I16_exactly_one_of_many_concurrent_claims_on_four_replicas_wins_and_the_rest_lose_to_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = new List<(string Id, RedisClaims Claims)>();
        foreach (var id in new[] { "a", "b", "c", "d" })
        {
            claims.Add((id, (await rig.AddReplicaAsync(id)).Claims(OnRedisUnavailable.Refuse)));
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 64).Select(i => claims[i % 4].Claims.ClaimAsync("k", TimeSpan.FromMinutes(1))));

        var winners = results.OfType<ClaimResult.Won>().ToList();
        var winner = Assert.Single(winners);
        var winnerId = claims[Array.IndexOf(results, winner) % 4].Id;
        Assert.All(results.OfType<ClaimResult.Lost>(), lost => Assert.Equal(winnerId, lost.Holder));
        Assert.Equal(63, results.OfType<ClaimResult.Lost>().Count());
    }

    [Fact]
    public async Task The_claim_is_one_key_whose_value_carries_the_token_and_the_holder_and_whose_ttl_is_the_claim_s()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = await rig.AddReplicaAsync("a");

        var won = Assert.IsType<ClaimResult.Won>(await a.Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", TimeSpan.FromMinutes(1)));

        var tag = RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k");
        Assert.Equal($"{{{rig.Prefix}claim:k}}", tag);
        Assert.Equal(won.Token.ToString("N") + ":a", (string?)await rig.Direct.StringGetAsync(tag + ":token"));
        Assert.False(await rig.Direct.KeyExistsAsync(tag + ":holder"));
        var ttl = await rig.Direct.KeyTimeToLiveAsync(tag + ":token");
        Assert.InRange(ttl!.Value.TotalSeconds, 1, 60);
    }

    [Fact]
    public async Task A_loser_learns_the_holder_from_the_claim_key_and_a_holder_with_colons_survives()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = await rig.AddReplicaAsync("zone:a:1");
        var b = await rig.AddReplicaAsync("b");
        Assert.IsType<ClaimResult.Won>(await a.Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", TimeSpan.FromMinutes(1)));

        var lost = Assert.IsType<ClaimResult.Lost>(await b.Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", TimeSpan.FromMinutes(1)));

        Assert.Equal("zone:a:1", lost.Holder);
        Assert.False(await rig.Direct.KeyExistsAsync(RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k") + ":holder"));
    }

    [Fact]
    public async Task Claims_on_a_key_that_expires_between_a_lost_grant_and_its_read_all_end_in_an_answer_under_each_policy()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replicas = new List<RedisReplica>();
        foreach (var id in new[] { "a", "b", "c", "d" })
        {
            replicas.Add(await rig.AddReplicaAsync(id));
        }

        var calls = Enumerable.Range(0, 400).Select(i => replicas[i % 4].Claims(i % 2 == 0 ? OnRedisUnavailable.Refuse : OnRedisUnavailable.Grant)
            .ClaimAsync("k", TimeSpan.FromMilliseconds(1)));
        var results = await Task.WhenAll(calls).WaitAsync(Wait);

        Assert.All(results, r => Assert.True(r is ClaimResult.Won or ClaimResult.Lost or ClaimResult.Unknown));

        Assert.All(results.OfType<ClaimResult.Lost>(), lost => Assert.Contains(lost.Holder, new[] { "a", "b", "c", "d" }));
    }

    [Fact]
    public async Task A_claim_s_grant_opens_no_transaction_so_a_held_MULTI_does_not_delay_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var hold = rig.Hold.HoldNextMulti();

        var won = await claims.ClaimAsync("k", TimeSpan.FromMinutes(1)).WaitAsync(Wait);

        Assert.IsType<ClaimResult.Won>(won);
        Assert.False(hold.Held.IsCompleted);
        hold.Release();
    }

    [Fact]
    public async Task A_claim_ends_with_its_ttl_and_another_caller_wins()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Claims(OnRedisUnavailable.Refuse);
        Assert.IsType<ClaimResult.Won>(await a.ClaimAsync("k", TimeSpan.FromMilliseconds(300)));
        Assert.IsType<ClaimResult.Lost>(await b.ClaimAsync("k", TimeSpan.FromMinutes(1)));

        ClaimResult? later = null;
        await Poll.UntilAsync(async () => (later = await b.ClaimAsync("k", TimeSpan.FromMinutes(1))) is ClaimResult.Won, "the expired claim is won by b");
        Assert.True(later!.IsWon);
    }

    [Fact]
    public async Task A_release_by_the_token_ends_the_claim_and_a_stale_token_releases_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var won = Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync("k", TimeSpan.FromMinutes(1)));

        Assert.False(await claims.ReleaseAsync("k", Guid.NewGuid()));
        Assert.True(await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")));
        Assert.True(await claims.ReleaseAsync("k", won.Token));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")));
        Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync("k", TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task An_effect_under_a_held_claim_commits_in_the_claim_s_hash_tag()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var won = Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync("k", TimeSpan.FromMinutes(1)));
        var state = RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k") + ":state";

        var result = await claims.ExecuteAsync("k", won.Token, t => _ = t.StringSetAsync(state, "done"));

        Assert.Equal(RedisEffectResult.Committed, result);
        Assert.Equal("done", (string?)await rig.Direct.StringGetAsync(state));
    }

    [Fact]
    public async Task A_claim_s_effect_delayed_on_the_wire_past_the_claim_s_ttl_fails_at_EXEC()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var won = Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync("k", TimeSpan.FromMilliseconds(500)));
        var state = RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k") + ":state";
        var hold = rig.Hold.HoldNextMulti();

        var effect = claims.ExecuteAsync("k", won.Token, t => _ = t.StringSetAsync(state, "late"));
        await hold.Held.WaitAsync(Wait);
        await Poll.UntilAsync(async () => !await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")), "the claim key expires while the EXEC is held");
        hold.Release();

        Assert.Equal(RedisEffectResult.Refused, await effect.WaitAsync(Wait));
        Assert.False(await rig.Direct.KeyExistsAsync(state));
    }

    [Fact]
    public async Task A_claim_s_effect_delayed_past_a_successor_s_grant_is_refused_and_the_successor_s_commits()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Claims(OnRedisUnavailable.Refuse);
        var first = Assert.IsType<ClaimResult.Won>(await a.ClaimAsync("k", TimeSpan.FromMilliseconds(300)));
        var state = RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k") + ":state";
        ClaimResult.Won? second = null;
        await Poll.UntilAsync(async () => (second = await b.ClaimAsync("k", TimeSpan.FromMinutes(1)) as ClaimResult.Won) is not null, "b wins the expired claim");

        Assert.Equal(RedisEffectResult.Refused, await a.ExecuteAsync("k", first.Token, t => _ = t.StringSetAsync(state, "stale")));
        Assert.False(await rig.Direct.KeyExistsAsync(state));
        Assert.Equal(RedisEffectResult.Committed, await b.ExecuteAsync("k", second!.Token, t => _ = t.StringSetAsync(state, "current")));
        Assert.Equal("current", (string?)await rig.Direct.StringGetAsync(state));
    }

    [Fact]
    public async Task Parallel_effects_on_one_multiplexer_commit_under_the_right_tokens_and_a_stale_token_writes_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var claims = (await rig.AddReplicaAsync("a")).Claims(OnRedisUnavailable.Refuse);
        var tokens = new Guid[10];
        for (var k = 0; k < tokens.Length; k++)
        {
            tokens[k] = Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync($"k{k}", TimeSpan.FromMinutes(1))).Token;
        }

        var calls = Enumerable.Range(0, 50)
            .Select(n => (N: n, Key: $"k{n / 2 % 10}", Right: n % 2 == 0, State: RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, $"k{n / 2 % 10}") + $":state-{n}"))
            .ToList();
        var results = await Task.WhenAll(calls.Select(c => Task.Run(() => claims.ExecuteAsync(
            c.Key, c.Right ? tokens[c.N / 2 % 10] : Guid.NewGuid(), t => _ = t.StringSetAsync(c.State, $"v{c.N}")))));

        for (var i = 0; i < calls.Count; i++)
        {
            var call = calls[i];
            Assert.Equal(call.Right ? RedisEffectResult.Committed : RedisEffectResult.Refused, results[i]);
            Assert.Equal(call.Right ? $"v{call.N}" : null, (string?)await rig.Direct.StringGetAsync(call.State));
        }
    }

    [Fact]
    public async Task A_replica_that_does_not_serve_grants_nothing_and_writes_no_key()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        replica.Serving = false;

        Assert.IsType<ClaimResult.NotServing>(await replica.Claims(OnRedisUnavailable.Grant).ClaimAsync("k", TimeSpan.FromMinutes(1)));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")));
    }

    [Fact]
    public async Task The_registrations_resolve_the_Redis_primitives_over_the_applications_connection_and_the_key_prefix()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        await using var connection = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        var services = new ServiceCollection()
            .AddSingleton<IConnectionMultiplexer>(connection)
            .AddSingleton<IClaimGate>(replica)
            .AddSingleton(TimeProvider.System)
            .AddReeveClusterRedis(o => o.KeyPrefix = rig.Prefix);
        services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);
        services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse);
        services.AddReeveClusterRedisBudget(OnRedisUnavailable.Refuse);
        services.AddReeveClusterRedisCounter();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<ClaimResult.Won>(await provider.GetRequiredService<IRedisClaims>().ClaimAsync("k", TimeSpan.FromMinutes(1)));
        Assert.True(await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")));
        Assert.NotNull(provider.GetRequiredService<IRedisSingleFlight>());
        Assert.NotNull(provider.GetRequiredService<IRedisBudget>());
        Assert.Equal(new CounterResult.Incremented(1), await provider.GetRequiredService<IRedisCounter>().IncrementAsync("c"));
    }
}
