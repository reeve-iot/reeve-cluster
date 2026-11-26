using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisPolicyCutTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(1);

    private readonly RedisFixture redis;

    public RedisPolicyCutTests(RedisFixture redis) => this.redis = redis;

    private static async Task<(RedisPrimitivesRig Rig, RedisReplica Replica)> CutRigAsync(RedisFixture redis)
    {
        var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");

        Assert.IsType<CounterResult.Incremented>(await replica.Counter().IncrementAsync("warm"));
        rig.CutOff.Cut();
        return (rig, replica);
    }

    [Fact]
    public async Task Refuse_under_a_cut_grants_nothing_in_any_primitive_and_counts_each_use()
    {
        var (rig, replica) = await CutRigAsync(redis);
        await using var _ = rig;
        using var uses = new PolicyUseListener();
        var computations = 0;

        Assert.IsType<ClaimResult.Unknown>(await replica.Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", Ttl));
        Assert.IsType<SingleFlightResult.Unknown>(await replica.Flight(OnRedisUnavailable.Refuse).RunAsync("k", _ => { computations++; return Task.FromResult<byte[]>([1]); }, Ttl, Ttl));
        Assert.IsType<BudgetResult.Unknown>(await replica.Budget(OnRedisUnavailable.Refuse).ConsumeAsync("k", "i1", 3, Ttl));

        Assert.Equal(0, computations);
        Assert.Equal(1, uses.Count("claim", "refuse"));
        Assert.Equal(1, uses.Count("single_flight", "refuse"));
        Assert.Equal(1, uses.Count("budget", "refuse"));
        Assert.Equal(3, uses.Total());
    }

    [Fact]
    public async Task Grant_under_a_cut_proceeds_in_each_primitive_without_a_confirmed_success_and_counts_each_use()
    {
        var (rig, replica) = await CutRigAsync(redis);
        await using var _ = rig;
        using var uses = new PolicyUseListener();
        var computations = 0;

        Assert.True((await replica.Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl)).IsWon);
        var flight = await replica.Flight(OnRedisUnavailable.Grant).RunAsync("k", _ => { computations++; return Task.FromResult<byte[]>([1]); }, Ttl, Ttl);
        var budget = await replica.Budget(OnRedisUnavailable.Grant).ConsumeAsync("k", "i1", 3, Ttl);

        Assert.IsType<SingleFlightResult.ComputedUnknown>(flight);
        Assert.Equal(new BudgetResult.Granted(0), budget);
        Assert.Equal(1, computations);
        Assert.Equal(1, uses.Count("claim", "grant"));
        Assert.Equal(1, uses.Count("single_flight", "grant"));
        Assert.Equal(1, uses.Count("budget", "grant"));
        Assert.Equal(3, uses.Total());
    }

    [Fact]
    public async Task A_counter_under_a_cut_is_Unknown_and_uses_no_policy()
    {
        var (rig, replica) = await CutRigAsync(redis);
        await using var _ = rig;
        using var uses = new PolicyUseListener();

        Assert.IsType<CounterResult.Unknown>(await replica.Counter().IncrementAsync("c"));
        Assert.Equal(0, uses.Total());
    }

    [Fact]
    public async Task A_cut_between_the_grant_and_the_effect_leaves_the_effect_and_the_release_unknown_under_either_policy()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        using var uses = new PolicyUseListener();
        var claims = replica.Claims(OnRedisUnavailable.Grant);
        var won = Assert.IsType<ClaimResult.Won>(await claims.ClaimAsync("k", Ttl));
        var state = RedisPrimitiveKeys.ClaimHashTag(rig.Prefix, "k") + ":state";

        rig.CutOff.Cut();

        Assert.Equal(RedisEffectResult.Unknown, await claims.ExecuteAsync("k", won.Token, t => _ = t.StringSetAsync(state, "x")));
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => claims.ReleaseAsync("k", won.Token));
        Assert.Equal(0, uses.Total());
        rig.CutOff.Restore();
        Assert.False(await rig.Direct.KeyExistsAsync(state));
    }

    [Fact]
    public async Task A_cut_during_a_computation_makes_its_result_write_unknown_and_keeps_the_value()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        using var uses = new PolicyUseListener();

        var result = await replica.Flight(OnRedisUnavailable.Refuse).RunAsync(
            "k",
            _ =>
            {
                rig.CutOff.Cut();
                return Task.FromResult<byte[]>([7]);
            },
            Ttl,
            Ttl);

        FlightAnswer.Is<SingleFlightResult.ComputedUnknown>([7], result);
        Assert.Equal(0, uses.Total());
    }

    [Fact]
    public async Task The_primitives_work_again_when_the_cut_ends_and_nothing_was_granted_meanwhile()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        var claims = replica.Claims(OnRedisUnavailable.Refuse);
        Assert.IsType<CounterResult.Incremented>(await replica.Counter().IncrementAsync("warm"));
        rig.CutOff.Cut();
        Assert.IsType<ClaimResult.Unknown>(await claims.ClaimAsync("k", Ttl));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.ClaimTokenKey("k")));

        rig.CutOff.Restore();

        ClaimResult? after = null;
        await Poll.UntilAsync(async () => (after = await claims.ClaimAsync("k", Ttl)) is ClaimResult.Won, "the claim is granted after the cut", TimeSpan.FromSeconds(60));
        Assert.True(after!.IsWon);
    }
}
