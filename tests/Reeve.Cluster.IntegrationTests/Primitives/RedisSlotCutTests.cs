using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisSlotCutTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly RedisFixture redis;

    public RedisSlotCutTests(RedisFixture redis) => this.redis = redis;

    private static async Task<(RedisPrimitivesRig Rig, RedisReplica Replica)> RigAsync(RedisFixture redis)
    {
        var rig = await RedisPrimitivesRig.CreateAsync(redis);
        return (rig, await rig.AddReplicaAsync("a"));
    }

    [Fact]
    public async Task Refuse_under_a_cut_answers_Unknown_grants_nothing_and_counts_the_use()
    {
        var (rig, replica) = await RigAsync(redis);
        await using var _ = rig;
        var slots = replica.Slots(OnRedisUnavailable.Refuse);
        Assert.True((await slots.AcquireAsync("warm", "i", 1, Minute)).IsAcquired);
        using var uses = new PolicyUseListener();
        rig.CutOff.Cut();

        Assert.IsType<SlotAcquireResult.Unknown>(await slots.AcquireAsync("k", "i", 1, Minute));

        Assert.Equal(1, uses.Count("slot", "refuse"));
        Assert.Equal(1, uses.Total());
        rig.CutOff.Restore();
        await Poll.UntilAsync(async () => (await slots.AcquireAsync("k", "i", 1, Minute)).IsAcquired, "Redis is back and the slot is granted");
    }

    [Fact]
    public async Task Grant_under_a_cut_answers_Acquired_with_a_token_no_member_holds_whose_renewal_and_effect_find_nothing_to_extend()
    {
        var (rig, replica) = await RigAsync(redis);
        await using var _ = rig;
        var slots = replica.Slots(OnRedisUnavailable.Grant);
        Assert.True((await slots.AcquireAsync("warm", "i", 1, Minute)).IsAcquired);
        using var uses = new PolicyUseListener();
        rig.CutOff.Cut();

        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;

        Assert.Equal(1, uses.Count("slot", "grant"));
        Assert.Equal(1, uses.Total());
        rig.CutOff.Restore();
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", token, Minute));
        Assert.Equal(RedisEffectResult.NotServing, await slots.ExecuteAsync("k", "i", token, _ => { }));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.SlotHeldKey("k")));
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Refuse)]
    [InlineData(OnRedisUnavailable.Grant)]
    public async Task A_renewal_a_release_and_an_effect_under_a_cut_are_unknown_under_either_policy_and_count_nothing(OnRedisUnavailable policy)
    {
        var (rig, replica) = await RigAsync(redis);
        await using var _ = rig;
        var slots = replica.Slots(policy);
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await slots.AcquireAsync("k", "i", 1, Minute)).Token;
        using var uses = new PolicyUseListener();
        rig.CutOff.Cut();

        Assert.Equal(SlotRenewResult.Unknown, await slots.RenewAsync("k", "i", token, Minute));
        Assert.Equal(RedisEffectResult.Unknown, await slots.ExecuteAsync("k", "i", token, _ => { }));
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => slots.ReleaseAsync("k", "i", token));

        Assert.Equal(0, uses.Total());
        rig.CutOff.Restore();
        Assert.InRange(await rig.Direct.SortedSetLengthAsync(rig.SlotHeldKey("k")), 0, 1);
    }
}
