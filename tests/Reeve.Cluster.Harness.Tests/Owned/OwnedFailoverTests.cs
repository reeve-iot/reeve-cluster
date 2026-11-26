using System.Diagnostics;
using Harness.Failover;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;

namespace Harness.Owned;

[Collection("harness-failover")]
public sealed class OwnedFailoverTests(ITestOutputHelper output)
{
    private const string Placed = "widget";
    private const string Keyed = "conn";
    private const string Key = "k";

    private const int SuffixDraws = 100;

    [Fact]
    public async Task I12_I32_no_second_owner_across_a_failover_the_new_tenure_writes_after_every_effect_of_the_old_one()
    {
        await using var rig = await OwnedRig.CreateWithStandbyAsync(output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(2, TimeSpan.FromMilliseconds(200), Redis: true));
                services.AddClusterUnit<OwnedWriter>(Placed, o => o.RedisFence = true);
            },
            configure: Configure,
            redis: true,
            profile: FailoverRig.Profile);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        var first = await rig.AwaitTenureAsync(Placed, Placed, 0, 6);
        Assert.Equal(1, OwnedRig.TimelineOf(first.Token));
        var epochs = await FailoverTests.EpochsAsync(rig.Failover, r1, r2);

        var before = rig.Run.Sequence.Next();
        var onTheTimeline = await FailoverTests.MoveAsync(rig.Failover, r1, rig.Failover.Stack.FailOverAsync);
        await FailoverTests.WaitForRejoinAsync(rig.Failover, epochs, r1, r2);
        var second = await rig.AwaitTenureAsync(Placed, Placed, first.Token, 4);

        Assert.True(second.Token > first.Token);
        Assert.Equal(2, OwnedRig.TimelineOf(second.Token));
        if (onTheTimeline)
        {
            FailoverTests.AssertFencedOnceOnTheTimeline(rig.Failover, r1, before);
            FailoverTests.AssertFencedOnceOnTheTimeline(rig.Failover, r2, before);
        }

        var tenures = (await rig.EffectsAsync()).Select(e => e.Tenure).ToList();
        Assert.Contains(first.Token, tenures);
        Assert.Contains(second.Token, tenures);
        OwnedRig.AssertNoInterleave(tenures, "the fenced effects in commit order");
        await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(second)).Contains(second.Token), "the new tenure's write in Redis");
        var redis = await rig.RedisLogAsync(second);
        Assert.Contains(first.Token, redis);
        OwnedRig.AssertNoInterleave(redis, "the Redis list in the order Redis took it");

        Assert.Equal("RC002", await rig.AssertOwnerStateAsync(Placed, Placed, first.Token, 1));
        Assert.Equal("RC001", await rig.AssertOwnerStateAsync(Placed, Placed, first.Token, 2));

        var holders = rig.Run.Replicas.Where(r => r.IsRunning && r.Services.GetRequiredService<UnitRuntime>().Holds(Placed, Placed)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { (await rig.RowAsync(Placed, Placed))!.Value.Owner }, holders);
    }

    [Fact]
    public async Task I10_I32_a_lost_adoption_the_old_holders_fenced_writes_are_refused_on_the_new_timeline_and_a_token_drawn_there_is_above_the_lost_one_in_PostgreSQL_and_in_Redis()
    {
        await using var rig = await OwnedRig.CreateWithStandbyAsync(output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(2, TimeSpan.FromMilliseconds(150), Redis: true));

                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k =>
                {
                    k.Policy = AdoptionPolicy.None;
                    k.RedisFence = true;
                });
            },
            configure: Configure,
            redis: true,
            profile: FailoverRig.Profile);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);

        var ghost = await rig.ScalarAsync(
            "INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token) VALUES ('conn', 'k', 'ghost', 1, reeve_cluster.draw_token()) RETURNING token");
        var epochs = await FailoverTests.EpochsAsync(rig.Failover, r1, r2);

        var before = rig.Run.Sequence.Next();
        var since = (rig.Run.Stalls.Begin(), Stopwatch.StartNew());
        await rig.Failover.Stack.ReplicationLink.CutAsync();
        await rig.Failover.WaitStandbyGoneAsync();
        OwnedStart lostStart;
        int lostEffects;
        await using (rig.Failover.CancelSyncWaits())
        {
            await DrawAsync(rig, SuffixDraws);
            Assert.Equal(CreateOutcome.Created, await r2.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
            lostStart = await rig.Log.StartAsync(s => s.Kind == Keyed && s.Replica == "r2", "r2 to run the key it adopted");
            await OwnedRig.UntilAsync(
                async () => (await rig.EffectsAsync()).Count(e => e.Tenure == lostStart.Token) >= 3 && (await rig.RedisLogAsync(lostStart)).Contains(lostStart.Token),
                "the lost tenure's writes in PostgreSQL and in Redis");
            lostEffects = (await rig.EffectsAsync()).Count(e => e.Tenure == lostStart.Token);
        }

        var lost = lostStart.Token;
        Assert.Equal(1, OwnedRig.TimelineOf(lost));
        Assert.True(lost > ghost);
        Assert.Equal(("r2", lost), (await rig.RowAsync(Keyed, Key)) is { Owner: { } owner, Token: var token } ? (owner, token) : default);
        var redisDb = await rig.RedisAsync();
        Assert.Equal(lost, (long)await redisDb.StringGetAsync($"{lostStart.RedisTag}:hw"));

        var onTheTimeline = await FailoverTests.MoveAsync(rig.Failover, r1, rig.Failover.Stack.FailOverAsync, since);
        await FailoverTests.WaitForRejoinAsync(rig.Failover, epochs, r1, r2);
        Assert.Equal(2, await rig.Failover.TimelineAsync());
        if (onTheTimeline)
        {
            FailoverTests.AssertFencedOnceOnTheTimeline(rig.Failover, r1, before);
            FailoverTests.AssertFencedOnceOnTheTimeline(rig.Failover, r2, before);
        }

        Assert.Equal(("ghost", ghost), (await rig.RowAsync(Keyed, Key)) is { Owner: { } orphan, Token: var kept } ? (orphan, kept) : default);
        Assert.True(lostEffects >= 3);
        Assert.DoesNotContain(lost, (await rig.EffectsAsync()).Select(e => e.Tenure));

        Assert.Equal("RC002", await rig.AssertOwnerStateAsync(Keyed, Key, lost, 1));
        Assert.Equal("RC001", await rig.AssertOwnerStateAsync(Keyed, Key, lost, 2));
        await using (var connection = new NpgsqlConnection(r2.Options.ConnectionString))
        {
            await connection.OpenAsync();
            await Assert.ThrowsAsync<OwnershipLostException>(() => lostStart.Context.BeginFencedTransactionAsync(connection));
        }

        Assert.Equal(CreateOutcome.Created, await r1.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var again = await rig.Log.StartAsync(s => s.Kind == Keyed && OwnedRig.TimelineOf(s.Token) == 2, "r1 to run the key on the new timeline");

        Assert.True(
            OwnedRig.SequenceValueOf(again.Token) <= OwnedRig.SequenceValueOf(lost),
            $"the new token {again.Token} does not reuse a sequence value of the suffix (lost token {lost}): the history did not meet a reused value");
        Assert.True(again.Token > lost, $"the new token {again.Token} is not above the lost token {lost}");
        Assert.Equal(1, OwnedRig.TimelineOf(lost));
        Assert.Equal(2, OwnedRig.TimelineOf(again.Token));

        await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(again)).Contains(again.Token), "the new owner's writes in Redis");
        Assert.Equal(again.Token, (long)await redisDb.StringGetAsync($"{again.RedisTag}:hw"));
        var staleKey = OwnedWriter.RedisLogKey(lostStart.RedisTag!);
        await Assert.ThrowsAsync<OwnershipLostException>(() => lostStart.Context.FenceRedisAsync(t => _ = t.ListRightPushAsync(staleKey, lost)));
        var list = await rig.RedisLogAsync(again);
        Assert.Contains(lost, list);
        OwnedRig.AssertNoInterleave(list, "the Redis list in the order Redis took it");
        output.WriteLine($"lost token {lost}, new token {again.Token}, on the timeline: {onTheTimeline}");
    }

    private static async Task DrawAsync(OwnedRig rig, int draws)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        for (var i = 0; i < draws; i++)
        {
            await using var command = new NpgsqlCommand("SELECT reeve_cluster.draw_token()", connection);
            await command.ExecuteScalarAsync();
        }
    }

    private static void Configure(ClusterOptions o)
    {
        o.ReplicationCheckInterval = TimeSpan.FromSeconds(1);
        o.RejoinBackoff = TestProfile.RejoinBackoff;
        o.StabilisationWindow = TimeSpan.FromMinutes(10);
    }
}
