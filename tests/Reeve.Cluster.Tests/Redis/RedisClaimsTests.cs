using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;
using Unit.Claims;
using Unit.Diagnostics;

namespace Unit.Redis;

[Collection(MetricsCollection.Name)]
public sealed class RedisClaimsTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    private readonly ScriptedClaimRows rows = new();
    private readonly ScriptedGate gate = new();

    public static IEnumerable<object[]> Outages() => RedisFailures.Outages();

    private RedisClaims Claims(OnRedisUnavailable policy = OnRedisUnavailable.Refuse) => new(rows, gate, policy);

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_claims_nothing_and_touches_no_key()
    {
        gate.Serving = false;

        Assert.IsType<ClaimResult.NotServing>(await Claims().ClaimAsync("k", Ttl));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_grant_writes_a_drawn_token_with_the_ttl_and_the_serving_replica_as_holder_and_answers_Won()
    {
        rows.Grant.Returns(true);

        var result = await Claims().ClaimAsync("k", Ttl);

        var grant = Assert.Single(rows.Grants);
        Assert.Equal((RedisSpace.Claim, "k", "r1", Ttl), (grant.Space, grant.Key, grant.Holder, grant.Ttl));
        Assert.NotEqual(Guid.Empty, grant.Token);
        Assert.Equal(new ClaimResult.Won(grant.Token), result);
    }

    [Fact]
    public async Task Each_grant_draws_its_own_token()
    {
        rows.Grant.Returns(true).Returns(true);

        await Claims().ClaimAsync("k", Ttl);
        await Claims().ClaimAsync("k", Ttl);

        Assert.NotEqual(rows.Grants[0].Token, rows.Grants[1].Token);
    }

    [Fact]
    public async Task I16_a_claim_that_exists_is_lost_to_its_holder()
    {
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(true, "r2", null));

        Assert.Equal(new ClaimResult.Lost("r2"), await Claims().ClaimAsync("k", Ttl));
    }

    [Fact]
    public async Task A_lost_claim_whose_value_names_no_holder_is_Lost_with_an_empty_holder()
    {
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(true, string.Empty, null));

        Assert.Equal(new ClaimResult.Lost(string.Empty), await Claims().ClaimAsync("k", Ttl));
    }

    [Fact]
    public async Task A_claim_that_ended_between_the_grant_and_the_read_is_granted_again()
    {
        rows.Grant.Returns(false).Returns(true);
        rows.Read.Returns(new RedisRecord(false, null, null));

        Assert.IsType<ClaimResult.Won>(await Claims().ClaimAsync("k", Ttl));
        Assert.Equal(["grant", "read", "grant"], rows.Calls);
        Assert.NotEqual(rows.Grants[0].Token, rows.Grants[1].Token);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_lost_grant_whose_read_is_unanswered_is_Unknown_under_Refuse_never_Lost_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Returns(false);
        rows.Read.Throws(outage);

        var result = await Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.Unknown>(result);
        Assert.Equal(["grant", "read"], rows.Calls);
        AssertUsed(recorder, "claim", "refuse", 1);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_lost_grant_whose_read_is_unanswered_admits_under_Grant_with_a_token_no_key_holds_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Returns(false);
        rows.Read.Throws(outage);

        var result = await Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl);

        Assert.True(result.TryGetToken(out var token));
        Assert.NotEqual(rows.Grants[0].Token, token);
        Assert.Equal(["grant", "read"], rows.Calls);
        AssertUsed(recorder, "claim", "grant", 1);
    }

    [Fact]
    public async Task A_read_that_is_rejected_for_its_own_sake_after_a_lost_grant_is_thrown_and_uses_no_policy()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Returns(false);
        rows.Read.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl));
        Assert.Empty(recorder.Measurements());
    }

    private void ScriptGoneAtEveryRead()
    {
        for (var i = 0; i < 3; i++)
        {
            rows.Grant.Returns(false);
            rows.Read.Returns(new RedisRecord(false, null, null));
        }
    }

    [Fact]
    public async Task A_claim_that_is_gone_at_every_read_ends_after_three_grants_with_Unknown_under_Refuse_and_counts_the_use()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        ScriptGoneAtEveryRead();

        var result = await Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.Unknown>(result);
        Assert.Equal(["grant", "read", "grant", "read", "grant", "read"], rows.Calls);
        AssertUsed(recorder, "claim", "refuse", 1);
    }

    [Fact]
    public async Task A_claim_that_is_gone_at_every_read_ends_after_three_grants_with_a_token_no_key_holds_under_Grant_and_counts_the_use()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        ScriptGoneAtEveryRead();

        var result = await Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl);

        Assert.True(result.TryGetToken(out var token));
        Assert.DoesNotContain(token, rows.Grants.Select(g => g.Token));
        Assert.Equal(3, rows.Grants.Count);
        AssertUsed(recorder, "claim", "grant", 1);
    }

    [Fact]
    public async Task A_replica_that_stops_serving_between_two_grants_of_one_claim_grants_no_more()
    {
        rows.Grant.Returns(false);
        rows.Read.Does(() =>
        {
            gate.Serving = false;
            return new RedisRecord(false, null, null);
        });

        Assert.IsType<ClaimResult.NotServing>(await Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl));
        Assert.Equal(["grant", "read"], rows.Calls);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Refuse_answers_an_unavailable_Redis_with_Unknown_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(outage);

        var result = await Claims(OnRedisUnavailable.Refuse).ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.Unknown>(result);
        Assert.False(result.IsWon);
        AssertUsed(recorder, "claim", "refuse", 1);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Grant_answers_an_unavailable_Redis_with_a_Won_token_no_key_holds_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(outage);

        var result = await Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl);

        Assert.True(result.TryGetToken(out var token));
        Assert.NotEqual(Guid.Empty, token);
        Assert.NotEqual(rows.Grants[0].Token, token);
        AssertUsed(recorder, "claim", "grant", 1);
    }

    [Fact]
    public async Task Every_use_of_the_policy_is_counted_and_a_call_Redis_answered_is_not()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(RedisFailures.Connection()).Returns(true).Throws(RedisFailures.Timeout()).Throws(RedisFailures.ReadOnly());
        var claims = Claims(OnRedisUnavailable.Refuse);

        await claims.ClaimAsync("k", Ttl);
        await claims.ClaimAsync("k", Ttl);
        await claims.ClaimAsync("k", Ttl);
        await claims.ClaimAsync("k", Ttl);

        AssertUsed(recorder, "claim", "refuse", 3);
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Grant)]
    [InlineData(OnRedisUnavailable.Refuse)]
    public async Task A_rejection_that_is_not_an_outage_is_thrown_whatever_the_policy_and_uses_none(OnRedisUnavailable policy)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Claims(policy).ClaimAsync("k", Ttl));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task A_cancel_by_the_caller_throws_and_uses_no_policy()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(new OperationCanceledException());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Claims(OnRedisUnavailable.Grant).ClaimAsync("k", Ttl));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public void A_value_that_is_not_a_policy_is_refused_at_construction_as_a_configuration_error() =>
        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, Assert.Throws<ClusterConfigurationException>(() => Claims(default)).Reason);

    [Fact]
    public async Task The_arguments_are_checked_before_anything_is_sent()
    {
        var claims = Claims();

        await Assert.ThrowsAsync<ArgumentNullException>(() => claims.ClaimAsync(null!, Ttl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => claims.ClaimAsync("k", TimeSpan.Zero));
        Assert.Empty(rows.Calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_release_answers_what_the_token_compare_did(bool deleted)
    {
        var token = Guid.NewGuid();
        rows.Release.Returns(deleted);

        Assert.Equal(deleted, await Claims().ReleaseAsync("k", token));
        Assert.Equal([(RedisSpace.Claim, "k", token)], rows.Releases);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_release_Redis_did_not_answer_is_unknown_never_released_and_uses_no_policy(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Release.Throws(outage);

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => Claims(OnRedisUnavailable.Grant).ReleaseAsync("k", Guid.NewGuid()));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task An_effect_the_claim_key_admitted_is_committed()
    {
        var token = Guid.NewGuid();
        rows.Execute.Returns(true);

        Assert.Equal(RedisEffectResult.Committed, await Claims().ExecuteAsync("k", token, _ => { }));
        Assert.Equal([(RedisSpace.Claim, "k", token)], rows.Executions);
    }

    [Fact]
    public async Task An_EXEC_that_did_not_run_the_effect_because_the_watched_key_changed_or_expired_is_refused()
    {
        rows.Execute.Returns(false);

        Assert.Equal(RedisEffectResult.Refused, await Claims().ExecuteAsync("k", Guid.NewGuid(), _ => { }));
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task An_effect_Redis_did_not_answer_is_unknown_under_either_policy_and_uses_none(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Execute.Throws(outage).Throws(outage);

        Assert.Equal(RedisEffectResult.Unknown, await Claims(OnRedisUnavailable.Grant).ExecuteAsync("k", Guid.NewGuid(), _ => { }));
        Assert.Equal(RedisEffectResult.Unknown, await Claims(OnRedisUnavailable.Refuse).ExecuteAsync("k", Guid.NewGuid(), _ => { }));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task A_replica_that_does_not_serve_sends_no_effect_and_answers_NotServing()
    {
        gate.Serving = false;

        Assert.Equal(RedisEffectResult.NotServing, await Claims().ExecuteAsync("k", Guid.NewGuid(), _ => { }));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task An_effect_rejected_for_its_own_sake_is_thrown()
    {
        rows.Execute.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Claims().ExecuteAsync("k", Guid.NewGuid(), _ => { }));
    }

    internal static void AssertUsed(InstrumentRecorder recorder, string primitive, string policy, int times)
    {
        var uses = recorder.Measurements();
        Assert.Equal(times, uses.Count);
        Assert.All(uses, use =>
        {
            Assert.Equal(1, use.Value);
            Assert.Equal(primitive, use.Tags["primitive"]);
            Assert.Equal(policy, use.Tags["policy"]);
            Assert.Equal(2, use.Tags.Count);
        });
    }
}
