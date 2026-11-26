using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Unit.Redis;

public sealed class RedisDecisionsTests
{
    public static IEnumerable<object[]> Outages() => RedisFailures.Outages();

    public static IEnumerable<object[]> Defects() =>
    [
        [RedisFailures.WrongType()],
        [RedisFailures.Server(RedisErrorKind.NoPermission, "NOPERM this user has no permissions to run the 'set' command")],
        [RedisFailures.Server(RedisErrorKind.UnknownCommand, "ERR unknown command")],
        [new RedisCommandException("Command cannot be issued to a replica")],
        [new InvalidOperationException("a defect")],
        [new OperationCanceledException()],
    ];

    [Theory]
    [MemberData(nameof(Outages))]
    public void A_cut_connection_a_timeout_and_a_failover_state_are_an_outage(Exception exception) =>
        Assert.True(RedisFailure.IsUnavailable(exception));

    [Theory]
    [InlineData(RedisErrorKind.Loading)]
    [InlineData(RedisErrorKind.MasterDown)]
    [InlineData(RedisErrorKind.ClusterDown)]
    [InlineData(RedisErrorKind.TryAgain)]
    [InlineData(RedisErrorKind.NoReplicas)]
    [InlineData(RedisErrorKind.ReadOnly)]
    public void The_states_of_a_restart_and_a_failover_are_an_outage(RedisErrorKind kind) =>
        Assert.True(RedisFailure.IsUnavailable(RedisFailures.Server(kind, "reply")));

    [Theory]
    [MemberData(nameof(Defects))]
    public void A_rejection_for_the_command_s_own_sake_and_a_cancel_are_not_an_outage(Exception exception) =>
        Assert.False(RedisFailure.IsUnavailable(exception));

    [Fact]
    public void Grant_goes_on_and_Refuse_stops()
    {
        Assert.Equal(UnavailableAction.Proceed, RedisDecisions.OnUnavailable(OnRedisUnavailable.Grant));
        Assert.Equal(UnavailableAction.Stop, RedisDecisions.OnUnavailable(OnRedisUnavailable.Refuse));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(-1)]
    public void A_value_that_is_not_a_policy_is_refused(int value) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => RedisDecisions.OnUnavailable((OnRedisUnavailable)value));

    [Fact]
    public void The_default_value_of_the_policy_is_not_a_policy() =>
        Assert.False(Enum.IsDefined(default(OnRedisUnavailable)));

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(-1)]
    public void A_value_that_is_not_a_policy_is_a_named_configuration_error_not_a_default(int value)
    {
        var refusal = Assert.Throws<ClusterConfigurationException>(() => RedisDecisions.RequirePolicy((OnRedisUnavailable)value));

        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, refusal.Reason);
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Grant)]
    [InlineData(OnRedisUnavailable.Refuse)]
    public void A_policy_is_kept_as_it_is(OnRedisUnavailable policy) =>
        Assert.Equal(policy, RedisDecisions.RequirePolicy(policy));

    [Theory]
    [InlineData(true, 1, 3, 1)]
    [InlineData(true, 3, 3, 3)]
    public void A_new_id_within_the_limit_is_granted_with_the_count(bool added, long count, int limit, int used) =>
        Assert.Equal(new BudgetResult.Granted(used), RedisDecisions.AfterConsume(new RedisBudgetStatement(added, count, 10), limit));

    [Fact]
    public void A_new_id_that_takes_the_set_past_the_limit_is_exhausted() =>
        Assert.Equal(new BudgetResult.Exhausted(), RedisDecisions.AfterConsume(new RedisBudgetStatement(true, 4, 10), 3));

    [Theory]
    [InlineData(1, 3)]
    [InlineData(4, 3)]
    public void An_id_already_in_the_set_is_already_counted_whatever_the_count(long count, int limit) =>
        Assert.Equal(new BudgetResult.AlreadyCounted(), RedisDecisions.AfterConsume(new RedisBudgetStatement(false, count, 10), limit));

    [Fact]
    public void A_grant_on_an_unavailable_Redis_counts_nothing() =>
        Assert.Equal(0, RedisDecisions.UncountedGrant);

    [Fact]
    public void The_keys_of_one_record_share_a_hash_tag_named_by_the_prefix_the_space_and_the_key()
    {
        var claimTag = RedisPrimitiveKeys.ClaimHashTag("reeve:", "order-1");
        var flightTag = RedisPrimitiveKeys.FlightHashTag("reeve:", "order-1");

        Assert.Equal("{reeve:claim:order-1}", claimTag);
        Assert.Equal("{reeve:budget:order-1}", RedisPrimitiveKeys.BudgetHashTag("reeve:", "order-1"));
        Assert.Equal("{reeve:counter:order-1}", RedisPrimitiveKeys.CounterHashTag("reeve:", "order-1"));
        Assert.Equal("{reeve:flight:order-1}", flightTag);
        Assert.Equal("{a:claim:k}", RedisPrimitiveKeys.ClaimHashTag("a:", "k"));

        AssertKeysShareHashTag(claimTag, ":token", ":result");
        AssertKeysShareHashTag(flightTag, ":token", ":result");
    }

    private static void AssertKeysShareHashTag(string tag, params string[] suffixes)
    {
        var keys = suffixes.Select(suffix => tag + suffix).ToArray();
        var hashTags = keys.Select(key => key[(key.IndexOf('{') + 1)..key.IndexOf('}')]).Distinct();

        Assert.Single(hashTags);
    }

    [Theory]
    [InlineData("r1")]
    [InlineData("")]
    [InlineData("zone:a:1")]
    public void A_claim_value_carries_the_token_and_everything_after_the_first_colon_is_the_holder(string holder)
    {
        var token = Guid.NewGuid();

        var value = RedisClaimValue.Encode(token, holder);

        Assert.Equal(token.ToString("N") + ":" + holder, value);
        Assert.Equal(holder, RedisClaimValue.Holder(value));
        Assert.True(RedisClaimValue.Holds(value, token));
        Assert.False(RedisClaimValue.Holds(value, Guid.NewGuid()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("short")]
    [InlineData("0123456789abcdef0123456789abcdefXholder")]
    public void A_value_that_is_not_of_that_form_names_no_holder_and_holds_no_token(string value)
    {
        Assert.Equal(string.Empty, RedisClaimValue.Holder(value));
        Assert.False(RedisClaimValue.Holds(value, Guid.Parse("0123456789abcdef0123456789abcdef")));
    }

    [Fact]
    public void A_grant_is_made_at_most_three_times_in_one_call() =>
        Assert.Equal(3, RedisDecisions.MaxGrantAttempts);

    [Fact]
    public void A_token_is_128_random_bits_and_not_a_version_4_uuid()
    {
        var tokens = Enumerable.Range(0, 512).Select(_ => RedisTokens.Draw()).ToList();

        Assert.DoesNotContain(Guid.Empty, tokens);
        Assert.Equal(tokens.Count, tokens.Distinct().Count());

        var versions = tokens.Select(t => t.ToByteArray()[7] >> 4).Distinct().Count();
        Assert.True(versions >= 8, $"only {versions} version nibbles in 512 draws");
        var variants = tokens.Select(t => t.ToByteArray()[8] >> 6).Distinct().Count();
        Assert.Equal(4, variants);
    }

    [Theory]
    [InlineData(1, 1, true)]
    [InlineData(2, 1, false)]
    [InlineData(3, 3, true)]
    [InlineData(4, 3, false)]
    [InlineData(0, 1, true)]
    public void A_slot_grant_is_admitted_while_the_count_it_read_is_at_most_the_limit(long count, int limit, bool admitted) =>
        Assert.Equal(admitted, RedisDecisions.SlotAdmitted(count, limit));

    [Theory]
    [InlineData(null, false)]
    [InlineData(31_000d, true)]
    [InlineData(0d, true)]
    public void A_slot_renewal_extended_the_slot_when_a_score_came_back(double? score, bool renewed) =>
        Assert.Equal(renewed, RedisDecisions.SlotRenewed(score));
}
