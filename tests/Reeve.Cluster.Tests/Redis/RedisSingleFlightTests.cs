using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;
using Unit.Claims;
using Unit.Diagnostics;
using Unit.Testing;

namespace Unit.Redis;

[Collection(MetricsCollection.Name)]
public sealed class RedisSingleFlightTests
{
    private static readonly TimeSpan ClaimTtl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ResultTtl = TimeSpan.FromMinutes(5);
    private static readonly byte[] Value = [1, 2, 3];

    private readonly ScriptedClaimRows rows = new();
    private readonly ScriptedGate gate = new();
    private readonly SignallingTimeProvider time = new();
    private int computations;

    private static readonly RedisRecord Held = new(true, "r1", null);

    public static IEnumerable<object[]> Outages() => RedisFailures.Outages();

    private RedisSingleFlight Flight(OnRedisUnavailable policy = OnRedisUnavailable.Refuse) => new(rows, gate, policy, time);

    private Func<CancellationToken, Task<byte[]>> Compute(Action? during = null) => _ =>
    {
        computations++;
        during?.Invoke();
        return Task.FromResult(Value);
    };

    private Task<SingleFlightResult> Run(RedisSingleFlight flight, Func<CancellationToken, Task<byte[]>>? compute = null) =>
        flight.RunAsync("k", compute ?? Compute(), ClaimTtl, ResultTtl);

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_computes_nothing_and_touches_no_key()
    {
        gate.Serving = false;

        Assert.IsType<SingleFlightResult.NotServing>(await Run(Flight()));
        Assert.Empty(rows.Calls);
        Assert.Equal(0, computations);
    }

    [Fact]
    public async Task The_winner_computes_and_the_result_write_that_ran_makes_it_Computed()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Complete.Returns(true);

        var result = await Run(Flight());

        Assert.Equal(new SingleFlightResult.Computed(Value), result);
        Assert.Equal(["grant", "read", "complete"], rows.Calls);
        Assert.Equal(1, computations);
        var grant = Assert.Single(rows.Grants);
        Assert.Equal((RedisSpace.Flight, "k", "r1", ClaimTtl), (grant.Space, grant.Key, grant.Holder, grant.Ttl));
        var completion = Assert.Single(rows.Completions);
        Assert.Equal((RedisSpace.Flight, "k", grant.Token, ResultTtl), (completion.Space, completion.Key, completion.Token, completion.OutcomeTtl));
        Assert.Equal(Value, completion.Outcome);
    }

    [Fact]
    public async Task A_late_computation_whose_result_write_did_not_run_is_refused_and_keeps_its_value()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Complete.Returns(false);

        Assert.Equal(new SingleFlightResult.ComputedRefused(Value), await Run(Flight()));
    }

    [Fact]
    public async Task A_replica_that_stopped_serving_during_the_computation_writes_no_result()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);

        var result = await Run(Flight(), Compute(() => gate.Serving = false));

        Assert.Equal(new SingleFlightResult.ComputedRefused(Value), result);
        Assert.Equal(["grant", "read"], rows.Calls);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_result_write_Redis_did_not_answer_is_ComputedUnknown_and_uses_no_policy(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Complete.Throws(outage);

        Assert.Equal(new SingleFlightResult.ComputedUnknown(Value), await Run(Flight(OnRedisUnavailable.Grant)));
        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task A_result_write_rejected_for_its_own_sake_is_thrown()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Complete.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Run(Flight()));
    }

    [Fact]
    public async Task A_computation_that_throws_releases_its_claim_and_propagates()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Release.Returns(true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(Flight(), _ => throw new InvalidOperationException("boom")));

        Assert.Equal(["grant", "read", "release"], rows.Calls);
        Assert.Equal(rows.Grants[0].Token, rows.Releases[0].Token);
        Assert.Equal(RedisSpace.Flight, rows.Releases[0].Space);
    }

    [Fact]
    public async Task A_release_that_fails_leaves_the_claim_to_its_ttl_and_the_computation_s_exception_propagates()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Release.Throws(RedisFailures.Connection());

        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(Flight(), _ => throw new InvalidOperationException("boom")));
    }

    [Fact]
    public async Task A_computation_that_returns_null_is_an_error_and_releases_its_claim()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Release.Returns(true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(Flight(), _ => Task.FromResult<byte[]>(null!)));
        Assert.Equal(["grant", "read", "release"], rows.Calls);
    }

    [Fact]
    public async Task I16_a_live_result_is_returned_as_Waited_whatever_the_claim_and_nothing_is_computed()
    {
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(false, null, Value));

        Assert.Equal(new SingleFlightResult.Waited(Value), await Run(Flight()));
        Assert.Equal(0, computations);
    }

    [Fact]
    public async Task A_caller_that_finds_another_s_claim_polls_for_the_result_and_holds_nothing()
    {
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(true, "r2", null)).Returns(new RedisRecord(true, "r2", null)).Returns(new RedisRecord(false, null, Value));

        var (result, _) = await time.RunDelaysAsync(() => Run(Flight()), TimeSpan.FromMinutes(1));

        Assert.Equal(new SingleFlightResult.Waited(Value), result);
        Assert.Equal(["grant", "read", "read", "read"], rows.Calls);
        Assert.Equal(0, computations);
    }

    [Fact]
    public async Task A_claim_that_ended_without_a_result_is_granted_again_and_computed()
    {
        rows.Grant.Returns(false).Returns(true);
        rows.Read.Returns(new RedisRecord(false, null, null)).Returns(Held);
        rows.Complete.Returns(true);

        Assert.IsType<SingleFlightResult.Computed>(await Run(Flight()));
        Assert.Equal(["grant", "read", "grant", "read", "complete"], rows.Calls);
    }

    [Fact]
    public async Task A_grant_that_finds_a_live_result_answers_Waited_hands_the_grant_back_and_computes_nothing()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(new RedisRecord(true, "r1", Value));
        rows.Release.Returns(true);

        Assert.Equal(new SingleFlightResult.Waited(Value), await Run(Flight()));

        Assert.Equal(["grant", "read", "release"], rows.Calls);
        Assert.Equal((RedisSpace.Flight, "k", rows.Grants[0].Token), rows.Releases[0]);
        Assert.Equal(0, computations);
    }

    [Fact]
    public async Task A_grant_handed_back_that_Redis_does_not_answer_is_left_to_its_ttl_and_the_result_is_still_Waited()
    {
        rows.Grant.Returns(true);
        rows.Read.Returns(new RedisRecord(true, "r1", Value));
        rows.Release.Throws(RedisFailures.Timeout());

        Assert.Equal(new SingleFlightResult.Waited(Value), await Run(Flight()));
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Refuse, typeof(SingleFlightResult.Unknown), 0, "refuse")]
    [InlineData(OnRedisUnavailable.Grant, typeof(SingleFlightResult.ComputedUnknown), 1, "grant")]
    public async Task A_won_grant_whose_result_read_is_unanswered_follows_the_policy_and_counts_the_use(
        OnRedisUnavailable policy, Type answer, int expected, string tag)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Returns(true);
        rows.Read.Throws(RedisFailures.Timeout());

        Assert.IsType(answer, await Run(Flight(policy)));

        Assert.Equal(expected, computations);
        Assert.Equal(["grant", "read"], rows.Calls);
        RedisClaimsTests.AssertUsed(recorder, "single_flight", tag, 1);
    }

    private void ScriptGoneAtEveryRead()
    {
        for (var i = 0; i < 3; i++)
        {
            rows.Grant.Returns(false);
            rows.Read.Returns(new RedisRecord(false, null, null));
        }
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Refuse, typeof(SingleFlightResult.Unknown), 0, "refuse")]
    [InlineData(OnRedisUnavailable.Grant, typeof(SingleFlightResult.ComputedUnknown), 1, "grant")]
    public async Task A_claim_that_is_gone_at_every_read_ends_after_three_grants_by_the_policy_and_counts_the_use(
        OnRedisUnavailable policy, Type answer, int expected, string tag)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        ScriptGoneAtEveryRead();

        Assert.IsType(answer, await Run(Flight(policy)));

        Assert.Equal(expected, computations);
        Assert.Equal(["grant", "read", "grant", "read", "grant", "read"], rows.Calls);
        RedisClaimsTests.AssertUsed(recorder, "single_flight", tag, 1);
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Refuse)]
    [InlineData(OnRedisUnavailable.Grant)]
    public async Task Waiting_behind_holders_that_end_without_a_result_never_counts_toward_the_bound_and_uses_no_policy(OnRedisUnavailable policy)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);

        for (var round = 0; round < 4; round++)
        {
            rows.Grant.Returns(false);
            rows.Read.Returns(new RedisRecord(true, "r2", null)).Returns(new RedisRecord(false, null, null));
        }

        rows.Grant.Returns(true);
        rows.Read.Returns(Held);
        rows.Complete.Returns(true);

        var (result, _) = await time.RunDelaysAsync(() => Run(Flight(policy)), TimeSpan.FromMinutes(1));

        Assert.Equal(new SingleFlightResult.Computed(Value), result);
        Assert.Equal(1, computations);
        Assert.Equal(15, rows.Calls.Count);
        Assert.Empty(recorder.Measurements());
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Refuse_on_an_outage_before_a_claim_is_held_computes_nothing_and_counts_the_use(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(outage);

        Assert.IsType<SingleFlightResult.Unknown>(await Run(Flight(OnRedisUnavailable.Refuse)));
        Assert.Equal(0, computations);
        RedisClaimsTests.AssertUsed(recorder, "single_flight", "refuse", 1);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Grant_on_an_outage_before_a_claim_is_held_computes_once_unclaimed_and_never_answers_Computed(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Grant.Throws(outage);

        var result = await Run(Flight(OnRedisUnavailable.Grant));

        Assert.Equal(new SingleFlightResult.ComputedUnknown(Value), result);
        Assert.Equal(1, computations);
        Assert.Equal(["grant"], rows.Calls);
        RedisClaimsTests.AssertUsed(recorder, "single_flight", "grant", 1);
    }

    [Theory]
    [InlineData(OnRedisUnavailable.Refuse, typeof(SingleFlightResult.Unknown), 0)]
    [InlineData(OnRedisUnavailable.Grant, typeof(SingleFlightResult.ComputedUnknown), 1)]
    public async Task An_outage_while_polling_follows_the_policy(OnRedisUnavailable policy, Type answer, int expected)
    {
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(true, "r2", null)).Throws(RedisFailures.Timeout());

        var (result, _) = await time.RunDelaysAsync(() => Run(Flight(policy)), TimeSpan.FromMinutes(1));

        Assert.IsType(answer, result);
        Assert.Equal(expected, computations);
    }

    [Fact]
    public async Task A_cancel_while_polling_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.Grant.Returns(false);
        rows.Read.Returns(new RedisRecord(true, "r2", null));

        var waiting = Flight().RunAsync("k", Compute(), ClaimTtl, ResultTtl, cancel.Token);
        await Task.Delay(20);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task The_arguments_are_checked_before_anything_is_sent()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Flight().RunAsync(null!, Compute(), ClaimTtl, ResultTtl));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Flight().RunAsync("k", null!, ClaimTtl, ResultTtl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Flight().RunAsync("k", Compute(), TimeSpan.Zero, ResultTtl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Flight().RunAsync("k", Compute(), ClaimTtl, TimeSpan.Zero));
        Assert.Empty(rows.Calls);
        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, Assert.Throws<ClusterConfigurationException>(() => Flight(default)).Reason);
    }
}
