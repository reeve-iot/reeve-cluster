using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using Reeve.Cluster.Slots;
using StackExchange.Redis;
using Unit.Diagnostics;

namespace Unit.Redis;

[Collection(MetricsCollection.Name)]
public sealed class RedisSlotsTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly Guid Existing = Guid.Parse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedSlotRows rows = new();
    private readonly ScriptedSlotGate gate = new();
    private readonly RecordingSlotObserver observer = new();
    private readonly SlotRegistry registry;

    public RedisSlotsTests() => registry = new SlotRegistry(time);

    public static IEnumerable<object[]> Outages() => RedisFailures.Outages();

    private RedisSlots Slots(OnRedisUnavailable policy = OnRedisUnavailable.Refuse) =>
        new(rows, gate, registry, policy, Options.Create(new ClusterOptions()), time, NullLogger<RedisSlots>.Instance, observer);

    private static void AssertUsed(InstrumentRecorder recorder, string policy, int count)
    {
        var measurements = recorder.Measurements();
        Assert.Equal(count, measurements.Count);
        Assert.All(measurements, m => Assert.Equal(("slot", policy, 1d), ((string)m.Tags["primitive"]!, (string)m.Tags["policy"]!, m.Value)));
    }

    private void ScriptAdmittedGrant(long now = 1_000)
    {
        rows.Read.Returns(new RedisSlotRead(now, null));
        rows.Grant.Returns(1);
        rows.Admit.Returns(true);
    }

    [Theory]
    [InlineData("", "i")]
    [InlineData("k", "")]
    [InlineData("a{b", "i")]
    [InlineData("a}b", "i")]
    [InlineData("k", "i{")]
    [InlineData("k", "}i")]
    public async Task A_key_or_an_id_that_is_empty_or_has_a_brace_is_refused_before_any_check_or_command(string key, string id)
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => Slots().AcquireAsync(key, id, 1, Ttl));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => Slots().RenewAsync(key, id, Existing, Ttl));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => Slots().ReleaseAsync(key, id, Existing));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => Slots().ExecuteAsync(key, id, Existing, _ => { }));

        Assert.Empty(rows.Calls);
        Assert.Equal(0, gate.ServingChecks);
        Assert.Empty(observer.Acquires);
        Assert.Empty(observer.Renewals);
    }

    [Fact]
    public async Task A_limit_below_one_and_a_ttl_not_longer_than_the_safety_margin_are_refused_before_any_I_O()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Slots().AcquireAsync("k", "i", 0, Ttl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Slots().AcquireAsync("k", "i", 1, new ClusterOptions().SafetyMargin));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Slots().RenewAsync("k", "i", Existing, new ClusterOptions().SafetyMargin));

        Assert.Empty(rows.Calls);
        Assert.Equal(0, gate.ServingChecks);
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_acquires_nothing_and_touches_no_key()
    {
        gate.Serving = false;

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.NotServing>(result);
        Assert.Empty(rows.Calls);
        Assert.Equal([result], observer.Acquires);
    }

    [Fact]
    public async Task A_grant_is_decided_by_the_replies_of_its_one_transaction_and_writes_the_id_key_after_the_verdict()
    {
        ScriptAdmittedGrant(now: 5_000);

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        var token = Assert.IsType<SlotAcquireResult.Acquired>(result).Token;
        Assert.NotEqual(Guid.Empty, token);
        Assert.Equal(["read", "grant", "admit"], rows.Calls);
        Assert.Equal(("k", 5_000L, Ttl, token), Assert.Single(rows.Grants));
        Assert.Equal(("k", "i", token, Ttl), Assert.Single(rows.Admissions));
        Assert.Equal([result], observer.Acquires);
        Assert.NotNull(registry.TryGet("k", "i", token));
        Assert.Equal(4, registry.TryGet("k", "i", token)!.Epoch);
    }

    [Theory]
    [InlineData(1, 1, true)]
    [InlineData(2, 1, false)]
    [InlineData(3, 3, true)]
    [InlineData(4, 3, false)]
    [InlineData(1, 5, true)]
    public async Task The_count_of_the_transaction_includes_the_new_member_and_is_admitted_up_to_the_limit(long count, int limit, bool admitted)
    {
        rows.Read.Returns(new RedisSlotRead(1_000, null));
        rows.Grant.Returns(count);
        rows.Admit.Returns(true);
        rows.Refuse.Returns(true);

        var result = await Slots().AcquireAsync("k", "i", limit, Ttl);

        Assert.Equal(admitted, result.IsAcquired);
        Assert.Equal(admitted ? ["read", "grant", "admit"] : ["read", "grant", "refuse"], rows.Calls);
    }

    [Fact]
    public async Task A_refused_grant_removes_its_member_and_never_writes_the_id_key()
    {
        rows.Read.Returns(new RedisSlotRead(1_000, null));
        rows.Grant.Returns(2);
        rows.Refuse.Returns(true);

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.Full>(result);
        Assert.Equal(("k", rows.Grants[0].Token), Assert.Single(rows.Refusals));
        Assert.Empty(rows.Admissions);
        Assert.Equal(0, registry.Live());
        Assert.Equal([result], observer.Acquires);
    }

    [Fact]
    public async Task C5_a_refused_grant_whose_ZREM_fails_is_still_Full_and_the_same_id_is_Full_again_never_the_refused_member()
    {
        rows.Read.Returns(new RedisSlotRead(1_000, null)).Returns(new RedisSlotRead(1_500, null));
        rows.Grant.Returns(2).Returns(3);
        rows.Refuse.Throws(RedisFailures.Connection()).Returns(true);

        var first = await Slots().AcquireAsync("k", "i", 1, Ttl);
        var second = await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.Full>(first);
        Assert.IsType<SlotAcquireResult.Full>(second);
        Assert.Empty(rows.Admissions);
        Assert.Empty(rows.Renewals);
        Assert.Equal(0, registry.Live());
    }

    [Fact]
    public async Task An_id_key_that_cannot_be_written_after_an_admitted_grant_is_logged_and_the_acquire_still_answers_Acquired()
    {
        rows.Read.Returns(new RedisSlotRead(1_000, null));
        rows.Grant.Returns(1);
        rows.Admit.Throws(RedisFailures.Timeout());

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        var token = Assert.IsType<SlotAcquireResult.Acquired>(result).Token;
        Assert.Equal(token, rows.Grants[0].Token);
        Assert.NotNull(registry.TryGet("k", "i", token));
    }

    [Fact]
    public async Task A_re_acquire_renews_the_slot_the_id_key_names_with_the_time_it_read_and_grants_nothing()
    {
        rows.Read.Returns(new RedisSlotRead(7_000, Existing));
        rows.Renew.Returns(true);

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.Equal(new SlotAcquireResult.Acquired(Existing), result);
        Assert.Equal(["read", "renew"], rows.Calls);
        Assert.Equal(("k", "i", Existing, 7_000L, Ttl), Assert.Single(rows.Renewals));
        Assert.NotNull(registry.TryGet("k", "i", Existing));
    }

    [Fact]
    public async Task A_re_acquire_whose_slot_has_ended_grants_a_new_one_judged_by_the_limit()
    {
        rows.Read.Returns(new RedisSlotRead(7_000, Existing));
        rows.Renew.Returns(false);
        rows.Grant.Returns(2);
        rows.Refuse.Returns(true);

        var result = await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.Full>(result);
        Assert.Equal(["read", "renew", "grant", "refuse"], rows.Calls);
        Assert.Equal(7_000L, rows.Grants[0].Now);
        Assert.NotEqual(Existing, rows.Grants[0].Token);
        Assert.Null(registry.TryGet("k", "i", Existing));
    }

    [Fact]
    public async Task A_re_acquire_of_a_slot_whose_renewal_grants_compares_no_limit()
    {
        rows.Read.Returns(new RedisSlotRead(7_000, Existing));
        rows.Renew.Returns(true);

        Assert.True((await Slots().AcquireAsync("k", "i", 1, Ttl)).IsAcquired);
        Assert.Empty(rows.Grants);
    }

    public static IEnumerable<object[]> AcquireStages() => [["read"], ["renew"], ["grant"]];

    private void ScriptAcquireUpTo(string stage, Exception outage)
    {
        rows.Read.Does(() => stage == "read" ? throw outage : new RedisSlotRead(1_000, stage == "renew" ? Existing : null));
        if (stage == "renew")
        {
            rows.Renew.Throws(outage);
        }

        if (stage == "grant")
        {
            rows.Grant.Throws(outage);
        }
    }

    [Theory]
    [MemberData(nameof(AcquireStages))]
    public async Task An_outage_at_any_command_of_the_decision_is_Unknown_under_Refuse_writes_nothing_and_counts_the_use(string stage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        ScriptAcquireUpTo(stage, RedisFailures.Timeout());

        var result = await Slots(OnRedisUnavailable.Refuse).AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.Unknown>(result);
        Assert.Empty(rows.Admissions);
        Assert.Empty(rows.Refusals);
        Assert.Equal(0, registry.Live());
        AssertUsed(recorder, "refuse", 1);
        Assert.Equal([result], observer.Acquires);
    }

    [Theory]
    [MemberData(nameof(AcquireStages))]
    public async Task An_outage_at_any_command_of_the_decision_is_Acquired_under_Grant_with_a_token_no_member_holds_and_counts_the_use(string stage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        ScriptAcquireUpTo(stage, RedisFailures.Connection());

        var result = await Slots(OnRedisUnavailable.Grant).AcquireAsync("k", "i", 1, Ttl);

        Assert.True(result.TryGetToken(out var token));
        Assert.DoesNotContain(token, rows.Grants.Select(g => g.Token));
        Assert.NotEqual(Existing, token);
        Assert.Empty(rows.Admissions);
        AssertUsed(recorder, "grant", 1);
        Assert.Equal([result], observer.Acquires);
    }

    [Fact]
    public async Task A_token_the_Grant_policy_answered_is_in_no_registry_so_its_renewal_is_Ended_and_its_effect_is_not_sent()
    {
        ScriptAcquireUpTo("read", RedisFailures.Connection());
        var token = Assert.IsType<SlotAcquireResult.Acquired>(await Slots(OnRedisUnavailable.Grant).AcquireAsync("k", "i", 1, Ttl)).Token;
        var calls = rows.Calls.Count;

        Assert.Equal(SlotRenewResult.Ended, await Slots(OnRedisUnavailable.Grant).RenewAsync("k", "i", token, Ttl));
        Assert.Equal(RedisEffectResult.NotServing, await Slots(OnRedisUnavailable.Grant).ExecuteAsync("k", "i", token, _ => { }));

        Assert.Equal(calls, rows.Calls.Count);
        Assert.Equal(0, registry.Live());
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task Every_outage_of_the_list_falls_under_the_policy(Exception outage)
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Read.Throws(outage);

        Assert.IsType<SlotAcquireResult.Unknown>(await Slots(OnRedisUnavailable.Refuse).AcquireAsync("k", "i", 1, Ttl));
        AssertUsed(recorder, "refuse", 1);
    }

    [Fact]
    public async Task A_command_Redis_rejects_for_its_own_sake_is_thrown_uses_no_policy_and_is_not_observed()
    {
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Read.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Slots(OnRedisUnavailable.Grant).AcquireAsync("k", "i", 1, Ttl));

        Assert.Empty(recorder.Measurements());
        Assert.Empty(observer.Acquires);
    }

    private SlotAcquireResult.Acquired Held()
    {
        ScriptAdmittedGrant();
        var acquired = Slots().AcquireAsync("k", "i", 1, Ttl).GetAwaiter().GetResult();
        rows.Calls.Clear();
        observer.Acquires.Clear();
        return Assert.IsType<SlotAcquireResult.Acquired>(acquired);
    }

    [Fact]
    public async Task A_renewal_reads_the_time_then_renews_with_it_and_moves_the_local_deadline()
    {
        var held = Held();
        time.Advance(TimeSpan.FromSeconds(20));
        rows.Time.Returns(9_000);
        rows.Renew.Returns(true);

        var result = await Slots().RenewAsync("k", "i", held.Token, Ttl);

        Assert.Equal(SlotRenewResult.Renewed, result);
        Assert.Equal(["time", "renew"], rows.Calls);
        Assert.Equal(("k", "i", held.Token, 9_000L, Ttl), Assert.Single(rows.Renewals));
        Assert.Equal([SlotRenewResult.Renewed], observer.Renewals);
        time.Advance(TimeSpan.FromSeconds(25));
        Assert.NotNull(registry.TryGet("k", "i", held.Token));
        time.Advance(TimeSpan.FromSeconds(5));
        Assert.Null(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task A_renewal_that_finds_no_score_is_Ended_and_the_entry_leaves()
    {
        var held = Held();
        rows.Time.Returns(9_000);
        rows.Renew.Returns(false);

        Assert.Equal(SlotRenewResult.Ended, await Slots().RenewAsync("k", "i", held.Token, Ttl));
        Assert.Null(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task A_renewal_of_a_token_this_process_does_not_hold_is_Ended_without_a_command()
    {
        var held = Held();

        Assert.Equal(SlotRenewResult.Ended, await Slots().RenewAsync("k", "i", Existing, Ttl));
        Assert.Equal(SlotRenewResult.Ended, await Slots().RenewAsync("k", "other", held.Token, Ttl));

        Assert.Empty(rows.Calls);
        Assert.Equal([SlotRenewResult.Ended, SlotRenewResult.Ended], observer.Renewals);
        Assert.NotNull(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task A_renewal_the_slot_holder_check_refuses_is_NotServing_without_a_command_and_the_entry_leaves()
    {
        var held = Held();
        gate.Holder = false;

        Assert.Equal(SlotRenewResult.NotServing, await Slots().RenewAsync("k", "i", held.Token, Ttl));

        Assert.Empty(rows.Calls);
        Assert.Equal([4L], gate.HolderChecks);
        Assert.Null(registry.TryGet("k", "i", held.Token));
        Assert.Equal([SlotRenewResult.NotServing], observer.Renewals);
    }

    [Fact]
    public async Task A_draining_replica_whose_serving_check_refuses_still_renews_what_the_holder_check_passes()
    {
        var held = Held();
        gate.Serving = false;
        rows.Time.Returns(9_000);
        rows.Renew.Returns(true);

        Assert.Equal(SlotRenewResult.Renewed, await Slots().RenewAsync("k", "i", held.Token, Ttl));
        Assert.Equal(1, gate.ServingChecks);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_renewal_Redis_does_not_answer_is_Unknown_under_either_policy_uses_none_and_keeps_the_entry(Exception outage)
    {
        var held = Held();
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Time.Returns(9_000).Returns(9_100);
        rows.Renew.Throws(outage).Throws(outage);

        Assert.Equal(SlotRenewResult.Unknown, await Slots(OnRedisUnavailable.Refuse).RenewAsync("k", "i", held.Token, Ttl));
        Assert.Equal(SlotRenewResult.Unknown, await Slots(OnRedisUnavailable.Grant).RenewAsync("k", "i", held.Token, Ttl));

        Assert.Empty(recorder.Measurements());
        Assert.NotNull(registry.TryGet("k", "i", held.Token));
        Assert.Equal([SlotRenewResult.Unknown, SlotRenewResult.Unknown], observer.Renewals);
    }

    [Fact]
    public async Task A_time_Redis_does_not_answer_makes_the_renewal_Unknown_too()
    {
        var held = Held();
        rows.Time.Throws(RedisFailures.Connection());

        Assert.Equal(SlotRenewResult.Unknown, await Slots().RenewAsync("k", "i", held.Token, Ttl));
        Assert.Equal(["time"], rows.Calls);
    }

    [Fact]
    public async Task A_release_by_token_removes_the_member_and_the_entry_whatever_it_answers()
    {
        var held = Held();
        rows.Release.Returns(false);

        Assert.False(await Slots().ReleaseAsync("k", "i", held.Token));

        Assert.Equal(("k", "i", (Guid?)held.Token), Assert.Single(rows.Releases));
        Assert.Null(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task A_release_by_id_passes_no_token_and_ends_whatever_entry_the_id_has()
    {
        var held = Held();
        rows.Release.Returns(true);

        Assert.True(await Slots().ReleaseAsync("k", "i", null));

        Assert.Equal(("k", "i", (Guid?)null), Assert.Single(rows.Releases));
        Assert.Null(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task A_late_release_of_an_earlier_token_leaves_the_current_entry()
    {
        var held = Held();
        rows.Release.Returns(false);

        await Slots().ReleaseAsync("k", "i", Existing);

        Assert.NotNull(registry.TryGet("k", "i", held.Token));
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task A_release_Redis_does_not_answer_is_an_unknown_outcome_under_either_policy_and_the_entry_leaves(Exception outage)
    {
        var held = Held();
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Release.Throws(outage).Throws(outage);

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => Slots(OnRedisUnavailable.Grant).ReleaseAsync("k", "i", held.Token));
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => Slots(OnRedisUnavailable.Refuse).ReleaseAsync("k", "i", null));

        Assert.Empty(recorder.Measurements());
        Assert.Null(registry.TryGet("k", "i", held.Token));
    }

    [Fact]
    public async Task An_effect_passes_the_callers_commands_to_the_store_and_answers_by_its_verdict()
    {
        var held = Held();
        Action<ITransaction> effect = _ => { };
        rows.Execute.Returns(true).Returns(false);

        Assert.Equal(RedisEffectResult.Committed, await Slots().ExecuteAsync("k", "i", held.Token, effect));
        Assert.Equal(RedisEffectResult.Refused, await Slots().ExecuteAsync("k", "i", held.Token, effect));

        Assert.All(rows.Effects, e => Assert.Same(effect, e));
        Assert.Equal([4L, 4L], gate.HolderChecks);
    }

    [Fact]
    public async Task An_effect_under_a_token_this_process_does_not_hold_or_the_holder_check_refuses_is_NotServing_and_sends_nothing()
    {
        var held = Held();

        Assert.Equal(RedisEffectResult.NotServing, await Slots().ExecuteAsync("k", "i", Existing, _ => { }));
        gate.Holder = false;
        Assert.Equal(RedisEffectResult.NotServing, await Slots().ExecuteAsync("k", "i", held.Token, _ => { }));

        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task An_effect_after_the_local_deadline_is_NotServing()
    {
        var held = Held();
        time.Advance(Ttl);

        Assert.Equal(RedisEffectResult.NotServing, await Slots().ExecuteAsync("k", "i", held.Token, _ => { }));
        Assert.Empty(rows.Calls);
    }

    [Theory]
    [MemberData(nameof(Outages))]
    public async Task An_effect_Redis_does_not_answer_is_Unknown_under_either_policy_and_uses_none(Exception outage)
    {
        var held = Held();
        using var recorder = new InstrumentRecorder(RedisDiagnostics.PolicyUses);
        rows.Execute.Throws(outage).Throws(outage);

        Assert.Equal(RedisEffectResult.Unknown, await Slots(OnRedisUnavailable.Refuse).ExecuteAsync("k", "i", held.Token, _ => { }));
        Assert.Equal(RedisEffectResult.Unknown, await Slots(OnRedisUnavailable.Grant).ExecuteAsync("k", "i", held.Token, _ => { }));

        Assert.Empty(recorder.Measurements());
    }

    [Fact]
    public async Task An_effect_Redis_rejects_for_its_own_sake_is_thrown()
    {
        var held = Held();
        rows.Execute.Throws(RedisFailures.WrongType());

        await Assert.ThrowsAsync<RedisServerException>(() => Slots().ExecuteAsync("k", "i", held.Token, _ => { }));
    }

    [Fact]
    public void A_policy_that_is_not_one_is_refused_at_construction()
    {
        var refusal = Assert.Throws<ClusterConfigurationException>(() => Slots(default));

        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, refusal.Reason);
    }

    [Fact]
    public async Task Every_answer_reaches_the_observer_once_and_a_call_that_throws_reaches_it_never()
    {
        gate.Serving = false;
        await Slots().AcquireAsync("k", "i", 1, Ttl);
        gate.Serving = true;
        rows.Read.Returns(new RedisSlotRead(1_000, null));
        rows.Grant.Returns(2);
        rows.Refuse.Returns(true);
        await Slots().AcquireAsync("k", "i", 1, Ttl);
        ScriptAdmittedGrant();
        await Slots().AcquireAsync("k", "i", 1, Ttl);
        rows.Read.Throws(RedisFailures.Timeout());
        await Slots().AcquireAsync("k", "j", 1, Ttl);
        rows.Read.Throws(RedisFailures.WrongType());
        await Assert.ThrowsAsync<RedisServerException>(() => Slots().AcquireAsync("k", "j", 1, Ttl));

        Assert.Equal(
            [typeof(SlotAcquireResult.NotServing), typeof(SlotAcquireResult.Full), typeof(SlotAcquireResult.Acquired), typeof(SlotAcquireResult.Unknown)],
            observer.Acquires.Select(a => a.GetType()));
    }
}
