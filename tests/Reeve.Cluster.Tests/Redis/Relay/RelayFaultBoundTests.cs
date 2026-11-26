using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;
using Unit.Ask;

namespace Unit.Redis.Relay;

public sealed class RelayFaultBoundTests
{
    private static byte[] Bytes(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task With_every_command_queued_in_the_clients_backlog_the_request_is_NotTaken_and_no_withdrawal_is_sent()
    {
        var clock = new FakeTimeProvider();
        var timeout = TimeSpan.FromSeconds(1);
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock);
        await rig.StartAsync();
        rig.Wire.Hang("Write", "Publish", "Read", "Replace");
        var started = clock.GetTimestamp();

        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, timeout: timeout);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.True(clock.GetElapsedTime(started) <= timeout, $"took {clock.GetElapsedTime(started)}");
        Assert.DoesNotContain("Replace", rig.Wire.Commands);
    }

    [Fact]
    public async Task With_only_the_publish_queued_in_the_clients_backlog_the_withdrawal_wins_and_the_request_is_NotTaken()
    {
        var clock = new FakeTimeProvider();
        var timeout = TimeSpan.FromSeconds(1);
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();
        rig.Wire.Hang("Publish");
        var started = clock.GetTimestamp();

        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, timeout: timeout);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.True(clock.GetElapsedTime(started) <= timeout, $"took {clock.GetElapsedTime(started)}");
        Assert.Contains("Replace", rig.Wire.Commands);
        Assert.Equal(0, inbound.Reserved);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_record_write_that_timed_out_after_it_was_sent_publishes_nothing_and_ends_NotTaken(bool landed)
    {
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();
        rig.Wire.Fail("Write", new RedisTimeoutException(CommandFlags.None, "timeout", CommandStatus.Sent), runFirst: landed);

        var outcome = await rig.AskTargetAsync();
        await Task.Delay(100);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Reserved);
        Assert.DoesNotContain("Publish", rig.Wire.Commands);
        Assert.DoesNotContain("Replace", rig.Wire.Commands);
    }

    private static async Task<T> EndAsync<T>(Task<T> call, FakeTimeProvider time, FakeWire wire, TimeSpan window, TimeSpan timeout)
    {
        await RelayRig.SettledAsync(() => wire.HasLiveHang("Read"));
        time.Advance(window);
        await RelayRig.SettledAsync(() => wire.HasLiveHang("Read"));
        time.Advance(timeout - window);
        return await call.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Theory]
    [InlineData(400)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(10000)]
    public async Task A_call_whose_reads_and_withdrawal_hang_ends_within_its_timeout_with_the_production_budgets(int milliseconds)
    {
        var timeout = TimeSpan.FromMilliseconds(milliseconds);
        var time = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: time, timings: AskRelayTimings.Default);
        await rig.StartAsync();
        rig.Wire.Hang("Read", "Replace");
        var started = time.GetTimestamp();

        var outcome = await EndAsync(rig.AskTargetAsync(timeout: timeout), time, rig.Wire, AskRelayDecisions.AnswerWindow(timeout, AskRelayTimings.Default.WithdrawBudget), timeout);
        var elapsed = time.GetElapsedTime(started);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.True(elapsed <= timeout, $"took {elapsed} of {timeout}");
    }

    [Fact]
    public async Task A_keyed_call_re_delivered_with_little_time_left_ends_within_its_timeout()
    {
        var time = new FakeTimeProvider();
        var wire = new FakeWire();
        var target = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var self = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var options = Options.Create(new ClusterOptions { ReplicaId = "a" });
        await using var targetRelay = new AskRelay(wire, ReservingInbound.Declining(DeclineReason.Absent), target, Options.Create(new ClusterOptions { ReplicaId = "b" }), AskRelayTimings.Default, time, NullLogger<AskRelay>.Instance);
        await using var callerRelay = new AskRelay(wire, ReservingInbound.Echoing(), self, options, AskRelayTimings.Default, time, NullLogger<AskRelay>.Instance);
        targetRelay.Start();
        callerRelay.Start();
        await targetRelay.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
        await callerRelay.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
        var directory = new ScriptedDirectory { Otherwise = new AskOwner("b", target.ProcessId, 7) };
        var timeout = TimeSpan.FromSeconds(3);
        directory.OnResolve = () =>
        {
            if (directory.OwnerResolutions >= 2)
            {
                wire.Hang("Read", "Replace");
                time.Advance(ClusterAsk.OwnerPollInterval);
            }
        };
        var ask = new ClusterAsk(directory, new ScriptedInbound(), callerRelay, self, [], options, time, NullLogger<ClusterAsk>.Instance);
        var started = time.GetTimestamp();

        var left = timeout - ClusterAsk.OwnerPollInterval;
        var result = await EndAsync(ask.AskAsync("scheduler", "k1", Bytes("x"), timeout), time, wire, AskRelayDecisions.AnswerWindow(left, AskRelayTimings.Default.WithdrawBudget), left);
        var elapsed = time.GetElapsedTime(started);

        Assert.True(directory.OwnerResolutions >= 2);
        Assert.True(elapsed <= timeout, $"took {elapsed} of {timeout}");
        Assert.NotEqual(AskOutcome.Answered, result.Outcome);
    }

    [Fact]
    public async Task After_failed_subscription_attempts_one_request_is_received_once()
    {
        var inbound = ReservingInbound.Echoing();
        var wire = new FakeWire();
        wire.FailSubscribe("ask:q:", 3);
        await using var rig = new RelayRig(inbound, wire);

        await rig.StartAsync();
        var outcome = await rig.AskTargetAsync();
        await Task.Delay(100);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(1, inbound.Reserved);
        Assert.Equal(1, inbound.Started);
        Assert.Equal(4, wire.Handlers);
    }

    [Fact]
    public async Task After_a_partial_success_the_channel_that_held_is_not_subscribed_again()
    {
        var inbound = ReservingInbound.Echoing();
        var wire = new FakeWire();
        wire.FailSubscribe("ask:r:", 2);
        await using var rig = new RelayRig(inbound, wire);

        await rig.StartAsync();
        var outcome = await rig.AskTargetAsync();
        await Task.Delay(100);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(1, inbound.Reserved);
        Assert.Equal(4, wire.Handlers);
    }
}
