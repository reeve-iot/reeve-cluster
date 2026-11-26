using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;

namespace Int.Ask;

public sealed class AskRelayTests : IClassFixture<RedisFixture>
{
    private static readonly AskRelayTimings Timings = new(
        TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(50));

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(4);

    private readonly RedisFixture redis;

    public AskRelayTests(RedisFixture redis)
    {
        this.redis = redis;
    }

    private sealed class Echo : IAskInbound
    {
        private int started;
        private int reserved;
        private readonly TaskCompletionSource handling = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Echo(bool hold = false, DeclineReason? declines = null, AskAnswerKind answers = AskAnswerKind.Response)
        {
            Hold = hold;
            Declines = declines;
            Answers = answers;
        }

        public bool Hold { get; }

        public DeclineReason? Declines { get; }

        public AskAnswerKind Answers { get; }

        public int Started => Volatile.Read(ref started);

        public int Reserved => Volatile.Read(ref reserved);

        public Task Handling => handling.Task;

        public void Release() => release.TrySetResult();

        public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy) => throw new NotSupportedException();

        public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy)
        {
            Interlocked.Increment(ref reserved);
            return ValueTask.FromResult(Declines is { } reason
                ? AskReservation.Decline(reason)
                : AskReservation.Reserve(async () =>
                {
                    Interlocked.Increment(ref started);
                    handling.TrySetResult();
                    if (Hold)
                    {
                        await release.Task.ConfigureAwait(false);
                    }

                    return new AskAnswer(Answers, Answers == AskAnswerKind.Response ? delivery.Payload.ToArray() : default, Answers == AskAnswerKind.HandlerError ? "boom" : null);
                }));
        }
    }

    private async Task<(ConnectionMultiplexer Multiplexer, AskRelay Relay, MemberIdentity Identity)> RelayAsync(string replica, IAskInbound inbound, string endpoint, string prefix, bool clientDefaults = false)
    {
        var options = ConfigurationOptions.Parse(endpoint);
        options.AbortOnConnectFail = false;
        if (!clientDefaults)
        {
            options.ConnectTimeout = 2000;
            options.AsyncTimeout = 2000;
            options.SyncTimeout = 2000;
            options.ReconnectRetryPolicy = new LinearRetry(250);
        }

        var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        var identity = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var relay = new AskRelay(
            new RedisAskWire(multiplexer, Options.Create(new ClusterRedisOptions { KeyPrefix = prefix })),
            inbound, identity, Options.Create(new ClusterOptions { ReplicaId = replica }), Timings, TimeProvider.System, NullLogger<AskRelay>.Instance);
        return (multiplexer, relay, identity);
    }

    private static string NewPrefix() => "relay" + Guid.NewGuid().ToString("N") + ":";

    private static AskDelivery Member(string text = "ping") => new(null, null, null, System.Text.Encoding.UTF8.GetBytes(text));

    [Fact]
    public async Task An_answer_a_handler_error_and_an_owner_lost_answer_reach_the_caller()
    {
        var prefix = NewPrefix();
        foreach (var kind in new[] { AskAnswerKind.Response, AskAnswerKind.HandlerError, AskAnswerKind.OwnerLost })
        {
            var (tm, target, tid) = await RelayAsync("b", new Echo(answers: kind), redis.ConnectionString, prefix);
            var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix);
            await using var _1 = tm;
            await using var _2 = cm;
            await using var _3 = target;
            await using var _4 = caller;
            target.Start();
            caller.Start();
            await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
            await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

            var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), Timeout, CancellationToken.None);

            Assert.Equal(DeliveryKind.Answered, outcome.Kind);
            Assert.Equal(kind, outcome.Answer!.Kind);
        }
    }

    [Fact]
    public async Task A_decline_reaches_the_caller_and_a_process_nobody_listens_on_ends_NotTaken_within_the_timeout()
    {
        var prefix = NewPrefix();
        var decliner = new Echo(declines: DeclineReason.NotServing);
        var (tm, target, tid) = await RelayAsync("b", decliner, redis.ConnectionString, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        var declined = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), Timeout, CancellationToken.None);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var exited = await caller.DeliverAsync(new AskTarget("b", Guid.NewGuid()), Member(), TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.Equal(DeliveryKind.Declined, declined.Kind);
        Assert.Equal(DeclineReason.NotServing, declined.Reason);
        Assert.Equal(0, decliner.Started);
        Assert.Equal(DeliveryKind.NotTaken, exited.Kind);
        Assert.True(clock.Elapsed < TimeSpan.FromMilliseconds(1150), $"took {clock.Elapsed}");
    }

    [Fact]
    public async Task With_Redis_cut_before_the_call_the_request_is_NotTaken()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        var (tm, target, tid) = await RelayAsync("b", new Echo(), redis.ConnectionString, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), proxy.Endpoint, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        proxy.Cut();
        await Task.Delay(300);
        var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), Timeout, CancellationToken.None);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
    }

    [Fact]
    public async Task A_caller_connection_cut_during_a_request_nobody_takes_ends_TakenUnanswered()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        var (tm, target, tid) = await RelayAsync("b", new Echo(), redis.ConnectionString, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), proxy.Endpoint, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        caller.Start();
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        var asking = caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), Timeout, CancellationToken.None);
        await Task.Delay(250);
        proxy.Cut();
        await Task.Delay(250);
        proxy.Restore();
        var outcome = await asking;

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task With_the_callers_Redis_cut_after_the_receipt_the_withdrawal_fails_and_the_request_is_TakenUnanswered()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        var holding = new Echo(hold: true);
        var (tm, target, tid) = await RelayAsync("b", holding, redis.ConnectionString, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), proxy.Endpoint, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        var asking = caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(3), CancellationToken.None);
        await holding.Handling.WaitAsync(TimeSpan.FromSeconds(30));
        proxy.Cut();
        var outcome = await asking;
        holding.Release();

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.Equal(1, holding.Started);
    }

    [Fact]
    public async Task A_handler_that_outlasts_the_callers_wait_leaves_the_request_TakenUnanswered_on_a_healthy_Redis()
    {
        var prefix = NewPrefix();
        var holding = new Echo(hold: true);
        var (tm, target, tid) = await RelayAsync("b", holding, redis.ConnectionString, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(1), CancellationToken.None);
        holding.Release();

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task With_Redis_down_when_the_relay_starts_it_subscribes_once_Redis_returns_and_then_takes_requests()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        proxy.Cut();
        var echo = new Echo();

        var (tm, target, tid) = await RelayAsync("b", echo, proxy.Endpoint, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(500);
        Assert.False(target.Subscribed.IsCompleted);
        var whileDown = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(1), CancellationToken.None);

        proxy.Restore();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(60));
        var afterwards = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member("back"), Timeout, CancellationToken.None);

        Assert.Equal(DeliveryKind.NotTaken, whileDown.Kind);
        Assert.Equal(DeliveryKind.Answered, afterwards.Kind);
        Assert.Equal("back", System.Text.Encoding.UTF8.GetString(afterwards.Answer!.Response.Span));
    }

    [Fact]
    public async Task A_subscription_survives_a_cut_that_happens_after_it_held()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        var (tm, target, tid) = await RelayAsync("b", new Echo(), proxy.Endpoint, prefix);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        proxy.Cut();
        await Task.Delay(500);
        proxy.Restore();
        AskResultHolder? last = null;
        for (var attempt = 0; attempt < 30 && last?.Outcome.Kind != DeliveryKind.Answered; attempt++)
        {
            last = new AskResultHolder(await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(1), CancellationToken.None));
        }

        Assert.Equal(DeliveryKind.Answered, last!.Outcome.Kind);
    }

    private async Task<(AskRelay Target, AskRelay Caller, MemberIdentity TargetId, Echo Echo, IAsyncDisposable Cleanup)> PairAsync(string endpointForCaller, Echo echo, bool clientDefaults)
    {
        var prefix = NewPrefix();
        var (tm, target, tid) = await RelayAsync("b", echo, redis.ConnectionString, prefix, clientDefaults);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), endpointForCaller, prefix, clientDefaults);
        target.Start();
        caller.Start();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));
        return (target, caller, tid, echo, new Cleanup(tm, cm, target, caller));
    }

    private sealed class Cleanup(params IAsyncDisposable[] parts) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            foreach (var part in parts)
            {
                await part.DisposeAsync();
            }
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(10)]
    public async Task K3_with_the_clients_defaults_and_Redis_cut_before_the_call_the_request_is_NotTaken_within_its_timeout(int seconds)
    {
        await using var proxy = new CutProxy(redis.ConnectionString);
        var (_, caller, tid, _, cleanup) = await PairAsync(proxy.Endpoint, new Echo(), clientDefaults: true);
        await using var _1 = cleanup;
        proxy.Cut();
        await Task.Delay(500);

        var clock = System.Diagnostics.Stopwatch.StartNew();
        var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(seconds), CancellationToken.None);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(seconds) + TimeSpan.FromMilliseconds(300), $"took {clock.Elapsed}");
    }

    [Theory]
    [InlineData(3)]
    [InlineData(10)]
    public async Task K4_with_the_connection_paused_the_request_is_NotTaken_within_its_timeout(int seconds)
    {
        await using var proxy = new CutProxy(redis.ConnectionString);
        var (_, caller, tid, echo, cleanup) = await PairAsync(proxy.Endpoint, new Echo(), clientDefaults: true);
        await using var _1 = cleanup;
        proxy.Pause();

        var clock = System.Diagnostics.Stopwatch.StartNew();
        var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(seconds), CancellationToken.None);
        proxy.Resume();

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, echo.Reserved);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(seconds) + TimeSpan.FromMilliseconds(300), $"took {clock.Elapsed}");
    }

    [Fact]
    public async Task K1_a_subscription_attempted_while_Redis_is_down_leaves_one_handler_so_one_request_is_received_once()
    {
        var prefix = NewPrefix();
        await using var proxy = new CutProxy(redis.ConnectionString);
        proxy.Cut();
        var echo = new Echo();
        var (tm, target, tid) = await RelayAsync("b", echo, proxy.Endpoint, prefix, clientDefaults: true);
        var (cm, caller, _) = await RelayAsync("a", new Echo(), redis.ConnectionString, prefix, clientDefaults: true);
        await using var _1 = tm;
        await using var _2 = cm;
        await using var _3 = target;
        await using var _4 = caller;
        target.Start();
        caller.Start();
        await caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(30));

        await Task.Delay(TimeSpan.FromSeconds(17));
        proxy.Restore();
        await target.Subscribed.WaitAsync(TimeSpan.FromSeconds(90));
        var outcome = await caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), Timeout, CancellationToken.None);
        await Task.Delay(300);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(1, echo.Reserved);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task K6_an_answer_written_while_the_caller_is_cut_off_is_seen_if_the_caller_is_back_in_time(bool backInTime)
    {
        await using var proxy = new CutProxy(redis.ConnectionString);
        var holding = new Echo(hold: true);
        var (_, caller, tid, _, cleanup) = await PairAsync(proxy.Endpoint, holding, clientDefaults: false);
        await using var _1 = cleanup;

        var asking = caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(6), CancellationToken.None);
        await holding.Handling.WaitAsync(TimeSpan.FromSeconds(30));
        proxy.Cut();
        holding.Release();
        await Task.Delay(backInTime ? 1000 : 7000);
        proxy.Restore();
        var outcome = await asking;

        Assert.Equal(backInTime ? DeliveryKind.Answered : DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task K7_a_Redis_that_lost_its_data_after_the_receipt_leaves_the_request_TakenUnanswered_with_the_handler_run_once()
    {
        var holding = new Echo(hold: true);
        var (_, caller, tid, _, cleanup) = await PairAsync(redis.ConnectionString, holding, clientDefaults: false);
        await using var _1 = cleanup;
        await using var admin = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString + ",allowAdmin=true");

        var asking = caller.DeliverAsync(new AskTarget("b", tid.ProcessId), Member(), TimeSpan.FromSeconds(3), CancellationToken.None);
        await holding.Handling.WaitAsync(TimeSpan.FromSeconds(30));
        await admin.GetServers()[0].FlushAllDatabasesAsync();
        holding.Release();
        var outcome = await asking;

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.Equal(1, holding.Started);
    }

    private sealed record AskResultHolder(DeliveryOutcome Outcome);
}
