using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;

namespace Unit.Redis.Relay;

public sealed class RelayProtocolTests
{
    private static byte[] Bytes(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task A_request_is_taken_and_answered_by_a_replica_that_never_asked()
    {
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        var outcome = await rig.AskTargetAsync(Bytes("ping"));

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(AskAnswerKind.Response, outcome.Answer!.Kind);
        Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(outcome.Answer.Response.Span));
        Assert.Equal(1, inbound.Started);
    }

    [Fact]
    public async Task The_receipt_is_written_before_the_handler_starts()
    {
        byte[]? recordWhenStarted = null;
        RelayRig? rig = null;
        var inbound = ReservingInbound.Handling((delivery, _) =>
        {
            recordWhenStarted = rig!.Wire.Records().Select(r => rig.Wire.Peek(r)).FirstOrDefault();
            return Task.FromResult(new AskAnswer(AskAnswerKind.Response, delivery.Payload.ToArray(), null));
        });
        rig = new RelayRig(inbound);
        await using var owned = rig;
        await rig.StartAsync();

        var outcome = await rig.AskTargetAsync();

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal((byte)'T', recordWhenStarted![1]);
    }

    [Fact]
    public async Task A_keyed_request_carries_its_kind_key_and_token_to_the_target()
    {
        AskDelivery? seen = null;
        var inbound = ReservingInbound.Handling((delivery, _) =>
        {
            seen = delivery;
            return Task.FromResult(new AskAnswer(AskAnswerKind.Response, default, null));
        });
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        var outcome = await rig.Caller.DeliverAsync(rig.TargetAddress, new AskDelivery("scheduler", "k1", 42, Bytes("x")), RelayRig.Patience, CancellationToken.None);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(("scheduler", "k1", 42L), (seen!.Kind, seen.Key, seen.Token));
    }

    [Fact]
    public async Task A_handlers_error_and_an_owner_lost_answer_reach_the_caller_as_answers()
    {
        var inbound = ReservingInbound.Handling((_, _) => Task.FromResult(new AskAnswer(AskAnswerKind.HandlerError, default, "boom")));
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        var outcome = await rig.AskTargetAsync();

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal((AskAnswerKind.HandlerError, "boom"), (outcome.Answer!.Kind, outcome.Answer.Error));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task A_decline_reaches_the_caller_with_its_reason_and_no_handler_starts(int code)
    {
        var reason = (DeclineReason)code;
        var inbound = ReservingInbound.Declining(reason);
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        var outcome = await rig.AskTargetAsync();

        Assert.Equal(DeliveryKind.Declined, outcome.Kind);
        Assert.Equal(reason, outcome.Reason);
        Assert.Equal(0, inbound.Started);
    }

    [Fact]
    public async Task A_request_nobody_takes_after_a_connection_event_ends_TakenUnanswered()
    {
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock);
        await rig.StartAsync();
        var asking = rig.Caller.DeliverAsync(new AskTarget("b", Guid.NewGuid()), new AskDelivery(null, null, null, new byte[] { 1 }), RelayRig.Timeout, CancellationToken.None);
        await RelayRig.SettledAsync(() => rig.Wire.Commands.Contains("Publish"));
        rig.Wire.ConnectionEvents++;
        clock.Advance(AskRelayDecisions.AnswerWindow(RelayRig.Timeout, RelayRig.Timings.WithdrawBudget));

        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task A_request_nobody_takes_at_the_take_bound_after_a_connection_event_ends_TakenUnanswered()
    {
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock);
        await rig.StartAsync();
        var takeTimeout = TimeSpan.FromMilliseconds(500);
        var asking = rig.Caller.DeliverAsync(new AskTarget("b", Guid.NewGuid()), new AskDelivery(null, null, null, Bytes("x")), RelayRig.Timeout, takeTimeout, CancellationToken.None);
        await RelayRig.SettledAsync(() => rig.Wire.Commands.Contains("Publish"));
        rig.Wire.ConnectionEvents++;
        clock.Advance(takeTimeout);

        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task A_connection_event_before_the_call_does_not_change_NotTaken()
    {
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock);
        await rig.StartAsync();
        rig.Wire.ConnectionEvents++;
        var asking = rig.Caller.DeliverAsync(new AskTarget("b", Guid.NewGuid()), new AskDelivery(null, null, null, Bytes("x")), RelayRig.Timeout, CancellationToken.None);
        await RelayRig.SettledAsync(() => rig.Wire.Commands.Contains("Publish"));
        clock.Advance(AskRelayDecisions.AnswerWindow(RelayRig.Timeout, RelayRig.Timings.WithdrawBudget));

        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
    }

    [Fact]
    public async Task A_request_to_a_process_id_nobody_listens_on_ends_NotTaken_within_the_timeout()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();
        var replaced = new AskTarget("b", Guid.NewGuid());
        var started = clock.GetTimestamp();

        var asking = rig.Caller.DeliverAsync(replaced, new AskDelivery(null, null, null, Bytes("x")), RelayRig.Timeout, CancellationToken.None);
        clock.Advance(AskRelayDecisions.AnswerWindow(RelayRig.Timeout, RelayRig.Timings.WithdrawBudget));
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Reserved);
        Assert.True(clock.GetElapsedTime(started) <= RelayRig.Timeout, $"took {clock.GetElapsedTime(started)}");
    }

    [Fact]
    public async Task A_replica_id_alone_does_not_address_a_process()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        var timeout = TimeSpan.FromMilliseconds(400);
        var asking = rig.Caller.DeliverAsync(new AskTarget("b", rig.CallerIdentity.ProcessId), new AskDelivery(null, null, null, Bytes("x")), timeout, CancellationToken.None);
        clock.Advance(AskRelayDecisions.AnswerWindow(timeout, RelayRig.Timings.WithdrawBudget));
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Reserved);
    }

    [Fact]
    public async Task With_Redis_down_at_the_call_the_request_is_NotTaken_at_once()
    {
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock);
        await rig.StartAsync();
        rig.Wire.Down = true;
        var started = clock.GetTimestamp();

        var outcome = await rig.AskTargetAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(TimeSpan.Zero, clock.GetElapsedTime(started));
    }

    [Fact]
    public async Task A_submit_whose_answer_was_lost_is_withdrawn_and_the_request_is_NotTaken()
    {
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        rig.Wire.Fail("Write", FakeWire.Lost(), runFirst: true);
        var outcome = await rig.AskTargetAsync();

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Reserved);
    }

    [Fact]
    public async Task A_publish_that_failed_after_the_record_was_written_is_withdrawn_and_NotTaken()
    {
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        rig.Wire.Fail("Publish", FakeWire.Lost());
        var outcome = await rig.AskTargetAsync();

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Started);
    }

    [Fact]
    public async Task A_publish_whose_answer_was_lost_after_the_message_went_out_is_never_NotTaken_for_a_request_a_handler_ran()
    {
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        rig.Wire.Fail("Publish", FakeWire.Lost(), runFirst: true);
        var outcome = await rig.AskTargetAsync();
        await Task.Delay(200);

        Assert.Contains(outcome.Kind, new[] { DeliveryKind.NotTaken, DeliveryKind.TakenUnanswered, DeliveryKind.Answered });
        Assert.Equal(outcome.Kind == DeliveryKind.NotTaken ? 0 : 1, inbound.Started);
    }

    [Fact]
    public async Task With_Redis_down_after_the_receipt_the_withdrawal_fails_and_the_request_is_TakenUnanswered()
    {
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
            return new AskAnswer(AskAnswerKind.Response, default, null);
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        var asking = rig.AskTargetAsync(timeout: RelayRig.Patience);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        rig.Wire.Down = true;
        RelayRig.EndWindow(clock, RelayRig.Patience);
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));
        release.SetResult();

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task With_Redis_back_before_the_window_ends_the_answer_arrives()
    {
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (delivery, _) =>
        {
            started.SetResult();
            await release.Task;
            return new AskAnswer(AskAnswerKind.Response, delivery.Payload.ToArray(), null);
        });
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();

        var asking = rig.AskTargetAsync(Bytes("late"));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        rig.Wire.Down = true;
        await Task.Delay(100);
        rig.Wire.Down = false;
        release.SetResult();
        var outcome = await asking;

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
    }

    [Fact]
    public async Task A_receipt_Redis_did_not_take_starts_no_handler_and_the_request_is_NotTaken()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        rig.Wire.Fail("Replace", FakeWire.Lost(), runFirst: false, times: 100, when: FakeWire.Writing('T'));
        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, targetSettled: () => rig.Wire.Commands.Contains("Replace"));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(1, inbound.Reserved);
        Assert.Equal(0, inbound.Started);
    }

    [Fact]
    public async Task A_receipt_whose_answer_was_lost_starts_no_handler_and_the_request_is_TakenUnanswered()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        rig.Wire.Fail("Replace", FakeWire.Lost(), runFirst: true, when: FakeWire.Writing('T'));
        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, targetSettled: () => rig.Wire.HasRecordTagged('T'), timeout: RelayRig.Patience);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.Equal(0, inbound.Started);
    }

    [Fact]
    public async Task An_answer_Redis_did_not_take_once_is_written_again_and_arrives()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        rig.Wire.Fail("Replace", FakeWire.Lost(), runFirst: false, times: 3, when: FakeWire.Writing('A'));
        var outcome = await RelayRig.StepUntilAsync(rig.AskTargetAsync(Bytes("again"), TimeSpan.FromHours(1)), clock, RelayRig.Timings.AnswerRetry);

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(1, inbound.Started);
        Assert.True(rig.Wire.Commands.Count(c => c == "Replace") >= 5, "the receipt, three refused answers and the answer");
    }

    [Fact]
    public async Task An_answer_Redis_never_takes_leaves_the_request_TakenUnanswered()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        rig.Wire.Fail("Replace", FakeWire.Lost(), runFirst: false, times: int.MaxValue, when: FakeWire.Writing('A'));
        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, targetSettled: () => rig.Wire.Commands.Count(c => c == "Replace") >= 2, timeout: RelayRig.Patience);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.Equal(1, inbound.Started);
    }

    [Fact]
    public async Task A_decline_Redis_did_not_take_costs_the_caller_its_wait_and_the_request_is_NotTaken()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Declining(DeclineReason.NotServing);
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        rig.Wire.Fail("Replace", FakeWire.Lost(), runFirst: false, times: 100, when: FakeWire.Writing('D'));
        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, targetSettled: () => rig.Wire.Commands.Contains("Replace"), timeout: TimeSpan.FromMilliseconds(500));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
    }

    [Fact]
    public async Task A_record_that_vanished_after_a_confirmed_submit_is_TakenUnanswered()
    {
        var started = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
            return new AskAnswer(AskAnswerKind.Response, default, null);
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        var asking = rig.AskTargetAsync(timeout: RelayRig.Patience);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        foreach (var record in rig.Wire.Records().ToList())
        {
            rig.Wire.Delete(record);
        }

        RelayRig.EndWindow(clock, RelayRig.Patience);
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));
        release.SetResult();

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
    }

    [Fact]
    public async Task A_target_too_slow_to_take_the_request_loses_it_to_the_withdrawal_and_runs_no_handler()
    {
        var late = new TaskCompletionSource();
        ReservingInbound? inbound = null;
        inbound = new ReservingInbound(async (delivery, token) =>
        {
            await late.Task;
            return AskReservation.Reserve(() => Task.FromResult(new AskAnswer(AskAnswerKind.Response, default, null)));
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock, targetTime: new FakeTimeProvider());
        await rig.StartAsync();

        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, timeout: TimeSpan.FromMilliseconds(400));
        late.SetResult();
        await Task.Delay(200);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Started);
        Assert.Empty(rig.Wire.Records());
    }

    [Fact]
    public async Task A_handler_slower_than_the_window_leaves_the_request_TakenUnanswered_and_its_token_is_cancelled()
    {
        CancellationToken seen = default;
        var ended = new TaskCompletionSource<bool>();
        var started = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (_, answerBy) =>
        {
            seen = answerBy;
            started.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(30), CancellationToken.None).WaitAsync(answerBy).ContinueWith(_ => ended.TrySetResult(true), TaskScheduler.Default);
            return new AskAnswer(AskAnswerKind.Unanswered, default, null);
        });
        var clock = new FakeTimeProvider();
        var timeout = TimeSpan.FromMilliseconds(500);
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();
        var begun = clock.GetTimestamp();

        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, targetSettled: () => started.Task.IsCompleted, timeout: timeout);

        Assert.Equal(DeliveryKind.TakenUnanswered, outcome.Kind);
        Assert.True(clock.GetElapsedTime(begun) <= timeout, $"took {clock.GetElapsedTime(begun)}");
        Assert.True(await ended.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(seen.IsCancellationRequested);
    }

    [Fact]
    public async Task The_caller_leaving_ends_the_wait_with_a_withdrawal()
    {
        var inbound = ReservingInbound.Declining(DeclineReason.NotServing);
        await using var rig = new RelayRig(new ReservingInbound((_, _) => new ValueTask<AskReservation>(new TaskCompletionSource<AskReservation>().Task)));
        await rig.StartAsync();
        using var left = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var outcome = await rig.AskTargetAsync(timeout: TimeSpan.FromSeconds(20), cancellationToken: left.Token);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        _ = inbound;
    }

    [Fact]
    public async Task A_request_whose_timeout_is_short_leaves_room_for_its_withdrawal_within_it()
    {
        var clock = new FakeTimeProvider();
        var timeout = TimeSpan.FromMilliseconds(200);
        await using var rig = new RelayRig(new ReservingInbound((_, _) => new ValueTask<AskReservation>(new TaskCompletionSource<AskReservation>().Task)), time: clock);
        await rig.StartAsync();
        var started = clock.GetTimestamp();

        var outcome = await rig.AskUntilTheWindowEndsAsync(clock, timeout: timeout);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.True(clock.GetElapsedTime(started) < timeout, $"took {clock.GetElapsedTime(started)}");
    }

    [Fact]
    public async Task The_record_of_a_finished_request_is_removed()
    {
        await using var rig = new RelayRig(ReservingInbound.Echoing());
        await rig.StartAsync();

        await rig.AskTargetAsync();

        Assert.Empty(rig.Wire.Records());
    }

    private static readonly TimeSpan TakeBound = TimeSpan.FromMilliseconds(300);

    private static readonly TimeSpan LongTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_request_nobody_takes_is_withdrawn_at_the_take_bound_and_ends_NotTaken()
    {
        var clock = new FakeTimeProvider();
        var inbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();
        var gone = new AskTarget("b", Guid.NewGuid());

        var asking = rig.Caller.DeliverAsync(gone, new AskDelivery(null, null, null, Bytes("x")), LongTimeout, TakeBound, CancellationToken.None);
        clock.Advance(TakeBound);
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Reserved);
        Assert.Empty(rig.Wire.Records());
    }

    [Fact]
    public async Task The_end_of_the_take_bound_wakes_a_wait_whose_next_poll_is_later()
    {
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), time: clock, timings: RelayRig.Timings with { PollInterval = TimeSpan.FromSeconds(10) });
        await rig.StartAsync();
        var gone = new AskTarget("b", Guid.NewGuid());

        var asking = rig.Caller.DeliverAsync(gone, new AskDelivery(null, null, null, Bytes("x")), LongTimeout, TakeBound, CancellationToken.None);
        clock.Advance(TakeBound);
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
    }

    [Fact]
    public async Task A_request_taken_before_the_take_bound_is_waited_on_and_answered_after_it()
    {
        var release = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (_, _) =>
        {
            await release.Task;
            return new AskAnswer(AskAnswerKind.Response, Bytes("late"), null);
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        var asking = rig.Caller.DeliverAsync(rig.TargetAddress, new AskDelivery(null, null, null, Bytes("x")), LongTimeout, TakeBound, CancellationToken.None);
        await RelayRig.SettledAsync(() => inbound.Started == 1);
        clock.Advance(TakeBound);
        await RelayRig.SettledAsync(() => rig.Wire.Commands.Count(c => c == "Read") > 0);
        var reads = rig.Wire.Commands.Count(c => c == "Read");
        await Task.Delay(200);
        Assert.False(asking.IsCompleted, "a taken request is not ended at the take bound");
        Assert.True(rig.Wire.Commands.Count(c => c == "Read") - reads <= 4, "one withdrawal at the take bound, then the wait goes on as before");

        release.SetResult();
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal("late", System.Text.Encoding.UTF8.GetString(outcome.Answer!.Response.Span));
    }

    [Fact]
    public async Task A_target_that_comes_to_take_after_the_take_bound_cannot_take_the_request_and_runs_no_handler()
    {
        var late = new TaskCompletionSource();
        var arrived = new TaskCompletionSource();
        var inbound = new ReservingInbound(async (_, _) =>
        {
            arrived.TrySetResult();
            await late.Task;
            return AskReservation.Reserve(() => Task.FromResult(new AskAnswer(AskAnswerKind.Response, default, null)));
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock, targetTime: new FakeTimeProvider());
        await rig.StartAsync();

        var asking = rig.Caller.DeliverAsync(rig.TargetAddress, new AskDelivery(null, null, null, Bytes("x")), LongTimeout, TakeBound, CancellationToken.None);
        await arrived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        clock.Advance(TakeBound);
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));
        late.SetResult();
        await Task.Delay(200);

        Assert.Equal(DeliveryKind.NotTaken, outcome.Kind);
        Assert.Equal(0, inbound.Started);
    }

    [Fact]
    public async Task A_withdrawal_Redis_did_not_answer_at_the_take_bound_does_not_end_a_request_whose_handler_runs()
    {
        var release = new TaskCompletionSource();
        var inbound = ReservingInbound.Handling(async (_, _) =>
        {
            await release.Task;
            return new AskAnswer(AskAnswerKind.Response, Bytes("ran"), null);
        });
        var clock = new FakeTimeProvider();
        await using var rig = new RelayRig(inbound, time: clock);
        await rig.StartAsync();

        var asking = rig.Caller.DeliverAsync(rig.TargetAddress, new AskDelivery(null, null, null, Bytes("x")), LongTimeout, TakeBound, CancellationToken.None);
        await RelayRig.SettledAsync(() => inbound.Started == 1);
        var reads = rig.Wire.Commands.Count(c => c == "Read");
        rig.Wire.Fail("Read", FakeWire.Lost(), times: 2);
        clock.Advance(TakeBound);
        await RelayRig.SettledAsync(() => rig.Wire.Commands.Count(c => c == "Read") >= reads + 2);
        release.SetResult();
        var outcome = await asking.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Equal(1, inbound.Started);
    }
}
