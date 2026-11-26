using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Log;
using Unit.Testing;

namespace Unit.Generations;

public sealed class AttemptTests
{
    private static readonly Guid TokenA = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    private static readonly Guid TokenB = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    [Fact]
    public async Task I7_an_attempt_released_after_its_retry_generation_was_published_publishes_nothing_and_activates_nothing()
    {
        var store = Stores.Create("a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));

        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var timeout = new TaskCompletionSource();
        var pass2 = store.BeginPass(2, TokenB, ["a"]);
        var running = pass2.RunAttemptAsync("a", _ => hung.Task, timeout.Task);
        timeout.SetResult();
        var outcome = await running;
        var hungAttempt = outcome.Attempt;

        Assert.Equal(AttemptOutcomeKind.TimedOut, outcome.Kind);
        Assert.True(hungAttempt.Token.IsCancellationRequested);

        Assert.True(pass2.IsAbandoned);
        Assert.Null(store.Publish(pass2));

        var retry = await store.PublishAsync(2, TokenB, ("a", "a2-retry"));

        var activated = false;
        var deferredRan = false;
        hung.SetResult(new ReconcileResult("a2-late")
        {
            Deferred = (g, _) =>
            {
                deferredRan = true;
                activated = g.TryActivate("a", () => { });
                return Task.CompletedTask;
            },
        });

        await hungAttempt.Returned.WaitAsync(Bounds.Safety);

        Assert.Same(retry, store.Current);
        Assert.Equal("a2-retry", store.Current!.Get("a"));
        Assert.False(deferredRan);
        Assert.False(activated);
        Assert.True(first.Superseded("a").IsCancellationRequested);
        Assert.False(retry.Superseded("a").IsCancellationRequested);
    }

    [Fact]
    public async Task A_stall_ends_the_pass_and_a_further_attempt_in_it_never_runs()
    {
        var store = Stores.Create("a", "b");
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(1, TokenA, ["a", "b"]);

        var timedOut = await pass.RunAttemptAsync("a", _ => hung.Task, Task.CompletedTask);
        var ran = false;
        var after = await pass.RunAttemptAsync("b", _ => { ran = true; return Task.FromResult(new ReconcileResult("b1")); }, Stores.Never);
        hung.SetResult(new ReconcileResult("a-late"));
        await timedOut.Attempt.Returned;

        Assert.Equal(AttemptOutcomeKind.TimedOut, timedOut.Kind);
        Assert.True(pass.IsAbandoned);
        Assert.Equal(AttemptOutcomeKind.Abandoned, after.Kind);
        Assert.False(ran);
        Assert.Equal(1, timedOut.Attempt.Number);
        Assert.Equal(2, after.Attempt.Number);
        Assert.Null(store.Publish(pass));
        Assert.Null(store.Current);
    }

    [Fact]
    public void Passes_are_numbered_in_order()
    {
        var store = Stores.Create("a");

        var one = store.BeginPass(1, TokenA, ["a"]);
        var two = store.BeginPass(1, TokenA, ["a"]);

        Assert.Equal(one.Number + 1, two.Number);
    }

    [Fact]
    public async Task A_new_pass_abandons_the_open_one_which_then_publishes_nothing()
    {
        var store = Stores.Create("a");
        var old = store.BeginPass(1, TokenA, ["a"]);
        await old.AcceptAsync("a", new ReconcileResult("old"));

        var fresh = store.BeginPass(2, TokenB, ["a"]);

        Assert.True(old.IsAbandoned);
        Assert.Null(store.Publish(old));
        await fresh.AcceptAsync("a", new ReconcileResult("fresh"));
        Assert.Equal("fresh", store.Publish(fresh)!.Get("a"));
    }

    [Fact]
    public async Task I7_an_attempt_that_throws_fails_and_its_result_is_not_taken()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var thrown = await pass.RunAttemptAsync("a", _ => Task.FromException<ReconcileResult>(new InvalidOperationException("boom")), Stores.Never);
        var syncPass = store.BeginPass(1, TokenA, ["a"]);
        var syncThrown = await syncPass.RunAttemptAsync("a", _ => throw new InvalidOperationException("sync"), Stores.Never);

        Assert.Equal(AttemptOutcomeKind.Failed, thrown.Kind);
        Assert.Equal("boom", thrown.Error!.Message);
        Assert.Equal(AttemptOutcomeKind.Failed, syncThrown.Kind);
        Assert.Equal("sync", syncThrown.Error!.Message);
        Assert.True(thrown.Attempt.Token.IsCancellationRequested);
        Assert.True(syncPass.IsAbandoned);
        Assert.Null(store.Publish(syncPass));
        Assert.Null(store.Current);
    }

    [Fact]
    public async Task An_attempt_cancelled_by_its_own_token_fails()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);

        var outcome = await pass.RunAttemptAsync("a", _ => Task.FromCanceled<ReconcileResult>(new CancellationToken(true)), Stores.Never);

        Assert.Equal(AttemptOutcomeKind.Failed, outcome.Kind);
        Assert.IsAssignableFrom<OperationCanceledException>(outcome.Error);
    }

    [Fact]
    public async Task An_attempt_that_returns_a_null_state_fails()
    {
        var store = Stores.Create("a");

        var nullResult = await store.BeginPass(1, TokenA, ["a"]).RunAttemptAsync("a", _ => Task.FromResult<ReconcileResult>(null!), Stores.Never);
        var nullState = await store.BeginPass(1, TokenA, ["a"]).RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult(null!)), Stores.Never);

        Assert.Equal(AttemptOutcomeKind.Failed, nullResult.Kind);
        Assert.Equal(AttemptOutcomeKind.Failed, nullState.Kind);
    }

    [Fact]
    public async Task Abandoning_a_pass_ends_its_awaited_attempt_cancels_its_token_and_refuses_publication()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never);

        pass.Abandon();
        var outcome = await running.WaitAsync(Bounds.Safety);
        hung.SetResult(new ReconcileResult("late"));
        await outcome.Attempt.Returned;

        Assert.Equal(AttemptOutcomeKind.Abandoned, outcome.Kind);
        Assert.True(outcome.Attempt.Token.IsCancellationRequested);
        Assert.Null(store.Publish(pass));
        Assert.Null(store.Current);
        var ran = false;
        var after = await pass.RunAttemptAsync("a", _ => { ran = true; return hung.Task; }, Stores.Never);
        Assert.Equal(AttemptOutcomeKind.Abandoned, after.Kind);
        Assert.False(ran);
        Assert.True(after.Attempt.Token.IsCancellationRequested);
        Assert.True(after.Attempt.Returned.IsCompleted);
        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task Abandoning_after_an_attempt_was_accepted_still_refuses_publication()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var outcome = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), Stores.Never);

        pass.Abandon();

        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
        Assert.True(outcome.Attempt.Token.IsCancellationRequested);
        Assert.Null(store.Publish(pass));
    }

    [Fact]
    public async Task Abandoning_after_publication_changes_nothing()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var accepted = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), Stores.Never);
        var generation = store.Publish(pass);

        pass.Abandon();

        Assert.False(pass.IsAbandoned);
        Assert.Same(generation, store.Current);
        Assert.False(generation!.Superseded("a").IsCancellationRequested);
        Assert.False(accepted.Attempt.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task A_pass_publishes_once()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1"));
        store.Publish(pass);

        Assert.Throws<InvalidOperationException>(() => store.Publish(pass));
    }

    [Fact]
    public async Task A_pass_awaits_one_attempt_at_a_time_and_only_its_dirty_types()
    {
        var store = Stores.Create("a", "b");
        var pass = store.BeginPass(1, TokenA, ["a", "b"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never);

        Assert.Throws<InvalidOperationException>(() => { _ = pass.RunAttemptAsync("b", _ => hung.Task, Stores.Never); });
        hung.SetResult(new ReconcileResult("a1"));
        Assert.Equal(AttemptOutcomeKind.Accepted, (await running).Kind);

        Assert.Throws<InvalidOperationException>(() => { _ = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never); });
        Assert.Equal(AttemptOutcomeKind.Accepted, (await pass.RunAttemptAsync("b", _ => hung.Task, Stores.Never)).Kind);
        store.Publish(pass);

        var narrow = store.BeginPass(2, TokenB, ["a"]);
        Assert.Throws<ArgumentException>(() => { _ = narrow.RunAttemptAsync("b", _ => hung.Task, Stores.Never); });
    }

    [Fact]
    public async Task A_pass_publishes_only_when_every_dirty_type_was_accepted()
    {
        var store = Stores.Create("a", "b");
        var pass = store.BeginPass(1, TokenA, ["a", "b"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1"));

        Assert.Throws<InvalidOperationException>(() => store.Publish(pass));
        await pass.AcceptAsync("b", new ReconcileResult("b1"));
        Assert.NotNull(store.Publish(pass));
    }

    [Fact]
    public void A_pass_of_another_store_is_refused()
    {
        var store = Stores.Create("a");
        var other = Stores.Create("a");

        Assert.Throws<ArgumentException>(() => store.Publish(other.BeginPass(1, TokenA, ["a"])));
    }

    [Fact]
    public async Task The_attempt_sees_its_pass_and_number_and_type()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        ApplierAttempt? seen = null;

        await pass.RunAttemptAsync("a", attempt => { seen = attempt; return Task.FromResult(new ReconcileResult("a1")); }, Stores.Never);

        Assert.Equal(pass.Number, seen!.PassNumber);
        Assert.Equal(1, seen.Number);
        Assert.Equal("a", seen.AggregateType);
        Assert.False(seen.Token.IsCancellationRequested);
        await seen.Returned.WaitAsync(Bounds.Safety);
    }

    [Fact]
    public async Task A_timed_out_attempt_is_unreturned_from_its_timeout_until_it_returns()
    {
        var time = new FakeTimeProvider();
        var store = new GenerationStore(["a"], NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, TimeSpan.FromSeconds(5));

        Assert.Empty(store.Unreturned);
        time.Advance(TimeSpan.FromSeconds(5));
        var outcome = await running.WaitAsync(Bounds.Safety);
        var stuckSince = time.GetTimestamp();
        time.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(AttemptOutcomeKind.TimedOut, outcome.Kind);
        var unreturned = Assert.Single(store.Unreturned);
        Assert.Same(outcome.Attempt, unreturned.Attempt);
        Assert.Equal(AttemptOutcomeKind.TimedOut, unreturned.Why);
        Assert.Equal(stuckSince, unreturned.Since);

        hung.SetResult(new ReconcileResult("late"));
        await outcome.Attempt.Returned;
        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task An_attempt_that_returns_before_its_timeout_is_accepted_and_its_timer_ends()
    {
        var time = new FakeTimeProvider();
        var store = new GenerationStore(["a"], NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);
        var pass = store.BeginPass(1, TokenA, ["a"]);

        var outcome = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
        Assert.Empty(store.Unreturned);
        Assert.Equal("a1", store.Publish(pass)!.Get("a"));
    }

    [Fact]
    public async Task An_abandoned_attempt_is_unreturned_until_it_returns()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never);

        store.BeginPass(2, TokenB, ["a"]);
        var outcome = await running.WaitAsync(Bounds.Safety);

        Assert.Equal(AttemptOutcomeKind.Abandoned, Assert.Single(store.Unreturned).Why);
        hung.SetResult(new ReconcileResult("late"));
        await outcome.Attempt.Returned;
        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task I7_a_pass_abandoned_between_an_attempts_return_and_its_acceptance_publishes_nothing_and_activates_nothing()
    {
        var held = new HeldPoint(LogFaultPoints.BeforeAttemptAccepted);
        var store = Stores.Create(held, "a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var deferredRan = false;
        var pass = store.BeginPass(2, TokenB, ["a"]);
        held.Arm();

        var running = pass.RunAttemptAsync(
            "a",
            _ => Task.FromResult(new ReconcileResult("a2") { Deferred = (_, _) => { deferredRan = true; return Task.CompletedTask; } }),
            Stores.Never);
        await held.Reached.WaitAsync(Bounds.Safety);
        pass.Abandon();
        held.Release();
        var outcome = await running.WaitAsync(Bounds.Safety);

        Assert.Equal(AttemptOutcomeKind.Abandoned, outcome.Kind);
        Assert.True(outcome.Attempt.Token.IsCancellationRequested);
        Assert.Null(store.Publish(pass));
        Assert.Same(first, store.Current);
        Assert.False(first.Superseded("a").IsCancellationRequested);
        Assert.False(deferredRan);
        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task I7_a_retry_pass_begun_while_the_old_attempt_awaits_acceptance_is_the_only_one_published()
    {
        var held = new HeldPoint(LogFaultPoints.BeforeAttemptAccepted);
        var store = Stores.Create(held, "a");
        await store.PublishAsync(1, TokenA, ("a", "a1"));
        var old = store.BeginPass(2, TokenB, ["a"]);
        held.Arm();

        var running = old.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a-old")), Stores.Never);
        await held.Reached.WaitAsync(Bounds.Safety);
        var retry = store.BeginPass(2, TokenB, ["a"]);
        held.Release();
        var outcome = await running.WaitAsync(Bounds.Safety);
        await retry.AcceptAsync("a", new ReconcileResult("a-retry"));

        Assert.Equal(AttemptOutcomeKind.Abandoned, outcome.Kind);
        Assert.Null(store.Publish(old));
        Assert.Equal("a-retry", store.Publish(retry)!.Get("a"));
    }

    [Fact]
    public async Task I7_a_timeout_that_fires_while_the_returned_attempt_awaits_acceptance_does_not_discard_it()
    {
        var held = new HeldPoint(LogFaultPoints.BeforeAttemptAccepted);
        var store = Stores.Create(held, "a");
        await store.PublishAsync(1, TokenA, ("a", "a1"));
        var pass = store.BeginPass(2, TokenB, ["a"]);
        var timeout = new TaskCompletionSource();
        held.Arm();

        var running = pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a2")), timeout.Task);
        await held.Reached.WaitAsync(Bounds.Safety);
        timeout.SetResult();
        held.Release();
        var outcome = await running.WaitAsync(Bounds.Safety);

        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
        Assert.Equal("a2", store.Publish(pass)!.Get("a"));
    }

    [Fact]
    public async Task Model_events_record_each_attempt_at_its_transition()
    {
        var events = new RecordingModelEvents();
        var store = Stores.Create(events, "a", "b");

        var ok = store.BeginPass(1, TokenA, ["a", "b"]);
        await ok.AcceptAsync("a", new ReconcileResult("a1"));
        var failed = await ok.RunAttemptAsync("b", _ => throw new InvalidOperationException("boom"), Stores.Never);

        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hang = store.BeginPass(1, TokenA, ["a", "b"]);
        var timedOut = await hang.RunAttemptAsync("a", _ => hung.Task, Task.CompletedTask);
        hung.SetResult(new ReconcileResult("late"));
        await timedOut.Attempt.Returned;

        var abandoned = store.BeginPass(1, TokenA, ["a", "b"]);
        var running = abandoned.RunAttemptAsync("a", _ => new TaskCompletionSource<ReconcileResult>().Task, Stores.Never);
        abandoned.Abandon();
        await running;

        Assert.Collection(
            events.Events,
            e => AssertAttempt(e, LogActions.AttemptOK, ok.Number, 1, "a"),
            e => AssertAttempt(e, LogActions.AttemptFail, ok.Number, 2, "b"),
            e => AssertAttempt(e, LogActions.AttemptHang, hang.Number, 1, "a"),
            e => AssertAttempt(e, LogActions.LateReturn, hang.Number, 1, "a"));
        Assert.Equal(AttemptOutcomeKind.Failed, failed.Kind);

        static void AssertAttempt(ModelEvent e, string action, long pass, int attempt, string type)
        {
            Assert.Equal(action, e.Action);
            Assert.Equal(pass, e.State!["pass"]);
            Assert.Equal(attempt, e.State["attempt"]);
            Assert.Equal(type, e.State["type"]);
        }
    }

    [Fact]
    public async Task An_attempt_is_dated_by_the_store_clock_from_its_start_to_its_deadline()
    {
        var time = new FakeTimeProvider();
        var store = new GenerationStore(["a", "b", "c"], NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);
        var pass = store.BeginPass(1, TokenA, ["a", "b", "c"]);
        time.Advance(TimeSpan.FromSeconds(3));
        var started = time.GetTimestamp();

        var timed = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), TimeSpan.FromSeconds(5));
        var unbounded = await pass.RunAttemptAsync("b", _ => Task.FromResult(new ReconcileResult("b1")), Timeout.InfiniteTimeSpan);
        var byTask = await pass.RunAttemptAsync("c", _ => Task.FromResult(new ReconcileResult("c1")), Stores.Never);

        Assert.Equal(started, timed.Attempt.Started);
        Assert.Equal(started + (5 * time.TimestampFrequency), timed.Attempt.Deadline);
        Assert.Equal(AttemptOutcomeKind.Accepted, unbounded.Kind);
        Assert.Null(unbounded.Attempt.Deadline);
        Assert.Equal(started, byTask.Attempt.Started);
        Assert.Null(byTask.Attempt.Deadline);
    }

    [Fact]
    public async Task An_attempt_that_fails_after_its_pass_was_abandoned_reports_Abandoned_and_does_not_count_as_a_stall()
    {
        for (var i = 0; i < 200; i++)
        {
            var events = new RecordingModelEvents();
            var store = Stores.Create(events, "a");
            var pass = store.BeginPass(1, TokenA, ["a"]);

            var outcome = await pass.RunAttemptAsync(
                "a",
                _ =>
                {
                    pass.Abandon();
                    throw new InvalidOperationException("after the abandon");
                },
                Stores.Never).WaitAsync(Bounds.Safety);

            Assert.Equal(AttemptOutcomeKind.Abandoned, outcome.Kind);
            Assert.Null(outcome.Error);
            Assert.Empty(events.Events);
            await outcome.Attempt.Returned.WaitAsync(Bounds.Safety);
            Assert.Empty(store.Unreturned);
        }
    }

    [Fact]
    public async Task No_attempt_stays_unreturned_once_it_returned_whatever_an_abandon_races_with()
    {
        for (var i = 0; i < 2_000; i++)
        {
            var store = Stores.Create("a");
            var pass = store.BeginPass(1, TokenA, ["a"]);
            using var go = new Barrier(2);

            var running = Task.Run(() =>
            {
                go.SignalAndWait();
                return pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), Stores.Never);
            });
            var abandoning = Task.Run(() =>
            {
                go.SignalAndWait();
                pass.Abandon();
            });
            await abandoning.WaitAsync(Bounds.Safety);
            var outcome = await running.WaitAsync(Bounds.Safety);
            await outcome.Attempt.Returned.WaitAsync(Bounds.Safety);

            Assert.Contains(outcome.Kind, new[] { AttemptOutcomeKind.Accepted, AttemptOutcomeKind.Abandoned });
            Assert.True(outcome.Attempt.Token.IsCancellationRequested);
            Assert.Empty(store.Unreturned);
            Assert.Null(store.Publish(pass));
        }
    }

    [Fact]
    public async Task RunAttemptAsync_refuses_a_null_argument_before_an_attempt_begins()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        Task<ReconcileResult> Run(ApplierAttempt _) => Task.FromResult(new ReconcileResult("a1"));

        Assert.Throws<ArgumentNullException>("run", () => { _ = pass.RunAttemptAsync("a", null!, Stores.Never); });
        Assert.Throws<ArgumentNullException>("timeout", () => { _ = pass.RunAttemptAsync("a", Run, (Task)null!); });
        Assert.Throws<ArgumentNullException>("aggregateType", () => { _ = pass.RunAttemptAsync(null!, Run, Stores.Never); });
        Assert.Throws<ArgumentNullException>("aggregateType", () => { _ = pass.RunAttemptAsync(null!, Run, TimeSpan.FromSeconds(5)); });

        var outcome = await pass.RunAttemptAsync("a", Run, Stores.Never);
        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
        Assert.Equal(1, outcome.Attempt.Number);
    }

    [Fact]
    public async Task A_pass_cannot_run_an_attempt_from_inside_an_activation()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var pass = store.BeginPass(2, TokenB, ["a"]);
        var ran = false;

        Assert.Throws<InvalidOperationException>(() => first.TryActivate("a", () =>
        {
            _ = pass.RunAttemptAsync("a", _ => { ran = true; return Task.FromResult(new ReconcileResult("a2")); }, Stores.Never);
        }));

        Assert.False(ran);
    }

    [Fact]
    public async Task The_timer_of_an_attempt_that_returned_in_time_is_disposed_with_the_attempt()
    {
        var time = new DisposalTrackingTime();
        var store = new GenerationStore(["a"], NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);
        var pass = store.BeginPass(1, TokenA, ["a"]);

        var outcome = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), TimeSpan.FromSeconds(5));

        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
        Assert.True(time.Created > 0);
        Assert.Equal(time.Created, time.Disposed);
    }

    [Fact]
    public async Task The_unreturned_handlers_are_told_only_when_a_pass_ended_with_a_discarded_attempt()
    {
        var store = Stores.Create("a");
        var told = 0;
        using var subscription = store.OnUnreturned(() => Interlocked.Increment(ref told));

        store.BeginPass(1, TokenA, ["a"]).Abandon();
        Assert.Equal(0, told);

        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(2, TokenB, ["a"]);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never);
        pass.Abandon();
        var outcome = await running.WaitAsync(Bounds.Safety);

        Assert.Equal(1, told);
        hung.SetResult(new ReconcileResult("late"));
        await outcome.Attempt.Returned.WaitAsync(Bounds.Safety);
    }

    [Fact]
    public async Task A_disposed_unreturned_subscription_stops_its_handler_and_leaves_the_others()
    {
        var store = Stores.Create("a");
        var gone = 0;
        var kept = 0;
        var subscription = store.OnUnreturned(() => Interlocked.Increment(ref gone));
        using var other = store.OnUnreturned(() => Interlocked.Increment(ref kept));
        subscription.Dispose();

        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var running = pass.RunAttemptAsync("a", _ => hung.Task, Stores.Never);
        pass.Abandon();
        var outcome = await running.WaitAsync(Bounds.Safety);

        Assert.Equal(0, gone);
        Assert.Equal(1, kept);
        hung.SetResult(new ReconcileResult("late"));
        await outcome.Attempt.Returned.WaitAsync(Bounds.Safety);
    }

    [Fact]
    public void An_unreturned_handler_must_not_be_null()
    {
        Assert.Throws<ArgumentNullException>("handler", () => Stores.Create("a").OnUnreturned(null!));
    }

    private sealed class DisposalTrackingTime : FakeTimeProvider
    {
        private int created;
        private int disposed;

        public int Created => Volatile.Read(ref created);

        public int Disposed => Volatile.Read(ref disposed);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Interlocked.Increment(ref created);
            return new Tracked(base.CreateTimer(callback, state, dueTime, period), () => Interlocked.Increment(ref disposed));
        }

        private sealed class Tracked(ITimer inner, Action onDispose) : ITimer
        {
            private int once;

            public bool Change(TimeSpan dueTime, TimeSpan period) => inner.Change(dueTime, period);

            public void Dispose()
            {
                if (Interlocked.Exchange(ref once, 1) == 0)
                {
                    onDispose();
                }

                inner.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
