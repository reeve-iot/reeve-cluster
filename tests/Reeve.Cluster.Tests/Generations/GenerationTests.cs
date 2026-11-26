using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.Log;
using Unit.Testing;

namespace Unit.Generations;

public sealed class GenerationTests
{
    private static readonly Guid TokenA = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    private static readonly Guid TokenB = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    [Fact]
    public void Before_the_first_publication_there_is_no_current_generation()
    {
        var store = Stores.Create("a", "b");

        Assert.Null(store.Current);
    }

    [Fact]
    public async Task I6_a_published_generation_is_labelled_with_its_head_and_token_and_becomes_current()
    {
        var store = Stores.Create("a", "b");

        var generation = await store.PublishAsync(7, TokenA, ("a", "a1"), ("b", "b1"));

        Assert.Same(generation, store.Current);
        Assert.Equal(7, generation.Head);
        Assert.Equal(TokenA, generation.HeadToken);
        Assert.Equal("a1", generation.Get("a"));
        Assert.Equal("b1", generation.Get<string>("b"));
    }

    [Fact]
    public async Task I6_an_empty_log_publishes_a_generation_at_head_0_without_a_token()
    {
        var store = Stores.Create("a");

        var generation = await store.PublishAsync(0, null, ("a", "a0"));

        Assert.Equal(0, generation.Head);
        Assert.Null(generation.HeadToken);
    }

    [Fact]
    public async Task I6_the_first_pass_must_reconcile_every_type()
    {
        var store = Stores.Create("a", "b");

        Assert.Throws<InvalidOperationException>(() => store.BeginPass(1, TokenA, ["a"]));
        await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));
    }

    [Fact]
    public void A_store_refuses_a_type_declared_twice()
    {
        Assert.Throws<ArgumentException>(() => Stores.Create("a", "b", "a"));
    }

    [Fact]
    public async Task A_pass_refuses_a_type_no_reconciler_owns()
    {
        var store = Stores.Create("a");
        await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<ArgumentException>(() => store.BeginPass(2, TokenB, ["x"]));
    }

    [Fact]
    public async Task I6_a_type_not_dirty_in_a_pass_shares_its_state_and_its_superseded_token()
    {
        var store = Stores.Create("a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));

        var second = await store.PublishAsync(2, TokenB, ("a", "a2"));

        Assert.Equal("a2", second.Get("a"));
        Assert.Same(first.Get("b"), second.Get("b"));
        Assert.Equal(first.Superseded("b"), second.Superseded("b"));
        Assert.False(second.Superseded("b").IsCancellationRequested);
    }

    [Fact]
    public async Task I21_publication_cancels_the_superseded_token_of_each_replaced_type_and_only_those()
    {
        var store = Stores.Create("a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));

        var second = await store.PublishAsync(2, TokenB, ("a", "a2"));

        Assert.True(first.Superseded("a").IsCancellationRequested);
        Assert.False(first.Superseded("b").IsCancellationRequested);
        Assert.False(second.Superseded("a").IsCancellationRequested);
        Assert.NotEqual(first.Superseded("a"), second.Superseded("a"));
    }

    [Fact]
    public async Task A_pass_with_no_dirty_type_publishes_a_new_head_over_the_same_states()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));

        var second = await store.PublishAsync(2, TokenB);

        Assert.NotSame(first, second);
        Assert.Equal(2, second.Head);
        Assert.Same(first.Get("a"), second.Get("a"));
        Assert.False(first.Superseded("a").IsCancellationRequested);
    }

    [Fact]
    public async Task A_type_no_reconciler_owns_is_not_found_in_a_generation()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<KeyNotFoundException>(() => generation.Get("x"));
        Assert.Throws<KeyNotFoundException>(() => generation.Superseded("x"));
        Assert.Throws<KeyNotFoundException>(() => generation.TryActivate("x", () => { }));
    }

    [Fact]
    public async Task I7_TryActivate_runs_the_activation_while_the_type_state_is_current()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var ran = 0;

        Assert.True(generation.TryActivate("a", () => ran++));
        Assert.Equal(1, ran);
    }

    [Fact]
    public async Task I7_TryActivate_refuses_once_the_type_state_is_replaced_and_does_not_run_the_activation()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        await store.PublishAsync(2, TokenB, ("a", "a2"));
        var ran = 0;

        Assert.False(first.TryActivate("a", () => ran++));
        Assert.Equal(0, ran);
    }

    [Fact]
    public async Task I7_a_deferred_start_survives_the_publication_of_another_type_and_is_refused_once_its_own_type_is_replaced()
    {
        var store = Stores.Create("a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));

        var afterOther = await store.PublishAsync(2, TokenB, ("b", "b2"));

        Assert.NotSame(first, store.Current);
        Assert.True(first.TryActivate("a", () => { }));
        Assert.False(first.Superseded("a").IsCancellationRequested);

        await store.PublishAsync(3, TokenA, ("a", "a3"));

        Assert.False(first.TryActivate("a", () => { }));
        Assert.False(afterOther.TryActivate("a", () => { }));
        Assert.True(first.Superseded("a").IsCancellationRequested);
    }

    [Fact]
    public async Task I7_an_activation_that_throws_propagates_and_leaves_the_swap_working()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<InvalidOperationException>(() => generation.TryActivate("a", () => throw new InvalidOperationException("boom")));

        var next = await store.PublishAsync(2, TokenB, ("a", "a2"));
        Assert.Same(next, store.Current);
        Assert.False(generation.TryActivate("a", () => { }));
    }

    [Fact]
    public async Task An_activation_that_throws_emits_no_TryActivate_event()
    {
        var events = new RecordingModelEvents();
        var store = Stores.Create(events, "a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<InvalidOperationException>(() => generation.TryActivate("a", () => throw new InvalidOperationException("boom")));

        Assert.DoesNotContain(events.Events, e => e.Action == LogActions.TryActivate);
    }

    [Fact]
    public async Task I21_an_activation_cannot_call_back_into_the_swap()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var open = store.BeginPass(2, TokenB, ["a"]);
        await open.AcceptAsync("a", new ReconcileResult("a2"));
        var refused = new List<string>();

        void Refused(string call, Action act)
        {
            try
            {
                act();
            }
            catch (InvalidOperationException)
            {
                refused.Add(call);
            }
        }

        generation.TryActivate("a", () =>
        {
            Refused("BeginPass", () => store.BeginPass(3, TokenA, ["a"]));
            Refused("Publish", () => store.Publish(open));
            Refused("Abandon", open.Abandon);
            Refused("RunAttemptAsync", () => { _ = open.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("x")), Stores.Never); });
            Refused("TryActivate", () => generation.TryActivate("a", () => { }));
        });

        Assert.Equal(["BeginPass", "Publish", "Abandon", "RunAttemptAsync", "TryActivate"], refused);
        Assert.Same(generation, store.Current);
        Assert.Equal("a2", store.Publish(open)!.Get("a"));
    }

    [Fact]
    public async Task TryActivate_refuses_a_null_activation()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<ArgumentNullException>(() => generation.TryActivate("a", null!));
    }

    [Fact]
    public async Task I21_a_superseded_callback_sees_the_new_generation_current_and_the_old_state_refused()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var seen = new TaskCompletionSource<(IGeneration? Current, bool Activated)>(TaskCreationOptions.RunContinuationsAsynchronously);
        first.Superseded("a").Register(() => seen.SetResult((store.Current, first.TryActivate("a", () => { }))));

        var second = await store.PublishAsync(2, TokenB, ("a", "a2"));

        var (currentInCallback, activatedInCallback) = await seen.Task.WaitAsync(Bounds.Safety);
        Assert.Same(second, currentInCallback);
        Assert.False(activatedInCallback);
    }

    [Fact]
    public async Task A_superseded_callback_that_throws_does_not_fail_the_publication_or_stop_other_callbacks_and_is_logged()
    {
        var logger = new RecordingLogger();
        var store = Stores.Create(logger, "a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));
        first.Superseded("a").Register(() => throw new InvalidOperationException("callback"));
        var otherRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        first.Superseded("b").Register(() => otherRan.SetResult());

        var second = await store.PublishAsync(2, TokenB, ("a", "a2"), ("b", "b2"));

        Assert.Same(second, store.Current);
        await otherRan.Task.WaitAsync(Bounds.Safety);
        var (message, exception) = await logger.FirstError.WaitAsync(Bounds.Safety);
        Assert.Contains("cancellation callback", message);
        Assert.Contains("type a threw", message);
        Assert.Contains(Assert.IsType<AggregateException>(exception).Flatten().InnerExceptions, e => e.Message == "callback");
    }

    [Fact]
    public async Task Deferred_work_runs_after_the_swap_with_its_generation_and_its_type_superseded_token()
    {
        var store = Stores.Create("a", "b");
        var ran = new TaskCompletionSource<(IGeneration Generation, CancellationToken Token, IGeneration? Current)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(1, TokenA, ["a", "b"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1")
        {
            Deferred = (g, token) =>
            {
                ran.SetResult((g, token, store.Current));
                return Task.CompletedTask;
            },
        });
        await pass.AcceptAsync("b", new ReconcileResult("b1"));

        var generation = store.Publish(pass);

        var (seen, token, current) = await ran.Task.WaitAsync(Bounds.Safety);
        Assert.NotNull(generation);
        Assert.Same(generation, seen);
        Assert.Same(generation, current);
        Assert.Equal(generation.Superseded("a"), token);
    }

    [Fact]
    public async Task Deferred_work_that_throws_is_contained_and_logged()
    {
        var logger = new RecordingLogger();
        var store = Stores.Create(logger, "a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1") { Deferred = (_, _) => throw new InvalidOperationException("deferred") });

        var generation = store.Publish(pass);

        Assert.NotNull(generation);
        var (message, exception) = await logger.FirstError.WaitAsync(Bounds.Safety);
        Assert.Contains("Deferred work", message);
        Assert.Equal("deferred", exception!.Message);
        await store.PublishAsync(2, TokenB, ("a", "a2"));
    }

    [Fact]
    public async Task Deferred_work_cancelled_by_its_supersession_is_not_logged()
    {
        var logger = new RecordingLogger();
        var store = Stores.Create(logger, "a");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var work = Task.CompletedTask;
        var pass = store.BeginPass(1, TokenA, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1")
        {
            Deferred = (_, token) =>
            {
                work = Task.Delay(Timeout.Infinite, token);
                started.SetResult();
                return work;
            },
        });
        store.Publish(pass);
        await started.Task.WaitAsync(Bounds.Safety);

        await store.PublishAsync(2, TokenB, ("a", "a2"));

        Assert.IsType<TaskCanceledException>(await Record.ExceptionAsync(() => work.WaitAsync(Bounds.Safety)));
        await Task.Delay(100);

        Assert.False(logger.FirstError.IsCompleted);
    }

    [Fact]
    public async Task Deferred_work_does_not_run_in_the_applier_execution_context()
    {
        var store = Stores.Create("a");
        var applierScope = new AsyncLocal<string?> { Value = "applier" };
        var seen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(1, TokenA, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a1")
        {
            Deferred = (_, _) =>
            {
                seen.SetResult(applierScope.Value);
                return Task.CompletedTask;
            },
        });

        store.Publish(pass);

        Assert.Null(await seen.Task.WaitAsync(Bounds.Safety));
        Assert.Equal("applier", applierScope.Value);
    }

    [Fact]
    public async Task Model_events_record_each_publication_and_each_TryActivate()
    {
        var events = new RecordingModelEvents();
        var store = Stores.Create(events, "a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));
        var second = await store.PublishAsync(2, TokenB, ("a", "a2"));
        first.TryActivate("b", () => { });
        first.TryActivate("a", () => { });

        Assert.Collection(
            events.Events.Where(e => e.Action is LogActions.Publish or LogActions.TryActivate),
            e =>
            {
                Assert.Equal(LogActions.Publish, e.Action);
                Assert.Equal(1L, e.State!["generation"]);
                Assert.Equal(1L, e.State["head"]);
                Assert.Equal(TokenA, e.State["headToken"]);
                Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)e.State["dirty"]!);
            },
            e =>
            {
                Assert.Equal(LogActions.Publish, e.Action);
                Assert.Equal(2L, e.State!["generation"]);
                Assert.Equal(2L, e.State["head"]);
                Assert.Equal(new[] { "a" }, (IEnumerable<string>)e.State["dirty"]!);
            },
            e =>
            {
                Assert.Equal(LogActions.TryActivate, e.Action);
                Assert.Equal("b", e.State!["type"]);
                Assert.Equal(1L, e.State["stateGeneration"]);
                Assert.Equal(true, e.State["activated"]);
            },
            e =>
            {
                Assert.Equal(LogActions.TryActivate, e.Action);
                Assert.Equal("a", e.State!["type"]);
                Assert.Equal(1L, e.State["stateGeneration"]);
                Assert.Equal(false, e.State["activated"]);
            });
        Assert.Equal(2, second.Head);
    }

    [Fact]
    public async Task Pinning_takes_the_current_generation_into_the_scope()
    {
        var store = Stores.Create("a");
        var scope = new PinScope();

        Assert.Throws<InvalidOperationException>(() => scope.PinCurrent(store));

        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var pinned = scope.PinCurrent(store);
        await store.PublishAsync(2, TokenB, ("a", "a2"));

        Assert.Same(generation, pinned);
        Assert.Same(generation, scope.PinnedGeneration);
        Assert.Equal("a1", scope.PinnedGeneration!.Get("a"));
    }

    [Fact]
    public void A_store_refuses_a_null_argument()
    {
        var graph = ReconcilerGraph.Independent(["a"]);

        Assert.Throws<ArgumentNullException>("graph", () => new GenerationStore((ReconcilerGraph)null!, NoFaultPoints.Instance, NoModelEvents.Instance, TimeProvider.System, NullLogger.Instance));
        Assert.Throws<ArgumentNullException>("faultPoints", () => new GenerationStore(graph, null!, NoModelEvents.Instance, TimeProvider.System, NullLogger.Instance));
        Assert.Throws<ArgumentNullException>("events", () => new GenerationStore(graph, NoFaultPoints.Instance, null!, TimeProvider.System, NullLogger.Instance));
        Assert.Throws<ArgumentNullException>("time", () => new GenerationStore(graph, NoFaultPoints.Instance, NoModelEvents.Instance, null!, NullLogger.Instance));
        Assert.Throws<ArgumentNullException>("logger", () => new GenerationStore(graph, NoFaultPoints.Instance, NoModelEvents.Instance, TimeProvider.System, null!));
    }

    [Fact]
    public async Task BeginPass_Publish_and_pinning_refuse_a_null_argument()
    {
        var store = Stores.Create("a");
        var generation = await store.PublishAsync(1, TokenA, ("a", "a1"));

        Assert.Throws<ArgumentNullException>("dirtyTypes", () => store.BeginPass(2, TokenB, null!));
        Assert.Throws<ArgumentNullException>("pass", () => store.Publish(null!));
        Assert.Throws<ArgumentNullException>("scope", () => GenerationPinning.PinCurrent(null!, store));
        Assert.Throws<ArgumentNullException>("source", () => new PinScope().PinCurrent(null!));
        Assert.Same(generation, store.Current);
    }
}
