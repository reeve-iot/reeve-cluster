using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.Log;
using Unit.Testing;

namespace Unit.Generations;

public sealed class CallbackIsolationTests
{
    private static readonly Guid TokenA = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    private static readonly Guid TokenB = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    [ThreadStatic]
    private static bool onAbandoningThread;

    [Fact]
    public async Task Abandon_survives_a_throwing_callback_on_the_attempt_token_and_wakes_the_runner()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync(
            "a",
            attempt =>
            {
                attempt.Token.Register(() => throw new InvalidOperationException("the reconciler's cancellation callback"));
                registered.SetResult();
                return hung.Task;
            },
            Stores.Never);
        await registered.Task.WaitAsync(Bounds.Safety);

        try
        {
            var thrown = Record.Exception(() => pass.Abandon());
            Assert.True(thrown is null, $"Abandon threw {thrown?.GetType().Name}: {thrown?.Message}");

            var woke = await Record.ExceptionAsync(() => running.WaitAsync(Bounds.Safety));
            Assert.True(woke is null, "the runner was not woken by the abandon; it waits for the reconciler or its timeout");
            Assert.Equal(AttemptOutcomeKind.Abandoned, (await running).Kind);
        }
        finally
        {
            hung.TrySetResult(new ReconcileResult("late"));
        }
    }

    [Fact]
    public async Task BeginPass_returns_the_new_pass_when_a_callback_on_the_abandoned_attempt_token_throws()
    {
        var store = Stores.Create("a");
        var old = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = old.RunAttemptAsync(
            "a",
            attempt =>
            {
                attempt.Token.Register(() => throw new InvalidOperationException("the reconciler's cancellation callback"));
                registered.SetResult();
                return hung.Task;
            },
            Stores.Never);
        await registered.Task.WaitAsync(Bounds.Safety);

        try
        {
            ApplierPass? fresh = null;
            var thrown = Record.Exception(() => fresh = store.BeginPass(2, TokenB, ["a"]));

            Assert.True(thrown is null, $"BeginPass threw {thrown?.GetType().Name}: {thrown?.Message}");
            Assert.NotNull(fresh);
            Assert.True(old.IsAbandoned);
        }
        finally
        {
            hung.TrySetResult(new ReconcileResult("late"));
            await running.WaitAsync(Bounds.Safety).ContinueWith(_ => { }, TaskScheduler.Default);
        }
    }

    [Fact]
    public async Task A_timed_out_attempt_reports_TimedOut_when_a_callback_on_its_token_throws()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timeout = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync(
            "a",
            attempt =>
            {
                attempt.Token.Register(() => throw new InvalidOperationException("the reconciler's cancellation callback"));
                registered.SetResult();
                return hung.Task;
            },
            timeout.Task);
        await registered.Task.WaitAsync(Bounds.Safety);

        try
        {
            timeout.SetResult();
            AttemptOutcome? outcome = null;
            var thrown = await Record.ExceptionAsync(async () => outcome = await running.WaitAsync(Bounds.Safety));

            Assert.True(thrown is null, $"RunAttemptAsync faulted with {thrown?.GetType().Name}: {thrown?.Message}");
            Assert.Equal(AttemptOutcomeKind.TimedOut, outcome!.Kind);
            Assert.Single(store.Unreturned);
        }
        finally
        {
            hung.TrySetResult(new ReconcileResult("late"));
        }
    }

    [Fact]
    public async Task Abandon_does_not_run_the_reconciler_cancellation_callbacks_on_the_abandoning_thread()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);
        var hung = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ranOnAbandoningThread = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync(
            "a",
            attempt =>
            {
                attempt.Token.Register(() => ranOnAbandoningThread.TrySetResult(onAbandoningThread));
                registered.SetResult();
                return hung.Task;
            },
            Stores.Never);
        await registered.Task.WaitAsync(Bounds.Safety);

        try
        {
            onAbandoningThread = true;
            try
            {
                pass.Abandon();
            }
            finally
            {
                onAbandoningThread = false;
            }

            Assert.False(
                await ranOnAbandoningThread.Task.WaitAsync(Bounds.Safety),
                "the reconciler's cancellation callback ran inline on the thread that called Abandon");
        }
        finally
        {
            hung.TrySetResult(new ReconcileResult("late"));
            await running.WaitAsync(Bounds.Safety);
        }
    }

    [Fact]
    public async Task I21_a_blocked_superseded_callback_of_one_type_does_not_hold_back_another_type_supersession()
    {
        var store = Stores.Create("a", "b");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"), ("b", "b1"));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        first.Superseded("a").Register(() =>
        {
            entered.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(60));
        });
        var bSuperseded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        first.Superseded("b").Register(() => bSuperseded.TrySetResult());

        var publishing = Task.Run(() => store.PublishAsync(2, TokenB, ("a", "a2"), ("b", "b2")));
        try
        {
            await entered.Task.WaitAsync(Bounds.Safety);

            Assert.Equal(2, store.Current!.Head);
            Assert.False(first.TryActivate("b", () => { }));

            var inTime = await Record.ExceptionAsync(() => bSuperseded.Task.WaitAsync(Bounds.Safety));
            Assert.True(inTime is null, "b's state was replaced and the new generation is current, but b's Superseded token stays uncancelled while a's callback runs");
        }
        finally
        {
            release.Set();
            await publishing.WaitAsync(Bounds.Safety);
        }
    }

    [Fact]
    public async Task Publish_returns_while_a_superseded_callback_is_blocked()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        first.Superseded("a").Register(() =>
        {
            entered.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(60));
        });

        var publishing = Task.Run(() => store.PublishAsync(2, TokenB, ("a", "a2")));
        try
        {
            await entered.Task.WaitAsync(Bounds.Safety);

            var returned = await Record.ExceptionAsync(() => publishing.WaitAsync(Bounds.Safety));

            Assert.True(returned is null, "Publish has not returned: the applier is held by an application Superseded callback");
        }
        finally
        {
            release.Set();
            await publishing.WaitAsync(Bounds.Safety);
        }
    }

    [Fact]
    public async Task A_timeout_the_clock_refuses_leaves_the_pass_usable()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a0")), TimeSpan.FromDays(60));
        });

        var retry = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a1")), Stores.Never);
        Assert.Equal(AttemptOutcomeKind.Accepted, retry.Kind);
        Assert.Equal(1, retry.Attempt.Number);
    }

    [Fact]
    public void An_attempt_whose_reconciler_never_started_is_not_listed_as_unreturned()
    {
        var store = Stores.Create("a");
        var pass = store.BeginPass(1, TokenA, ["a"]);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a0")), TimeSpan.FromDays(60));
        });
        pass.Abandon();

        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task A_reconciler_does_not_start_on_a_pass_abandoned_as_its_attempt_is_set_up()
    {
        var time = new AbandoningTimeProvider();
        var store = new GenerationStore(["a"], NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);
        var pass = store.BeginPass(1, TokenA, ["a"]);
        time.Pass = pass;
        var ran = false;

        var outcome = await pass.RunAttemptAsync(
            "a",
            _ =>
            {
                ran = true;
                return Task.FromResult(new ReconcileResult("a1"));
            },
            TimeSpan.FromSeconds(30)).WaitAsync(Bounds.Safety);
        await outcome.Attempt.Returned.WaitAsync(Bounds.Safety);

        Assert.Equal(AttemptOutcomeKind.Abandoned, outcome.Kind);
        Assert.False(ran, "the reconciler ran on a pass that was abandoned before it started");
    }

    [Fact]
    public async Task I21_a_publication_waits_for_an_activation_in_progress()
    {
        var store = Stores.Create("a");
        var first = await store.PublishAsync(1, TokenA, ("a", "a1"));
        var pass = store.BeginPass(2, TokenB, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a2"));

        Task<IGeneration?>? publishing = null;
        IGeneration? currentAtEnd = null;
        var publishedDuring = true;
        var activated = first.TryActivate("a", () =>
        {
            publishing = Task.Run(() => store.Publish(pass));
            publishedDuring = publishing.Wait(TimeSpan.FromMilliseconds(500));
            currentAtEnd = store.Current;
        });
        var second = await publishing!.WaitAsync(Bounds.Safety);

        Assert.True(activated);
        Assert.False(publishedDuring, "a publication completed while an activation of the state it replaces was running");
        Assert.Same(first, currentAtEnd);
        Assert.NotNull(second);
        Assert.False(first.TryActivate("a", () => { }));
    }

    private sealed class AbandoningTimeProvider : TimeProvider
    {
        public ApplierPass? Pass { get; set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Pass?.Abandon();
            return base.CreateTimer(callback, state, dueTime, period);
        }
    }
}
