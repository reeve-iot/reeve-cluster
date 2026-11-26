using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Time;

namespace Harness.SelfTests;

public sealed class ReconcilerScriptTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    private static ReconcileContext Context(string aggregate = "widget", long head = 5, int attempt = 1) =>
        new(aggregate, head, 1, attempt, false, null, null, new StubTransaction(), _ => throw new NotSupportedException());

    private sealed class StubTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.RepeatableRead;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }

    private sealed class WidgetReconciler(ReconcilerScript script) : FaultyReconciler("widget", script);

    [Fact]
    public async Task With_nothing_scripted_an_attempt_succeeds_and_is_recorded()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        var reconciler = new WidgetReconciler(script);

        var result = await reconciler.ReconcileAsync(Context(), CancellationToken.None);

        Assert.Equal(new ReconciledState("widget", 5), result.State);
        var call = Assert.Single(script.Calls);
        Assert.Equal("returned", call.Outcome);
        Assert.Equal(5, call.Head);
    }

    [Fact]
    public async Task A_scripted_throw_is_used_up_in_order_and_recorded()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        script.ThrowNext("widget", () => new InvalidOperationException("boom"), times: 2);
        var reconciler = new WidgetReconciler(script);

        await Assert.ThrowsAsync<InvalidOperationException>(() => reconciler.ReconcileAsync(Context(), CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reconciler.ReconcileAsync(Context(attempt: 2), CancellationToken.None));
        await reconciler.ReconcileAsync(Context(attempt: 3), CancellationToken.None);

        Assert.Equal(["threw", "threw", "returned"], script.Calls.Select(c => c.Outcome));
    }

    [Fact]
    public async Task A_cooperative_hang_ends_when_its_token_is_cancelled()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        using var hang = script.HangNext("widget");
        var reconciler = new WidgetReconciler(script);
        using var cts = new CancellationTokenSource();
        var attempt = reconciler.ReconcileAsync(Context(), cts.Token);
        await hang.WaitEnteredAsync();

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => attempt).WaitAsync(Limit);
        Assert.Equal("cancelled", Assert.Single(script.Calls).Outcome);
    }

    [Fact]
    public async Task A_stuck_hang_ignores_its_token_until_it_is_released()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        using var hang = script.HangNext("widget", HangMode.Stuck);
        var reconciler = new WidgetReconciler(script);
        using var cts = new CancellationTokenSource();
        var attempt = reconciler.ReconcileAsync(Context(), cts.Token);
        await hang.WaitEnteredAsync();

        cts.Cancel();
        await Task.Delay(200);

        Assert.False(attempt.IsCompleted);

        hang.Release();
        var result = await attempt.WaitAsync(Limit);
        Assert.IsType<ReconciledState>(result.State);
    }

    [Fact]
    public async Task A_hang_holds_only_the_attempts_it_was_given_and_the_next_one_passes()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        using var hang = script.HangNext("widget");
        var reconciler = new WidgetReconciler(script);
        var first = reconciler.ReconcileAsync(Context(), CancellationToken.None);
        await hang.WaitEnteredAsync();

        await reconciler.ReconcileAsync(Context(attempt: 2), CancellationToken.None).WaitAsync(Limit);

        Assert.False(first.IsCompleted);
        hang.Release();
        await first.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_scripted_delay_takes_the_replicas_own_time()
    {
        var real = new FakeTimeProvider();
        var clock = new DriftingTimeProvider(real);
        clock.SetRate(10);
        var script = new ReconcilerScript(clock);
        script.DelayNext("widget", TimeSpan.FromSeconds(3));
        var reconciler = new WidgetReconciler(script);

        var attempt = reconciler.ReconcileAsync(Context(), CancellationToken.None);

        real.Advance(TimeSpan.FromMilliseconds(299));
        Assert.False(attempt.IsCompleted);
        real.Advance(TimeSpan.FromMilliseconds(1));
        await attempt.WaitAsync(Limit);
    }

    [Fact]
    public async Task A_standing_result_is_what_every_unscripted_attempt_returns()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        script.Returns("widget", context => $"state@{context.Head}");
        var reconciler = new WidgetReconciler(script);

        var result = await reconciler.ReconcileAsync(Context(head: 9), CancellationToken.None);

        Assert.Equal("state@9", result.State);
    }

    [Fact]
    public async Task Behaviours_of_one_aggregate_type_do_not_touch_another()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        script.ThrowNext("gadget", () => new InvalidOperationException("boom"));
        var reconciler = new WidgetReconciler(script);

        await reconciler.ReconcileAsync(Context("widget"), CancellationToken.None);

        Assert.Equal("widget", reconciler.AggregateType);
        Assert.Equal("returned", Assert.Single(script.Calls).Outcome);
    }

    [Fact]
    public async Task A_blocked_thread_ignores_its_token_and_holds_the_thread_that_called_it_until_released()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        using var hang = script.HangNext("widget", HangMode.BlockThread);
        var reconciler = new WidgetReconciler(script);
        using var cts = new CancellationTokenSource();
        var callingThread = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        var caller = Task.Factory.StartNew(
            () =>
            {
                callingThread.SetResult(Environment.CurrentManagedThreadId);
                return reconciler.ReconcileAsync(Context(), cts.Token).GetAwaiter().GetResult();
            },
            TaskCreationOptions.LongRunning);
        await hang.WaitEnteredAsync();
        cts.Cancel();
        await Task.Delay(200);

        Assert.False(caller.IsCompleted);
        Assert.True(callingThread.Task.IsCompleted);

        hang.Release();
        var result = await caller.WaitAsync(Limit);
        Assert.IsType<ReconciledState>(result.State);
    }

    [Fact]
    public async Task Clearing_the_faults_drops_what_is_scripted_and_not_yet_used_and_keeps_the_standing_result()
    {
        var script = new ReconcilerScript(TimeProvider.System);
        script.Returns("widget", context => $"state@{context.Head}");
        script.ThrowNext("widget", () => new InvalidOperationException("boom"), 3);
        script.DelayNext("widget", TimeSpan.FromHours(1));
        using var hang = script.HangNext("widget");
        var reconciler = new WidgetReconciler(script);

        script.ClearFaults();

        var result = await reconciler.ReconcileAsync(Context(head: 4), CancellationToken.None).WaitAsync(Limit);
        Assert.Equal("state@4", result.State);
        Assert.Equal(0, hang.Entered);
    }
}
