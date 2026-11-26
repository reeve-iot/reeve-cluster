using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;
using Unit.Generations;
using Unit.Status;
using Unit.Testing;

namespace Unit.Log;

public sealed class ReadIndexTests : IDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Poll = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly CountingTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);
    private readonly LocalPhaseGate gate;
    private readonly GenerationStore generations = Stores.Create("a", "b");
    private readonly FakeStore store = new();
    private readonly InProcessWakeupChannel channel = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly HeldPoint points = new(LogFaultPoints.BetweenWaitAndPin);
    private readonly ClusterScope scope = new(ClusterScopeKind.ClientRequest);
    private readonly CapturingLogger<ReadIndex> logger = new();
    private readonly ReadIndex index;

    public ReadIndexTests()
    {
        fenceTimer.Arm();
        gate = new LocalPhaseGate(Timing, time, NoModelEvents.Instance, fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        var options = new ClusterOptions { ReadIndexTimeout = Timeout, ApplyPollInterval = Poll };
        index = new ReadIndex(gate, generations, store, channel, Options.Create(options), time, points, events, logger);
    }

    public void Dispose()
    {
        index.Dispose();
        gate.Dispose();
    }

    private static Guid Token(long seq) => new((int)seq, 0, 0, [0, 0, 0, 0, 0, 0, 0, 0]);

    private void ToServing()
    {
        Assert.True(gate.Joined(1, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private Task<IGeneration> PublishAsync(long head) => generations.PublishAsync(head, Token(head), ("a", $"a{head}"), ("b", $"b{head}"));

    private Task NotifyAsync(long seq) => channel.PublishAsync(new Wakeup(WakeupKind.Applied, seq)).AsTask();

    private async Task ReadsAsync(int n)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (store.Reads < n)
        {
            await Task.Delay(1, bound.Token);
        }
    }

    private IReadOnlyList<string> Actions => [.. events.Events.Select(e => e.Action)];

    [Fact]
    public async Task A_current_generation_that_stands_for_the_head_is_pinned_after_one_read()
    {
        ToServing();
        var current = await PublishAsync(5);
        store.Head = 5;

        var result = await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(current, result.Generation);
        Assert.Same(current, scope.PinnedGeneration);
        Assert.Equal(1, store.Reads);
        Assert.Equal(new Cursor(5, Token(5)), store.Asked.Single());
        Assert.Equal(["QStart", "QPin", "QRead"], Actions);
        Assert.Equal(5L, events.Events[0].State![ObservationKeys.Head]);

        Assert.All(events.Events, e => Assert.Equal(scope.Id, e.State![ObservationKeys.Request]));
        var read = events.Events[2].State!;
        Assert.Equal(((Generation)current).Id, read[ObservationKeys.Generation]);
        Assert.Equal(5L, read[ObservationKeys.Head]);
    }

    [Fact]
    public async Task A_generation_ahead_of_the_head_passes()
    {
        ToServing();
        var current = await PublishAsync(7);
        store.Head = 5;

        var result = await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait);

        Assert.Same(current, result.Generation);
    }

    [Fact]
    public async Task An_applied_notice_ends_the_wait_once_the_new_generation_stands_for_the_head()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;
        var armed = time.TimersCreated;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);

        await time.TimersAsync(armed + 2);
        Assert.False(entering.IsCompleted);

        await PublishAsync(4);
        await NotifyAsync(4);
        await time.TimersAsync(armed + 3);
        Assert.False(entering.IsCompleted);
        Assert.Equal(1, store.Reads);

        var caughtUp = await PublishAsync(5);
        await NotifyAsync(5);
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(caughtUp, scope.PinnedGeneration);
        Assert.Equal(TimeSpan.Zero, time.GetUtcNow() - time.Start);
    }

    [Fact]
    public async Task Without_a_notice_the_poll_finds_the_new_generation()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;
        var armed = time.TimersCreated;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);
        var caughtUp = await PublishAsync(5);

        await time.TimersAsync(armed + 2);
        time.Advance(Poll);
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(caughtUp, scope.PinnedGeneration);
    }

    [Fact]
    public async Task An_invalid_cursor_at_the_head_is_behind_and_the_next_valid_generation_is_pinned()
    {
        ToServing();
        var lost = await PublishAsync(5);
        store.Head = 5;
        store.Valid = c => c.Seq != 5;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);
        Assert.False(entering.IsCompleted);

        var reloaded = await PublishAsync(6);
        await NotifyAsync(6);
        var result = await entering.WaitAsync(Wait);

        Assert.Same(reloaded, result.Generation);
        Assert.NotSame(lost, scope.PinnedGeneration);
        Assert.Equal(new Cursor(6, Token(6)), store.Asked.Last());
    }

    [Fact]
    public async Task An_invalid_generation_is_read_once_until_another_is_published()
    {
        ToServing();
        await PublishAsync(5);
        store.Head = 5;
        store.Valid = _ => false;
        var armed = time.TimersCreated;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);
        await time.TimersAsync(armed + 2);

        await NotifyAsync(5);
        await time.TimersAsync(armed + 3);
        for (var i = 1; i <= 3; i++)
        {
            time.Advance(Poll);
            await time.TimersAsync(armed + 3 + i);
        }

        Assert.Equal(1, store.Reads);
        Assert.False(entering.IsCompleted);
    }

    [Fact]
    public async Task Behind_comes_at_ReadIndexTimeout_and_not_before()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;
        var start = time.GetUtcNow();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);

        time.Advance(Timeout - TimeSpan.FromTicks(1));
        Assert.False(store.LastToken.IsCancellationRequested);
        Assert.False(entering.IsCompleted);
        time.Advance(TimeSpan.FromTicks(1));
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Behind, result.Outcome);
        Assert.Null(result.Generation);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(Timeout, time.GetUtcNow() - start);
        Assert.Equal(["QStart", "QTimeout"], Actions);
    }

    [Fact]
    public async Task A_stretch_of_behind_answers_logs_one_warning_and_the_next_pin_ends_it_with_one_information_line()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;

        var first = index.EnterAsync(new ClusterScope(ClusterScopeKind.ClientRequest), wait: true, CancellationToken.None);
        var second = index.EnterAsync(new ClusterScope(ClusterScopeKind.ClientRequest), wait: true, CancellationToken.None);
        await ReadsAsync(2);
        time.Advance(Timeout);
        Assert.Equal(ReadIndexOutcome.Behind, (await first.WaitAsync(Wait)).Outcome);
        Assert.Equal(ReadIndexOutcome.Behind, (await second.WaitAsync(Wait)).Outcome);

        var warning = Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning);
        Assert.Contains("replica_behind", warning.Message, StringComparison.Ordinal);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Debug);

        store.Head = 3;
        Assert.Equal(ReadIndexOutcome.Pinned, (await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait)).Outcome);
        var caughtUp = Assert.Single(logger.Entries, e => e.Level == LogLevel.Information);
        Assert.Contains("after 2 requests", caughtUp.Message, StringComparison.Ordinal);

        store.Head = 9;
        var again = index.EnterAsync(new ClusterScope(ClusterScopeKind.ClientRequest), wait: true, CancellationToken.None);
        await ReadsAsync(4);
        time.Advance(Timeout);
        Assert.Equal(ReadIndexOutcome.Behind, (await again.WaitAsync(Wait)).Outcome);
        Assert.Equal(2, logger.Entries.Count(e => e.Level == LogLevel.Warning));
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Information);
    }

    [Fact]
    public async Task A_head_read_that_does_not_answer_is_behind_at_ReadIndexTimeout()
    {
        ToServing();
        await PublishAsync(3);
        store.Hang = true;
        var start = time.GetUtcNow();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);

        time.Advance(Timeout - TimeSpan.FromTicks(1));
        Assert.False(store.LastToken.IsCancellationRequested);
        Assert.False(entering.IsCompleted);
        time.Advance(TimeSpan.FromTicks(1));
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Behind, result.Outcome);
        Assert.Equal(Timeout, time.GetUtcNow() - start);
        Assert.Empty(Actions);
    }

    [Fact]
    public async Task A_failed_read_is_retried_at_the_next_poll()
    {
        ToServing();
        var current = await PublishAsync(5);
        store.Head = 5;
        store.FailNext = 1;
        var armed = time.TimersCreated;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);

        await time.TimersAsync(armed + 2);
        time.Advance(Poll);
        var result = await entering.WaitAsync(Wait);

        Assert.Same(current, result.Generation);
        Assert.Equal(2, store.Reads);
    }

    [Fact]
    public async Task A_replica_that_is_not_serving_refuses_without_a_read()
    {
        await PublishAsync(5);

        var result = await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.NotServing, result.Outcome);
        Assert.Equal(ServingCheckOutcome.NotServing, result.Refusal!.Value.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(0, store.Reads);
        Assert.Empty(Actions);
    }

    [Fact]
    public async Task A_fence_start_passed_during_the_wait_refuses_at_the_next_wake_up_without_a_pin()
    {
        ToServing();
        time.Advance(TimeSpan.FromSeconds(20));
        await PublishAsync(3);
        store.Head = 5;
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await ReadsAsync(1);

        time.Advance(TimeSpan.FromSeconds(2.5));
        await PublishAsync(5);
        await NotifyAsync(5);
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.NotServing, result.Outcome);
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, result.Refusal!.Value.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.DoesNotContain("QPin", Actions);
    }

    [Fact]
    public async Task A_pause_between_the_wait_and_the_pin_that_outlasts_fence_start_refuses_the_pin()
    {
        ToServing();
        time.Advance(TimeSpan.FromSeconds(20));
        await PublishAsync(5);
        store.Head = 5;
        points.Arm();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Reached.WaitAsync(Wait);

        time.Advance(TimeSpan.FromSeconds(2.5));
        points.Release();
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.NotServing, result.Outcome);
        Assert.Equal(ServingCheckOutcome.FenceStartPassed, result.Refusal!.Value.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task A_generation_published_during_the_pause_is_pinned_only_once_its_own_cursor_is_read()
    {
        ToServing();
        await PublishAsync(5);
        store.Head = 5;
        points.Arm();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Reached.WaitAsync(Wait);

        var next = await PublishAsync(8);
        points.Release();
        var result = await entering.WaitAsync(Wait);

        Assert.Same(next, result.Generation);
        Assert.Equal(new Cursor(8, Token(8)), store.Asked.Last());
        Assert.Equal(2, store.Reads);
        var pin = events.Events.Single(e => e.Action == "QPin");
        Assert.Equal(((Generation)next).Id, pin.State!["generation"]);
    }

    [Fact]
    public async Task A_generation_published_during_the_pause_whose_cursor_is_invalid_is_not_pinned()
    {
        ToServing();
        await PublishAsync(5);
        store.Head = 5;
        points.Arm();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Reached.WaitAsync(Wait);

        await PublishAsync(8);
        store.Valid = c => c.Seq != 8;
        var armed = time.TimersCreated;
        points.Release();
        await ReadsAsync(2);

        await time.TimersAsync(armed + 1);
        Assert.False(entering.IsCompleted);

        var reloaded = await PublishAsync(9);
        await NotifyAsync(9);
        var result = await entering.WaitAsync(Wait);

        Assert.Same(reloaded, scope.PinnedGeneration);
        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
    }

    [Fact]
    public async Task With_the_wait_turned_off_the_current_generation_is_pinned_without_a_read_or_a_check()
    {
        var current = await PublishAsync(5);

        var result = await index.EnterAsync(scope, wait: false, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(current, scope.PinnedGeneration);
        Assert.Equal(0, store.Reads);
        Assert.Empty(Actions);
    }

    [Fact]
    public async Task A_cancelled_request_ends_the_wait_with_its_cancellation()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;
        using var aborted = new CancellationTokenSource();
        var entering = index.EnterAsync(scope, wait: true, aborted.Token);
        await ReadsAsync(1);

        await aborted.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => entering.WaitAsync(Wait));
        Assert.Null(scope.PinnedGeneration);
    }

    [Fact]
    public async Task Concurrent_requests_are_woken_by_one_notice()
    {
        ToServing();
        await PublishAsync(3);
        store.Head = 5;
        var scopes = Enumerable.Range(0, 8).Select(_ => new ClusterScope(ClusterScopeKind.ClientRequest)).ToArray();
        var entering = scopes.Select(s => index.EnterAsync(s, wait: true, CancellationToken.None)).ToArray();
        await ReadsAsync(scopes.Length);

        var caughtUp = await PublishAsync(5);
        await NotifyAsync(5);
        await Task.WhenAll(entering).WaitAsync(Wait);

        Assert.All(scopes, s => Assert.Same(caughtUp, s.PinnedGeneration));
    }

    private sealed class FakeStore : IReadIndexStore
    {
        private int reads;

        public long Head { get; set; }

        public Func<Cursor, bool> Valid { get; set; } = _ => true;

        public CancellationToken LastToken { get; private set; }

        public bool Hang { get; set; }

        public int FailNext { get; set; }

        public int Reads => Volatile.Read(ref reads);

        public System.Collections.Concurrent.ConcurrentQueue<Cursor> Asked { get; } = new();

        public async Task<ReadIndexRead> ReadAsync(Cursor cursor, CancellationToken cancellationToken)
        {
            Asked.Enqueue(cursor);
            LastToken = cancellationToken;
            Interlocked.Increment(ref reads);
            if (Hang)
            {
                await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            }

            if (FailNext > 0)
            {
                FailNext--;
                throw new InvalidOperationException("The connection failed.");
            }

            return new ReadIndexRead(Head, Valid(cursor));
        }
    }
}
