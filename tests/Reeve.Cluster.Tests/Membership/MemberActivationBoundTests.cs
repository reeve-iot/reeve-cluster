using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;
using Unit.Testing;

namespace Unit.Membership;

public sealed class MemberActivationBoundTests : IDisposable
{
    private const long Epoch = 7;
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly SignallingTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly BoundStore store = new();
    private readonly BoundLoad load = new();
    private readonly InProcessWakeupChannel channel = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly LocalPhaseGate gate;
    private readonly MemberActivation activation;
    private readonly List<FenceRequest> fences = [];
    private readonly ErrorLog log = new();
    private readonly IDisposable fenceSubscription;

    public MemberActivationBoundTests()
    {
        var wrapped = Options.Create(options);
        gate = new LocalPhaseGate(wrapped, time, events, NullLogger<LocalPhaseGate>.Instance);
        var identity = new MemberIdentity(Guid.NewGuid(), "1.4.0", MemberIdentity.CurrentProtocol, []);
        activation = new MemberActivation(gate, store, load, identity, channel, wrapped, time, NoFaultPoints.Instance, log, Reeve.Cluster.Floors.NoUpgradePathGuard.Instance);
        fenceSubscription = gate.OnFenceRequested(fences.Add);
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Apply(MemberTrigger.StateLoaded).To);
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
    }

    public void Dispose()
    {
        fenceSubscription.Dispose();
        activation.Dispose();
        gate.Dispose();
    }

    [Theory]
    [InlineData("connection-failed")]
    [InlineData("statement-error")]
    [InlineData("cancelled")]
    public async Task F2_an_activation_that_fails_before_its_commit_is_sent_raises_no_fence_and_is_retried_after_RenewInterval(string how)
    {
        store.ThrowsBeforeCommit = how switch
        {
            "connection-failed" => new IOException("connection reset"),
            "statement-error" => new InvalidOperationException("a statement after the lock failed"),
            _ => new OperationCanceledException("the lock wait was cancelled"),
        };
        var never = new TaskCompletionSource().Task;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        await store.ActivationCalled.WaitAsync(Wait);
        await channel.PublishAsync(new Wakeup(WakeupKind.Applied, 11));
        time.Advance(options.RenewInterval - Tick);
        await Task.Delay(50);

        Assert.False(round.IsCompleted, "a round whose activation failed before its commit is retried after RenewInterval, not sooner");
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(fences);
        Assert.Empty(events.Of(MembershipActions.FenceAny));
        Assert.False(store.MayCommitAsked, "the store failed before it asked whether it may commit");

        time.Advance(Tick);
        await round.WaitAsync(Wait);

        store.ThrowsBeforeCommit = null;
        await activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Equal(2, store.Activations);
        Assert.Empty(fences);
    }

    [Fact]
    public async Task R2_a_failure_before_the_commit_logs_an_error_and_raises_activation_failing_until_an_answer_clears_it()
    {
        var failure = new InvalidOperationException("reeve_cluster.cluster has no row");
        store.ThrowsBeforeCommit = failure;

        await FailedRoundAsync();
        await FailedRoundAsync();

        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(fences);
        Assert.Equal(2, log.Errors.Count);
        Assert.All(log.Errors, e => Assert.Same(failure, e.Exception));
        Assert.Contains($"epoch {Epoch}", log.Errors[0].Message, StringComparison.Ordinal);
        var reason = Assert.Single(gate.Readiness.Current);
        Assert.Equal(ActivationRules.FailingReason, reason.Code);
        Assert.Equal("activation.failing", reason.Code);
        Assert.Contains("reeve_cluster.cluster has no row", reason.Description, StringComparison.Ordinal);
        Assert.Contains(nameof(InvalidOperationException), reason.Description, StringComparison.Ordinal);

        store.ThrowsBeforeCommit = null;
        await activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Empty(gate.Readiness.Current);
        Assert.Equal(2, log.Errors.Count);
    }

    [Fact]
    public async Task R2_a_change_of_phase_clears_activation_failing()
    {
        store.ThrowsBeforeCommit = new IOException("connection reset");
        await FailedRoundAsync();
        Assert.Contains(gate.Readiness.Current, r => r.Code == ActivationRules.FailingReason);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "fence start"));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.DoesNotContain(gate.Readiness.Current, r => r.Code == ActivationRules.FailingReason);
    }

    [Fact]
    public async Task R2_a_failure_before_the_commit_that_lands_after_the_phase_changed_leaves_no_reason()
    {
        store.ThrowsBeforeCommit = new IOException("connection reset");
        store.During = () => gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "fence start"));

        await FailedRoundAsync();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Empty(gate.Readiness.Current);
    }

    [Fact]
    public async Task R2_a_graceful_stop_before_the_commit_logs_no_error_and_raises_no_reason()
    {
        using var stop = new CancellationTokenSource();
        store.Hang = true;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, stop.Token);
        await store.ActivationCalled.WaitAsync(Wait);
        await stop.CancelAsync();
        await Record.ExceptionAsync(() => round.WaitAsync(Wait));

        Assert.Empty(log.Errors);
        Assert.Empty(gate.Readiness.Current);
    }

    [Fact]
    public async Task F2_an_activation_whose_commit_was_sent_and_did_not_answer_still_fences()
    {
        store.ThrowsAfterMayCommit = new IOException("connection reset during the commit");

        await activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None).WaitAsync(Wait);

        Assert.True(store.MayCommitAsked);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.UnansweredActivation, Assert.Single(fences).Trigger);
    }

    [Fact]
    public async Task F2_a_graceful_stop_during_a_statement_before_the_commit_propagates_the_cancel_and_raises_no_fence()
    {
        using var stop = new CancellationTokenSource();
        store.Hang = true;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, stop.Token);
        await store.ActivationCalled.WaitAsync(Wait);
        await stop.CancelAsync();

        var error = await Record.ExceptionAsync(() => round.WaitAsync(Wait));
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(fences);
    }

    [Fact]
    public async Task F6_the_caught_up_checks_read_is_cancelled_at_RenewTimeout_on_the_injected_clock()
    {
        store.HangBacklog = true;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None);
        var token = await store.BacklogToken.Task.WaitAsync(Wait);
        time.Advance(options.RenewTimeout - Tick);
        Assert.False(token.IsCancellationRequested, "cancelled before RenewTimeout");

        time.Advance(Tick);

        Assert.True(token.IsCancellationRequested, "the bound runs on the injected TimeProvider, not the system clock");
        await round.WaitAsync(Wait);
        Assert.Equal(0, store.Activations);
        Assert.Empty(fences);
    }

    [Fact]
    public async Task F6_the_activation_is_cancelled_at_RenewTimeout_on_the_injected_clock_and_before_its_commit_it_fences_nothing()
    {
        store.Hang = true;
        var never = new TaskCompletionSource().Task;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        var token = await store.ActivationToken.Task.WaitAsync(Wait);
        var retry = time.DelayArmed(options.RenewInterval);
        time.Advance(options.RenewTimeout - Tick);
        Assert.False(token.IsCancellationRequested, "cancelled before RenewTimeout");

        time.Advance(Tick);

        Assert.True(token.IsCancellationRequested, "the bound runs on the injected TimeProvider, not the system clock");
        await store.ActivationEnded.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(fences);

        await retry.WaitAsync(Wait);
        time.Advance(options.RenewInterval);
        await round.WaitAsync(Wait);
        Assert.Empty(fences);
    }

    [Fact]
    public async Task F6_the_serving_since_write_is_cancelled_at_RenewTimeout_on_the_injected_clock()
    {
        await activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        load.Cursor = new Cursor(12, Guid.NewGuid());
        store.HangServing = true;

        var round = activation.RunAsync(ReplicaPhase.Activated, Epoch, Task.CompletedTask, CancellationToken.None);
        var token = await store.ServingToken.Task.WaitAsync(Wait);
        time.Advance(options.RenewTimeout - Tick);
        Assert.False(token.IsCancellationRequested, "cancelled before RenewTimeout");

        time.Advance(Tick);

        Assert.True(token.IsCancellationRequested, "the bound runs on the injected TimeProvider, not the system clock");
        await round.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    private async Task FailedRoundAsync()
    {
        var calls = store.Activations;
        await activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, Task.CompletedTask, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(calls + 1, store.Activations);
    }

    private sealed class ErrorLog : ILogger<MemberActivation>
    {
        private readonly List<(string Message, Exception? Exception)> errors = [];

        public IReadOnlyList<(string Message, Exception? Exception)> Errors
        {
            get
            {
                lock (errors)
                {
                    return [.. errors];
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
            {
                lock (errors)
                {
                    errors.Add((formatter(state, exception), exception));
                }
            }
        }
    }

    private sealed class BoundLoad : IMemberLoad
    {
        public Cursor Cursor { get; set; } = Cursor.Empty;

        public PublishedPass? LastPublished { get; set; }

        public long LastPassEntries => LastPublished?.Entries ?? 0;

        public Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken) => Task.FromResult(LoadOutcome.Loaded);

        public void StartApplying(long epoch)
        {
        }

        public void StopForGood()
        {
        }
    }

    private sealed class BoundStore : IActivationStore
    {
        private readonly SemaphoreSlim activationCalled = new(0);
        private readonly SemaphoreSlim activationEnded = new(0);

        public Backlog Backlog { get; set; }

        public Exception? ThrowsBeforeCommit { get; set; }

        public Exception? ThrowsAfterMayCommit { get; set; }

        public bool Hang { get; set; }

        public Action? During { get; set; }

        public bool HangBacklog { get; set; }

        public bool HangServing { get; set; }

        public bool MayCommitAsked { get; private set; }

        public int Activations { get; private set; }

        public TaskCompletionSource<CancellationToken> BacklogToken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<CancellationToken> ActivationToken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<CancellationToken> ServingToken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ActivationCalled => activationCalled.WaitAsync();

        public Task ActivationEnded => activationEnded.WaitAsync();

        public async Task<Backlog> ReadBacklogAsync(Cursor cursor, long threshold, CancellationToken cancellationToken)
        {
            if (HangBacklog)
            {
                BacklogToken.TrySetResult(cancellationToken);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            return Backlog;
        }

        public async Task<ActivationAnswer> ActivateAsync(ActivationRequest request, Func<bool> mayCommit, CancellationToken cancellationToken)
        {
            Activations++;
            activationCalled.Release();
            try
            {
                if (Hang)
                {
                    ActivationToken.TrySetResult(cancellationToken);
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }

                During?.Invoke();
                if (ThrowsBeforeCommit is { } before)
                {
                    throw before;
                }

                MayCommitAsked = true;
                if (!mayCommit())
                {
                    return new ActivationAnswer(ActivationOutcome.LeftCatchingUp, 12, 0, null, FloorsRead: true);
                }

                if (ThrowsAfterMayCommit is { } after)
                {
                    throw after;
                }

                return new ActivationAnswer(ActivationOutcome.Committed, 12, 0, null, FloorsRead: true);
            }
            finally
            {
                activationEnded.Release();
            }
        }

        public async Task<bool> SetServingSinceAsync(string replicaId, long epoch, Cursor applied, CancellationToken cancellationToken)
        {
            if (HangServing)
            {
                ServingToken.TrySetResult(cancellationToken);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            return true;
        }
    }
}
