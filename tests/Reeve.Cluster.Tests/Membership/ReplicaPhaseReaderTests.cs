using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Membership;

public sealed class ReplicaPhaseReaderTests : IDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly LocalPhaseGate inner;
    private readonly CountingGate gate;
    private readonly ReplicaPhaseReader phase;

    public ReplicaPhaseReaderTests()
    {
        inner = new LocalPhaseGate(Timing, time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        gate = new CountingGate(inner);
        phase = new ReplicaPhaseReader(gate);
    }

    public void Dispose() => inner.Dispose();

    private void Join() => Assert.True(inner.Joined(1, 1, inner.Now));

    [Fact]
    public async Task A_stale_seen_completes_at_once_with_the_phase_now_and_unsubscribes()
    {
        Join();

        var change = phase.WaitForChangeAsync(ReplicaPhase.Starting);

        Assert.True(change.IsCompletedSuccessfully);
        Assert.Equal(ReplicaPhase.Loading, await change);
        Assert.Equal(ReplicaPhase.Loading, phase.Current);
        Assert.Equal(0, gate.Subscribers);
    }

    [Fact]
    public void An_already_cancelled_token_answers_a_cancelled_task_without_subscribing()
    {
        Join();

        var change = phase.WaitForChangeAsync(ReplicaPhase.Starting, new CancellationToken(canceled: true));

        Assert.True(change.IsCanceled);
        Assert.Equal(0, gate.Subscribed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task The_wait_completes_on_another_thread_than_the_transition_while_the_transition_is_still_running(bool liveToken)
    {
        using var cancel = new CancellationTokenSource();
        var change = phase.WaitForChangeAsync(ReplicaPhase.Starting, liveToken ? cancel.Token : CancellationToken.None);
        Assert.False(change.IsCompleted);
        var continued = change.ContinueWith(
            t => (Thread: Environment.CurrentManagedThreadId, Phase: t.Result),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        var transitionThread = 0;
        using var held = new ManualResetEventSlim();
        using var hold = inner.Subscribe(_ =>
        {
            transitionThread = Environment.CurrentManagedThreadId;
            held.Wait(Wait);
        });
        var transition = Task.Factory.StartNew(Join, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        var (thread, seen) = await continued.WaitAsync(Wait);
        Assert.False(transition.IsCompleted);
        held.Set();
        await transition.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Loading, seen);
        Assert.NotEqual(transitionThread, thread);
        await gate.AllUnsubscribed.WaitAsync(Wait);
        Assert.Equal(0, gate.Subscribers);
    }

    [Fact]
    public async Task The_wait_is_cancelled_by_its_token_off_the_cancelling_thread_and_unsubscribes()
    {
        using var cancel = new CancellationTokenSource();
        var change = phase.WaitForChangeAsync(ReplicaPhase.Starting, cancel.Token);
        Assert.False(change.IsCompleted);
        Assert.Equal(1, gate.Subscribers);
        var continued = change.ContinueWith(
            t => (Thread: Environment.CurrentManagedThreadId, t.IsCanceled),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        var cancelling = new Thread(cancel.Cancel);
        cancelling.Start();
        var (thread, cancelled) = await continued.WaitAsync(Wait);
        Assert.True(cancelling.Join(Wait));

        Assert.True(cancelled);
        Assert.NotEqual(cancelling.ManagedThreadId, thread);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => change);
        Assert.Equal(ReplicaPhase.Starting, phase.Current);
        await gate.AllUnsubscribed.WaitAsync(Wait);
        Assert.Equal(0, gate.Subscribers);
    }

    private sealed class CountingGate(LocalPhaseGate gate) : IPhaseGate
    {
        private readonly TaskCompletionSource allUnsubscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int subscribers;
        private int subscribed;

        public int Subscribers => Volatile.Read(ref subscribers);

        public int Subscribed => Volatile.Read(ref subscribed);

        public Task AllUnsubscribed => allUnsubscribed.Task;

        public ReplicaPhase Phase => gate.Phase;

        public long? FenceStartTimestamp => gate.FenceStartTimestamp;

        public bool ExitAfterFence => gate.ExitAfterFence;

        public IReadinessReasons Readiness => gate.Readiness;

        public IDisposable Subscribe(Action<PhaseEvent> handler)
        {
            Interlocked.Increment(ref subscribed);
            Interlocked.Increment(ref subscribers);
            return new Counted(gate.Subscribe(handler), this);
        }

        public ServingCheckResult CheckServing() => gate.CheckServing();

        public FenceDecision RequestFence(FenceRequest request) => gate.RequestFence(request);

        public FenceDecision ReportFatalFault(string reason) => gate.ReportFatalFault(reason);

        public IDisposable OnStepZero(Action<FenceRequest> hook) => gate.OnStepZero(hook);

        public IDisposable OnFenceRequested(Action<FenceRequest> handler) => gate.OnFenceRequested(handler);

        public IDisposable OnExitRequested(Action<FenceRequest> handler) => gate.OnExitRequested(handler);

        private sealed class Counted(IDisposable subscription, CountingGate owner) : IDisposable
        {
            private int disposed;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) != 0)
                {
                    return;
                }

                subscription.Dispose();
                if (Interlocked.Decrement(ref owner.subscribers) == 0)
                {
                    owner.allUnsubscribed.TrySetResult();
                }
            }
        }
    }
}
