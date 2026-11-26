using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class FenceBoundTests
{
    private const long Epoch = 7;
    private const long Timeline = 1;
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private static Task OffContext(Func<Task> test) => Task.Run(test);

    [Fact]
    public Task Step_3_ends_at_RenewTimeout_counted_from_step_0_when_step_1_took_part_of_it() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var store = new ScriptedFenceStore { HangRelease = true };
        var close = new ProbeParticipant(FenceStep.CloseTraffic) { Hang = true };
        using var rig = new Rig(time, store, [close]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await close.Started.WaitAsync(Wait);
        time.Advance(S(1.5));
        close.Release();
        await store.ReleaseCalled.WaitAsync(Wait);

        time.Advance(S(0.5) - TimeSpan.FromTicks(1));
        await Task.Delay(50);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        Assert.Empty(rig.StepEvents(3));

        time.Advance(TimeSpan.FromTicks(1));
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        Assert.Equal(false, Assert.Single(rig.StepEvents(3)).State!["released"]);
    });

    [Fact]
    public Task Steps_1_and_2_slower_than_RenewTimeout_leave_step_3_no_time_and_steps_4_to_6_still_run() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var store = new ScriptedFenceStore { HangRelease = true };
        var close = new ProbeParticipant(FenceStep.CloseTraffic) { Hang = true };
        var applier = new ProbeParticipant(FenceStep.StopApplier);
        using var rig = new Rig(time, store, [close, applier]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await close.Started.WaitAsync(Wait);
        time.Advance(S(3));
        close.Release();
        await rig.AwaitPhase(ReplicaPhase.Fenced);

        Assert.Equal([1, 2, 3, 4, 5, 6], rig.Steps());
        Assert.True(applier.Started.IsCompleted, "step 6's participant did not run");
        Assert.Equal(false, Assert.Single(rig.StepEvents(3)).State!["released"]);
        Assert.Single(store.Records);
    });

    [Fact]
    public Task A_step_3_participant_that_ignores_its_token_is_not_awaited_past_RenewTimeout() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var store = new ScriptedFenceStore();
        var step3 = new ProbeParticipant(FenceStep.ReleaseLease) { Hang = true };
        bool? step3CancelledAtStep4 = null;
        var step4 = new ProbeParticipant(FenceStep.AbortConnections) { OnRun = () => step3CancelledAtStep4 = step3.Token.IsCancellationRequested };
        using var rig = new Rig(time, store, [step3, step4]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await step3.Started.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);

        time.Advance(S(2));
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        Assert.Equal([1, 2, 3, 4, 5, 6], rig.Steps());

        Assert.True(step3CancelledAtStep4, "the step-3 participant's token was not cancelled at RenewTimeout");
        await step3.Cancelled.WaitAsync(Wait);
        step3.Release();
    });

    [Fact]
    public Task Step_3s_statement_is_cancelled_at_RenewTimeout_whichever_timer_fires_first() => OffContext(async () =>
    {
        var clock = new SkewedClock(state => state is CancellationTokenSource);
        var store = new ScriptedFenceStore { HangRelease = true };
        using var rig = new Rig(clock, store, []);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await store.ReleaseCalled.WaitAsync(Wait);
        clock.Advance(S(2));
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        clock.Advance(S(1));

        await store.ReleaseCancelled.WaitAsync(Wait);
    });

    [Fact]
    public Task Step_3s_statement_is_cancelled_when_step_1_took_half_a_millisecond() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var store = new ScriptedFenceStore { HangRelease = true };
        var close = new ProbeParticipant(FenceStep.CloseTraffic) { Hang = true };
        using var rig = new Rig(time, store, [close]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await close.Started.WaitAsync(Wait);
        time.Advance(TimeSpan.FromTicks(5_000));
        close.Release();
        await store.ReleaseCalled.WaitAsync(Wait);
        time.Advance(S(2));
        await rig.AwaitPhase(ReplicaPhase.Fenced);

        await store.ReleaseCancelled.WaitAsync(Wait);
    });

    [Fact]
    public Task An_overrunning_step_4_participants_token_is_cancelled_at_StopBudget_whichever_timer_fires_first() => OffContext(async () =>
    {
        var clock = new SkewedClock(state => state is CancellationTokenSource);
        var abort = new ProbeParticipant(FenceStep.AbortConnections) { Hang = true };
        using var rig = new Rig(clock, new ScriptedFenceStore(), [abort]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await abort.Started.WaitAsync(Wait);
        clock.Advance(S(5));
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        clock.Advance(S(1));

        await abort.Cancelled.WaitAsync(Wait);
        abort.Release();
    });

    [Fact]
    public Task An_overrunning_step_5_participants_token_is_cancelled_at_StopBudget_whichever_timer_fires_first() => OffContext(async () =>
    {
        var clock = new SkewedClock(state => state is CancellationTokenSource);
        var units = new ProbeParticipant(FenceStep.StopUnits) { Hang = true };
        using var rig = new Rig(clock, new ScriptedFenceStore(), [units]);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await units.Started.WaitAsync(Wait);
        clock.Advance(S(5));
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        clock.Advance(S(1));

        await units.Cancelled.WaitAsync(Wait);
        units.Release();
    });

    [Fact]
    public Task I33_a_release_answered_as_its_cancel_fires_counts_as_not_released() => OffContext(async () =>
    {
        var clock = new SkewedClock(state => state is not CancellationTokenSource);
        var store = new AnswerOnCancelStore();
        using var rig = new Rig(clock, store, []);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await store.Called.WaitAsync(Wait);
        clock.Advance(S(2) + TimeSpan.FromTicks(1));
        await rig.AwaitPhase(ReplicaPhase.Fenced);

        Assert.Equal(false, Assert.Single(rig.StepEvents(3)).State!["released"]);
    });

    [Fact]
    public Task The_step_5_record_is_cancelled_at_StopBudget_and_not_made_again_from_Fenced() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var store = new ScriptedFenceStore { HangFirstRecord = true };
        using var rig = new Rig(time, store, []);
        rig.ToServing();

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        await store.RecordCalled.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        time.Advance(S(5));

        await store.RecordCancelled.WaitAsync(Wait);
        await rig.AwaitPhase(ReplicaPhase.Fenced);
        Assert.Equal(false, Assert.Single(rig.StepEvents(5)).State!["recorded"]);
        await store.NextRead();
        Assert.Empty(store.Records);
        Assert.Equal(1, store.RecordCalls);
    });

    private sealed class Rig : IDisposable
    {
        public Rig(TimeProvider time, IFenceStore store, IEnumerable<IFenceParticipant> participants)
        {
            var configured = MembershipTestOptions.Default();
            configured.RenewTimeout = TimeSpan.FromSeconds(2);
            var options = Options.Create(configured);
            Store = store;
            Gate = new LocalPhaseGate(options, time, Events, NullLogger<LocalPhaseGate>.Instance);
            Coordinator = new FenceCoordinator(
                Gate, participants, store, new FencePassConnection(), options, time, NoFaultPoints.Instance, Events, NullLogger<FenceCoordinator>.Instance);
        }

        public RecordingModelEvents Events { get; } = new();

        public IFenceStore Store { get; }

        public LocalPhaseGate Gate { get; }

        public FenceCoordinator Coordinator { get; }

        public void ToServing()
        {
            Assert.True(Gate.Joined(Epoch, Timeline, Gate.Now));
            Gate.Apply(MemberTrigger.StateLoaded);
            Gate.Apply(MemberTrigger.ActivationCommitted);
            Gate.Apply(MemberTrigger.AppliedToActivationHead);
            Assert.Equal(ReplicaPhase.Serving, Gate.Phase);
        }

        public async Task AwaitPhase(ReplicaPhase phase)
        {
            while (true)
            {
                var changed = Gate.NextPhaseChange;
                if (Gate.Phase == phase)
                {
                    return;
                }

                await changed.WaitAsync(Wait);
            }
        }

        public IReadOnlyList<ModelEvent> StepEvents(int k) =>
            [.. Events.Of(MembershipActions.FStep).Where(e => (int)e.State!["k"]! == k)];

        public IReadOnlyList<int> Steps() => [.. Events.Of(MembershipActions.FStep).Select(e => (int)e.State!["k"]!)];

        public void Dispose()
        {
            (Store as ScriptedFenceStore)?.ReleaseAll();
            Coordinator.Dispose();
            Gate.Dispose();
        }
    }

    private sealed class ProbeParticipant(FenceStep step) : IFenceParticipant
    {
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public FenceStep Step => step;

        public bool Hang { get; init; }

        public Action? OnRun { get; init; }

        public CancellationToken Token { get; private set; }

        public Task Started => started.Task;

        public Task Cancelled => cancelled.Task;

        public void Release() => release.TrySetResult();

        public async ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken)
        {
            Token = cancellationToken;
            _ = cancellationToken.Register(() => cancelled.TrySetResult());
            OnRun?.Invoke();
            started.TrySetResult();
            if (Hang)
            {
                await release.Task;
            }
        }
    }

    private sealed class AnswerOnCancelStore : IFenceStore
    {
        private readonly TaskCompletionSource called = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Called => called.Task;

        public Task<bool> ReleaseLeaseAsync(string replicaId, long epoch, CancellationToken cancellationToken)
        {
            var answer = new TaskCompletionSource<bool>();
            _ = cancellationToken.Register(() => answer.TrySetResult(true));
            called.TrySetResult();
            return answer.Task;
        }

        public Task RecordFenceAsync(FenceRecord record, TimeSpan age, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<OwnRowObservation> ReadRejoinStateAsync(string replicaId, CancellationToken cancellationToken) =>
            new TaskCompletionSource<OwnRowObservation>().Task;
    }
}

internal sealed class SkewedClock(Func<object?, bool> late) : TimeProvider
{
    private readonly FakeTimeProvider inner = new();

    public override long TimestampFrequency => inner.TimestampFrequency;

    public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

    public void Advance(TimeSpan delta) => inner.Advance(delta);

    public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

    public override long GetTimestamp() => inner.GetTimestamp();

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        if (late(state) && dueTime > TimeSpan.Zero && dueTime != Timeout.InfiniteTimeSpan)
        {
            dueTime += TimeSpan.FromTicks(1);
        }

        return inner.CreateTimer(callback, state, dueTime, period);
    }
}
