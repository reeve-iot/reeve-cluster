using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Generations;
using RecordingModelEvents = Unit.Membership.RecordingModelEvents;

namespace Unit.Log;

public sealed class ApplierStallTests : IDisposable
{
    private const long Epoch = 7;
    private static readonly TimeSpan Rt = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Allowance = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly LocalPhaseGate gate;
    private readonly GenerationStore store;
    private readonly List<IDisposable> disposables = [];

    public ApplierStallTests()
    {
        var options = Options.Create(new ClusterOptions { ReplicaId = "r1", ConnectionString = "Host=unused", ReconcileTimeout = Rt });
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        store = new GenerationStore(["a", "b"], NoFaultPoints.Instance, events, time, NullLogger.Instance);
        disposables.Add(gate);
    }

    public void Dispose()
    {
        foreach (var disposable in disposables)
        {
            disposable.Dispose();
        }
    }

    [Fact]
    public void FenceStall_a_stall_in_Serving_fences_at_the_stalled_pass_start_plus_the_allowance_and_not_before()
    {
        var clock = StallClock();
        ToServing();
        var passStarted = time.GetTimestamp();
        time.Advance(Rt);

        clock.Stalled(Epoch, passStarted);
        time.Advance(Allowance - Rt - Tick);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        time.Advance(Tick);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.ApplierStall, gate.CurrentFence!.Request.Trigger);
        Assert.False(gate.ExitAfterFence);
        Assert.Single(events.Of(MembershipActions.FenceStall));
    }

    [Theory]
    [InlineData(ReplicaPhase.Loading)]
    [InlineData(ReplicaPhase.CatchingUp)]
    public void A_stall_in_Loading_or_CatchingUp_keeps_retrying_and_fences_nothing(ReplicaPhase phase)
    {
        var clock = StallClock();
        To(phase);

        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(TimeSpan.FromSeconds(10));
        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(phase, gate.Phase);
        Assert.Null(gate.CurrentFence);
        Assert.Empty(events.Of(MembershipActions.FenceStall));
        Assert.True(clock.IsStalled);
    }

    [Fact]
    public void Recovered_disarms_the_stall_clock_and_the_next_stall_counts_from_its_own_pass_start()
    {
        var clock = StallClock();
        ToServing();
        Assert.Null(clock.Left);

        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(Allowance - Tick);
        Assert.Equal(Tick, clock.Left);
        clock.Recovered();
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.False(clock.IsStalled);
        Assert.Null(clock.Left);

        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(Allowance);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public void A_retry_that_stalls_again_keeps_the_first_stalls_clock()
    {
        var clock = StallClock();
        ToServing();
        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(TimeSpan.FromSeconds(2));

        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(Allowance - TimeSpan.FromSeconds(2));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public void A_replica_that_activates_while_still_stalled_past_the_limit_fences_at_its_next_stall()
    {
        var clock = StallClock();
        To(ReplicaPhase.CatchingUp);
        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(Allowance + TimeSpan.FromSeconds(1));
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);

        gate.Apply(MemberTrigger.ActivationCommitted);
        clock.Stalled(Epoch, time.GetTimestamp());

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.ApplierStall, gate.CurrentFence!.Request.Trigger);
    }

    [Fact]
    public void A_stall_of_an_ended_epoch_fences_nothing()
    {
        var clock = StallClock();
        ToServing();

        clock.Stalled(Epoch - 1, time.GetTimestamp());
        time.Advance(Allowance);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Fact]
    public async Task A_publish_while_the_decision_is_held_at_its_fault_point_fences_nothing()
    {
        var held = new HeldPoint(LogFaultPoints.StallLimitDecision);
        var clock = StallClock(held);
        ToServing();
        held.Arm();

        clock.Stalled(Epoch, time.GetTimestamp());
        time.Advance(Allowance);
        await held.Reached.WaitAsync(Wait);
        clock.Recovered();
        held.Release();
        await clock.Deciding.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Fact]
    public async Task FenceStuck_an_attempt_that_times_out_in_Serving_and_never_returns_ends_the_process_at_its_start_plus_the_allowance()
    {
        StuckWatch();
        ToServing();
        var pass = store.BeginPass(1, null, ["a", "b"]);

        var outcome = await TimeOutAsync(pass, "a");
        Assert.Equal(AttemptOutcomeKind.TimedOut, outcome.Kind);
        time.Advance(Allowance - Rt - Tick);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        time.Advance(Tick);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.StuckAttempt, gate.CurrentFence!.Request.Trigger);
        Assert.True(gate.ExitAfterFence);
        Assert.Single(events.Of(MembershipActions.FenceStuck));
        Assert.Empty(events.Of(MembershipActions.FenceStall));
    }

    [Fact]
    public async Task FenceStuck_an_attempt_abandoned_before_its_timeout_in_CatchingUp_fences_and_exits_at_its_start_plus_the_allowance()
    {
        StuckWatch();
        To(ReplicaPhase.CatchingUp);
        var pass = store.BeginPass(1, null, ["a", "b"]);
        var running = pass.RunAttemptAsync("a", _ => Stores.Never.ContinueWith(_ => new ReconcileResult("x"), TaskScheduler.Default), Rt);
        time.Advance(TimeSpan.FromMilliseconds(100));

        pass.Abandon();
        Assert.Equal(AttemptOutcomeKind.Abandoned, (await running.WaitAsync(Wait)).Kind);
        Assert.Equal(AttemptOutcomeKind.Abandoned, Assert.Single(store.Unreturned).Why);
        time.Advance(Allowance - TimeSpan.FromMilliseconds(100) - Tick);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);

        time.Advance(Tick);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(gate.ExitAfterFence);
        Assert.Single(events.Of(MembershipActions.FenceStuck));
    }

    [Fact]
    public async Task An_attempt_that_returns_before_its_allowance_is_not_stuck()
    {
        StuckWatch();
        ToServing();
        var pass = store.BeginPass(1, null, ["a", "b"]);
        var late = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => late.Task, Rt);
        time.Advance(Rt);
        Assert.Equal(AttemptOutcomeKind.TimedOut, (await running.WaitAsync(Wait)).Kind);
        time.Advance(Allowance - Rt - Tick);

        late.SetResult(new ReconcileResult("late"));
        await (await running).Attempt.Returned.WaitAsync(Wait);
        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Empty(store.Unreturned);
    }

    [Fact]
    public async Task A_later_attempt_is_stuck_at_its_own_allowance_after_an_earlier_one_returned()
    {
        StuckWatch();
        ToServing();
        var first = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass1 = store.BeginPass(1, null, ["a", "b"]);
        var running = pass1.RunAttemptAsync("a", _ => first.Task, Rt);
        time.Advance(Rt);
        await running.WaitAsync(Wait);

        var pass2 = store.BeginPass(1, null, ["a", "b"]);
        await TimeOutAsync(pass2, "a");
        first.SetResult(new ReconcileResult("late"));
        await (await running).Attempt.Returned.WaitAsync(Wait);

        time.Advance(Allowance - Rt - Rt);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        time.Advance(Rt);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(gate.ExitAfterFence);
    }

    [Fact]
    public async Task A_stuck_attempt_during_a_fence_marks_it_to_exit_at_step_6()
    {
        StuckWatch();
        ToServing();
        var pass = store.BeginPass(1, null, ["a", "b"]);
        await TimeOutAsync(pass, "a");
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "a test"));
        Assert.False(gate.ExitAfterFence);

        time.Advance(Allowance - Rt);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(gate.ExitAfterFence);
    }

    [Fact]
    public async Task StuckExit_an_attempt_outstanding_in_Fenced_ends_the_process_at_once_instead_of_a_rejoin()
    {
        StuckWatch();
        ToServing();
        var pass = store.BeginPass(1, null, ["a", "b"]);
        await TimeOutAsync(pass, "a");
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "a test"));
        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());

        time.Advance(Allowance - Rt);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Single(events.Of(MembershipActions.StuckExit));
    }

    [Fact]
    public async Task An_attempt_outstanding_in_Starting_ends_the_process_at_once()
    {
        StuckWatch();
        ToServing();
        var pass = store.BeginPass(1, null, ["a", "b"]);
        await TimeOutAsync(pass, "a");
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "a test"));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);

        time.Advance(Allowance - Rt);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
    }

    [Fact]
    public async Task The_decision_is_held_at_its_fault_point_and_an_attempt_that_returns_meanwhile_is_not_stuck()
    {
        var held = new HeldPoint(LogFaultPoints.StuckAttemptDecision);
        var watch = StuckWatch(held);
        ToServing();
        var late = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pass = store.BeginPass(1, null, ["a", "b"]);
        var running = pass.RunAttemptAsync("a", _ => late.Task, Rt);
        time.Advance(Rt);
        await running.WaitAsync(Wait);
        held.Arm();

        time.Advance(Allowance - Rt);
        await held.Reached.WaitAsync(Wait);
        late.SetResult(new ReconcileResult("late"));
        await (await running).Attempt.Returned.WaitAsync(Wait);
        held.Release();
        await watch.Deciding.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private ApplierStallClock StallClock(IFaultPoints? points = null)
    {
        var clock = new ApplierStallClock(gate, Allowance, time, points ?? NoFaultPoints.Instance, NullLogger.Instance);
        disposables.Add(clock);
        return clock;
    }

    private StuckAttemptWatch StuckWatch(IFaultPoints? points = null)
    {
        var watch = new StuckAttemptWatch(store, gate, Allowance, TimeSpan.FromSeconds(1), points ?? NoFaultPoints.Instance, NullLogger.Instance);
        disposables.Add(watch);
        return watch;
    }

    private async Task<AttemptOutcome> TimeOutAsync(ApplierPass pass, string type)
    {
        var never = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync(type, _ => never.Task, Rt);
        time.Advance(Rt);
        return await running.WaitAsync(Wait);
    }

    private void ToServing()
    {
        To(ReplicaPhase.Activated);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private void To(ReplicaPhase phase)
    {
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        if (phase == ReplicaPhase.Loading)
        {
            return;
        }

        gate.Apply(MemberTrigger.StateLoaded);
        if (phase == ReplicaPhase.Activated)
        {
            gate.Apply(MemberTrigger.ActivationCommitted);
        }

        Assert.Equal(phase, gate.Phase);
    }
}
