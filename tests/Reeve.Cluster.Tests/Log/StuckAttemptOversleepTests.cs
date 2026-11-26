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

public sealed class StuckAttemptOversleepTests : IDisposable
{
    private const long Epoch = 7;
    private static readonly TimeSpan Rt = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Allowance = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan Grace = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Oversleep = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly LocalPhaseGate gate;
    private readonly GenerationStore store;
    private readonly List<IDisposable> disposables = [];

    public StuckAttemptOversleepTests()
    {
        var options = Options.Create(new ClusterOptions { ReplicaId = "r1", ConnectionString = "Host=unused", ReconcileTimeout = Rt });
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        store = new GenerationStore(["a", "b"], NoFaultPoints.Instance, events, time, NullLogger.Instance);
        disposables.Add(gate);
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    public void Dispose()
    {
        foreach (var disposable in disposables)
        {
            disposable.Dispose();
        }
    }

    [Fact]
    public async Task F3_a_decision_that_runs_late_waits_the_grace_and_an_attempt_that_returns_in_it_is_not_stuck()
    {
        var (watch, late, running) = await OversleptDecisionAsync();

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        late.SetResult(new ReconcileResult("late"));
        await (await running).Attempt.Returned.WaitAsync(Wait);
        time.Advance(Grace);
        await watch.Deciding.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Empty(store.Unreturned);
        Assert.Empty(events.Of(MembershipActions.FenceStuck));
    }

    [Fact]
    public async Task F3_an_attempt_still_out_when_the_grace_ends_is_stuck_and_the_grace_is_given_once()
    {
        var (watch, _, _) = await OversleptDecisionAsync();

        time.Advance(Grace - Tick);
        await watch.Deciding.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        time.Advance(Tick);
        await watch.Deciding.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.StuckAttempt, gate.CurrentFence!.Request.Trigger);
        Assert.True(gate.ExitAfterFence);
    }

    [Fact]
    public async Task F3_an_attempt_discarded_after_its_allowance_already_ran_out_waits_the_grace_before_it_is_judged()
    {
        var watch = Watch(NoFaultPoints.Instance);
        var pass = store.BeginPass(1, null, ["a", "b"]);
        var late = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var timeout = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => late.Task, timeout.Task);
        time.Advance(Allowance + Oversleep);

        timeout.SetResult();
        Assert.Equal(AttemptOutcomeKind.TimedOut, (await running.WaitAsync(Wait)).Kind);
        await watch.Deciding.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        late.SetResult(new ReconcileResult("late"));
        await (await running).Attempt.Returned.WaitAsync(Wait);
        time.Advance(Grace);
        await watch.Deciding.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Empty(store.Unreturned);
    }

    private async Task<(StuckAttemptWatch Watch, TaskCompletionSource<ReconcileResult> Late, Task<AttemptOutcome> Running)> OversleptDecisionAsync()
    {
        var held = new HeldPoint(LogFaultPoints.StuckAttemptDecision);
        var watch = Watch(held);
        var pass = store.BeginPass(1, null, ["a", "b"]);
        var late = new TaskCompletionSource<ReconcileResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = pass.RunAttemptAsync("a", _ => late.Task, Rt);
        time.Advance(Rt);
        Assert.Equal(AttemptOutcomeKind.TimedOut, (await running.WaitAsync(Wait)).Kind);
        held.Arm();

        time.Advance(Allowance - Rt);
        await held.Reached.WaitAsync(Wait);
        time.Advance(Oversleep);
        held.Release();
        await watch.Deciding.WaitAsync(Wait);
        return (watch, late, running);
    }

    private StuckAttemptWatch Watch(IFaultPoints points)
    {
        var watch = new StuckAttemptWatch(store, gate, Allowance, Grace, points, NullLogger.Instance);
        disposables.Add(watch);
        return watch;
    }
}
