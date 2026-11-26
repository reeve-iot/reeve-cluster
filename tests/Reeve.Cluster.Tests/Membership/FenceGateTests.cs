using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Membership;

public sealed class FenceGateTests : IDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private const long Epoch = 7;
    private const long Timeline = 1;

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly HoldingPoints points = new();
    private readonly LocalPhaseGate gate;
    private readonly List<FenceRequest> fences = [];

    public FenceGateTests()
    {
        gate = new LocalPhaseGate(Timing, time, events, points, NullLogger<LocalPhaseGate>.Instance);
        gate.OnFenceRequested(fences.Add);
    }

    public void Dispose() => gate.Dispose();

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private void Join(long epoch = Epoch, long timeline = Timeline) => Assert.True(gate.Joined(epoch, timeline, gate.Now));

    private void ToServing()
    {
        Join();
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private IReadOnlyList<string> Actions => [.. events.All.Select(e => e.Action)];

    [Theory]
    [InlineData("FenceStart", "Serving", "FenceAtStart")]
    [InlineData("FatalFault", "Serving", "FenceAny")]
    [InlineData("ApplierStall", "Serving", "FenceStall")]
    [InlineData("StuckAttempt", "Serving", "FenceStuck")]
    [InlineData("StuckAttempt", "Loading", "FenceStuck")]
    [InlineData("FatalFault", "Loading", "FenceAny")]
    [InlineData("Timeline", "CatchingUp", "TimelineFence")]
    public void Step0_emits_the_action_of_the_trigger_that_started_the_fence(string triggerName, string phase, string action)
    {
        var trigger = Enum.Parse<FenceTrigger>(triggerName);
        if (phase == "Serving")
        {
            ToServing();
        }
        else
        {
            Join();
            if (phase == "CatchingUp")
            {
                gate.Apply(MemberTrigger.StateLoaded);
            }
        }

        gate.RequestFence(new FenceRequest(trigger));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        var step0 = Assert.Single(events.Of(action));
        Assert.Equal(Epoch, step0.State!["epoch"]);
    }

    [Fact]
    public void A_zero_row_renewal_and_a_renewal_on_another_timeline_carry_their_renewal_event_and_no_second_one()
    {
        Join();
        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.OtherTimeline);

        Assert.Equal([MembershipActions.Join, MembershipActions.RenewAnswer], Actions);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public void I32_TimelineFence_a_pass_that_finds_another_timeline_than_the_joins_fences_and_one_on_it_passes()
    {
        Join();
        gate.Apply(MemberTrigger.StateLoaded);

        Assert.True(gate.CheckTimeline(Epoch, Timeline));
        Assert.Empty(fences);

        Assert.False(gate.CheckTimeline(Epoch, Timeline + 1));

        Assert.Equal(FenceTrigger.Timeline, Assert.Single(fences).Trigger);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Single(events.Of(MembershipActions.TimelineFence));
        Assert.False(gate.CurrentFence!.SkipLeaseRelease);
    }

    [Theory]
    [InlineData("Loading")]
    [InlineData("CatchingUp")]
    [InlineData("Activated")]
    [InlineData("Serving")]
    [InlineData("Draining")]
    public void I32_TimelineFence_the_timeline_trigger_fences_from_every_phase_that_fences_and_step_3_runs(string phase)
    {
        Join();
        string[] path = ["Loading", "CatchingUp", "Activated", "Serving", "Draining"];
        MemberTrigger[] steps = [MemberTrigger.StateLoaded, MemberTrigger.ActivationCommitted, MemberTrigger.AppliedToActivationHead, MemberTrigger.GracefulStop];
        foreach (var step in steps.Take(Array.IndexOf(path, phase)))
        {
            gate.Apply(step);
        }

        Assert.Equal(phase, gate.Phase.ToString());

        Assert.False(gate.CheckTimeline(Epoch, Timeline + 1));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.Timeline, gate.CurrentFence!.Request.Trigger);
        Assert.False(gate.CurrentFence.SkipLeaseRelease);
        Assert.Single(events.Of(MembershipActions.TimelineFence));
    }

    [Fact]
    public void I32_a_pass_of_an_earlier_epoch_fences_nothing_and_does_not_pass()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);
        Join(Epoch + 1, Timeline + 1);

        Assert.False(gate.CheckTimeline(Epoch, Timeline));
        Assert.False(gate.CheckTimeline(Epoch, Timeline + 2));

        Assert.Single(fences);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.True(gate.CheckTimeline(Epoch + 1, Timeline + 1));
    }

    [Fact]
    public void I23_the_timeline_check_after_step_0_adds_nothing_to_the_fence_under_way()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault));

        Assert.False(gate.CheckTimeline(Epoch, Timeline + 1));

        Assert.Single(fences);
        Assert.Empty(events.Of(MembershipActions.TimelineFence));
        Assert.Equal(FenceTrigger.FatalFault, gate.CurrentFence!.Request.Trigger);
    }

    [Fact]
    public void The_fence_records_its_epoch_trigger_step_0_time_and_the_span_from_the_join()
    {
        time.Advance(S(4));
        Join();
        time.Advance(S(9));

        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "disk full"));

        var fence = gate.CurrentFence!;
        Assert.Equal(Epoch, fence.Epoch);
        Assert.Equal(FenceTrigger.FatalFault, fence.Request.Trigger);
        Assert.Equal("disk full", fence.Request.Detail);
        Assert.Equal(ReplicaPhase.Loading, fence.From);
        Assert.Equal(gate.Now, fence.Step0At);
        Assert.Equal(S(9), fence.Span);
        Assert.False(fence.SkipLeaseRelease);
    }

    [Fact]
    public void A_zero_row_renewal_marks_the_fence_to_skip_step_3()
    {
        Join();
        gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.NotMine);

        Assert.True(gate.CurrentFence!.SkipLeaseRelease);
    }

    [Fact]
    public async Task FenceAtStart_the_fence_timer_is_held_at_its_point_before_its_call_into_the_gate()
    {
        points.Hold(MembershipFaultPoints.FenceBeforeStep0.Name);
        Join();

        time.Advance(S(22));

        Assert.True(points.Reached.IsCompleted);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.Empty(fences);

        var fenced = new TaskCompletionSource<FenceRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = gate.OnFenceRequested(fenced.SetResult);
        points.Release();
        var request = await fenced.Task.WaitAsync(Bounds.Safety);

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.FenceStart, request.Trigger);
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fences).Trigger);
        Assert.Equal([MembershipActions.Join, MembershipActions.ReachFS, MembershipActions.FenceAtStart], Actions);
    }

    [Fact]
    public Task A_fence_timer_released_after_a_renewal_moved_fence_start_does_not_fence_and_fences_at_the_new_fence_start() => Task.Run(() =>
    {
        points.Hold(MembershipFaultPoints.FenceBeforeStep0.Name);
        Join();
        time.Advance(S(22));
        Assert.True(points.Reached.IsCompleted);

        gate.ApplyRenewal(new RenewalAttempt(Epoch, S(21)), RenewalOutcome.Renewed);
        var fenceStart = gate.Deadline!.Value - Timing.StopBudget;
        Assert.Equal(S(43), fenceStart - gate.Now + S(22));
        points.Release();

        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.Empty(fences);

        time.Advance(fenceStart - gate.Now - TimeSpan.FromTicks(1));
        Assert.Empty(fences);
        time.Advance(TimeSpan.FromTicks(1));
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fences).Trigger);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    });

    [Fact]
    public void FStep_the_fence_end_emits_step_6_and_enters_Fenced()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());

        var step6 = Assert.Single(events.Of(MembershipActions.FStep));
        Assert.Equal(6, step6.State!["k"]);
        Assert.Equal(Epoch, step6.State["epoch"]);
        Assert.Empty(events.Of(MembershipActions.StuckExit));
        Assert.Empty(events.Of(MembershipActions.FencedExit));
    }

    [Theory]
    [InlineData("stuck-trigger", "StuckExit")]
    [InlineData("stuck-during", "StuckExit")]
    [InlineData("fault-during", "FencedExit")]
    [InlineData("drain", "FencedExit")]
    public void StuckExit_FencedExit_a_fence_marked_to_exit_ends_with_step_6_then_the_exit_of_its_cause(string cause, string exit)
    {
        ToServing();
        switch (cause)
        {
            case "stuck-trigger":
                gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
                break;
            case "stuck-during":
                gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
                gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
                break;
            case "fault-during":
                gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
                gate.ReportFatalFault("disk full");
                break;
            default:
                gate.Apply(MemberTrigger.GracefulStop);
                gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
                break;
        }

        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());

        var tail = Actions.SkipWhile(a => a != MembershipActions.FStep).ToList();
        Assert.Equal([MembershipActions.FStep, exit], tail);
    }

    [Fact]
    public void A_graceful_stop_in_Fencing_exits_at_step_6_with_no_model_exit()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.Apply(MemberTrigger.GracefulStop);

        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());

        Assert.Equal(MembershipActions.FStep, Actions[^1]);
    }

    [Fact]
    public void StuckExit_FencedExit_a_stuck_attempt_or_a_fault_in_Fenced_exits_with_its_action()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();

        gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Equal(MembershipActions.StuckExit, Actions[^1]);
    }

    [Fact]
    public void FencedExit_a_fatal_fault_in_Starting_exits_with_FencedExit()
    {
        gate.ReportFatalFault("disk full");

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        var exit = Assert.Single(events.All);
        Assert.Equal(MembershipActions.FencedExit, exit.Action);
        Assert.Equal(nameof(ReplicaPhase.Starting), exit.State!["from"]);
    }

    [Fact]
    public void FencedExit_a_fault_in_Fenced_emits_FaultFenced_then_FencedExit()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();

        gate.ReportFatalFault("disk full");

        Assert.Equal([MembershipActions.FaultFenced, MembershipActions.FencedExit], Actions.TakeLast(2));
    }

    [Fact]
    public void Rejoin_the_rejoin_and_the_exit_on_a_higher_epoch_emit_Rejoin()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);

        var rejoin = Assert.Single(events.Of(MembershipActions.Rejoin));
        Assert.Equal(Epoch, rejoin.State!["epoch"]);
        Assert.Equal(false, rejoin.State["exit"]);

        Join(Epoch + 1);
        gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RowHasHigherEpoch);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Equal(true, events.Of(MembershipActions.Rejoin)[1].State!["exit"]);
    }

    [Fact]
    public void I23_a_fence_is_never_undone_triggers_and_a_join_in_Fencing_change_nothing()
    {
        Join();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        var fence = gate.CurrentFence;

        foreach (var trigger in Enum.GetValues<FenceTrigger>().Where(t => t is not (FenceTrigger.StuckAttempt or FenceTrigger.FatalFault)))
        {
            Assert.Equal(FenceDecision.Ignored, gate.RequestFence(new FenceRequest(trigger)));
        }

        Assert.False(gate.Joined(Epoch + 1, Timeline, gate.Now));
        Assert.True(gate.Apply(MemberTrigger.RejoinPermitted).IsIgnored);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Same(fence, gate.CurrentFence);
        Assert.Single(fences);
    }
}

internal sealed class HoldingPoints : IFaultPoints
{
    private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource release = new();
    private string? held;

    public Task Reached => reached.Task;

    public List<string> Passed { get; } = [];

    public void Hold(string point)
    {
        held = point;
        release = new TaskCompletionSource();
    }

    public void Release() => release.TrySetResult();

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        lock (Passed)
        {
            Passed.Add(point);
        }

        if (point != held)
        {
            return ValueTask.CompletedTask;
        }

        reached.TrySetResult();
        return new ValueTask(cancellationToken.CanBeCanceled ? release.Task.WaitAsync(cancellationToken) : release.Task);
    }
}
