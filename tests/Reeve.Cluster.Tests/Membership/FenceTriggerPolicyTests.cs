using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class FenceTriggerPolicyTests
{
    private static readonly FenceTrigger[] Triggers =
        [FenceTrigger.FenceStart, FenceTrigger.ApplierStall, FenceTrigger.StuckAttempt, FenceTrigger.FatalFault, FenceTrigger.ZeroRowRenewal, FenceTrigger.Timeline,
         FenceTrigger.UnansweredActivation];

    private static FenceDecision Ig => FenceDecision.Ignored;

    private static FenceDecision F => FenceDecision.Fence;

    private static FenceDecision FX => FenceDecision.FenceThenExit;

    private static Dictionary<ReplicaPhase, FenceDecision[]> Expected() => new()
    {
        [ReplicaPhase.Starting] = [Ig, Ig, FenceDecision.ExitNow, FenceDecision.ExitNow, Ig, Ig, Ig],
        [ReplicaPhase.Loading] = [F, Ig, FX, F, F, F, F],
        [ReplicaPhase.CatchingUp] = [F, Ig, FX, F, F, F, F],
        [ReplicaPhase.Activated] = [F, F, FX, F, F, F, F],
        [ReplicaPhase.Serving] = [F, F, FX, F, F, F, F],
        [ReplicaPhase.Draining] = [FX, FX, FX, FX, FX, FX, FX],
        [ReplicaPhase.Fencing] = [Ig, Ig, FenceDecision.ExitAfterFence, FenceDecision.ExitAfterFence, Ig, Ig, Ig],
        [ReplicaPhase.Fenced] = [Ig, Ig, FenceDecision.ExitNow, FenceDecision.ExitNow, Ig, Ig, Ig],
        [ReplicaPhase.Exited] = [Ig, Ig, Ig, Ig, Ig, Ig, Ig],
    };

    [Fact]
    public void Every_phase_and_trigger_is_decided_as_the_spec_says()
    {
        var expected = Expected();

        Assert.Equal(Enum.GetValues<ReplicaPhase>().Order(), expected.Keys.Order());
        foreach (var (phase, row) in expected)
        {
            for (var i = 0; i < Triggers.Length; i++)
            {
                Assert.True(
                    row[i] == FenceTriggerPolicy.Decide(phase, Triggers[i]),
                    $"{phase} + {Triggers[i]}: expected {row[i]}, got {FenceTriggerPolicy.Decide(phase, Triggers[i])}");
            }
        }
    }

    [Fact]
    public void The_trigger_list_is_every_trigger()
    {
        Assert.Equal(Enum.GetValues<FenceTrigger>().Order(), Triggers.Order());
    }

    [Fact]
    public void A_fatal_fault_in_Starting_exits_because_the_process_has_no_row_to_fence()
    {
        Assert.Equal(FenceDecision.ExitNow, FenceTriggerPolicy.Decide(ReplicaPhase.Starting, FenceTrigger.FatalFault));
    }

    [Fact]
    public void A_stuck_attempt_in_Fenced_exits_instead_of_rejoining_with_the_attempt_outstanding()
    {
        Assert.Equal(FenceDecision.ExitNow, FenceTriggerPolicy.Decide(ReplicaPhase.Fenced, FenceTrigger.StuckAttempt));
    }

    [Fact]
    public void A_stuck_attempt_reported_during_the_fence_is_not_dropped_because_step_6_exits_whether_or_not_it_was_the_trigger()
    {
        Assert.Equal(FenceDecision.ExitAfterFence, FenceTriggerPolicy.Decide(ReplicaPhase.Fencing, FenceTrigger.StuckAttempt));
    }

    [Fact]
    public void An_applier_stall_is_a_trigger_only_in_Activated_Serving_and_Draining()
    {
        var fences = Enum.GetValues<ReplicaPhase>()
            .Where(p => FenceTriggerPolicy.Decide(p, FenceTrigger.ApplierStall) is FenceDecision.Fence or FenceDecision.FenceThenExit)
            .ToList();

        Assert.Equal([ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining], fences);
    }

    [Fact]
    public void I23_nothing_undoes_a_fence_a_later_trigger_never_starts_a_second_one()
    {
        foreach (var trigger in Triggers)
        {
            var decision = FenceTriggerPolicy.Decide(ReplicaPhase.Fencing, trigger);

            Assert.NotEqual(FenceDecision.Fence, decision);
            Assert.NotEqual(FenceDecision.FenceThenExit, decision);
        }
    }

    [Fact]
    public void After_the_draining_write_the_row_is_draining_so_only_a_fatal_fault_or_a_stuck_attempt_do_anything_and_they_exit()
    {
        foreach (var trigger in Triggers)
        {
            var expected = trigger is FenceTrigger.FatalFault or FenceTrigger.StuckAttempt ? FenceDecision.ExitNow : FenceDecision.Ignored;

            Assert.Equal(expected, FenceTriggerPolicy.Decide(ReplicaPhase.Draining, trigger, drainRowWriteStarted: true));
        }
    }

    [Fact]
    public void The_draining_write_flag_changes_nothing_in_any_other_phase()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>().Where(p => p != ReplicaPhase.Draining))
        {
            foreach (var trigger in Triggers)
            {
                Assert.Equal(FenceTriggerPolicy.Decide(phase, trigger), FenceTriggerPolicy.Decide(phase, trigger, drainRowWriteStarted: true));
            }
        }
    }

    [Fact]
    public void A_fence_that_takes_over_a_drain_exits_at_step_6_whatever_triggered_it()
    {
        foreach (var trigger in Triggers)
        {
            Assert.Equal(FenceDecision.FenceThenExit, FenceTriggerPolicy.Decide(ReplicaPhase.Draining, trigger));
        }
    }

    [Fact]
    public void A_fatal_fault_in_Fencing_is_recorded_so_that_step_6_exits_and_one_in_Fenced_exits_at_once()
    {
        Assert.Equal(FenceDecision.ExitAfterFence, FenceTriggerPolicy.Decide(ReplicaPhase.Fencing, FenceTrigger.FatalFault));
        Assert.Equal(FenceDecision.ExitNow, FenceTriggerPolicy.Decide(ReplicaPhase.Fenced, FenceTrigger.FatalFault));
    }

    [Theory]
    [InlineData("Serving", "StuckAttempt", "FenceThenExit", "Stuck")]
    [InlineData("Loading", "StuckAttempt", "FenceThenExit", "Stuck")]
    [InlineData("Draining", "FenceStart", "FenceThenExit", "Drain")]
    [InlineData("Draining", "StuckAttempt", "FenceThenExit", "Stuck, Drain")]
    [InlineData("Fencing", "FatalFault", "ExitAfterFence", "Fault")]
    [InlineData("Fencing", "StuckAttempt", "ExitAfterFence", "Stuck")]
    [InlineData("Serving", "FatalFault", "Fence", "None")]
    [InlineData("Fenced", "FatalFault", "ExitNow", "Fault")]
    [InlineData("Starting", "StuckAttempt", "ExitNow", "Stuck")]
    public void The_verdict_names_why_the_process_exits(string phase, string trigger, string decision, string cause)
    {
        var verdict = FenceTriggerPolicy.Judge(Enum.Parse<ReplicaPhase>(phase), Enum.Parse<FenceTrigger>(trigger));

        Assert.Equal(Enum.Parse<FenceDecision>(decision), verdict.Decision);
        Assert.Equal(Enum.Parse<ExitCauses>(cause), verdict.Cause);
    }

    [Fact]
    public void Every_decision_that_ends_the_process_names_a_cause_and_no_other_does()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>())
        {
            foreach (var trigger in Triggers)
            {
                foreach (var written in new[] { false, true })
                {
                    var verdict = FenceTriggerPolicy.Judge(phase, trigger, written);
                    Assert.Equal(FenceTriggerPolicy.Decide(phase, trigger, written), verdict.Decision);
                    var exits = verdict.Decision is FenceDecision.FenceThenExit or FenceDecision.ExitAfterFence or FenceDecision.ExitNow;
                    Assert.True(exits == (verdict.Cause != ExitCauses.None), $"{phase}, {trigger}: {verdict}");
                }
            }
        }
    }

    [Theory]
    [InlineData("Stuck", "StuckExit")]
    [InlineData("Stuck, Fault", "StuckExit")]
    [InlineData("Stuck, Drain", "StuckExit")]
    [InlineData("Fault", "FencedExit")]
    [InlineData("Drain", "FencedExit")]
    [InlineData("Stop", null)]
    [InlineData("None", null)]
    public void The_exit_action_at_step_6_follows_the_cause(string causes, string? action)
    {
        Assert.Equal(action, FenceTriggerPolicy.ExitAction(Enum.Parse<ExitCauses>(causes)));
    }
}
