using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using static Reeve.Cluster.Core.MemberEffects;

namespace Unit.Membership;

public sealed class MemberStateMachineTests
{
    private static readonly ReplicaPhase[] AllPhases = Enum.GetValues<ReplicaPhase>();
    private static readonly MemberTrigger[] AllTriggers = Enum.GetValues<MemberTrigger>();

    private static MemberTransition Expect(
        ReplicaPhase phase,
        MemberTrigger trigger,
        string row,
        ReplicaPhase to,
        MemberEffects effects,
        RowKind? toRow = null)
    {
        var t = MemberStateMachine.Decide(phase, trigger);

        Assert.Equal(row, t.RowId);
        Assert.Equal(phase, t.From);
        Assert.Equal(to, t.To);
        Assert.Equal(effects, t.Effects);
        if (toRow is { } expectedRow)
        {
            Assert.Equal(expectedRow, t.ToRow);
        }

        return t;
    }

    private static void ExpectIgnored(ReplicaPhase phase, MemberTrigger trigger)
    {
        var t = MemberStateMachine.Decide(phase, trigger);

        Assert.True(t.IsIgnored, $"{phase} + {trigger} should be ignored, got row {t.RowId}");
        Assert.Equal(phase, t.To);
        Assert.Equal(None, t.Effects);
        Assert.Null(t.ModelAction);
    }

    [Fact]
    public void M01_a_successful_join_writes_a_joining_row_starts_renewal_and_closes_to_traffic()
    {
        var t = Expect(ReplicaPhase.Starting, MemberTrigger.JoinSucceeded, "M01", ReplicaPhase.Loading,
            WriteJoiningRow | StartRenewal | CloseToTraffic | ClearReadinessReason, RowKind.Joining);

        Assert.Equal(RowKind.Free, t.FromRow);
        Assert.Equal(MembershipActions.Join, t.ModelAction);
    }

    [Fact]
    public void M02_a_refused_join_stays_in_Starting_retries_and_names_its_reason()
    {
        var t = Expect(ReplicaPhase.Starting, MemberTrigger.JoinRefused, "M02", ReplicaPhase.Starting,
            RetryJoin | RaiseReadinessReason, RowKind.Unchanged);

        Assert.Equal(RowKind.Held, t.FromRow);
        Assert.Null(t.ModelAction);
    }

    [Fact]
    public void M03_a_fatal_fault_in_Starting_exits_the_process_it_has_no_row_to_fence()
    {
        Expect(ReplicaPhase.Starting, MemberTrigger.FatalFault, "M03", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
    }

    [Fact]
    public void M04_state_loaded_moves_Loading_to_CatchingUp_and_a_failed_load_retries_in_Loading()
    {
        var t = Expect(ReplicaPhase.Loading, MemberTrigger.StateLoaded, "M04", ReplicaPhase.CatchingUp, RetainCursor, RowKind.Joining);
        Assert.Equal("LoadDone", t.ModelAction);

        Expect(ReplicaPhase.Loading, MemberTrigger.LoadFailed, "M04", ReplicaPhase.Loading, RetryWithBackoff, RowKind.Joining);
    }

    [Fact]
    public void M05_in_CatchingUp_a_failure_or_a_stall_retries_with_backoff_and_is_no_fence_trigger()
    {
        Expect(ReplicaPhase.CatchingUp, MemberTrigger.ApplyFailed, "M05", ReplicaPhase.CatchingUp, RetryWithBackoff, RowKind.Joining);
        Expect(ReplicaPhase.CatchingUp, MemberTrigger.ApplierStall, "M05", ReplicaPhase.CatchingUp, RetryWithBackoff, RowKind.Joining);
        Expect(ReplicaPhase.Loading, MemberTrigger.ApplierStall, "M04", ReplicaPhase.Loading, RetryWithBackoff, RowKind.Joining);
    }

    [Fact]
    public void M06_a_committed_activation_moves_CatchingUp_to_Activated_and_an_active_row()
    {
        var t = Expect(ReplicaPhase.CatchingUp, MemberTrigger.ActivationCommitted, "M06", ReplicaPhase.Activated, ClearReadinessReason, RowKind.Active);

        Assert.Equal(RowKind.Joining, t.FromRow);
        Assert.Equal(MembershipActions.Activate, t.ModelAction);
    }

    [Fact]
    public void M06_an_activation_that_updates_no_row_is_abandoned_and_one_the_floor_refuses_stays_joining_with_a_reason()
    {
        Expect(ReplicaPhase.CatchingUp, MemberTrigger.ActivationAbandoned, "M06", ReplicaPhase.CatchingUp, AbandonActivation, RowKind.Unchanged);
        Expect(ReplicaPhase.CatchingUp, MemberTrigger.ActivationRefusedByFloor, "M06", ReplicaPhase.CatchingUp, RaiseReadinessReason, RowKind.Joining);
    }

    [Fact]
    public void M06_the_phase_moves_to_Activated_only_if_it_is_still_CatchingUp_when_the_commit_returns()
    {
        foreach (var phase in AllPhases.Where(p => p != ReplicaPhase.CatchingUp))
        {
            ExpectIgnored(phase, MemberTrigger.ActivationCommitted);
        }
    }

    [Fact]
    public void M07_applying_up_to_the_activation_head_enters_Serving_and_opens_what_Serving_governs()
    {
        var t = Expect(ReplicaPhase.Activated, MemberTrigger.AppliedToActivationHead, "M07", ReplicaPhase.Serving,
            SetServingSince | OpenToTraffic | EnableServingActions, RowKind.Active);

        Assert.Equal(MembershipActions.EnterServing, t.ModelAction);
    }

    [Fact]
    public void M08_a_fence_trigger_starts_fence_steps_0_to_6_in_every_phase_that_holds_a_row()
    {
        var cases = new (MemberTrigger Trigger, MemberEffects Effects, RowKind RowAfter)[]
        {
            (MemberTrigger.FenceStart, RunFenceSteps, RowKind.Fenced),
            (MemberTrigger.FatalFault, RunFenceSteps, RowKind.Fenced),
            (MemberTrigger.ZeroRowRenewal, RunFenceSteps | SkipLeaseRelease, RowKind.Unchanged),
            (MemberTrigger.StuckAttempt, RunFenceSteps | ExitAtFenceEnd, RowKind.Fenced),
            (MemberTrigger.ApplierStall, RunFenceSteps, RowKind.Fenced),
            (MemberTrigger.TimelineChanged, RunFenceSteps, RowKind.Fenced),
            (MemberTrigger.ActivationUnanswered, RunFenceSteps, RowKind.Fenced),
        };

        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining })
        {
            var beforeActivation = phase is ReplicaPhase.Loading or ReplicaPhase.CatchingUp;
            foreach (var (trigger, effects, rowAfter) in cases)
            {
                if (trigger == MemberTrigger.ApplierStall && beforeActivation)
                {
                    continue;
                }

                var action = trigger switch
                {
                    MemberTrigger.FenceStart => MembershipActions.FenceAtStart,
                    MemberTrigger.FatalFault => MembershipActions.FenceAny,
                    MemberTrigger.ActivationUnanswered => MembershipActions.FenceAny,
                    MemberTrigger.ZeroRowRenewal => MembershipActions.RenewAnswer,
                    MemberTrigger.TimelineChanged => MembershipActions.RenewAnswer,
                    MemberTrigger.ApplierStall => MembershipActions.FenceStall,
                    _ => MembershipActions.FenceStuck,
                };

                var expected = phase == ReplicaPhase.Draining ? effects | SkipDrainRowWrite | ExitAtFenceEnd : effects;
                var t = Expect(phase, trigger, "M08", ReplicaPhase.Fencing, expected, rowAfter);
                Assert.Equal(action, t.ModelAction);
            }
        }
    }

    [Fact]
    public void M08_a_stall_is_ignored_as_a_fence_trigger_in_Loading_and_CatchingUp()
    {
        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp })
        {
            var t = MemberStateMachine.Decide(phase, MemberTrigger.ApplierStall);

            Assert.Equal(phase, t.To);
            Assert.False(t.Effects.HasFlag(RunFenceSteps));
        }
    }

    [Fact]
    public void M08_step_6_completes_the_fence_into_Fenced_or_exits_when_the_fence_is_marked_to_exit()
    {
        var t = Expect(ReplicaPhase.Fencing, MemberTrigger.FenceCompleted, "M08", ReplicaPhase.Fenced, None, RowKind.Unchanged);
        Assert.Equal(MembershipActions.FStep, t.ModelAction);

        var stuck = Expect(ReplicaPhase.Fencing, MemberTrigger.FenceCompletedToExit, "M08", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
        Assert.Null(stuck.ModelAction);
    }

    [Fact]
    public void M08_a_stuck_attempt_reported_during_a_fence_is_recorded_so_that_step_6_exits()
    {
        Expect(ReplicaPhase.Fencing, MemberTrigger.StuckAttempt, "M08", ReplicaPhase.Fencing, MarkExitAfterFence, RowKind.Unchanged);
    }

    [Fact]
    public void M08_and_M11_a_fatal_fault_in_Fencing_is_recorded_so_that_step_6_exits_and_one_in_Fenced_exits_at_once()
    {
        Expect(ReplicaPhase.Fencing, MemberTrigger.FatalFault, "M08", ReplicaPhase.Fencing, MarkExitAfterFence, RowKind.Unchanged);
        var t = Expect(ReplicaPhase.Fenced, MemberTrigger.FatalFault, "M11", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);

        Assert.Equal(MembershipActions.FencedExit, t.ModelAction);
    }

    [Fact]
    public void M09_from_Fenced_a_permitted_rejoin_re_enters_Starting_and_joins_by_the_first_two_rows()
    {
        var t = Expect(ReplicaPhase.Fenced, MemberTrigger.RejoinPermitted, "M09", ReplicaPhase.Starting, RejoinByJoin, RowKind.Unchanged);

        Assert.Equal(MembershipActions.Rejoin, t.ModelAction);
    }

    [Fact]
    public void M10_from_Fenced_a_row_with_a_higher_epoch_exits_the_process_it_never_takes_the_row_back()
    {
        var t = Expect(ReplicaPhase.Fenced, MemberTrigger.RowHasHigherEpoch, "M10", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);

        Assert.Equal(MembershipActions.Rejoin, t.ModelAction);
    }

    [Fact]
    public void M11_a_stuck_attempt_in_Fenced_or_Starting_exits_instead_of_rejoining()
    {
        var fenced = Expect(ReplicaPhase.Fenced, MemberTrigger.StuckAttempt, "M11", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
        Assert.Equal(MembershipActions.StuckExit, fenced.ModelAction);

        Expect(ReplicaPhase.Starting, MemberTrigger.StuckAttempt, "M11", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
    }

    [Fact]
    public void M12_a_graceful_stop_in_Serving_runs_drain_steps_1_to_4_and_moves_to_Draining()
    {
        var t = Expect(ReplicaPhase.Serving, MemberTrigger.GracefulStop, "M12", ReplicaPhase.Draining, RunDrainSteps1To4, RowKind.Active);

        Assert.Equal(MembershipActions.Drain1, t.ModelAction);
    }

    [Fact]
    public void M13_a_finished_drain_answers_pending_waits_writes_draining_stops_renewal_and_exits()
    {
        var t = Expect(ReplicaPhase.Draining, MemberTrigger.DrainCompleted, "M13", ReplicaPhase.Exited,
            AnswerPendingUnconfirmedDraining | WriteDrainingRow | StopRenewal | ExitProcess, RowKind.Draining);

        Assert.Equal(MembershipActions.Drain4, t.ModelAction);
    }

    [Fact]
    public void M14_a_graceful_stop_in_Loading_or_CatchingUp_writes_draining_and_exits()
    {
        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp })
        {
            var t = Expect(phase, MemberTrigger.GracefulStop, "M14", ReplicaPhase.Exited,
                WriteDrainingRow | StopRenewal | ExitProcess, RowKind.Draining);

            Assert.Equal("StopEarly", t.ModelAction);
        }
    }

    [Fact]
    public void M15_a_graceful_stop_in_Activated_runs_drain_steps_2_to_4_there_is_no_traffic_to_close()
    {
        var t = Expect(ReplicaPhase.Activated, MemberTrigger.GracefulStop, "M15", ReplicaPhase.Exited,
            RunDrainSteps2To4 | AnswerPendingUnconfirmedDraining | WriteDrainingRow | StopRenewal | ExitProcess, RowKind.Draining);

        Assert.Equal("StopEarly", t.ModelAction);
    }

    [Fact]
    public void M16_a_graceful_stop_in_Starting_Fencing_or_Fenced_exits_without_a_row_write_and_lets_a_fence_finish_first()
    {
        Expect(ReplicaPhase.Starting, MemberTrigger.GracefulStop, "M16", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
        Expect(ReplicaPhase.Fenced, MemberTrigger.GracefulStop, "M16", ReplicaPhase.Exited, ExitProcess, RowKind.Unchanged);
        Expect(ReplicaPhase.Fencing, MemberTrigger.GracefulStop, "M16", ReplicaPhase.Exited, CompleteFenceFirst | ExitProcess, RowKind.Unchanged);
    }

    [Fact]
    public void A_graceful_stop_while_already_draining_or_exited_changes_nothing()
    {
        ExpectIgnored(ReplicaPhase.Draining, MemberTrigger.GracefulStop);
        ExpectIgnored(ReplicaPhase.Exited, MemberTrigger.GracefulStop);
    }

    [Fact]
    public void M13_after_the_draining_write_nothing_fences_the_row_is_draining_and_the_process_is_leaving()
    {
        foreach (var trigger in new[] { MemberTrigger.FenceStart, MemberTrigger.ApplierStall, MemberTrigger.ZeroRowRenewal, MemberTrigger.TimelineChanged, MemberTrigger.GracefulStop, MemberTrigger.DrainCompleted })
        {
            var t = MemberStateMachine.Decide(ReplicaPhase.Draining, trigger, drainRowWriteStarted: true);

            Assert.True(t.IsIgnored, $"{trigger} after the draining write should change nothing, got {t.RowId} {t.Effects}");
            Assert.Equal(ReplicaPhase.Draining, t.To);
        }
    }

    [Fact]
    public void M13_after_the_draining_write_a_fatal_fault_or_a_stuck_attempt_ends_the_process_at_once()
    {
        foreach (var trigger in new[] { MemberTrigger.FatalFault, MemberTrigger.StuckAttempt })
        {
            var t = MemberStateMachine.Decide(ReplicaPhase.Draining, trigger, drainRowWriteStarted: true);

            Assert.Equal("M13", t.RowId);
            Assert.Equal(ReplicaPhase.Exited, t.To);
            Assert.Equal(ExitProcess, t.Effects);
            Assert.Equal(RowKind.Draining, t.FromRow);
            Assert.True(t.AfterDrainWrite);
        }
    }

    [Fact]
    public void M13_before_the_draining_write_a_fence_takes_over_the_drain_skips_the_row_write_and_exits_at_step_6()
    {
        var t = Expect(ReplicaPhase.Draining, MemberTrigger.FenceStart, "M08", ReplicaPhase.Fencing, RunFenceSteps | SkipDrainRowWrite | ExitAtFenceEnd, RowKind.Fenced);

        Assert.False(t.AfterDrainWrite);
        Assert.Equal(t, MemberStateMachine.Decide(ReplicaPhase.Draining, MemberTrigger.FenceStart, drainRowWriteStarted: false));
    }

    [Fact]
    public void The_drain_write_flag_means_nothing_outside_Draining()
    {
        foreach (var phase in AllPhases.Where(p => p != ReplicaPhase.Draining))
        {
            foreach (var trigger in AllTriggers)
            {
                Assert.Equal(
                    MemberStateMachine.Decide(phase, trigger, drainRowWriteStarted: false),
                    MemberStateMachine.Decide(phase, trigger, drainRowWriteStarted: true));
            }
        }
    }

    [Fact]
    public void A_readiness_reason_the_table_raises_is_cleared_by_the_next_successful_join_or_activation()
    {
        Assert.True(MemberStateMachine.Decide(ReplicaPhase.Starting, MemberTrigger.JoinSucceeded).Effects.HasFlag(ClearReadinessReason));
        Assert.True(MemberStateMachine.Decide(ReplicaPhase.CatchingUp, MemberTrigger.ActivationCommitted).Effects.HasFlag(ClearReadinessReason));
        foreach (var t in MemberStateMachine.Table.Where(t => t.Effects.HasFlag(RaiseReadinessReason)))
        {
            Assert.False(t.Effects.HasFlag(ClearReadinessReason));
        }
    }

    [Fact]
    public void I23_a_fence_is_never_undone_from_Fencing_only_Fenced_or_Exited_follow()
    {
        foreach (var trigger in AllTriggers)
        {
            var t = MemberStateMachine.Decide(ReplicaPhase.Fencing, trigger);

            Assert.Contains(t.To, new[] { ReplicaPhase.Fencing, ReplicaPhase.Fenced, ReplicaPhase.Exited });
            Assert.False(t.Effects.HasFlag(RunFenceSteps), $"{trigger} must not start a second fence");
        }
    }

    [Fact]
    public void I23_a_process_never_takes_a_row_back_by_force_from_Fenced_only_Starting_or_Exited_follow()
    {
        foreach (var trigger in AllTriggers)
        {
            var t = MemberStateMachine.Decide(ReplicaPhase.Fenced, trigger);

            Assert.Contains(t.To, new[] { ReplicaPhase.Fenced, ReplicaPhase.Starting, ReplicaPhase.Exited });
            Assert.False(t.Effects.HasFlag(WriteJoiningRow), $"{trigger} must not write a row from Fenced");
        }

        Assert.Equal(ReplicaPhase.Exited, MemberStateMachine.Decide(ReplicaPhase.Fenced, MemberTrigger.RowHasHigherEpoch).To);
    }

    [Fact]
    public void I23_a_zero_row_renewal_fences_every_phase_that_renews_and_skips_step_3()
    {
        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining })
        {
            var t = MemberStateMachine.Decide(phase, MemberTrigger.ZeroRowRenewal);

            Assert.Equal(ReplicaPhase.Fencing, t.To);
            Assert.True(t.Effects.HasFlag(SkipLeaseRelease), $"{phase}: step 3 must be skipped");
        }
    }

    [Fact]
    public void Only_a_zero_row_renewal_skips_step_3()
    {
        foreach (var t in MemberStateMachine.Table.Where(t => t.Effects.HasFlag(SkipLeaseRelease)))
        {
            Assert.Equal(MemberTrigger.ZeroRowRenewal, t.Trigger);
        }
    }

    [Fact]
    public void Every_cell_is_decided_once_and_every_unlisted_cell_is_ignored()
    {
        var keys = MemberStateMachine.Table.Select(t => (t.From, t.Trigger, t.AfterDrainWrite)).ToList();
        Assert.Equal(keys.Count, keys.Distinct().Count());

        foreach (var phase in AllPhases)
        {
            foreach (var trigger in AllTriggers)
            {
                foreach (var drainWrite in new[] { false, true })
                {
                    var t = MemberStateMachine.Decide(phase, trigger, drainWrite);
                    var inPhaseWithFlag = drainWrite && phase == ReplicaPhase.Draining;
                    Assert.Equal(keys.Contains((phase, trigger, inPhaseWithFlag)), !t.IsIgnored);
                    Assert.Equal(phase, t.From);
                    Assert.Equal(trigger, t.Trigger);
                }
            }
        }
    }

    [Fact]
    public void Every_trigger_and_every_row_of_the_spec_is_used()
    {
        var used = MemberStateMachine.Table.Select(t => t.Trigger).ToHashSet();
        Assert.Equal(AllTriggers.Order(), used.Order());

        var expectedRows = new[]
        {
            "M01", "M02", "M03", "M04", "M05", "M06", "M07", "M08", "M09", "M10", "M11", "M12", "M13", "M14", "M15", "M16",
        };
        Assert.Equal(expectedRows.Order(), MemberStateMachine.Table.Select(t => t.RowId).Distinct().Order());
    }

    [Fact]
    public void The_row_a_phase_implies_is_the_one_its_cells_start_from()
    {
        var expected = new Dictionary<ReplicaPhase, RowKind>
        {
            [ReplicaPhase.Loading] = RowKind.Joining,
            [ReplicaPhase.CatchingUp] = RowKind.Joining,
            [ReplicaPhase.Activated] = RowKind.Active,
            [ReplicaPhase.Serving] = RowKind.Active,
            [ReplicaPhase.Draining] = RowKind.Active,
        };

        foreach (var t in MemberStateMachine.Table.Where(t => expected.ContainsKey(t.From) && !t.AfterDrainWrite))
        {
            Assert.True(expected[t.From] == t.FromRow, $"{t.RowId} {t.From} + {t.Trigger}: from row {t.FromRow}, expected {expected[t.From]}");
        }
    }

    [Fact]
    public void The_fence_trigger_cells_agree_with_FenceTriggerPolicy()
    {
        var map = new Dictionary<MemberTrigger, FenceTrigger>
        {
            [MemberTrigger.FenceStart] = FenceTrigger.FenceStart,
            [MemberTrigger.ApplierStall] = FenceTrigger.ApplierStall,
            [MemberTrigger.StuckAttempt] = FenceTrigger.StuckAttempt,
            [MemberTrigger.FatalFault] = FenceTrigger.FatalFault,
            [MemberTrigger.ZeroRowRenewal] = FenceTrigger.ZeroRowRenewal,
            [MemberTrigger.TimelineChanged] = FenceTrigger.Timeline,
            [MemberTrigger.ActivationUnanswered] = FenceTrigger.UnansweredActivation,
        };

        foreach (var phase in AllPhases)
        {
            foreach (var (trigger, fence) in map)
            foreach (var drainWrite in new[] { false, true })
            {
                var t = MemberStateMachine.Decide(phase, trigger, drainWrite);
                switch (FenceTriggerPolicy.Decide(phase, fence, drainWrite))
                {
                    case FenceDecision.Fence:
                        Assert.Equal(ReplicaPhase.Fencing, t.To);
                        Assert.False(t.Effects.HasFlag(ExitAtFenceEnd));
                        break;
                    case FenceDecision.FenceThenExit:
                        Assert.Equal(ReplicaPhase.Fencing, t.To);
                        Assert.True(t.Effects.HasFlag(ExitAtFenceEnd));
                        break;
                    case FenceDecision.ExitAfterFence:
                        Assert.Equal(ReplicaPhase.Fencing, t.To);
                        Assert.True(t.Effects.HasFlag(MarkExitAfterFence));
                        break;
                    case FenceDecision.ExitNow:
                        Assert.Equal(ReplicaPhase.Exited, t.To);
                        Assert.True(t.Effects.HasFlag(ExitProcess));
                        break;
                    case FenceDecision.Ignored:
                        Assert.Equal(phase, t.To);
                        Assert.False(t.Effects.HasFlag(RunFenceSteps) || t.Effects.HasFlag(ExitProcess));
                        break;
                }
            }
        }
    }

    [Fact]
    public void A_phase_that_has_left_for_good_never_comes_back()
    {
        var serving = new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Draining };

        foreach (var trigger in AllTriggers)
        {
            Assert.True(MemberStateMachine.Decide(ReplicaPhase.Exited, trigger).IsIgnored);
            foreach (var phase in new[] { ReplicaPhase.Fencing, ReplicaPhase.Fenced })
            {
                Assert.DoesNotContain(MemberStateMachine.Decide(phase, trigger).To, serving);
            }
        }
    }
}
