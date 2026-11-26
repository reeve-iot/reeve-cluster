using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stimuli;

namespace Harness.SelfTests;

public sealed class StimulusTableTests
{
    private const string ConnectionString = "Host=localhost;Username=u;Database=d";
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    [Fact]
    public void A_replay_finds_the_row_of_an_action_and_an_unknown_action_is_a_trace_it_cannot_drive()
    {
        var renewSend = StimulusTable.For(MembershipActions.RenewSend);

        Assert.Equal([MembershipFaultPoints.RenewSend], renewSend.Points.Select(p => p.Point));
        Assert.Equal(StimulusKind.Point, renewSend.Kind);
        Assert.Throws<KeyNotFoundException>(() => StimulusTable.For("NoSuchAction"));
        Assert.Null(StimulusTable.Find("NoSuchAction"));
    }

    [Fact]
    public void The_actions_the_stage_one_traces_use_are_all_in_the_table()
    {
        string[] used =
        [
            "WBegin", "WAppend", "WCommit", "PassBegin", "AttemptHang", "LateReturn", "Publish", "CursorWrite", "AckBegin",
            "AckRecord", "AckDone", "SkipOpen", "OperatorDelete", "RenewSend", "RenewExec", "RenewAnswer", "Pause", "Restore",
        ];

        Assert.All(used, action => Assert.NotNull(StimulusTable.Find(action)));
    }

    [Fact]
    public void A_row_that_needs_the_harness_alone_is_ready_and_one_that_needs_a_point_is_not_until_a_task_places_it()
    {
        Assert.True(StimulusTable.For("Pause").IsReady);
        Assert.True(StimulusTable.For("Crash").IsReady);
        Assert.True(StimulusTable.For("OperatorDelete").IsReady);
        Assert.True(StimulusTable.For("Tick").IsReady);
        Assert.True(StimulusTable.For(MembershipActions.Join).IsReady);
        Assert.True(StimulusTable.For(MembershipActions.FenceAtStart).IsReady);
        Assert.True(StimulusTable.For(MembershipActions.Activate).IsReady);
        Assert.True(StimulusTable.For(MembershipActions.EnterServing).IsReady);
        var drain = StimulusTable.For(MembershipActions.Drain1);
        Assert.False(drain.IsReady);
        Assert.Equal(["1.20"], drain.Waiting);
        Assert.True(StimulusTable.For(LogActions.WBegin).IsReady);
        Assert.False(StimulusTable.For(LogActions.QStart).IsReady);
    }

    [Fact]
    public void Readiness_is_per_point_so_a_row_waits_for_every_task_that_places_one_of_its_points()
    {
        var append = StimulusTable.For(LogActions.WAppend);

        Assert.True(append.IsReady);
        Assert.All(append.Points, p => Assert.Null(p.PlacedBy));
        Assert.Equal(4, append.Points.Count);
    }

    [Fact]
    public void A_row_is_the_step_from_a_prepared_state_and_names_the_rows_that_prepare_it()
    {
        var stall = StimulusTable.For(MembershipActions.FenceStall);
        var zeroRow = StimulusTable.For(MembershipActions.RenewZeroL);

        Assert.Equal(StimulusKind.Elapse, stall.Kind);
        Assert.Equal(TimeAdvance.ReplicaClock, stall.Advance);
        Assert.Null(stall.Operation);
        Assert.Contains("AttemptFail", stall.Enabling, StringComparison.Ordinal);
        Assert.Contains("OperatorDelete", zeroRow.Enabling, StringComparison.Ordinal);
        Assert.Null(zeroRow.Operation);
        Assert.Equal([MembershipFaultPoints.RenewAnswer], zeroRow.Points.Select(p => p.Point));
        Assert.Equal(StimulusSubject.Write, StimulusTable.For(LogActions.AckBegin).Subject);
        Assert.Equal(StimulusSubject.Read, StimulusTable.For(LogActions.QPin).Subject);
        Assert.Equal(StimulusSubject.Fleet, StimulusTable.For("SkipOpen").Subject);
    }

    [Fact]
    public void A_row_of_a_library_action_with_no_point_waits_for_its_task_until_a_test_is_named_as_its_proof()
    {
        var tryActivate = StimulusTable.For(LogActions.TryActivate);

        Assert.False(tryActivate.IsReady);
        Assert.Equal(["1.19"], tryActivate.Waiting);

        var proven = tryActivate.Proven("TryActivate_deferred_work_becomes_observable_against_the_current_generation");

        Assert.True(proven.IsReady);
        Assert.Null(proven.PlacedBy);
        Assert.Equal("TryActivate_deferred_work_becomes_observable_against_the_current_generation", proven.ProvenBy);
        Assert.True(StimulusTable.For(MembershipActions.ReachFS).IsReady);
        Assert.True(StimulusTable.For(MembershipActions.StuckExit).IsReady);
    }

    [Fact]
    public void Every_operation_a_row_without_a_pending_task_names_has_a_method_in_the_harness()
    {
        var named = StimulusTable.All.Where(r => r.PlacedBy is null).Select(r => r.Operation).OfType<HarnessOperation>().Distinct();

        Assert.All(named, operation => Assert.Contains(operation, HarnessOperations.Implemented));
    }

    [Fact]
    public async Task The_pause_and_resume_operations_freeze_and_thaw_a_replica()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });

        await HarnessOperations.PauseReplica(replica);
        var reaching = replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend).AsTask();
        await Task.Delay(150);

        Assert.False(reaching.IsCompleted);

        await HarnessOperations.ResumeReplica(replica);
        await reaching.WaitAsync(Limit);
    }

    [Fact]
    public async Task The_crash_and_restart_operations_replace_a_process_under_the_same_id()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var first = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });

        await HarnessOperations.CrashReplica(first);
        var second = await HarnessOperations.RestartReplica(run, "r1", new ReplicaSpec { ConnectionString = ConnectionString });

        Assert.True(first.IsCrashed);
        Assert.Same(second, run.Get("r1"));
    }

    [Fact]
    public async Task The_clock_shift_operation_fires_what_fell_due_on_the_replicas_clock()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timer = replica.Clock.CreateTimer(_ => fired.TrySetResult(), null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);

        HarnessOperations.ClockShift(replica, TimeSpan.FromSeconds(31));

        await fired.Task.WaitAsync(Limit);
    }

    private sealed class WidgetReconciler(ReconcilerScript script) : FaultyReconciler("widget", script);

    [Fact]
    public async Task The_reconciler_operations_script_the_replicas_reconcilers_in_the_order_given()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });
        var reconciler = new WidgetReconciler(replica.Reconcilers);
        var context = new ReconcileContext("widget", 1, 1, 1, true, null, null, new StubTransaction(), _ => throw new NotSupportedException());

        HarnessOperations.ReconcilerThrow(replica, "widget", () => new InvalidOperationException("scripted"), 2);
        var hang = HarnessOperations.ReconcilerHang(replica, "widget", HangMode.Stuck);

        await Assert.ThrowsAsync<InvalidOperationException>(() => reconciler.ReconcileAsync(context, CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reconciler.ReconcileAsync(context, CancellationToken.None));
        var hung = reconciler.ReconcileAsync(context, CancellationToken.None);
        await hang.WaitEnteredAsync();

        Assert.False(hung.IsCompleted);
        HarnessOperations.ReconcilerRelease(hang);
        await hung.WaitAsync(Limit);
        Assert.Equal(["threw", "threw", "returned"], replica.Reconcilers.Calls.Select(c => c.Outcome));
    }

    private sealed class StubTransaction : System.Data.Common.DbTransaction
    {
        public override System.Data.IsolationLevel IsolationLevel => System.Data.IsolationLevel.RepeatableRead;

        protected override System.Data.Common.DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }

    [Fact]
    public async Task The_replicas_own_environment_steps_are_recorded_in_the_model_event_log_in_order()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var first = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });

        await first.PauseAsync();
        await first.ResumeAsync();
        await first.CrashAsync();
        await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });

        Assert.Equal(
            [EnvironmentActions.Pause, EnvironmentActions.Resume, EnvironmentActions.Crash, EnvironmentActions.Restart],
            run.Events.Snapshot().Select(e => e.Action));
        Assert.All(run.Events.Snapshot(), e => Assert.Equal("r1", e.State![EnvironmentActions.ReplicaKey]));
    }

    [Fact]
    public async Task A_resume_is_recorded_before_the_woken_replica_can_emit_anything()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });
        var sink = replica.Services.GetRequiredService<IModelEventSink>();
        using var hold = replica.Faults.HoldNext(MembershipFaultPoints.RenewSend);
        var reaching = Task.Run(async () =>
        {
            await replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend);
            sink.Emit(new ModelEvent(MembershipActions.RenewSend));
        });
        await hold.WaitReachedAsync();
        await replica.PauseAsync();
        hold.Release();

        await replica.ResumeAsync();
        await reaching.WaitAsync(Limit);

        Assert.Equal(
            [EnvironmentActions.Pause, EnvironmentActions.Resume, MembershipActions.RenewSend],
            run.Events.Snapshot().Select(e => e.Action));
    }

    [Fact]
    public async Task A_crashed_replicas_crash_is_recorded_and_nothing_after_it()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });
        var sink = replica.Services.GetRequiredService<IModelEventSink>();

        await replica.CrashAsync();
        sink.Emit(new ModelEvent(MembershipActions.RenewExec));

        Assert.Equal([EnvironmentActions.Crash], run.Events.Snapshot().Select(e => e.Action));
    }

    [Fact]
    public async Task The_fix_of_the_fault_clears_the_scripted_faults_of_every_replica_and_is_recorded_while_a_late_return_is_the_codes_step()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var a = await run.StartReplicaAsync("a", new ReplicaSpec { ConnectionString = ConnectionString });
        var b = await run.StartReplicaAsync("b", new ReplicaSpec { ConnectionString = ConnectionString });
        var reconciler = new WidgetReconciler(a.Reconcilers);
        var context = new ReconcileContext("widget", 1, 1, 1, true, null, null, new StubTransaction(), _ => throw new NotSupportedException());
        HarnessOperations.ReconcilerThrow(a, "widget", () => new InvalidOperationException("poison"), 5);
        var pending = HarnessOperations.ReconcilerHang(b, "widget");
        var lateHang = HarnessOperations.ReconcilerHang(a, "gadget");

        HarnessOperations.ReconcilerRelease(lateHang);
        Assert.Empty(run.Events.Snapshot());

        await Assert.ThrowsAsync<InvalidOperationException>(() => reconciler.ReconcileAsync(context, CancellationToken.None));
        HarnessOperations.FixFault(run);

        Assert.Equal([EnvironmentActions.FixFault], run.Events.Snapshot().Select(e => e.Action));
        Assert.Equal("returned", (await reconciler.ReconcileAsync(context, CancellationToken.None).WaitAsync(Limit)) is { } ? "returned" : "?");
        var onB = new WidgetReconciler(b.Reconcilers);
        await onB.ReconcileAsync(context, CancellationToken.None).WaitAsync(Limit);
        Assert.Equal(0, pending.Entered);
    }

    [Fact]
    public async Task A_stop_of_an_early_replica_records_stop_early_and_a_stop_in_service_leaves_the_drain_to_the_library()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var early = await run.StartReplicaAsync("early", new ReplicaSpec { ConnectionString = ConnectionString });
        var serving = await run.StartReplicaAsync("serving", new ReplicaSpec { ConnectionString = ConnectionString });

        await HarnessOperations.StopReplicaEarly(early);
        await HarnessOperations.StopReplica(serving);

        Assert.Equal([EnvironmentActions.StopEarly], run.Events.Snapshot().Select(e => e.Action));
    }

    [Fact]
    public async Task A_fatal_fault_reaches_the_gate_of_the_membership_area()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString });
        var gate = replica.Services.GetRequiredService<IPhaseGate>();

        HarnessOperations.ReportFatalFault(replica, "test");

        Assert.Contains(gate.Phase, new[] { ReplicaPhase.Exited, ReplicaPhase.Fencing });
    }
}
