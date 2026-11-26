using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Membership;

[Collection(FenceOverrunCollection.Name)]
public sealed class FenceCoordinatorTests : IDisposable
{
    private const long Epoch = 7;
    private const long Timeline = 1;
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly SignallingTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly ScriptedFenceStore store = new();
    private readonly FencePassConnection pass = new();
    private readonly List<string> order = [];
    private readonly Dictionary<FenceStep, ScriptedParticipant> participants;
    private readonly LocalPhaseGate gate;
    private readonly FenceCoordinator coordinator;

    public FenceCoordinatorTests()
    {
        var options = Options.Create(MembershipTestOptions.Default());
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        gate.OnStepZero(_ => Record("step0"));
        participants = Enum.GetValues<FenceStep>().ToDictionary(s => s, s => new ScriptedParticipant(s, Record));
        store.OnRelease = () => Record("release");
        store.OnRecord = () => Record("record");
        coordinator = new FenceCoordinator(
            gate, participants.Values, store, pass, options, time, NoFaultPoints.Instance, events, NullLogger<FenceCoordinator>.Instance);
    }

    public void Dispose()
    {
        foreach (var participant in participants.Values)
        {
            participant.Release();
        }

        store.ReleaseAll();
        coordinator.Dispose();
        gate.Dispose();
    }

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private static Task OffContext(Func<Task> test) => Task.Run(test);

    private void Record(string what)
    {
        lock (order)
        {
            order.Add(what);
        }
    }

    private List<string> Order()
    {
        lock (order)
        {
            return [.. order];
        }
    }

    private void Join(long epoch = Epoch, long timeline = Timeline) => Assert.True(gate.Joined(epoch, timeline, gate.Now));

    private void ToServing()
    {
        Join();
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
    }

    private async Task AwaitPhase(ReplicaPhase phase)
    {
        while (true)
        {
            var changed = gate.NextPhaseChange;
            if (gate.Phase == phase)
            {
                return;
            }

            await changed.WaitAsync(Wait);
        }
    }

    private void AssertStepOrder()
    {
        var seen = Order();
        var steps = seen.Where(s => s is not "record" and not "step3").ToList();
        Assert.Equal(["step0", "step1", "step2", "release", "step4", "step5", "step6"], steps);
        var step3 = seen.IndexOf("step3");
        Assert.True(step3 > seen.IndexOf("step2") && step3 < seen.IndexOf("step4"), string.Join(",", seen));
        var record = seen.IndexOf("record");
        Assert.True(record > seen.IndexOf("step4") && record < seen.IndexOf("step6"), string.Join(",", seen));
    }

    private IReadOnlyList<int> Steps() => [.. events.Of(MembershipActions.FStep).Select(e => (int)e.State!["k"]!)];

    private static OwnRowObservation OwnFencedRow(long epoch = Epoch) => new(true, epoch, MemberRowState.Fenced, false);

    [Fact]
    public async Task I2_step_0_comes_before_step_3_and_the_steps_run_in_order_to_Fenced()
    {
        await OffContext(async () =>
        {
            ToServing();

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Fenced);

            AssertStepOrder();
            Assert.Equal([1, 2, 3, 4, 5, 6], Steps());
            Assert.Equal(Epoch, Assert.Single(store.Releases));
            var actions = events.All.Select(e => e.Action).ToList();
            Assert.True(actions.IndexOf(MembershipActions.FenceAtStart) < actions.IndexOf(MembershipActions.FStep));
        });
    }

    [Fact]
    public async Task The_fence_runs_in_no_caller_scope_whichever_flow_began_it()
    {
        await OffContext(async () =>
        {
            var scopes = new ClusterScopeSource(new FenceSteps());
            var seen = new List<(string Where, IClusterScope? Scope)>();
            foreach (var participant in participants.Values)
            {
                var step = participant.Step;
                participant.OnRun = () => { lock (seen) { seen.Add(($"step{(int)step}", scopes.Current)); } };
            }

            store.OnRelease = () => { lock (seen) { seen.Add(("release", scopes.Current)); } };
            store.OnRecord = () => { lock (seen) { seen.Add(("record", scopes.Current)); } };
            ToServing();

            using (scopes.Begin(ClusterScopeKind.ClientRequest))
            {
                Assert.NotNull(scopes.Current);
                gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

                await AwaitPhase(ReplicaPhase.Fenced);
            }

            lock (seen)
            {
                Assert.Equal(8, seen.Count);
                Assert.All(seen, e => Assert.True(e.Scope is null, $"{e.Where} ran in scope {e.Scope}"));
            }
        });
    }

    [Fact]
    public async Task I32_a_timeline_fence_from_a_pass_runs_step_3_on_the_new_primary()
    {
        await OffContext(async () =>
        {
            Join();
            gate.Apply(MemberTrigger.StateLoaded);

            Assert.False(gate.CheckTimeline(Epoch, Timeline + 1));
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Equal(Epoch, Assert.Single(store.Releases));
            Assert.Equal("timeline", Assert.Single(store.Records).Trigger);
            Assert.Equal(true, events.Of(MembershipActions.FStep).Single(e => (int)e.State!["k"]! == 3).State!["released"]);
        });
    }

    [Fact]
    public async Task Step_3_is_skipped_after_a_zero_row_renewal()
    {
        await OffContext(async () =>
        {
            ToServing();

            gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.NotMine);
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Empty(store.Releases);
            Assert.DoesNotContain("release", Order());
            Assert.Equal([1, 2, 3, 4, 5, 6], Steps());
            Assert.Equal(true, events.Of(MembershipActions.FStep).Single(e => (int)e.State!["k"]! == 3).State!["skip3"]);
            Assert.Equal("zero-row-renewal", Assert.Single(store.Records).Trigger);
        });
    }

    [Fact]
    public async Task Step_3_is_cancelled_at_RenewTimeout_from_step_0_and_the_fence_goes_on_unanswered()
    {
        await OffContext(async () =>
        {
            store.HangRelease = true;
            ToServing();

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await store.ReleaseCalled.WaitAsync(Wait);

            time.Advance(MembershipTestOptions.Default().RenewTimeout - TimeSpan.FromTicks(1));
            Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
            Assert.False(store.ReleaseCancelled.IsCompleted);

            time.Advance(TimeSpan.FromTicks(1));
            await store.ReleaseCancelled.WaitAsync(Wait);
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Equal(false, events.Of(MembershipActions.FStep).Single(e => (int)e.State!["k"]! == 3).State!["released"]);
            Assert.Single(store.Records);
        });
    }

    [Fact]
    public async Task StopBudget_steps_4_and_5_that_overrun_are_counted_and_not_awaited()
    {
        await OffContext(async () =>
        {
            using var overruns = new OverrunListener();
            participants[FenceStep.AbortConnections].Hang();
            participants[FenceStep.StopUnits].Hang();
            bool? step4CancelledAtStep5 = null;
            participants[FenceStep.StopUnits].OnRun = () => step4CancelledAtStep5 = participants[FenceStep.AbortConnections].Token.IsCancellationRequested;
            ToServing();

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await participants[FenceStep.AbortConnections].Started.WaitAsync(Wait);

            time.Advance(S(5) - TimeSpan.FromTicks(1));
            Assert.False(participants[FenceStep.StopUnits].Started.IsCompleted);
            time.Advance(TimeSpan.FromTicks(1));
            await participants[FenceStep.StopUnits].Started.WaitAsync(Wait);

            Assert.True(step4CancelledAtStep5);
            await participants[FenceStep.AbortConnections].Cancelled.WaitAsync(Wait);

            time.Advance(S(5));
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Equal([4, 5], overruns.Steps());
            Assert.False(participants[FenceStep.AbortConnections].Returned.IsCompleted);
            Assert.Single(store.Records);
        });
    }

    [Fact]
    public async Task The_fence_history_record_carries_the_trigger_the_epoch_the_time_of_step_0_and_the_span_from_the_join()
    {
        await OffContext(async () =>
        {
            time.Advance(S(3));
            Join();
            time.Advance(S(11));
            participants[FenceStep.CloseTraffic].Hang();

            gate.ReportFatalFault("disk full");
            await participants[FenceStep.CloseTraffic].Started.WaitAsync(Wait);
            time.Advance(S(2));
            participants[FenceStep.CloseTraffic].Release();
            await AwaitPhase(ReplicaPhase.Fenced);

            var record = Assert.Single(store.Records);
            Assert.Equal(new FenceRecord("r1", Epoch, "fatal-fault", S(11)), record);
            Assert.Equal(S(2), Assert.Single(store.RecordAges));
        });
    }

    [Fact]
    public async Task Step_6_disposes_of_the_pass_connection_without_awaiting_a_dispose_that_does_not_return()
    {
        await OffContext(async () =>
        {
            using var overruns = new OverrunListener();
            var connection = new HangingConnection();
            using var use = pass.Use(connection);
            ToServing();

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await connection.DisposeStarted.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Fencing, gate.Phase);

            time.Advance(S(5));
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Equal([6], overruns.Steps());
            Assert.Null(pass.Take());
        });
    }

    [Fact]
    public async Task Step_6_awaits_a_dispose_that_returns()
    {
        await OffContext(async () =>
        {
            var connection = new HangingConnection { Returns = true };
            using var use = pass.Use(connection);
            ToServing();

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.True(connection.Disposed);
        });
    }

    [Fact]
    public async Task Rejoin_at_once_when_the_last_span_lasted_LeaseTtl_and_after_an_unanswered_read_RenewInterval_later()
    {
        await OffContext(async () =>
        {
            ToServing();
            time.Advance(S(30));
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

            var read = await store.NextRead();
            var retry = time.DelayArmed(S(3));
            read.Fail(new TimeoutException("network"));
            await retry.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
            time.Advance(S(3));
            (await store.NextRead()).Answer(OwnFencedRow());

            await AwaitPhase(ReplicaPhase.Starting);
        });
    }

    [Fact]
    public async Task Exit_on_a_higher_epoch_the_process_never_takes_the_row_back()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.NotMine);

            var taken = new OwnRowObservation(true, Epoch + 3, MemberRowState.Joining, true);
            (await store.NextRead()).Answer(taken);

            await AwaitPhase(ReplicaPhase.Exited);
            Assert.Equal(true, Assert.Single(events.Of(MembershipActions.Rejoin)).State!["exit"]);
        });
    }

    [Fact]
    public async Task A_rejoin_waits_while_its_own_row_holds_a_valid_lease_the_release_did_not_land()
    {
        await OffContext(async () =>
        {
            ToServing();
            time.Advance(S(30));
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

            var read = await store.NextRead();
            var retry = time.DelayArmed(S(3));
            read.Answer(new OwnRowObservation(true, Epoch, MemberRowState.Active, true));
            await retry.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
            time.Advance(S(3));
            (await store.NextRead()).Answer(OwnFencedRow());

            await AwaitPhase(ReplicaPhase.Starting);
        });
    }

    [Fact]
    public async Task SIMP11_a_fenced_replica_waits_RejoinBackoff_after_a_short_span_held_in_memory_whatever_fence_history_holds()
    {
        await OffContext(async () =>
        {
            ToServing();
            time.Advance(S(5));
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Fenced);

            var read = await store.NextRead();
            var backoff = time.DelayArmed(S(60));
            read.Answer(OwnFencedRow());
            await backoff.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);

            time.Advance(S(60) - TimeSpan.FromTicks(1));
            Assert.False(store.HasPendingRead);
            time.Advance(TimeSpan.FromTicks(1));
            (await store.NextRead()).Answer(OwnFencedRow());
            await AwaitPhase(ReplicaPhase.Starting);
            Assert.Equal(false, Assert.Single(events.Of(MembershipActions.Rejoin)).State!["exit"]);
        });
    }

    [Fact]
    public async Task SIMP11_RejoinBackoff_counts_from_step_0_not_from_the_entry_into_Fenced()
    {
        await OffContext(async () =>
        {
            ToServing();
            time.Advance(S(5));
            participants[FenceStep.StopApplier].Hang();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await participants[FenceStep.StopApplier].Started.WaitAsync(Wait);
            time.Advance(S(20));
            participants[FenceStep.StopApplier].Release();

            var read = await store.NextRead();
            var backoff = time.DelayArmed(S(40));
            read.Answer(OwnFencedRow());
            await backoff.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);

            time.Advance(S(40) - TimeSpan.FromTicks(1));
            Assert.False(store.HasPendingRead);
            time.Advance(TimeSpan.FromTicks(1));
            (await store.NextRead()).Answer(OwnFencedRow());
            await AwaitPhase(ReplicaPhase.Starting);
        });
    }

    [Fact]
    public async Task SIMP11_a_fence_history_record_that_does_not_answer_delays_neither_the_rejoin_nor_the_exit()
    {
        await OffContext(async () =>
        {
            store.RecordFailures = int.MaxValue;
            ToServing();
            time.Advance(S(30));
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

            (await store.NextRead()).Answer(OwnFencedRow());

            await AwaitPhase(ReplicaPhase.Starting);
            Assert.Equal(1, store.RecordCalls);
            Assert.Empty(store.Records);

            Join(Epoch + 1);
            gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
            await AwaitPhase(ReplicaPhase.Exited);
            Assert.Equal(2, store.RecordCalls);
            Assert.Empty(store.Records);
        });
    }

    [Fact]
    public async Task Stuck_a_stuck_attempt_ends_the_process_at_step_6_after_the_lease_release()
    {
        await OffContext(async () =>
        {
            var exits = 0;
            var exit = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            ToServing();
            gate.Subscribe(e =>
            {
                if (e.To == ReplicaPhase.Exited)
                {
                    Interlocked.Increment(ref exits);
                    exit.TrySetResult($"exit after {string.Join(",", Order())}");
                }
            });

            gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
            var exitedAfter = await exit.Task.WaitAsync(Wait);
            await AwaitPhase(ReplicaPhase.Exited);

            Assert.Equal(1, Volatile.Read(ref exits));
            Assert.EndsWith(",step6", exitedAfter);
            AssertStepOrder();
            Assert.Equal("stuck-attempt", Assert.Single(store.Records).Trigger);
            Assert.Equal([MembershipActions.FStep, MembershipActions.StuckExit], events.All.Select(e => e.Action).TakeLast(2));
            Assert.False(store.HasPendingRead);
        });
    }

    [Fact]
    public async Task Stuck_an_attempt_stuck_in_Fenced_ends_the_process_instead_of_a_rejoin()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            var read = await store.NextRead();
            var backoff = time.DelayArmed(S(60));
            read.Answer(OwnFencedRow());
            await backoff.WaitAsync(Wait);

            gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
            time.Advance(S(60));

            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            await coordinator.Running!.WaitAsync(Wait);
            Assert.False(store.HasPendingRead);
            Assert.Empty(events.Of(MembershipActions.Rejoin));
        });
    }

    [Theory]
    [InlineData(ReplicaPhase.Fencing)]
    [InlineData(ReplicaPhase.Fenced)]
    public async Task S1_a_fatal_fault_reported_in_Fencing_exits_at_step_6_and_one_in_Fenced_at_once(ReplicaPhase reportedIn)
    {
        await OffContext(async () =>
        {
            participants[FenceStep.RefuseCommits].Hang();
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await participants[FenceStep.RefuseCommits].Started.WaitAsync(Wait);

            if (reportedIn == ReplicaPhase.Fenced)
            {
                participants[FenceStep.RefuseCommits].Release();
                var read = await store.NextRead();
                gate.ReportFatalFault("disk full");
                read.Answer(OwnFencedRow());
            }
            else
            {
                gate.ReportFatalFault("disk full");
                participants[FenceStep.RefuseCommits].Release();
            }

            await AwaitPhase(ReplicaPhase.Exited);
            await coordinator.Running!.WaitAsync(Wait);

            Assert.Empty(events.Of(MembershipActions.Rejoin));
            Assert.Equal(MembershipActions.FencedExit, events.All[^1].Action);
            Assert.Single(store.Records);
        });
    }

    [Fact]
    public async Task S2_a_fence_that_took_over_a_drain_exits_at_step_6()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.Apply(MemberTrigger.GracefulStop);

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Exited);

            Assert.Contains("release", Order());
            Assert.Equal(MembershipActions.FencedExit, events.All[^1].Action);
        });
    }

    [Fact]
    public async Task A_graceful_stop_in_Fencing_lets_the_steps_finish_then_exits()
    {
        await OffContext(async () =>
        {
            participants[FenceStep.CloseTraffic].Hang();
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await participants[FenceStep.CloseTraffic].Started.WaitAsync(Wait);

            gate.Apply(MemberTrigger.GracefulStop);
            var stop = coordinator.StopAsync(CancellationToken.None);
            Assert.False(stop.IsCompleted);

            participants[FenceStep.CloseTraffic].Release();
            await stop.WaitAsync(Wait);

            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            AssertStepOrder();
            Assert.False(store.HasPendingRead);
        });
    }

    [Fact]
    public async Task A_graceful_stop_in_Fenced_ends_the_wait_for_the_rejoin()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            var read = await store.NextRead();
            var backoff = time.DelayArmed(S(60));
            read.Answer(OwnFencedRow());
            await backoff.WaitAsync(Wait);

            await coordinator.StopAsync(CancellationToken.None).WaitAsync(Wait);

            time.Advance(S(120));
            Assert.False(store.HasPendingRead);
            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
        });
    }

    [Fact]
    public async Task I23_a_fence_is_never_undone_a_trigger_during_the_steps_starts_no_second_run()
    {
        await OffContext(async () =>
        {
            participants[FenceStep.RefuseCommits].Hang();
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await participants[FenceStep.RefuseCommits].Started.WaitAsync(Wait);

            gate.ApplyRenewal(new RenewalAttempt(Epoch, gate.Now), RenewalOutcome.NotMine);
            gate.RequestFence(new FenceRequest(FenceTrigger.Timeline));
            participants[FenceStep.RefuseCommits].Release();
            await AwaitPhase(ReplicaPhase.Fenced);

            Assert.Equal(1, participants[FenceStep.CloseTraffic].Runs);
            Assert.Equal(Epoch, Assert.Single(store.Releases));
            Assert.Equal("fence-start", Assert.Single(store.Records).Trigger);
        });
    }

    [Fact]
    public async Task The_coordinator_runs_the_participants_registered_for_each_step_with_the_fences_request()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.ReportFatalFault("disk full");
            await AwaitPhase(ReplicaPhase.Fenced);

            foreach (var participant in participants.Values)
            {
                Assert.Equal(FenceTrigger.FatalFault, Assert.Single(participant.Contexts).Request.Trigger);
            }
        });
    }

    [Fact]
    public async Task An_unexpected_rejoin_wait_failure_exits_from_Fenced()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Fenced);

            var read = await store.NextRead();
            time.ThrowOnTimestamp = true;
            read.Answer(OwnFencedRow());

            await AwaitPhase(ReplicaPhase.Exited);
        });
    }

    [Fact]
    public async Task Stopping_with_a_rejoin_read_pending_leaves_the_gate_Fenced()
    {
        await OffContext(async () =>
        {
            ToServing();
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            await AwaitPhase(ReplicaPhase.Fenced);
            await store.NextRead();

            coordinator.Dispose();

            Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
        });
    }

    private sealed class ScriptedParticipant(FenceStep step, Action<string> record) : IFenceParticipant
    {
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource returned = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool hang;

        public FenceStep Step => step;

        public Task Started => started.Task;

        public Task Cancelled => cancelled.Task;

        public Task Returned => returned.Task;

        public List<FenceContext> Contexts { get; } = [];

        public CancellationToken Token { get; private set; }

        public Action? OnRun { get; set; }

        public int Runs => Contexts.Count;

        public void Hang() => hang = true;

        public void Release() => release.TrySetResult();

        public async ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken)
        {
            Contexts.Add(context);
            record($"step{(int)step}");
            Token = cancellationToken;
            cancellationToken.Register(() => cancelled.TrySetResult());
            OnRun?.Invoke();
            started.TrySetResult();
            if (hang)
            {
                await release.Task;
            }

            returned.TrySetResult();
        }
    }

    private sealed class HangingConnection : IAsyncDisposable
    {
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Returns { get; init; }

        public bool Disposed { get; private set; }

        public Task DisposeStarted => started.Task;

        public async ValueTask DisposeAsync()
        {
            started.TrySetResult();
            if (!Returns)
            {
                await Task.Delay(Timeout.Infinite);
            }

            Disposed = true;
        }
    }

    private sealed class OverrunListener : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly List<int> steps = [];

        public OverrunListener()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == ClusterDiagnostics.Name && instrument.Name == ClusterDiagnostics.FenceStepOverrunsName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "step")
                    {
                        lock (steps)
                        {
                            steps.Add((int)tag.Value!);
                        }
                    }
                }
            });
            listener.Start();
        }

        public IReadOnlyList<int> Steps()
        {
            lock (steps)
            {
                return [.. steps.Order()];
            }
        }

        public void Dispose()
        {
            listener.Dispose();
        }
    }
}

internal sealed class ScriptedFenceStore : IFenceStore
{
    private readonly Channel<PendingRead> reads = Channel.CreateUnbounded<PendingRead>();
    private readonly TaskCompletionSource releaseCalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource releaseCancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource recordCancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource recordCalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<TaskCompletionSource<bool>> hanging = [];

    public List<long> Releases { get; } = [];

    public List<FenceRecord> Records { get; } = [];

    public List<TimeSpan> RecordAges { get; } = [];

    public int RecordCalls { get; private set; }

    public int RecordFailures { get; set; }

    public bool HangRelease { get; set; }

    public bool HangFirstRecord { get; set; }

    public Task RecordCancelled => recordCancelled.Task;

    public Task RecordCalled => recordCalled.Task;

    public Action? OnRelease { get; set; }

    public Action? OnRecord { get; set; }

    public Task ReleaseCalled => releaseCalled.Task;

    public Task ReleaseCancelled => releaseCancelled.Task;

    public bool HasPendingRead => reads.Reader.Count > 0;

    public async Task<PendingRead> NextRead() => await reads.Reader.ReadAsync().AsTask().WaitAsync(Bounds.Safety);

    public void ReleaseAll()
    {
        lock (hanging)
        {
            foreach (var call in hanging)
            {
                call.TrySetCanceled();
            }
        }
    }

    public Task<bool> ReleaseLeaseAsync(string replicaId, long epoch, CancellationToken cancellationToken)
    {
        OnRelease?.Invoke();
        lock (Releases)
        {
            Releases.Add(epoch);
        }

        releaseCalled.TrySetResult();
        if (!HangRelease)
        {
            return Task.FromResult(true);
        }

        var call = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (hanging)
        {
            hanging.Add(call);
        }

        cancellationToken.Register(() =>
        {
            releaseCancelled.TrySetResult();
            call.TrySetCanceled(cancellationToken);
        });
        return call.Task;
    }

    public Task RecordFenceAsync(FenceRecord record, TimeSpan age, CancellationToken cancellationToken)
    {
        RecordCalls++;
        recordCalled.TrySetResult();
        if (HangFirstRecord && RecordCalls == 1)
        {
            var call = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(() =>
            {
                recordCancelled.TrySetResult();
                call.TrySetCanceled(cancellationToken);
            });
            return call.Task;
        }

        if (RecordFailures > 0)
        {
            RecordFailures--;
            return Task.FromException(new TimeoutException("network"));
        }

        OnRecord?.Invoke();
        lock (Records)
        {
            Records.Add(record);
            RecordAges.Add(age);
        }

        return Task.CompletedTask;
    }

    public Task<OwnRowObservation> ReadRejoinStateAsync(string replicaId, CancellationToken cancellationToken)
    {
        var read = new PendingRead();
        reads.Writer.TryWrite(read);
        return read.Task;
    }
}

internal sealed class PendingRead
{
    private readonly TaskCompletionSource<OwnRowObservation> answer = new();

    public Task<OwnRowObservation> Task => answer.Task;

    public void Answer(OwnRowObservation row) => answer.SetResult(row);

    public void Fail(Exception exception) => answer.SetException(exception);
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class FenceOverrunCollection
{
    public const string Name = "fence-overruns";
}
