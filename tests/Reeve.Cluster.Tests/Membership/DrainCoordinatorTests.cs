using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class DrainCoordinatorTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly ScriptedMemberStore store = new();
    private readonly LocalPhaseGate gate;
    private readonly List<string> order = [];
    private readonly ClusterOptions options = MembershipTestOptions.Default();

    public DrainCoordinatorTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, events, NullLogger<LocalPhaseGate>.Instance);
        store.OnStatement = Record;
        gate.Subscribe(e =>
        {
            if (e.Kind == PhaseEventKind.DrainStep4)
            {
                Record("drain-step-4");
            }
            else if (e.To == ReplicaPhase.Exited)
            {
                Record("exited");
            }
        });
    }

    public void Dispose() => gate.Dispose();

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

    private DrainCoordinator Coordinator(IFaultPoints? points = null, params IDrainParticipant[] participants) =>
        new(gate, participants, store, Options.Create(options), time, points ?? NoFaultPoints.Instance, events, NullLogger<DrainCoordinator>.Instance);

    private RecordingParticipant Participant(DrainStep step, string name, Func<CancellationToken, Task>? body = null) => new(step, ct =>
    {
        Record(name);
        return body?.Invoke(ct) ?? Task.CompletedTask;
    });

    private void ToCatchingUp()
    {
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
    }

    private void ToActivated()
    {
        ToCatchingUp();
        gate.Apply(MemberTrigger.ActivationCommitted);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    private void ToServing()
    {
        ToActivated();
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private List<string> DrainActions() =>
        [.. events.All.Select(e => e.Action).Where(a => a is MembershipActions.Drain1 or MembershipActions.Drain23 or MembershipActions.Drain4)];

    [Fact]
    public async Task M12_M13_from_Serving_steps_1_to_4_run_in_order_and_the_draining_write_comes_after_the_step_4_event()
    {
        ToServing();
        var phaseAtStep1 = ReplicaPhase.Serving;
        var drain = Coordinator(
            null,
            Participant(DrainStep.CloseTraffic, "close", _ =>
            {
                phaseAtStep1 = gate.Phase;
                return Task.CompletedTask;
            }),
            Participant(DrainStep.ReleaseUnits, "units"),
            Participant(DrainStep.AwaitRequests, "requests"));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Draining, phaseAtStep1);
        Assert.Equal(["close", "clear-serving-since", "units", "requests", "drain-step-4", "draining", "exited"], Order());
        Assert.Equal([Epoch], store.ServingSinceClears);
        Assert.Equal([Epoch], store.DrainingWrites);
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public async Task M12_the_drain_emits_Drain1_Drain23_twice_and_Drain4()
    {
        ToServing();

        await Coordinator().StopAsync().WaitAsync(Wait);

        Assert.Equal([MembershipActions.Drain1, MembershipActions.Drain23, MembershipActions.Drain23, MembershipActions.Drain4], DrainActions());
        Assert.Equal(Epoch, events.All.Single(e => e.Action == MembershipActions.Drain4).State!["epoch"]);
    }

    [Fact]
    public async Task M12_renewal_keeps_running_through_step_4_until_the_draining_write()
    {
        ToServing();
        var renewingAtStep4 = false;
        var drain = Coordinator(null, Participant(DrainStep.AwaitRequests, "requests", ct =>
        {
            renewingAtStep4 = gate.TryGetRenewal(out _);
            return Task.CompletedTask;
        }));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.True(renewingAtStep4);
        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public async Task M12_step_2_is_not_awaited_past_StopBudget()
    {
        ToServing();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drain = Coordinator(null, Participant(DrainStep.ReleaseUnits, "units", async _ =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, CancellationToken.None);
        }));

        var stop = Task.Run(drain.StopAsync);
        await entered.Task.WaitAsync(Wait);
        Assert.False(stop.IsCompleted);
        time.Advance(options.StopBudget);

        await stop.WaitAsync(Wait);
        Assert.Equal([Epoch], store.DrainingWrites);
    }

    [Fact]
    public async Task S2_a_fence_during_the_drain_takes_over_the_draining_write_is_skipped_and_the_fence_exits_at_step_6()
    {
        ToServing();
        var drain = Coordinator(null, Participant(DrainStep.AwaitRequests, "requests", _ =>
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            return Task.CompletedTask;
        }));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Empty(store.DrainingWrites);
        Assert.DoesNotContain("drain-step-4", Order());
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(gate.CurrentFence!.Effects.HasFlag(MemberEffects.SkipDrainRowWrite));
        Assert.Equal(ReplicaPhase.Exited, gate.CompleteFence());
    }

    [Fact]
    public async Task S2_a_fence_at_the_point_before_the_draining_write_skips_the_write()
    {
        ToServing();
        var points = new FenceAt(gate, MembershipFaultPoints.DrainBeforeDrainingWrite.Name);

        await Coordinator(points).StopAsync().WaitAsync(Wait);

        Assert.Empty(store.DrainingWrites);
        Assert.DoesNotContain(MembershipActions.Drain4, DrainActions());
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task S2_a_fence_after_step_1_skips_steps_2_and_4()
    {
        ToServing();
        var points = new FenceAt(gate, MembershipFaultPoints.DrainAfterStep1.Name);
        var drain = Coordinator(points, Participant(DrainStep.ReleaseUnits, "units"), Participant(DrainStep.AwaitRequests, "requests"));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Equal(["clear-serving-since"], Order());
        Assert.Empty(store.DrainingWrites);
    }

    [Fact]
    public async Task S2_a_fence_while_step_2_runs_takes_over_and_no_Drain23_is_emitted()
    {
        ToServing();
        var units = Participant(DrainStep.ReleaseUnits, "units", _ =>
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            return Task.CompletedTask;
        });

        await Coordinator(null, units, Participant(DrainStep.AwaitRequests, "requests")).StopAsync().WaitAsync(Wait);

        Assert.Equal([MembershipActions.Drain1], DrainActions());
        Assert.DoesNotContain("requests", Order());
        Assert.Empty(store.DrainingWrites);
    }

    [Fact]
    public async Task After_the_drain_has_exited_the_fence_timer_emits_nothing()
    {
        ToServing();
        await Coordinator().StopAsync().WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        var before = events.All.Count;

        time.Advance(options.LeaseTtl);

        Assert.Empty(events.All.Skip(before));
    }

    [Fact]
    public async Task After_a_stop_in_Activated_the_fence_timer_emits_nothing()
    {
        ToActivated();
        await Coordinator().StopAsync().WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        var before = events.All.Count;

        time.Advance(options.LeaseTtl);

        Assert.Empty(events.All.Skip(before));
    }

    [Fact]
    public async Task M13_a_draining_write_the_database_does_not_answer_still_ends_the_drain_in_Exited()
    {
        ToServing();
        store.DrainingFailure = new TimeoutException("no answer");

        await Coordinator().StopAsync().WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
    }

    [Fact]
    public async Task M15_from_Activated_steps_2_to_4_run_with_no_traffic_to_close_and_the_row_is_written_draining()
    {
        ToActivated();
        var drain = Coordinator(
            null,
            Participant(DrainStep.CloseTraffic, "close"),
            Participant(DrainStep.ReleaseUnits, "units"),
            Participant(DrainStep.AwaitRequests, "requests"));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Equal(["exited", "units", "requests", "draining"], Order());
        Assert.Empty(store.ServingSinceClears);
        Assert.Equal([Epoch], store.DrainingWrites);
        Assert.Empty(DrainActions());
    }

    [Fact]
    public async Task M14_from_CatchingUp_the_row_is_written_draining_and_nothing_else_runs()
    {
        ToCatchingUp();
        var drain = Coordinator(null, Participant(DrainStep.CloseTraffic, "close"), Participant(DrainStep.ReleaseUnits, "units"), Participant(DrainStep.AwaitRequests, "requests"));

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Equal(["exited", "draining"], Order());
        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
    }

    [Fact]
    public async Task M16_from_Starting_nothing_is_written()
    {
        await Coordinator(null, Participant(DrainStep.ReleaseUnits, "units")).StopAsync().WaitAsync(Wait);

        Assert.Equal(["exited"], Order());
        Assert.Empty(store.DrainingWrites);
    }

    [Fact]
    public async Task M16_from_Fencing_nothing_is_written_and_the_fence_is_marked_to_exit_at_step_6()
    {
        ToServing();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

        await Coordinator(null, Participant(DrainStep.ReleaseUnits, "units")).StopAsync().WaitAsync(Wait);

        Assert.Empty(Order());
        Assert.True(gate.ExitAfterFence);
    }

    [Fact]
    public async Task A_stop_from_Serving_past_fence_start_fences_first_and_writes_nothing()
    {
        var paused = new LocalPhaseGate(Options.Create(options), new TimerlessTimeProvider(time), events, NullLogger<LocalPhaseGate>.Instance);
        Assert.True(paused.Joined(Epoch, 1, paused.Now));
        paused.Apply(MemberTrigger.StateLoaded);
        paused.Apply(MemberTrigger.ActivationCommitted);
        paused.Apply(MemberTrigger.AppliedToActivationHead);
        time.Advance(options.LeaseTtl);
        Assert.Equal(ReplicaPhase.Serving, paused.Phase);
        var drain = new DrainCoordinator(paused, [], store, Options.Create(options), time, NoFaultPoints.Instance, events, NullLogger<DrainCoordinator>.Instance);

        await drain.StopAsync().WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Fencing, paused.Phase);
        Assert.True(paused.ExitAfterFence);
        Assert.Empty(store.ServingSinceClears);
        Assert.Empty(store.DrainingWrites);
        paused.Dispose();
    }

    private sealed class RecordingParticipant(DrainStep step, Func<CancellationToken, Task> body) : IDrainParticipant
    {
        public DrainStep Step { get; } = step;

        public async ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken) => await body(cancellationToken);
    }

    private sealed class FenceAt(LocalPhaseGate gate, string point) : IFaultPoints
    {
        public ValueTask ReachAsync(string name, CancellationToken cancellationToken = default)
        {
            if (name == point)
            {
                gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            }

            return ValueTask.CompletedTask;
        }
    }
}
