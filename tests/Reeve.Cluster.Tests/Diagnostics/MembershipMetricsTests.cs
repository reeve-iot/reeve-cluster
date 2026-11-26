using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Unit.Membership;
using Unit.Testing;
using Xunit;

namespace Unit.Diagnostics;

[Collection(MetricsCollection.Name)]
public sealed class MembershipMetricsTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(5);

    [Fact]
    public void Replica_phase_gauge_reports_nine_values_and_tracks_a_gate_until_disposed()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.ReplicaPhases);
        var time = new FakeTimeProvider();
        var before = ReadPhases(recorder);
        var gate = new LocalPhaseGate(Options.Create(MembershipTestOptions.Default()), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        try
        {
            var starting = ReadPhases(recorder);
            Assert.Equal(before["starting"] + 1, starting["starting"]);
            Assert.Equal(9, starting.Count);

            Assert.True(gate.Joined(5, 1, gate.Now));
            gate.Apply(MemberTrigger.StateLoaded);
            gate.Apply(MemberTrigger.ActivationCommitted);
            gate.Apply(MemberTrigger.AppliedToActivationHead);
            var serving = ReadPhases(recorder);
            Assert.Equal(before["serving"] + 1, serving["serving"]);
            Assert.Equal(before["starting"], serving["starting"]);

            gate.Dispose();
            var disposed = ReadPhases(recorder);
            Assert.Equal(before.Values.Sum(), disposed.Values.Sum());
        }
        finally
        {
            gate.Dispose();
        }
    }

    [Fact]
    public void Fence_counter_counts_only_the_trigger_that_started_step_zero()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.Fences);
        using var gate = ServingGate();
        gate.ApplyRenewal(new RenewalAttempt(5, gate.Now), RenewalOutcome.NotMine);
        gate.ReportFatalFault("second trigger during fence");
        var one = Assert.Single(recorder.Measurements());
        Assert.Equal(1, one.Value);
        Assert.Equal("zero_row_renewal", one.Tags["trigger"]);

        using var starting = new LocalPhaseGate(Options.Create(MembershipTestOptions.Default()), new FakeTimeProvider(), new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        starting.ReportFatalFault("starting exits without a fence");
        Assert.Single(recorder.Measurements());
    }

    [Fact]
    public async Task Renewal_duration_records_renewed_unknown_and_not_mine_outcomes()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.RenewalDuration);
        await RunRenewalAsync(recorder, RenewalOutcome.Renewed, TimeSpan.FromSeconds(1.5));
        await RunRenewalAsync(recorder, RenewalOutcome.Unknown, TimeSpan.FromSeconds(2));
        await RunRenewalAsync(recorder, RenewalOutcome.NotMine, TimeSpan.Zero);
        await RunRenewalAsync(recorder, RenewalOutcome.OtherTimeline, TimeSpan.Zero);

        var results = recorder.Measurements();
        Assert.Equal(4, results.Count);
        Assert.Equal(1.5, results[0].Value);
        Assert.Equal("renewed", results[0].Tags["outcome"]);
        Assert.True(results[1].Value >= 2);
        Assert.Equal("unknown", results[1].Tags["outcome"]);
        Assert.Equal("not_mine", results[2].Tags["outcome"]);
        Assert.Equal("other_timeline", results[3].Tags["outcome"]);
    }

    [Fact]
    public async Task Full_load_duration_records_loaded_and_failed_attempts_on_fake_time()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.FullLoadDuration);
        var loaded = await RunLoadAsync(recorder, LoadOutcome.Loaded, TimeSpan.FromSeconds(7));
        Assert.Equal(7, Assert.Single(recorder.Measurements()).Value);
        Assert.Equal("loaded", Assert.Single(recorder.Measurements()).Tags["outcome"]);
        await loaded.DisposeAsync();

        using var failedRecorder = new InstrumentRecorder(ClusterDiagnostics.FullLoadDuration);
        var failed = await RunLoadAsync(failedRecorder, LoadOutcome.Failed, TimeSpan.FromSeconds(2));
        var failure = Assert.Single(failedRecorder.Measurements());
        Assert.Equal(2, failure.Value);
        Assert.Equal("failed", failure.Tags["outcome"]);
        await failed.DisposeAsync();
    }

    [Fact]
    public void Rejoins_count_the_rejoin_and_the_exit_on_a_higher_epoch_by_decision()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.Rejoins);
        using var durations = new InstrumentRecorder(ClusterDiagnostics.RejoinDuration);
        var time = new FakeTimeProvider();
        using var gate = ServingGate(time);

        Fence(gate, FenceTrigger.ZeroRowRenewal);
        gate.Apply(MemberTrigger.RejoinPermitted);
        var rejoin = Assert.Single(recorder.Measurements());
        Assert.Equal(1, rejoin.Value);
        Assert.Equal("rejoin", rejoin.Tags["decision"]);

        Serve(gate, 6);
        Fence(gate, FenceTrigger.ZeroRowRenewal);
        Assert.Equal(ReplicaPhase.Exited, gate.Apply(MemberTrigger.RowHasHigherEpoch).To);
        Assert.Equal(["rejoin", "higher_epoch"], recorder.Measurements().Select(m => (string)m.Tags["decision"]!).ToArray());
        Assert.Single(durations.Measurements());
    }

    [Fact]
    public void Rejoin_duration_spans_the_fence_to_the_next_serving_and_a_first_start_records_none()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.RejoinDuration);
        var time = new FakeTimeProvider();
        using var gate = ServingGate(time);
        Assert.Empty(recorder.Measurements());

        time.Advance(TimeSpan.FromSeconds(3));
        Fence(gate, FenceTrigger.ZeroRowRenewal);
        gate.Apply(MemberTrigger.RejoinPermitted);
        time.Advance(TimeSpan.FromSeconds(70));
        Serve(gate, 6);

        var one = Assert.Single(recorder.Measurements());
        Assert.Equal(70, one.Value);
        Assert.Empty(one.Tags);

        time.Advance(TimeSpan.FromSeconds(4));
        Fence(gate, FenceTrigger.ZeroRowRenewal);
        gate.Apply(MemberTrigger.RejoinPermitted);
        time.Advance(TimeSpan.FromSeconds(5));
        Serve(gate, 7);
        Assert.Equal([70d, 5d], recorder.Measurements().Select(m => m.Value).ToArray());
    }

    [Fact]
    public void A_second_fence_before_Serving_keeps_the_first_step_zero()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.RejoinDuration);
        var time = new FakeTimeProvider();
        using var gate = ServingGate(time);

        Fence(gate, FenceTrigger.ZeroRowRenewal);
        gate.Apply(MemberTrigger.RejoinPermitted);
        time.Advance(TimeSpan.FromSeconds(3));
        Assert.True(gate.Joined(6, 1, gate.Now));
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Fence(gate, FenceTrigger.ZeroRowRenewal);
        gate.Apply(MemberTrigger.RejoinPermitted);
        time.Advance(TimeSpan.FromSeconds(27));
        Serve(gate, 7);

        Assert.Equal(30, Assert.Single(recorder.Measurements()).Value);
    }

    [Fact]
    public async Task Drain_duration_records_drained_unanswered_and_taken_over_and_nothing_for_a_stop_that_writes_nothing()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.DrainDuration);

        var drained = await StopAsync(recorder, store => { }, serving: true, advance: TimeSpan.FromSeconds(3));
        Assert.Equal(3, drained.Value);
        Assert.Equal("drained", drained.Tags["outcome"]);

        var noAnswer = await StopAsync(recorder, store => store.DrainingFailure = new TimeoutException("no answer"), serving: true, advance: TimeSpan.Zero);
        Assert.Equal("unanswered", noAnswer.Tags["outcome"]);

        var gone = await StopAsync(recorder, store => store.DrainingRowGone = true, serving: true, advance: TimeSpan.Zero);
        Assert.Equal("unanswered", gone.Tags["outcome"]);

        var takenOver = await StopAsync(recorder, store => { }, serving: true, advance: TimeSpan.FromSeconds(2), fence: true);
        Assert.Equal(2, takenOver.Value);
        Assert.Equal("taken_over", takenOver.Tags["outcome"]);

        var catchingUp = await StopAsync(recorder, store => { }, serving: false, advance: TimeSpan.Zero);
        Assert.Equal("drained", catchingUp.Tags["outcome"]);

        var before = recorder.Measurements().Count;
        var fencingTime = new FakeTimeProvider();
        using var gate = ServingGate(fencingTime);
        gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal));
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        var store = new ScriptedMemberStore();
        await NewDrain(gate, fencingTime, store, []).StopAsync().WaitAsync(Wait);
        Assert.True(gate.ExitAfterFence);
        Assert.Empty(store.DrainingWrites);
        Assert.Equal(before, recorder.Measurements().Count);
    }

    private static async Task<(double Value, IReadOnlyDictionary<string, object?> Tags)> StopAsync(
        InstrumentRecorder recorder, Action<ScriptedMemberStore> script, bool serving, TimeSpan advance, bool fence = false)
    {
        var before = recorder.Measurements().Count;
        var time = new FakeTimeProvider();
        using var gate = new LocalPhaseGate(Options.Create(MembershipTestOptions.Default()), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        Assert.True(gate.Joined(5, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        if (serving)
        {
            gate.Apply(MemberTrigger.ActivationCommitted);
            gate.Apply(MemberTrigger.AppliedToActivationHead);
        }

        var store = new ScriptedMemberStore();
        script(store);
        await NewDrain(gate, time, store, [new AdvancingParticipant(time, advance, fence ? gate : null)]).StopAsync().WaitAsync(Wait);
        var added = recorder.Measurements().Skip(before).ToList();
        return Assert.Single(added);
    }

    private static DrainCoordinator NewDrain(LocalPhaseGate gate, FakeTimeProvider time, ScriptedMemberStore store, IDrainParticipant[] participants) =>
        new(gate, participants, store, Options.Create(MembershipTestOptions.Default()), time, NoFaultPoints.Instance, new RecordingModelEvents(), NullLogger<DrainCoordinator>.Instance);

    private sealed class AdvancingParticipant(FakeTimeProvider time, TimeSpan advance, LocalPhaseGate? fence) : IDrainParticipant
    {
        public DrainStep Step => DrainStep.AwaitRequests;

        public ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken)
        {
            time.Advance(advance);
            fence?.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            return ValueTask.CompletedTask;
        }
    }

    private static void Fence(LocalPhaseGate gate, FenceTrigger trigger)
    {
        gate.RequestFence(new FenceRequest(trigger));
        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
    }

    private static void Serve(LocalPhaseGate gate, long epoch)
    {
        if (gate.Phase == ReplicaPhase.Starting)
            Assert.True(gate.Joined(epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private static LocalPhaseGate ServingGate(FakeTimeProvider time)
    {
        var gate = new LocalPhaseGate(Options.Create(MembershipTestOptions.Default()), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        Serve(gate, 5);
        return gate;
    }

    private static Dictionary<string, int> ReadPhases(InstrumentRecorder recorder)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = recorder.Measurements().Count;
        recorder.RecordObservable();
        var values = recorder.Measurements().Skip(before).ToDictionary(m => (string)m.Tags["phase"]!, m => (int)m.Value);
        Assert.Equal(new[] { "starting", "loading", "catching_up", "activated", "serving", "fencing", "fenced", "draining", "exited" }.Order(), values.Keys.Order());
        return values;
    }

    private static LocalPhaseGate ServingGate()
    {
        var gate = new LocalPhaseGate(Options.Create(MembershipTestOptions.Default()), new FakeTimeProvider(), new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        Assert.True(gate.Joined(5, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        return gate;
    }

    private static async Task RunRenewalAsync(InstrumentRecorder recorder, RenewalOutcome expected, TimeSpan elapsed)
    {
        var time = new FakeTimeProvider();
        var configured = MembershipTestOptions.Default();
        configured.RenewTimeout = TimeSpan.FromSeconds(2);
        var options = Options.Create(configured);
        using var gate = new LocalPhaseGate(options, time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        var store = new ScriptedMemberStore { IgnoreCancel = expected == RenewalOutcome.Unknown };
        var loop = new RenewalLoop(gate, store, options, time, NoFaultPoints.Instance, new RecordingModelEvents(), NullLogger<RenewalLoop>.Instance);
        Assert.True(gate.Joined(7, 1, gate.Now));
        loop.Restart();
        time.Advance(configured.RenewInterval);
        var call = await store.AttemptStarted.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        time.Advance(elapsed);
        if (expected == RenewalOutcome.Renewed)
            call.Answer(new RenewalAnswer(1, 1));
        else if (expected == RenewalOutcome.NotMine)
            call.Answer(new RenewalAnswer(0, null));
        else if (expected == RenewalOutcome.OtherTimeline)
            call.Answer(new RenewalAnswer(1, 2));
        else
            call.Fail(new OperationCanceledException());
        await recorder.WaitForMeasurementAsync(Wait);
        await loop.DisposeAsync().AsTask().WaitAsync(Wait);
    }

    private static async Task<LoadHarness> RunLoadAsync(InstrumentRecorder recorder, LoadOutcome outcome, TimeSpan elapsed)
    {
        var time = new FakeTimeProvider();
        var options = Options.Create(MembershipTestOptions.Default());
        var events = new RecordingModelEvents();
        var gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        var store = new ScriptedMemberStore();
        store.ScriptJoins(_ => new JoinAnswer(5, 1));
        var load = new ScriptedLoad { Gate = gate };
        var answer = new TaskCompletionSource<LoadOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        load.Pending = answer.Task;
        var loop = new RenewalLoop(gate, store, options, time, NoFaultPoints.Instance, events, NullLogger<RenewalLoop>.Instance);
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, options, time);
        var lifetime = new FakeLifetime();
        var driver = new MembershipDriver(
            gate, store, loop, load, new NoMemberActivation(),
            new DrainCoordinator(gate, [], store, options, time, NoFaultPoints.Instance, events, NullLogger<DrainCoordinator>.Instance),
            new MemberIdentity(Guid.NewGuid(), "1.2.3", MemberIdentity.CurrentProtocol, []), capabilities,
            lifetime, options, time, NoFaultPoints.Instance, NullLogger<MembershipDriver>.Instance);
        Assert.True(gate.Joined(5, 1, gate.Now));
        var stop = new CancellationTokenSource();
        var phaseChanged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loadTask = driver.LoadAsync(phaseChanged.Task, stop.Token);
        _ = await load.CallsStarted.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        time.Advance(elapsed);
        answer.SetResult(outcome);
        await recorder.WaitForMeasurementAsync(Wait);
        if (outcome == LoadOutcome.Failed)
        {
            await stop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => loadTask.WaitAsync(Wait));
            loadTask = Task.CompletedTask;
        }
        else
        {
            await loadTask.WaitAsync(Wait);
        }
        return new LoadHarness(gate, loop, driver, stop, loadTask);
    }

    private sealed class LoadHarness(LocalPhaseGate gate, RenewalLoop loop, MembershipDriver driver, CancellationTokenSource stop, Task loadTask) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            if (!stop.IsCancellationRequested)
                await stop.CancelAsync();
            await loadTask.WaitAsync(Wait);
            driver.Dispose();
            await loop.DisposeAsync();
            gate.Dispose();
            stop.Dispose();
        }
    }
}
