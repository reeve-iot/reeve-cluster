using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Membership;
using Unit.Owned;
using Unit.Testing;
using Xunit;

namespace Unit.Diagnostics;

[Collection(MetricsCollection.Name)]
public sealed class OwnedMetricsTests
{
    [Fact]
    public async Task Takeover_duration_spans_from_the_held_row_write_to_OnAcquiredAsync()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.TakeoverDuration);
        var time = new FakeTimeProvider();
        var options = MembershipTestOptions.Default();
        var gate = new LocalPhaseGate(Options.Create(options), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        var unit = new RecordingUnit();
        var rows = new ScriptedUnitRows { Held = [new HeldRow("widget", "a", 10, false, TimeSpan.FromSeconds(4))] };
        var tenureFence = new HeldTenureFence();
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var runtime = new UnitRuntime(
            [new UnitKind("widget", typeof(RecordingUnit), new UnitKindOptions { RedisFence = true })], services, rows, gate,
            new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"), NoFaultPoints.Instance,
            capabilities, new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), tenureFence,
            Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        Assert.True(gate.Joined(5, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);

        try
        {
            await runtime.PassAsync(CancellationToken.None);
            _ = await tenureFence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            time.Advance(TimeSpan.FromSeconds(1.25));
            tenureFence.Release();
            _ = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            var measurement = Assert.Single(recorder.Measurements());
            Assert.Equal(5.25, measurement.Value);
            Assert.Equal("widget", measurement.Tags["kind"]);
        }
        finally
        {
            tenureFence.Release();
            await runtime.StopAllAsync(drain: false, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
            runtime.Dispose();
            gate.Dispose();
            services.Dispose();
        }
    }

    [Fact]
    public async Task Owned_running_reports_each_registered_kind_with_the_tenures_it_runs_and_0_for_a_kind_it_runs_none_of()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.OwnedRunning);
        var time = new FakeTimeProvider();
        var options = MembershipTestOptions.Default();
        var gate = new LocalPhaseGate(Options.Create(options), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        var unit = new RecordingUnit();
        var rows = new ScriptedUnitRows { Held = [new HeldRow("running-a", "running-a", 10, false, TimeSpan.FromSeconds(4))] };
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var runtime = new UnitRuntime(
            [new UnitKind("running-a", typeof(RecordingUnit), new UnitKindOptions()), new UnitKind("running-b", typeof(RecordingUnit), new UnitKindOptions())],
            services, rows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"), NoFaultPoints.Instance,
            capabilities, new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), new HeldTenureFence(),
            Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        Assert.True(gate.Joined(5, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);

        try
        {
            Assert.Equal(new Dictionary<string, double> { ["running-a"] = 0, ["running-b"] = 0 }, Running(recorder));

            await runtime.PassAsync(CancellationToken.None);
            _ = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(new Dictionary<string, double> { ["running-a"] = 1, ["running-b"] = 0 }, Running(recorder));

            unit.HangLost("running-a");
            rows.Held = [new HeldRow("running-a", "running-a", 10, true, TimeSpan.FromSeconds(4))];
            await runtime.PassAsync(CancellationToken.None);
            await unit.LostStarted("running-a").WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(runtime.Holds("running-a", "running-a"));
            Assert.Equal(new Dictionary<string, double> { ["running-a"] = 0, ["running-b"] = 0 }, Running(recorder));
        }
        finally
        {
            unit.ReleaseAll();
            await runtime.StopAllAsync(drain: false, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
            runtime.Dispose();
            gate.Dispose();
            services.Dispose();
        }

        Assert.Empty(Running(recorder));
    }

    private static Dictionary<string, double> Running(InstrumentRecorder recorder)
    {
        var before = recorder.Measurements().Count;
        recorder.RecordObservable();
        return recorder.Measurements().Skip(before)
            .Where(m => m.Tags["kind"] is "running-a" or "running-b")
            .ToDictionary(m => (string)m.Tags["kind"]!, m => m.Value);
    }
}
