using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Status;
using Reeve.Cluster.Wakeup;
using Unit.Generations;
using Unit.Membership;
using Unit.Owned;

namespace Unit.Status;

public sealed class StatusSectionsTests : IDisposable
{
    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;

    public StatusSectionsTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
    }

    public void Dispose() => gate.Dispose();

    [Fact]
    public async Task The_lease_section_reports_the_epoch_the_timeline_and_the_lease_left()
    {
        var section = new LeaseStatusSection(gate, Options.Create(options));

        var before = await section.CollectAsync(CancellationToken.None);
        Assert.Equal(LeaseStatusSection.Name, before.Name);
        Assert.Null(before.Values["epoch"]);
        Assert.Null(before.Values["timeline"]);
        Assert.Null(before.Values["leaseRemainingSeconds"]);

        Assert.True(gate.Joined(7, 42, gate.Now));
        var joined = await section.CollectAsync(CancellationToken.None);
        var atJoin = (double)joined.Values["leaseRemainingSeconds"]!;
        time.Advance(TimeSpan.FromSeconds(10));
        var later = (double)(await section.CollectAsync(CancellationToken.None)).Values["leaseRemainingSeconds"]!;

        Assert.Equal(7L, joined.Values["epoch"]);
        Assert.Equal(42L, joined.Values["timeline"]);
        Assert.Equal(options.LeaseTtl.TotalSeconds, joined.Values["leaseTtlSeconds"]);
        Assert.InRange(atJoin, 0, options.LeaseTtl.TotalSeconds);
        Assert.Equal(10, atJoin - later, precision: 3);
    }

    [Fact]
    public async Task The_applier_section_reports_the_cursor_the_generation_and_no_stall_before_the_first_pass()
    {
        using var applier = NewApplier(out var disposables);
        var section = new ApplierStatusSection(applier);

        var collected = await section.CollectAsync(CancellationToken.None);
        var values = collected.Values;
        Assert.Equal(ApplierStatusSection.Name, collected.Name);

        Assert.Equal(0L, values["cursorSeq"]);
        Assert.Null(values["cursorToken"]);
        Assert.Null(values["generation"]);
        Assert.Null(values["observedHead"]);
        Assert.Equal(false, values["stalled"]);
        Assert.Null(values["stalledForSeconds"]);
        Assert.Null(values["failingType"]);
        Assert.Equal(1L, values["firstUnappliedSeq"]);
        foreach (var disposable in disposables)
        {
            await disposable.DisposeAsync();
        }
    }

    [Fact]
    public async Task The_owned_section_lists_no_tenure_before_any_is_held_and_reports_the_release_grace()
    {
        var capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var services = new ServiceCollection().BuildServiceProvider();
        using var runtime = new UnitRuntime(
            [], services, new ScriptedUnitRows(), gate, new ClusterScopeSource(new FenceSteps()), Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), new NoTenureFence(),
            Options.Create(options), time, NullLogger<UnitRuntime>.Instance);

        var section = await new OwnedStatusSection(runtime, Options.Create(options)).CollectAsync(CancellationToken.None);

        Assert.Equal(OwnedStatusSection.Name, section.Name);
        Assert.Empty(Assert.IsType<List<Dictionary<string, object?>>>(section.Values["units"]));
        Assert.Equal(options.ReleaseGrace.TotalSeconds, section.Values["releaseGraceSeconds"]);
    }

    [Fact]
    public async Task Every_library_source_is_registered_and_names_a_distinct_section()
    {
        var services = new ServiceCollection().AddLogging().AddSingleton<Microsoft.Extensions.Hosting.IHostApplicationLifetime, StubLifetime>().AddReeveCluster(
            new ClusterPorts([typeof(OneReconciler)], typeof(NoFenceSink)),
            o =>
            {
                o.ReplicaId = "r1";
                o.ConnectionString = "Host=unused";
                o.ReconcileTimeout = TimeSpan.FromSeconds(10);
            });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().ToList();

        var sources = provider.GetServices<IStatusSectionSource>().ToList();
        var names = new List<string>();
        foreach (var source in sources)
        {
            names.Add((await source.CollectAsync(CancellationToken.None)).Name);
        }

        Assert.Equal(
            ["applier", "floors", "lease", "owned", "phase", "releases", "replication", "skips"],
            names.Order(StringComparer.Ordinal));
        Assert.Equal(names.Count, names.Distinct().Count());
        Assert.Single(hosted, s => s is StatusReporter);
    }

    private Applier NewApplier(out List<IAsyncDisposable> disposables)
    {
        var wrapped = Options.Create(options);
        var passes = new ApplierDataSource(wrapped);
        var data = new ClusterDataSource(options);
        disposables = [passes, data];
        return new Applier(
            Stores.Create("a"), [], gate, new FencePassConnection(), passes, data,
            new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), new ClusterScopeSource(new FenceSteps()), wrapped, time,
            NoFaultPoints.Instance, NoModelEvents.Instance, NullLogger<Applier>.Instance);
    }

    private sealed class StubLifetime : Microsoft.Extensions.Hosting.IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }

    private sealed class OneReconciler : IReconciler
    {
        public string AggregateType => "one";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("one"));
    }
}
