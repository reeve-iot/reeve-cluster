using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis.Relay;
using Reeve.Cluster.Status;
using Unit.Membership;

namespace Unit.Status;

public sealed class StatusSectionRegistrationTests : IDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly FakeStatusStore store = new();
    private readonly CapturingLogger<StatusReporter> logger = new();

    public StatusSectionRegistrationTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
        Assert.True(gate.Joined(7, 1, gate.Now));
    }

    public void Dispose() => gate.Dispose();

    private StatusReporter Reporter(IStatusSectionSource[] sources, params IStatusSectionSource[] application) =>
        new([.. sources, .. application], store, gate, Options.Create(options), time, logger, application.Select(a => new ApplicationStatusSection(a.GetType())));

    [Fact]
    public async Task An_application_section_is_written_with_the_library_sections_every_interval()
    {
        var reporter = Reporter([new TestSource("phase")], new BillingSource());

        await reporter.StartAsync(CancellationToken.None);
        await store.NextAsync(Wait);
        time.Advance(options.StatusReportInterval);
        await store.NextAsync(Wait);
        await reporter.StopAsync(CancellationToken.None);

        Assert.Equal(2, store.Reports.Count);
        Assert.All(store.Reports, r =>
        {
            Assert.Equal(("r1", 7L), (r.ReplicaId, r.Epoch));
            Assert.Equal(["phase", "billing"], r.Sections.Select(s => s.Name));
        });
        Assert.Equal("""{"queued":3}""", store.Reports.First().Sections[1].Json);
    }

    [Fact]
    public async Task An_application_source_that_throws_is_dropped_for_the_tick_and_logged_once()
    {
        var billing = new BillingSource { Fail = true };
        var reporter = Reporter([new TestSource("phase")], billing);

        await reporter.ReportOnceAsync(CancellationToken.None);
        await reporter.ReportOnceAsync(CancellationToken.None);
        billing.Fail = false;
        await reporter.ReportOnceAsync(CancellationToken.None);
        billing.Fail = true;
        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.Equal(
            [["phase"], ["phase"], ["phase", "billing"], ["phase"]],
            store.Reports.Select(r => r.Sections.Select(s => s.Name).ToArray()));
        var warnings = logger.Entries.Where(e => e.Level == LogLevel.Warning).ToList();
        Assert.Equal(2, warnings.Count);
        Assert.All(warnings, w => Assert.Contains(nameof(BillingSource), w.Message, StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_section_that_is_not_named_like_its_source_is_dropped_and_logged_once()
    {
        var reporter = Reporter([new TestSource("phase")], new BillingSource { WrittenName = "lease" });

        await reporter.ReportOnceAsync(CancellationToken.None);
        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.All(store.Reports, r => Assert.Equal(["phase"], r.Sections.Select(s => s.Name)));
        Assert.Equal(2, store.Reports.Count);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(BillingSource), StringComparison.Ordinal));
    }

    [Fact]
    public void Two_application_sections_of_one_name_are_refused_when_the_reporter_is_built()
    {
        var error = Assert.Throws<ClusterConfigurationException>(() => Reporter([new TestSource("phase")], new BillingSource(), new OtherBillingSource()));

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameDuplicate, error.Reason);
        Assert.Contains("'billing'", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("applier")]
    [InlineData("ask")]
    [InlineData("floors")]
    [InlineData("lease")]
    [InlineData("owned")]
    [InlineData("phase")]
    [InlineData("releases")]
    [InlineData("replication")]
    [InlineData("skips")]
    public void An_application_section_named_like_a_library_section_is_refused_when_the_reporter_is_built(string libraryName)
    {
        IStatusSectionSource[] library = libraryName == "ask" ? [] : [new TestSource(libraryName)];

        var error = Assert.Throws<ClusterConfigurationException>(() => Reporter(library, new BillingSource { DeclaredName = libraryName }));

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameReserved, error.Reason);
        Assert.Contains($"'{libraryName}'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_application_section_named_like_a_registered_library_source_outside_the_fixed_set_is_refused()
    {
        var error = Assert.Throws<ClusterConfigurationException>(() => Reporter([new TestSource("newer")], new BillingSource { DeclaredName = "newer" }));

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameReserved, error.Reason);
    }

    [Fact]
    public async Task The_reserved_names_cover_every_library_section()
    {
        await using var provider = Container().BuildServiceProvider();
        var names = new List<string>();
        foreach (var source in provider.GetServices<IStatusSectionSource>())
        {
            names.Add(source.Name);
        }

        Assert.NotEmpty(names);
        Assert.All(names, n => Assert.Contains(n, StatusReporter.LibrarySectionNames));
        Assert.Contains(AskRelayStatusSection.Name, StatusReporter.LibrarySectionNames);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void A_source_without_a_name_is_refused_when_the_reporter_is_built(string? name)
    {
        var error = Assert.Throws<ClusterConfigurationException>(() => Reporter([new TestSource("phase")], new BillingSource { DeclaredName = name! }));

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameInvalid, error.Reason);
        Assert.Contains(nameof(BillingSource), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_name_is_read_once_so_a_source_that_changes_it_is_dropped_by_the_mismatch()
    {
        var shifting = new ShiftingNameSource();
        var reporter = Reporter([new TestSource("phase")], shifting);

        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.Equal(["phase"], Assert.Single(store.Reports).Sections.Select(s => s.Name));
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(ShiftingNameSource), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_NUL_in_a_key_or_a_nested_key_is_replaced_and_the_other_sections_are_written()
    {
        var reporter = Reporter(
            [new TestSource("phase")],
            new BillingSource
            {
                Values = new Dictionary<string, object?> { ["a\0b"] = 1, ["nested"] = new Dictionary<string, object?> { ["c\0d"] = new[] { new Dictionary<string, object?> { ["e\0f"] = "g\0h" } } } },
            });

        await reporter.ReportOnceAsync(CancellationToken.None);

        var report = Assert.Single(store.Reports);
        Assert.Equal(["phase", "billing"], report.Sections.Select(s => s.Name));
        var json = report.Sections[1].Json;
        Assert.DoesNotContain("\\u0000", json, StringComparison.Ordinal);
        var root = System.Text.Json.JsonDocument.Parse(json).RootElement;
        Assert.Equal(1, root.GetProperty("a\uFFFDb").GetInt32());
        Assert.Equal("g\uFFFDh", root.GetProperty("nested").GetProperty("c\uFFFDd")[0].GetProperty("e\uFFFDf").GetString());
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task A_lone_surrogate_in_a_value_or_a_key_is_written_as_a_replacement_character_and_the_report_goes_on()
    {
        var reporter = Reporter(
            [new TestSource("phase")],
            new BillingSource { Values = new Dictionary<string, object?> { ["a"] = "x\uD800y", ["k\uD800"] = 1 } });

        await reporter.ReportOnceAsync(CancellationToken.None);

        var report = Assert.Single(store.Reports);
        Assert.Equal("phase", report.Sections[0].Name);
        Assert.Equal(["phase", "billing"], report.Sections.Select(x => x.Name));
        Assert.All(report.Sections, s => Assert.DoesNotContain("\\u0000", s.Json, StringComparison.Ordinal));
        Assert.All(report.Sections, s => System.Text.Json.JsonDocument.Parse(s.Json).Dispose());
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task AddClusterStatusSection_points_the_registry_at_the_instance_a_hosted_worker_is_registered_as()
    {
        await using var provider = Container()
            .AddSingleton<BillingWorker>()
            .AddHostedService(sp => sp.GetRequiredService<BillingWorker>())
            .AddClusterStatusSection<BillingWorker>()
            .BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().OfType<BillingWorker>().ToList();

        Assert.Same(provider.GetRequiredService<BillingWorker>(), Assert.Single(hosted));
        Assert.Same(hosted[0], Assert.Single(provider.GetServices<IStatusSectionSource>(), s => s is BillingWorker));
    }

    [Fact]
    public async Task AddHostedService_of_the_type_alone_gives_the_hosted_service_an_instance_of_its_own()
    {
        await using var provider = Container().AddHostedService<BillingWorker>().AddClusterStatusSection<BillingWorker>().BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().OfType<BillingWorker>().Single();

        Assert.NotSame(hosted, Assert.Single(provider.GetServices<IStatusSectionSource>(), s => s is BillingWorker));
    }

    [Fact]
    public async Task AddClusterStatusSection_registers_a_source_the_reporter_collects()
    {
        await using var provider = Container().AddClusterStatusSection<BillingSource>().BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().ToList();

        Assert.Single(hosted, s => s is StatusReporter);
        Assert.Contains(provider.GetServices<IStatusSectionSource>(), s => s is BillingSource);
    }

    [Fact]
    public async Task AddClusterStatusSection_refuses_a_duplicate_and_a_library_name_when_the_host_starts()
    {
        await using var twice = Container().AddClusterStatusSection<BillingSource>().AddClusterStatusSection<BillingSource>().BuildServiceProvider();
        await using var twoTypes = Container().AddClusterStatusSection<BillingSource>().AddClusterStatusSection<OtherBillingSource>().BuildServiceProvider();
        await using var library = Container().AddClusterStatusSection<LeaseNamedSource>().BuildServiceProvider();

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameDuplicate, Assert.Throws<ClusterConfigurationException>(() => twice.GetServices<IHostedService>().ToList()).Reason);
        Assert.Equal(ClusterConfigurationReason.StatusSectionNameDuplicate, Assert.Throws<ClusterConfigurationException>(() => twoTypes.GetServices<IHostedService>().ToList()).Reason);
        Assert.Equal(ClusterConfigurationReason.StatusSectionNameReserved, Assert.Throws<ClusterConfigurationException>(() => library.GetServices<IHostedService>().ToList()).Reason);
    }

    private static IServiceCollection Container() =>
        new ServiceCollection().AddLogging().AddSingleton<IHostApplicationLifetime, StubLifetime>().AddReeveCluster(
            new ClusterPorts([typeof(OneReconciler)], typeof(NoFenceSink)),
            o =>
            {
                o.ReplicaId = "r1";
                o.ConnectionString = "Host=unused";
                o.ReconcileTimeout = TimeSpan.FromSeconds(10);
            });

    private sealed class TestSource(string name) : IStatusSectionSource
    {
        public string Name => name;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection { Name = name, Values = new Dictionary<string, object?> { ["n"] = 1 } });
    }

    private class BillingSource : IStatusSectionSource
    {
        public bool Fail { get; set; }

        public string DeclaredName { get; init; } = "billing";

        public string? WrittenName { get; init; }

        public IReadOnlyDictionary<string, object?> Values { get; init; } = new Dictionary<string, object?> { ["queued"] = 3 };

        public string Name => DeclaredName;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            Fail
                ? throw new InvalidOperationException("the billing queue is unreachable")
                : ValueTask.FromResult(new StatusSection { Name = WrittenName ?? DeclaredName, Values = Values });
    }

    private sealed class OtherBillingSource : BillingSource;

    private sealed class BillingWorker : BackgroundService, IStatusSectionSource
    {
        public string Name => "billing-worker";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection { Name = Name, Values = new Dictionary<string, object?>() });

        protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;
    }

    private sealed class ShiftingNameSource : IStatusSectionSource
    {
        private int reads;

        public string Name => reads++ == 0 ? "shifting" : "other";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection { Name = "other", Values = new Dictionary<string, object?>() });
    }

    private sealed class LeaseNamedSource : BillingSource
    {
        public LeaseNamedSource()
        {
            DeclaredName = "lease";
        }
    }

    private sealed class StubLifetime : IHostApplicationLifetime
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
