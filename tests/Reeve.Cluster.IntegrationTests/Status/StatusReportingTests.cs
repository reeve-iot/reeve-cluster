using Int.Log;
using Int.Membership;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Xunit.Abstractions;

namespace Int.Status;

public sealed class StatusReportingTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly string[] LibrarySections = ["applier", "floors", "lease", "owned", "phase", "releases", "replication", "skips"];

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<IHost> hosts = [];
    private MemberDatabase db = null!;

    public StatusReportingTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        foreach (var host in hosts)
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(bound.Token);
            host.Dispose();
        }

        await db.DisposeAsync();
    }

    private async Task<IHost> StartAsync(Action<IServiceCollection>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddProvider(new TestOutputLoggerProvider(output));
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
            o.StatusReportInterval = TimeSpan.FromMilliseconds(200);
        });
        configure?.Invoke(builder.Services);
        var host = builder.Build();
        hosts.Add(host);
        await host.StartAsync();
        return host;
    }

    private async Task<ClusterMemberStatus?> MemberAsync() => (await db.Status.ReadAsync(CancellationToken.None)).Members.SingleOrDefault(m => m.ReplicaId == "r1");

    [Fact]
    public async Task A_serving_replica_reports_every_section_of_the_library_and_a_fence_leaves_the_last_report_and_its_history_to_read()
    {
        var host = await StartAsync();
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await Poll.UntilAsync(() => gate.Phase == ReplicaPhase.Serving, "Serving", TimeSpan.FromSeconds(60));

        await Poll.UntilAsync(
            async () => await MemberAsync() is { } m && m.Sections.Select(s => s.Name).Order(StringComparer.Ordinal).SequenceEqual(LibrarySections)
                && m.Sections.Single(s => s.Name == "phase").Values.GetProperty("phase").GetString() == "Serving",
            "every section reported, the phase Serving",
            TimeSpan.FromSeconds(60));

        var serving = (await MemberAsync())!;
        var epoch = gate.Epoch!.Value;
        Assert.Equal((epoch, "active", true), (serving.Epoch, serving.State, serving.LeaseValid));
        Assert.Equal(epoch, serving.Sections.Single(s => s.Name == "lease").Values.GetProperty("epoch").GetInt64());
        Assert.All(serving.Sections, s => Assert.True(s.Age < TimeSpan.FromSeconds(30), $"{s.Name} is {s.Age} old"));
        Assert.Empty(serving.Fences);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await Poll.UntilAsync(
            async () => await MemberAsync() is { Epoch: var e, State: "fenced" } m && e == epoch && m.Fences.Count == 1
                && m.Sections.SingleOrDefault(s => s.Name == "phase") is { } p && p.Values.GetProperty("phase").GetString() is "Fenced" or "Fencing",
            "the fenced row, its fence_history record and a phase section that says so",
            TimeSpan.FromSeconds(60));

        var fenced = (await MemberAsync())!;
        Assert.Equal(("fence-start", false), (fenced.Fences[0].Trigger, fenced.LeaseValid));
        Assert.Contains(fenced.Sections, s => s.Name == "applier");
    }

    [Fact]
    public async Task An_application_section_is_written_every_interval_and_read_through_IClusterStatus()
    {
        var host = await StartAsync(services => services.AddClusterStatusSection<QueueDepthSource>());
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await Poll.UntilAsync(() => gate.Phase == ReplicaPhase.Serving, "Serving", TimeSpan.FromSeconds(60));

        var seen = new List<long>();
        await Poll.UntilAsync(
            async () =>
            {
                if (await MemberAsync() is { } m && m.Sections.SingleOrDefault(s => s.Name == QueueDepthSource.SectionName) is { } section)
                {
                    var ticks = section.Values.GetProperty("ticks").GetInt64();
                    if (seen.Count == 0 || seen[^1] != ticks)
                    {
                        seen.Add(ticks);
                    }
                }

                return seen.Count >= 3;
            },
            "the application section rewritten on three ticks",
            TimeSpan.FromSeconds(60));

        var member = (await MemberAsync())!;
        var queue = member.Sections.Single(s => s.Name == QueueDepthSource.SectionName);
        Assert.Equal("billing", queue.Values.GetProperty("queue").GetString());
        Assert.True(queue.Age < TimeSpan.FromSeconds(30), $"{queue.Name} is {queue.Age} old");
        Assert.Equal(
            LibrarySections.Append(QueueDepthSource.SectionName).Order(StringComparer.Ordinal).ToArray(),
            member.Sections.Select(s => s.Name).Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task A_key_with_a_NUL_or_a_lone_surrogate_in_an_application_section_does_not_stop_the_library_sections_from_being_written()
    {
        var host = await StartAsync(services => services.AddClusterStatusSection<NulKeySource>().AddClusterStatusSection<SurrogateSource>());
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await Poll.UntilAsync(() => gate.Phase == ReplicaPhase.Serving, "Serving", TimeSpan.FromSeconds(60));

        await Poll.UntilAsync(
            async () => await MemberAsync() is { } m && m.Sections.Select(s => s.Name).Order(StringComparer.Ordinal).SequenceEqual(
                LibrarySections.Append(NulKeySource.SectionName).Append(SurrogateSource.SectionName).Order(StringComparer.Ordinal))
                && m.Sections.Single(s => s.Name == "phase").Values.GetProperty("phase").GetString() == "Serving",
            "every library section and both application sections reported, the phase Serving",
            TimeSpan.FromSeconds(60));

        var member = (await MemberAsync())!;
        Assert.Equal(1, member.Sections.Single(s => s.Name == NulKeySource.SectionName).Values.GetProperty("a\uFFFDb").GetInt32());
        Assert.Equal("Serving", member.Sections.Single(s => s.Name == "phase").Values.GetProperty("phase").GetString());
    }

    private sealed class NulKeySource : IStatusSectionSource
    {
        public const string SectionName = "nul-key";

        public string Name => SectionName;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection
            {
                Name = SectionName,
                Values = new Dictionary<string, object?> { ["a\0b"] = 1, ["nested"] = new Dictionary<string, object?> { ["c\0d"] = "e\0f" } },
            });
    }

    private sealed class SurrogateSource : IStatusSectionSource
    {
        public const string SectionName = "surrogate";

        public string Name => SectionName;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection
            {
                Name = SectionName,
                Values = new Dictionary<string, object?> { ["v"] = "x\uD800y", ["k\uD800"] = 1 },
            });
    }

    private sealed class QueueDepthSource : IStatusSectionSource
    {
        public const string SectionName = "queue-depth";

        private long ticks;

        public string Name => SectionName;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection
            {
                Name = SectionName,
                Values = new Dictionary<string, object?> { ["queue"] = "billing", ["ticks"] = Interlocked.Increment(ref ticks) },
            });
    }
}
