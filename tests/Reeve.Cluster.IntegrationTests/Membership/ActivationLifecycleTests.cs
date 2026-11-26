using Int.Log;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Xunit.Abstractions;

namespace Int.Membership;

public sealed class ActivationLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<IHost> hosts = [];
    private MemberDatabase db = null!;

    public ActivationLifecycleTests(PostgresFixture postgres, ITestOutputHelper output)
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

    private async Task<IHost> StartAsync(RecordedEvents events)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton<IModelEventSink>(events);
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddProvider(new TestOutputLoggerProvider(output));
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
        });
        var host = builder.Build();
        hosts.Add(host);
        await host.StartAsync();
        return host;
    }

    private static async Task AwaitAsync(LocalPhaseGate gate, RecordedEvents events, Func<LocalPhaseGate, bool> reached, string what)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (reached(gate))
            {
                return;
            }

            try
            {
                await change.WaitAsync(bound.Token);
            }
            catch (OperationCanceledException) when (bound.IsCancellationRequested)
            {
                throw new Xunit.Sdk.XunitException(Describe(gate, events, what));
            }
        }
    }

    private static string Describe(LocalPhaseGate gate, RecordedEvents events, string what)
    {
        var reasons = gate.Readiness.Current.Select(r => $"{r.Code} ({r.Description})").ToList();
        var recorded = events.All;
        var last = recorded.Skip(Math.Max(0, recorded.Count - 50)).Select(e =>
            e.State is null ? e.Action : $"{e.Action} {{{string.Join(", ", e.State.Select(kv => $"{kv.Key}={kv.Value}"))}}}");
        return $"Waited {Wait} for {what}, and the replica is in phase {gate.Phase}, epoch {gate.Epoch?.ToString() ?? "none"}, "
            + $"readiness reasons [{string.Join("; ", reasons)}]. The last {last.Count()} of {recorded.Count} recorded events, oldest first:\n"
            + string.Join("\n", last);
    }

    private Task<int> WriteContinuouslyAsync(CancellationToken stop, TaskCompletionSource started) => Task.Run(async () =>
    {
        var writes = 0;
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        while (!stop.IsCancellationRequested)
        {
            await using var command = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('stream', 'k')", connection);
            await command.ExecuteNonQueryAsync(CancellationToken.None);
            writes++;
            started.TrySetResult();
            await Task.Delay(TimeSpan.FromMilliseconds(10), CancellationToken.None);
        }

        return writes;
    });

    [Fact]
    public async Task Under_a_continuous_stream_of_writes_the_replica_activates_the_first_time_and_after_a_rejoin()
    {
        using var stop = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = WriteContinuouslyAsync(stop.Token, started);
        await started.Task.WaitAsync(Wait);
        var events = new RecordedEvents();
        var host = await StartAsync(events);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();

        await AwaitAsync(gate, events, g => g.Phase == ReplicaPhase.Serving, "the first Serving");
        var first = gate.Epoch!.Value;
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(("active", first), (row.State, row.Epoch));
        Assert.NotNull(row.ServingSince);
        Assert.NotNull(row.ActivatedAtSeq);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await AwaitAsync(gate, events, g => g.Phase == ReplicaPhase.Serving && g.Epoch > first, $"Serving on an epoch after {first} following the fence");
        await stop.CancelAsync();
        Assert.True(await stream.WaitAsync(Wait) > 0);

        row = await db.ReadRowAsync("r1");
        Assert.Equal(("active", gate.Epoch!.Value), (row.State, row.Epoch));
        Assert.NotNull(row.ServingSince);
        Assert.True(row.ActivatedAtSeq > 0);
        var actions = events.All.Select(e => e.Action).Where(a => a is MembershipActions.Activate or MembershipActions.EnterServing).ToList();
        Assert.Equal([MembershipActions.Activate, MembershipActions.EnterServing, MembershipActions.Activate, MembershipActions.EnterServing], actions);
    }

    [Fact]
    public async Task With_a_write_made_while_the_host_starts_the_replica_still_activates()
    {
        await db.ExecuteAsync("INSERT INTO reeve_cluster.change_log (aggregate_type) VALUES ('early')");
        var events = new RecordedEvents();
        var host = await StartAsync(events);
        await db.ExecuteAsync("INSERT INTO reeve_cluster.change_log (aggregate_type) VALUES ('during')");
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();

        await AwaitAsync(gate, events, g => g.Phase == ReplicaPhase.Serving, "Serving");
        Assert.Equal("active", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task The_status_sections_of_the_phase_and_the_floors_are_registered_and_report_Serving()
    {
        var events = new RecordedEvents();
        var host = await StartAsync(events);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await AwaitAsync(gate, events, g => g.Phase == ReplicaPhase.Serving, "Serving");

        var sections = new Dictionary<string, StatusSection>();
        foreach (var source in host.Services.GetServices<IStatusSectionSource>())
        {
            var section = await source.CollectAsync(CancellationToken.None);
            sections[section.Name] = section;
        }

        Assert.Equal("Serving", sections[PhaseStatusSection.Name].Values["phase"]);
        Assert.Equal(true, sections[PhaseStatusSection.Name].Values["ready"]);
        Assert.Equal(0, sections[FloorStatusSection.Name].Values["protocolFloor"]);
    }
}
