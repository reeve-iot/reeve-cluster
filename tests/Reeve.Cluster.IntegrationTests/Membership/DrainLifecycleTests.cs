using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Int.Membership;

public sealed class DrainLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private TimeSpan wait = TimeSpan.Zero;

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<IHost> hosts = [];
    private readonly List<Int.Log.PointProbe> probes = [];
    private MemberDatabase db = null!;

    public DrainLifecycleTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        foreach (var probe in probes)
        {
            probe.Release();
        }

        foreach (var host in hosts)
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(bound.Token);
            host.Dispose();
        }

        await db.DisposeAsync();
    }

    private Int.Log.PointProbe Probe()
    {
        var probe = new Int.Log.PointProbe();
        probes.Add(probe);
        return probe;
    }

    private async Task<(IHost Host, LocalPhaseGate Gate)> StartAsync(IFaultPoints? points = null, Action<ClusterOptions>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddProvider(new TestOutputLoggerProvider(output));
        if (points is not null)
        {
            builder.Services.AddSingleton(points);
        }

        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            wait = StartupBound.For(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            configure?.Invoke(o);
        });
        var host = builder.Build();
        hosts.Add(host);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        try
        {
            await host.StartAsync().WaitAsync(wait);
        }
        catch (TimeoutException)
        {
            var reasons = gate.Readiness.Current.Select(r => $"{r.Code} ({r.Description})");
            throw new XunitException($"The host did not start in {wait}: phase {gate.Phase}, epoch {gate.Epoch?.ToString() ?? "none"}, readiness reasons [{string.Join("; ", reasons)}].");
        }

        return (host, gate);
    }

    private async Task AwaitAsync(LocalPhaseGate gate, ReplicaPhase phase)
    {
        using var bound = new CancellationTokenSource(wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (gate.Phase == phase)
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    [Fact]
    public async Task I2_the_drain_from_Serving_clears_serving_since_at_step_1_and_writes_draining_after_step_4()
    {
        var probe = Probe();
        probe.Hold(MembershipFaultPoints.DrainAfterStep1);
        var (host, gate) = await StartAsync(probe);
        await AwaitAsync(gate, ReplicaPhase.Serving);
        Assert.NotNull((await db.ReadRowAsync("r1")).ServingSince);

        var stop = host.StopAsync();
        await probe.Arrived.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Draining, gate.Phase);
        var during = await db.ReadRowAsync("r1");
        Assert.Equal("active", during.State);
        Assert.Null(during.ServingSince);
        Assert.True(during.LeaseLeft > TimeSpan.Zero, $"the lease has {during.LeaseLeft} left");

        probe.Release();
        await stop.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task M15_a_graceful_stop_in_Activated_writes_draining_and_the_same_id_joins_at_once()
    {
        Action<ClusterOptions> longLease = o => o.LeaseTtl = TimeSpan.FromSeconds(60);
        var probe = Probe();
        probe.Hold(MembershipFaultPoints.ActivateCommittedBeforeApplied);
        var (host, gate) = await StartAsync(probe, longLease);
        await probe.Arrived.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Equal("active", (await db.ReadRowAsync("r1")).State);

        await host.StopAsync().WaitAsync(Wait);
        probe.Release();
        hosts.Remove(host);
        host.Dispose();

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);

        var clock = Stopwatch.StartNew();
        var (_, next) = await StartAsync(configure: longLease);
        await AwaitAsync(next, ReplicaPhase.Serving);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30), $"the same-id restart took {clock.Elapsed}");
    }

    [Fact]
    public async Task S2_a_fence_during_the_drain_takes_over_the_row_is_fenced_not_draining_and_the_process_exits_with_no_rejoin()
    {
        var probe = Probe();
        probe.Hold(MembershipFaultPoints.DrainBeforeDrainingWrite);
        var (host, gate) = await StartAsync(probe);
        await AwaitAsync(gate, ReplicaPhase.Serving);
        var epoch = gate.Epoch!.Value;
        var stalls = StallProbe.Shared.Begin();

        var stop = host.StopAsync();
        await probe.Arrived.WaitAsync(Wait);
        Assert.Equal(FenceDecision.FenceThenExit, gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart)));
        probe.Release();
        await stop.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        await FenceSteps.LandedAsync(db, epoch, "fence-start", stalls, output.WriteLine);
        Assert.Equal(epoch, gate.Epoch);
    }

    [Fact]
    public async Task A_write_after_step_1_by_anything_but_a_running_request_answers_NotAwaited_Draining()
    {
        var probe = Probe();
        probe.Hold(MembershipFaultPoints.DrainAfterStep1);
        var (host, gate) = await StartAsync(probe);
        await AwaitAsync(gate, ReplicaPhase.Serving);
        var stop = host.StopAsync();
        await probe.Arrived.WaitAsync(Wait);

        long seq;
        await using (var data = NpgsqlDataSource.Create(db.ConnectionString))
        await using (var connection = await data.OpenConnectionAsync())
        {
            await using var transaction = await connection.BeginTransactionAsync();
            seq = await host.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction, [new ChangeEntry("widget", "k")]);
            await transaction.CommitAsync();
        }

        var outcome = await host.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq).WaitAsync(Wait);

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Draining), outcome);
        probe.Release();
        await stop.WaitAsync(Wait);
    }
}
