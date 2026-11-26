using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Int.Membership;

internal static class FenceSteps
{
    public static async Task<bool> LandedAsync(
        MemberDatabase db,
        long epoch,
        string trigger,
        StallSpan stalls,
        Action<string> log,
        [CallerMemberName] string member = "",
        [CallerFilePath] string file = "")
    {
        var reading = System.Diagnostics.Stopwatch.StartNew();
        var row = await db.ReadRowAsync("r1");
        var history = await db.ReadFenceHistoryAsync("r1");

        if (reading.Elapsed > TimeSpan.FromSeconds(1))
        {
            StallProbe.Shared.Note(reading.Elapsed, "reading the row and the fence history");
        }

        var released = row.Epoch == epoch && row.State == "fenced" && row.LeaseLeft <= TimeSpan.Zero;
        var recorded = history.Count == 1 && history[0].Epoch == epoch && history[0].Trigger == trigger;
        if (released && recorded)
        {
            return true;
        }

        Assert.True(
            stalls.Exceeds(TimeSpan.FromMilliseconds(500)),
            $"step 3 or step 5 of the fence did not land (row {row.State} at epoch {row.Epoch} with {row.LeaseLeft} of lease left, {history.Count} record(s)), and the runner was not stalled");
        Degraded.Report(log, $"{stalls.Describe()}: step 3 (released: {released}) or step 5 (recorded: {recorded}) of the fence did not land in its bound, which the spec allows, so what they wrote was not checked", member, file);
        return false;
    }
}

public sealed class FenceLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<IHost> hosts = [];
    private MemberDatabase db = null!;

    public FenceLifecycleTests(PostgresFixture postgres, ITestOutputHelper output)
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

    private async Task<(IHost Host, LocalPhaseGate Gate)> StartAsync(IFaultPoints? points = null, Action<ClusterOptions>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        if (points is not null)
        {
            builder.Services.AddSingleton(points);
        }

        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            configure?.Invoke(o);
        });
        var host = builder.Build();
        hosts.Add(host);
        await host.StartAsync().WaitAsync(Wait);
        return (host, host.Services.GetRequiredService<LocalPhaseGate>());
    }

    private static async Task AwaitAsync(LocalPhaseGate gate, Func<ReplicaPhase, bool> reached)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (reached(gate.Phase))
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private static Task AwaitJoinedAsync(LocalPhaseGate gate) =>
        AwaitAsync(gate, p => p is ReplicaPhase.Loading or ReplicaPhase.CatchingUp or ReplicaPhase.Activated or ReplicaPhase.Serving);

    private static bool Stopping(IHost host) =>
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested;

    [Fact]
    public async Task Rejoin_a_fenced_replica_releases_its_lease_records_the_fence_and_rejoins_after_RejoinBackoff_with_a_new_epoch()
    {
        var (_, gate) = await StartAsync();
        await AwaitJoinedAsync(gate);
        var first = gate.Epoch!.Value;
        var starting = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = gate.Subscribe(e =>
        {
            if (e.To == ReplicaPhase.Starting)
            {
                starting.TrySetResult(gate.Now);
            }
        });

        var stalls = StallProbe.Shared.Begin();
        var step0 = gate.Now;
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await AwaitAsync(gate, p => p == ReplicaPhase.Fenced);

        var landed = await FenceSteps.LandedAsync(db, first, "fence-start", stalls, output.WriteLine);
        if (landed)
        {
            Assert.Null((await db.ReadRowAsync("r1")).ServingSince);
            Assert.InRange((await db.ReadFenceHistoryAsync("r1"))[0].Span, TimeSpan.Zero, TestProfile.TolerantLeaseTtl);
        }

        var rejoinAt = await starting.Task.WaitAsync(Wait) - step0;
        var window = new Window("rejoin after RejoinBackoff", TestProfile.RejoinBackoff - TimeSpan.FromMilliseconds(50), TestProfile.RejoinBackoff + TestProfile.RenewTimeout + TimingWindows.DefaultSlack);
        if (landed)
        {
            window.AssertContains(rejoinAt, stalls, output.WriteLine);
        }
        else
        {
            Assert.True(rejoinAt >= window.Earliest, $"the rejoin came {rejoinAt} after the fence, before RejoinBackoff");
        }

        await AwaitJoinedAsync(gate);
        Assert.True(gate.Epoch > first);
        Assert.Equal(gate.Epoch, (await db.ReadRowAsync("r1")).Epoch);
        Assert.All(await db.ReadFenceHistoryAsync("r1"), fence => Assert.Equal(first, fence.Epoch));
    }

    [Fact]
    public async Task SIMP11_a_new_process_joins_at_once_whatever_fence_history_holds()
    {
        await db.ExecuteAsync("""
            INSERT INTO reeve_cluster.fence_history (replica_id, epoch, trigger, fenced_at, span)
            VALUES ('r1', 1, 'stuck-attempt', clock_timestamp(), interval '1 second')
            """);
        var clock = System.Diagnostics.Stopwatch.StartNew();

        var (_, gate) = await StartAsync(configure: o => o.RejoinBackoff = TimeSpan.FromMinutes(1));
        await AwaitJoinedAsync(gate);

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(20), $"the join took {clock.Elapsed}");
        Assert.Equal(gate.Epoch, (await db.ReadRowAsync("r1")).Epoch);
    }

    [Fact]
    public async Task StuckExit_a_stuck_attempt_ends_the_process_after_the_lease_release_with_the_fence_recorded_first()
    {
        var (host, gate) = await StartAsync();
        await AwaitJoinedAsync(gate);
        var epoch = gate.Epoch!.Value;
        var stalls = StallProbe.Shared.Begin();
        var recordedAtExit = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() =>
            recordedAtExit.TrySetResult(db.ReadFenceHistoryAsync("r1").GetAwaiter().GetResult().Count == 1));

        gate.RequestFence(new FenceRequest(FenceTrigger.StuckAttempt));
        await AwaitAsync(gate, p => p == ReplicaPhase.Exited);

        var recordedBeforeStop = await recordedAtExit.Task.WaitAsync(Wait);
        if (await FenceSteps.LandedAsync(db, epoch, "stuck-attempt", stalls, output.WriteLine))
        {
            Assert.True(recordedBeforeStop, "fence_history had no record when the process began to stop");
        }
    }

    [Fact]
    public async Task FencedExit_a_fatal_fault_reported_in_Fencing_ends_the_process_at_step_6_after_the_steps()
    {
        var hold = new HoldAt(MembershipFaultPoints.FenceBeforeStep4.Name);
        var (host, gate) = await StartAsync(hold);
        await AwaitJoinedAsync(gate);
        var epoch = gate.Epoch!.Value;
        var stalls = StallProbe.Shared.Begin();
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await hold.Reached.WaitAsync(Wait);
        gate.ReportFatalFault("disk full");
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.False(Stopping(host));
        hold.Release();

        await AwaitAsync(gate, p => p == ReplicaPhase.Exited);
        await FenceSteps.LandedAsync(db, epoch, "fence-start", stalls, output.WriteLine);
        await stopping.Task.WaitAsync(Wait);
    }

    [Fact]
    public async Task A_graceful_stop_in_Fencing_lets_the_steps_finish_and_writes_the_row_fenced()
    {
        var hold = new HoldAt(MembershipFaultPoints.FenceBeforeStep4.Name);
        var (host, gate) = await StartAsync(hold);
        await AwaitJoinedAsync(gate);
        var epoch = gate.Epoch!.Value;
        var stalls = StallProbe.Shared.Begin();
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        await hold.Reached.WaitAsync(Wait);

        var stop = host.StopAsync();
        await Task.Delay(TestProfile.RenewInterval);
        Assert.False(stop.IsCompleted);
        hold.Release();
        await stop.WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        await FenceSteps.LandedAsync(db, epoch, "fence-start", stalls, output.WriteLine);
    }

    private sealed class HoldAt(string point) : IFaultPoints
    {
        private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Reached => reached.Task;

        public void Release() => released.TrySetResult();

        public ValueTask ReachAsync(string name, CancellationToken cancellationToken = default)
        {
            if (name != point)
            {
                return ValueTask.CompletedTask;
            }

            reached.TrySetResult();
            return new ValueTask(released.Task.WaitAsync(cancellationToken));
        }
    }
}
