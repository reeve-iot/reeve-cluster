using System.Diagnostics;
using Int.Log;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Int.Membership;

public sealed class ActivationCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const long Epoch = 40;

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private TimeSpan startupBound = TimeSpan.Zero;
    private TimeSpan leaseTtl = TimeSpan.Zero;

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<IHost> hosts = [];
    private MemberDatabase db = null!;

    public ActivationCommitTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() =>
        db = await MemberDatabase.CreateAsync(postgres, c => new NpgsqlConnectionStringBuilder(c) { CommandTimeout = 1 }.ConnectionString);

    public async Task DisposeAsync()
    {
        await SetStandbyAsync(string.Empty);
        foreach (var host in hosts)
        {
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
            host.Dispose();
        }

        await db.DisposeAsync();
    }

    [Theory]
    [InlineData("cancelled")]
    [InlineData("connection-failed")]
    public async Task SIMP8_an_activation_whose_commit_does_not_answer_fences_the_replica_step_3_writes_fenced_and_the_rejoin_activates_under_a_new_epoch(string how)
    {
        await db.ExecuteAsync($"ALTER DATABASE \"{Database}\" SET synchronous_commit = on");
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateBeforeCommit);
        var events = new RecordedEvents();
        var (host, gate) = await StartAsync(probe, events, how);
        var left = new TaskCompletionSource<(ReplicaPhase To, FenceRun? Fence)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = gate.Subscribe(e =>
        {
            if (e.From == ReplicaPhase.CatchingUp)
            {
                left.TrySetResult((e.To, gate.CurrentFence));
            }
        });

        await probe.Arrived.WaitAsync(Wait);
        var first = gate.Epoch!.Value;
        await SyncStandby.StallAsync(postgres);
        probe.Release();

        if (how == "connection-failed")
        {
            await using var admin = await postgres.OpenConnectionAsync();
            var pid = await AwaitCommitWaiterAsync(admin);
            await using var terminate = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", admin);
            terminate.Parameters.AddWithValue("pid", pid);
            await terminate.ExecuteNonQueryAsync();
        }

        await Poll.UntilAsync(async () => (await db.ReadRowAsync("r1")).State == "active", "the activation's commit landed on the primary");
        var landed = await db.ReadRowAsync("r1");
        Assert.Equal((first, "active"), (landed.Epoch, landed.State));
        await SetStandbyAsync(string.Empty);

        var (to, fence) = await left.Task.WaitAsync(Wait);
        output.WriteLine($"the phase left CatchingUp for {to}, fence {fence?.Request}");
        Assert.Equal(ReplicaPhase.Fencing, to);
        Assert.Equal("unanswered-activation", FenceTriggerNames.Of(fence!.Request.Trigger));
        Assert.Equal(first, fence.Epoch);

        await AwaitAsync(gate, g => g.Phase is ReplicaPhase.Fenced or ReplicaPhase.Starting || g.Epoch > first);
        var step3 = Assert.Single(events.All, e => e.Action == MembershipActions.FStep && Equals(e.State!["k"], 3) && Equals(e.State["epoch"], first));
        Assert.Equal(true, step3.State!["released"]);

        await AwaitAsync(gate, g => g.Phase == ReplicaPhase.Serving && g.Epoch > first, Wait + leaseTtl);
        var row = await db.ReadRowAsync("r1");
        Assert.Equal((gate.Epoch!.Value, "active"), (row.Epoch, row.State));
        Assert.False(Stopping(host));
        Assert.Equal([gate.Epoch.Value], events.All.Where(e => e.Action == MembershipActions.Activate).Select(e => (long)e.State!["epoch"]!));
    }

    [Fact]
    public async Task A_serving_since_write_whose_wait_for_the_standby_its_token_cancels_throws_although_the_row_was_written()
    {
        await db.InsertRowAsync("r1", Epoch, "active", leaseSeconds: 60);
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET activated_at_seq = 0 WHERE replica_id = 'r1'");
        await StallTheStandbyAsync();
        using var cancel = new CancellationTokenSource();

        var write = db.Activation().SetServingSinceAsync("r1", Epoch, Cursor.Empty, cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, Database);
        var clock = Stopwatch.StartNew();
        cancel.CancelAfter(TimeSpan.FromSeconds(2));
        var error = await Record.ExceptionAsync(() => write.WaitAsync(Wait));

        output.WriteLine($"the write threw {error?.GetType().Name} after {clock.Elapsed}: {error?.Message}");
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.True(clock.Elapsed >= TimeSpan.FromSeconds(1.9), $"The write ended after {clock.Elapsed}, before its token.");
        Assert.NotNull((await db.ReadRowAsync("r1")).ServingSince);
    }

    private string Database => new NpgsqlConnectionStringBuilder(db.ConnectionString).Database!;

    private async Task<(IHost Host, LocalPhaseGate Gate)> StartAsync(IFaultPoints points, RecordedEvents events, string how)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(points);
        builder.Services.AddSingleton<IModelEventSink>(events);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyLibraryDefaults(o);
            startupBound = StartupBound.For(o);
            leaseTtl = o.LeaseTtl;
            if (how == "connection-failed")
            {
                o.RenewTimeout = TimeSpan.FromSeconds(10);
            }

            o.RejoinBackoff = TestProfile.RejoinBackoff;
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
            o.ReplicaId = "r1";
            o.ConnectionString = new NpgsqlConnectionStringBuilder(db.ConnectionString) { CommandTimeout = 30 }.ConnectionString;
        });
        var host = builder.Build();
        hosts.Add(host);
        await host.StartAsync().WaitAsync(startupBound);
        return (host, host.Services.GetRequiredService<LocalPhaseGate>());
    }

    private static bool Stopping(IHost host) =>
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested;

    private static async Task AwaitAsync(LocalPhaseGate gate, Func<LocalPhaseGate, bool> reached, TimeSpan? within = null)
    {
        using var bound = new CancellationTokenSource(within ?? Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (reached(gate))
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private async Task<int> AwaitCommitWaiterAsync(NpgsqlConnection admin)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pid FROM pg_stat_activity WHERE datname = @db AND wait_event = 'SyncRep' AND query = 'COMMIT' LIMIT 1", admin);
        command.Parameters.AddWithValue("db", Database);
        int? found = null;
        await Poll.UntilAsync(
            async () =>
            {
                found = await command.ExecuteScalarAsync() is int pid ? pid : null;
                return found is not null;
            },
            "the activation's commit waiting for the standby");
        return found!.Value;
    }

    private async Task StallTheStandbyAsync()
    {
        await db.ExecuteAsync($"ALTER DATABASE \"{Database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
    }

    private Task SetStandbyAsync(string names) => SyncStandby.SetAsync(postgres, names);
}
