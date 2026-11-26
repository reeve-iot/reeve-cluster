using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Int.Membership;

public sealed class MembershipLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan LeaseWait = TimeSpan.FromSeconds(90);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private readonly List<(IHost Host, Task Start)> hosts = [];
    private TimeSpan startupBound = TimeSpan.Zero;
    private MemberDatabase db = null!;

    public MembershipLifecycleTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        foreach (var (host, start) in hosts)
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(bound.Token);
            try
            {
                await start.WaitAsync(bound.Token);
            }
            catch (OperationCanceledException)
            {
            }

            host.Dispose();
        }

        await db.DisposeAsync();
    }

    private static Window ZeroRowFence(ClusterOptions options) =>
        new("zero-row fence", TimeSpan.Zero, options.RenewInterval + options.RenewTimeout + TimingWindows.DefaultSlack);

    private async Task<LocalPhaseGate> StartAsync(string id = "r1", IFaultPoints? points = null, Action<ClusterOptions>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        if (points is not null)
        {
            builder.Services.AddSingleton(points);
        }

        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            startupBound = StartupBound.For(o);
            o.ReplicaId = id;
            o.ConnectionString = db.ConnectionString;
            configure?.Invoke(o);
        });
        var host = builder.Build();

        hosts.Add((host, Task.Run(() => host.StartAsync())));
        return host.Services.GetRequiredService<LocalPhaseGate>();
    }

    private static async Task AwaitAsync(LocalPhaseGate gate, Func<ReplicaPhase, bool> reached, TimeSpan timeout)
    {
        using var bound = new CancellationTokenSource(timeout);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (reached(gate.Phase))
            {
                return;
            }

            try
            {
                await change.WaitAsync(bound.Token);
            }
            catch (OperationCanceledException) when (bound.IsCancellationRequested)
            {
                var reasons = gate.Readiness.Current.Select(r => $"{r.Code} ({r.Description})");
                throw new XunitException($"No awaited phase within {timeout}: phase {gate.Phase}, epoch {gate.Epoch?.ToString() ?? "none"}, readiness reasons [{string.Join("; ", reasons)}].");
            }
        }
    }

    private sealed class RenewalAttempts : IFaultPoints
    {
        private readonly List<string> afterChange = [];
        private bool changed;

        public void Change()
        {
            lock (afterChange)
            {
                changed = true;
            }
        }

        public (int Sent, int Answered) AfterChange()
        {
            lock (afterChange)
            {
                var (sent, answered) = (0, 0);
                foreach (var point in afterChange)
                {
                    if (point == MembershipFaultPoints.RenewSend.Name)
                    {
                        sent++;
                    }
                    else if (sent > 0)
                    {
                        answered++;
                    }
                }

                return (sent, answered);
            }
        }

        public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
        {
            lock (afterChange)
            {
                if (changed && (point == MembershipFaultPoints.RenewSend.Name || point == MembershipFaultPoints.RenewAnswer.Name))
                {
                    afterChange.Add(point);
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    private static async Task EventuallyAsync(Func<bool> condition, string what, TimeSpan? limit = null)
    {
        var bound = limit ?? Wait;
        var clock = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(clock.Elapsed < bound, $"{what} did not happen within {bound}");
            await Task.Delay(TimeSpan.FromMilliseconds(20));
        }
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string what, TimeSpan limit)
    {
        var clock = Stopwatch.StartNew();
        while (!await condition())
        {
            Assert.True(clock.Elapsed < limit, $"{what} did not happen within {limit}");
            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }
    }

    private async Task<DateTime> ReadDatabaseNowAsync()
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    private static bool Refused(LocalPhaseGate gate) => gate.Readiness.Current.Any(r => r.Code == MembershipDriver.JoinRefused);

    private Task AwaitJoinedAsync(LocalPhaseGate gate) =>
        AwaitAsync(gate, p => p is ReplicaPhase.Loading or ReplicaPhase.CatchingUp or ReplicaPhase.Activated or ReplicaPhase.Serving, startupBound);

    [Theory]
    [InlineData("restored-older-epoch")]
    [InlineData("same-id-takeover")]
    [InlineData("operator-delete")]
    public async Task Renewal_outcomes_a_restored_older_epoch_a_takeover_and_an_operators_delete_each_fence_within_RenewInterval_plus_RenewTimeout(string change)
    {
        var attempts = new RenewalAttempts();
        var gate = await StartAsync(points: attempts);
        await AwaitJoinedAsync(gate);

        var joinDeadline = gate.Deadline;
        await EventuallyAsync(() => gate.Deadline > joinDeadline, "a renewal answered after the join");
        Assert.True(gate.TryGetRenewal(out _), $"renewal stopped before the change, in {gate.Phase}");

        var fenced = new TaskCompletionSource<(FenceRequest Request, TimeSpan At)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new Stopwatch();
        gate.OnFenceRequested(r => fenced.TrySetResult((r, clock.Elapsed)));

        var stalls = StallProbe.Shared.Begin();
        await db.ExecuteAsync(change switch
        {
            "restored-older-epoch" => "UPDATE reeve_cluster.member SET epoch = epoch - 1, lease_expires_at = clock_timestamp() + interval '30 seconds'",
            "same-id-takeover" => "UPDATE reeve_cluster.member SET epoch = epoch + 1, process_id = gen_random_uuid(), lease_expires_at = clock_timestamp() + interval '30 seconds'",
            _ => "DELETE FROM reeve_cluster.member",
        });
        clock.Start();
        attempts.Change();

        var (request, at) = await fenced.Task.WaitAsync(Wait);

        Assert.Equal(FenceTrigger.ZeroRowRenewal, request.Trigger);
        var (sent, answered) = attempts.AfterChange();
        Assert.True(answered <= 1, $"{answered} attempts sent after the change were answered; the first must fence");
        var options = TestProfile.Options("r1", db.ConnectionString, profile: null);
        var unanswered = Math.Max(0, sent - 1);
        if (unanswered > 0)
        {
            output.WriteLine($"{unanswered} attempt(s) after the change got no answer within RenewTimeout; the window moves by RenewInterval each");
        }

        ZeroRowFence(options).Widen(TimeSpan.Zero, options.RenewInterval * unanswered).AssertContains(at, stalls, output.WriteLine);
        Assert.True(gate.Phase is ReplicaPhase.Fencing or ReplicaPhase.Fenced or ReplicaPhase.Exited, $"{gate.Phase}");
        Assert.False(gate.TryGetRenewal(out _));
    }

    [Fact]
    public async Task A_refused_compare_and_set_waits_out_the_valid_lease_with_a_named_reason()
    {
        await db.InsertRowAsync("r1", epoch: 40, "active", leaseSeconds: 3600);
        await db.ExecuteAsync("SELECT setval('reeve_cluster.member_epoch', 40)");

        var joins = new Int.Log.PointProbe();
        var gate = await StartAsync(points: joins);
        await EventuallyAsync(() => Refused(gate), "the refusal");
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);
        Assert.Equal(40, (await db.ReadRowAsync("r1")).Epoch);

        joins.Hold(MembershipFaultPoints.JoinBefore);
        await joins.Arrived.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() WHERE replica_id = 'r1'");
        joins.Release();
        await AwaitJoinedAsync(gate);

        await EventuallyAsync(() => gate.Readiness.Current.Count == 0, "the join's readiness reasons to clear");
        Assert.True(gate.Epoch > 40);
    }

    [Fact]
    public async Task Two_processes_start_with_one_replica_id_the_second_is_refused_while_the_first_renews_and_the_first_learns_of_a_takeover_from_a_zero_row_renewal()
    {
        var renewals = new Int.Log.PointProbe();
        var first = await StartAsync(points: renewals);
        await AwaitJoinedAsync(first);
        var firstEpoch = first.Epoch;
        var fenced = new TaskCompletionSource<FenceRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        first.OnFenceRequested(r => fenced.TrySetResult(r));

        var joins = new Int.Log.PointProbe();
        var second = await StartAsync(points: joins);
        await EventuallyAsync(() => Refused(second), "the second's refusal");

        var joinLease = await db.ReadExpiryAsync("r1");
        await EventuallyAsync(
            async () =>
            {
                Assert.Equal(ReplicaPhase.Starting, second.Phase);
                return await ReadDatabaseNowAsync() > joinLease;
            },
            "the database's time to pass the lease the first's join took",
            LeaseWait);
        Assert.Equal(ReplicaPhase.Starting, second.Phase);
        Assert.Equal(firstEpoch, (await db.ReadRowAsync("r1")).Epoch);

        joins.Hold(MembershipFaultPoints.JoinBefore);
        await joins.Arrived.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.Starting, second.Phase);

        renewals.Hold(MembershipFaultPoints.RenewSend);
        await renewals.Arrived.WaitAsync(Wait);
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp()");
        joins.Release();

        await AwaitJoinedAsync(second);
        Assert.False(fenced.Task.IsCompleted, $"the first fenced on {(fenced.Task.IsCompleted ? (await fenced.Task).Trigger : null)} before the second took the row over; its renewal was held for longer than it has until its fence start");
        renewals.Release();
        Assert.Equal(FenceTrigger.ZeroRowRenewal, (await fenced.Task.WaitAsync(Wait)).Trigger);
        Assert.True(second.Epoch > firstEpoch);
        Assert.Equal(second.Epoch, (await db.ReadRowAsync("r1")).Epoch);
    }

    [Fact]
    public async Task I23_a_replica_whose_renewals_get_no_answer_fences_at_fence_start_and_not_before()
    {
        var gate = await StartAsync();
        await AwaitAsync(gate, p => p == ReplicaPhase.Serving, startupBound);
        var fenced = new TaskCompletionSource<(FenceRequest Request, TimeSpan At, TimeSpan Deadline)>(TaskCreationOptions.RunContinuationsAsynchronously);

        gate.OnFenceRequested(r => fenced.TrySetResult((r, gate.Now, gate.Deadline!.Value)));

        var stalls = StallProbe.Shared.Begin();
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var lockRow = await connection.BeginTransactionAsync();
        await using (var command = new NpgsqlCommand("SELECT 1 FROM reeve_cluster.member FOR UPDATE", connection, lockRow))
        {
            await command.ExecuteNonQueryAsync();
        }

        var (request, at, deadline) = await fenced.Task.WaitAsync(Wait);

        Assert.Equal(FenceTrigger.FenceStart, request.Trigger);
        var stopBudget = TestProfile.Options("r1", db.ConnectionString, profile: null).StopBudget;
        new Window("fence start", deadline - stopBudget - TimeSpan.FromMilliseconds(50), deadline - (stopBudget / 4))
            .AssertContains(at, stalls, output.WriteLine);
        await lockRow.RollbackAsync();
    }

    [Fact]
    public async Task M14_a_graceful_stop_before_Serving_writes_draining_and_the_same_id_joins_at_once()
    {
        Action<ClusterOptions> longLease = o => o.LeaseTtl = TimeSpan.FromSeconds(60);
        var probe = new Int.Log.PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateBeforeLock);
        var gate = await StartAsync(points: probe, configure: longLease);
        await AwaitJoinedAsync(gate);
        await probe.Arrived.WaitAsync(Wait);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        var (host, start) = hosts[0];
        await start.WaitAsync(Wait);
        await host.StopAsync();
        hosts.RemoveAt(0);
        host.Dispose();

        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);

        var clock = Stopwatch.StartNew();
        var next = await StartAsync(configure: longLease);
        await AwaitJoinedAsync(next);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30), $"the same-id restart took {clock.Elapsed}");
    }
}

public sealed class CancelledRenewalCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public CancelledRenewalCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        await SetStandbyAsync(string.Empty);
        await db.DisposeAsync();
    }

    private async Task SetStandbyAsync(string names)
    {
        await using var admin = await postgres.OpenConnectionAsync();
        await using (var alter = new NpgsqlCommand($"ALTER SYSTEM SET synchronous_standby_names = '{names}'", admin))
        {
            await alter.ExecuteNonQueryAsync();
        }

        await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", admin);
        await reload.ExecuteNonQueryAsync();
    }

    private async Task WaitForStandbyNamesAsync(string names)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            await using var connection = await postgres.OpenConnectionAsync();
            await using var command = new NpgsqlCommand("SHOW synchronous_standby_names", connection);
            if ((string?)await command.ExecuteScalarAsync() == names)
            {
                return;
            }

            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30), $"synchronous_standby_names did not become '{names}' within 30 s");
            await Task.Delay(TimeSpan.FromMilliseconds(50));
        }
    }

    [Fact]
    public Task I23_I33_a_renewal_whose_synchronous_wait_is_ended_by_its_cancel_at_RenewTimeout_returns_one_row_and_is_not_counted() =>
        AssertCancelledCommitIsNotCountedAsync(db.ConnectionString, TestProfile.Apply);

    [Fact]
    public Task I33_a_Command_Timeout_in_the_connection_string_shorter_than_RenewTimeout_does_not_cancel_the_renewal_for_it() =>
        AssertCancelledCommitIsNotCountedAsync(
            new NpgsqlConnectionStringBuilder(db.ConnectionString) { CommandTimeout = 1 }.ConnectionString,
            o =>
            {
                TestProfile.Apply(o);
                o.RenewTimeout = TimeSpan.FromSeconds(2.5);
                o.RenewInterval = TimeSpan.FromSeconds(3);
            });

    private async Task AssertCancelledCommitIsNotCountedAsync(string connectionString, Action<ClusterOptions> profile)
    {
        var options = new ClusterOptions { ConnectionString = connectionString, ReplicaId = "r1" };
        profile(options);
        await using var renewal = new RenewalDataSource(Options.Create(options));
        await using var writes = new MemberWriteDataSource(Options.Create(options));
        var store = new MemberStore(renewal, writes);
        var events = new RecordingEvents();
        using var gate = new LocalPhaseGate(Options.Create(options), TimeProvider.System, events, NullLogger<LocalPhaseGate>.Instance);
        await using var loop = new RenewalLoop(gate, store, Options.Create(options), TimeProvider.System, NoFaultPoints.Instance, events, NullLogger<RenewalLoop>.Instance);

        var t0 = gate.Now;
        var joined = (await store.JoinAsync(new JoinRequest("r1", options.LeaseTtl, MemberIdentity.ForThisProcess()), CancellationToken.None))!.Value;
        Assert.True(gate.Joined(joined.Epoch, joined.Timeline, t0));
        var deadline = gate.Deadline;
        var expiresBefore = await db.ReadExpiryAsync("r1");

        await db.ExecuteAsync($"ALTER DATABASE \"{new NpgsqlConnectionStringBuilder(db.ConnectionString).Database}\" SET synchronous_commit = on");
        await SetStandbyAsync("ghost");
        await WaitForStandbyNamesAsync("ghost");
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        Assert.True(gate.TryGetRenewal(out var target));
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await loop.AttemptAsync(target).WaitAsync(TimeSpan.FromSeconds(30));

        var exec = Assert.Single(events.All, e => e.Action == MembershipActions.RenewExec);
        Assert.Equal(1, exec.State!["rows"]);
        Assert.True(await db.ReadExpiryAsync("r1") > expiresBefore);
        Assert.True(clock.Elapsed >= options.RenewTimeout - TimeSpan.FromMilliseconds(50), $"the wait ended after {clock.Elapsed}, before RenewTimeout");

        Assert.DoesNotContain(events.All, e => e.Action == MembershipActions.RenewAnswer);
        Assert.Equal(deadline, gate.Deadline);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    private sealed class RecordingEvents : IModelEventSink
    {
        private readonly List<ModelEvent> events = [];

        public IReadOnlyList<ModelEvent> All
        {
            get
            {
                lock (events)
                {
                    return [.. events];
                }
            }
        }

        public void Emit(in ModelEvent modelEvent)
        {
            lock (events)
            {
                events.Add(modelEvent);
            }
        }
    }
}
