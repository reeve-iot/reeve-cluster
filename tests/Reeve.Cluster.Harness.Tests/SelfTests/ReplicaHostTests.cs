using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Harness.Time;
using Reeve.Cluster.Harness.Timing;

namespace Harness.SelfTests;

public sealed class ReplicaHostTests
{
    private const string ConnectionString = "Host=localhost;Username=u;Database=d";
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    private static ReplicaSpec Spec() => new() { ConnectionString = ConnectionString };

    [Fact]
    public async Task Replicas_in_one_process_each_have_their_own_container_id_clock_and_fault_points()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        var replicas = await run.StartReplicasAsync(3, _ => Spec());

        Assert.Equal(["r1", "r2", "r3"], replicas.Select(r => r.Id));
        Assert.Equal(3, replicas.Select(r => r.Services).Distinct().Count());
        Assert.Equal(3, replicas.Select(r => r.Clock).Distinct().Count());
        Assert.Equal(3, replicas.Select(r => r.Faults).Distinct().Count());
        foreach (var replica in replicas)
        {
            Assert.Equal(replica.Id, replica.Services.GetRequiredService<IOptions<ClusterOptions>>().Value.ReplicaId);
            Assert.Same(replica.Clock, replica.Services.GetRequiredService<TimeProvider>());
            Assert.Same(replica.Faults, replica.Services.GetRequiredService<IFaultPoints>());
            Assert.Same(replica.Reconcilers, replica.Services.GetRequiredService<ReconcilerScript>());
        }
    }

    [Fact]
    public async Task A_replica_runs_the_stall_tolerant_profile_with_the_tests_changes_on_top()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            Configure = o => o.RenewInterval = TimeSpan.FromSeconds(1),
        });

        var options = replica.Services.GetRequiredService<IOptions<ClusterOptions>>().Value;
        Assert.Equal(TimeSpan.FromSeconds(20), options.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(10), options.ApplyStallLimit);
        Assert.Equal(TimeSpan.FromSeconds(1), options.RenewInterval);
        Assert.Equal(TimeSpan.FromSeconds(1), replica.Options.RenewInterval);
        var connection = new Npgsql.NpgsqlConnectionStringBuilder(options.ConnectionString);
        Assert.Equal("localhost", connection.Host);
        Assert.Equal("d", connection.Database);
    }

    [Fact]
    public async Task A_replica_runs_the_tight_profile_when_the_test_names_it()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString, Profile = TestProfile.ApplyTight });

        var options = replica.Services.GetRequiredService<IOptions<ClusterOptions>>().Value;
        Assert.Equal(TimeSpan.FromSeconds(8), options.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ApplyStallLimit);
    }

    [Fact]
    public async Task A_replica_on_a_slow_clock_has_it_from_the_start()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        var slowest = TimingWindows.SlowestClockRate(TestProfile.Options("r1", ConnectionString, profile: null));

        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { ConnectionString = ConnectionString, ClockRate = slowest });

        Assert.Equal(0.95, replica.Clock.Rate);
    }

    [Fact]
    public async Task Model_events_a_replica_emits_are_recorded_with_its_id_in_the_runs_order()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var a = await run.StartReplicaAsync("a", Spec());
        var b = await run.StartReplicaAsync("b", Spec());

        a.Services.GetRequiredService<IModelEventSink>().Emit(new ModelEvent(LogActions.WAppend, new Dictionary<string, object?> { ["seq"] = 1L }));
        b.Services.GetRequiredService<IModelEventSink>().Emit(new ModelEvent(MembershipActions.RenewSend));
        await a.Faults.ReachAsync(LogFaultPoints.AppendAfter);

        var events = run.Events.Snapshot();
        Assert.Equal(["a", "b"], events.Select(e => e.Replica));
        Assert.Equal([LogActions.WAppend, MembershipActions.RenewSend], events.Select(e => e.Action));
        Assert.Equal(1L, events[0].State!["seq"]);
        Assert.True(events[1].Sequence < run.Arrivals.Snapshot().Single(a => a.Point == LogFaultPoints.AppendAfter.Name).Sequence);
    }

    [Fact]
    public async Task A_replica_that_has_no_connection_string_and_no_stack_is_refused_at_build()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        await Assert.ThrowsAsync<InvalidOperationException>(() => run.PrepareAsync("r1"));
    }

    [Fact]
    public async Task A_stopped_replica_is_no_longer_running_and_a_crashed_one_is_gone()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var stopped = await run.StartReplicaAsync("s", Spec());
        var crashed = await run.StartReplicaAsync("c", Spec());

        await stopped.StopAsync();
        await crashed.CrashAsync();

        Assert.False(stopped.IsRunning);
        Assert.False(crashed.IsRunning);
        Assert.True(crashed.IsCrashed);
        Assert.Throws<KeyNotFoundException>(() => run.Get("c"));
        Assert.Same(stopped, run.Get("s"));
    }

    [Fact]
    public async Task A_crashed_replica_records_no_more_events()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", Spec());
        var sink = replica.Services.GetRequiredService<IModelEventSink>();
        sink.Emit(new ModelEvent(MembershipActions.RenewSend));

        await replica.CrashAsync();
        sink.Emit(new ModelEvent(MembershipActions.RenewExec));

        Assert.Equal([MembershipActions.RenewSend, EnvironmentActions.Crash], run.Events.Snapshot().Select(e => e.Action));
    }

    [Fact]
    public async Task A_replica_with_a_profile_of_its_own_takes_it_before_the_tests_changes()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            Profile = TestProfile.ApplyLibraryDefaults,
        });

        Assert.Equal(TimeSpan.FromSeconds(30), replica.Options.LeaseTtl);
        Assert.Equal(TestProfile.ReconcileTimeout, replica.Options.ReconcileTimeout);
    }

    [Fact]
    public async Task A_crashed_replica_stands_still_at_its_fault_points()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", Spec());

        await replica.CrashAsync();
        var reaching = replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend).AsTask();
        await Task.Delay(150);

        Assert.False(reaching.IsCompleted);
    }

    [Fact]
    public async Task A_whole_replica_pause_holds_its_timers_and_points_while_its_clock_runs_on()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", Spec());
        var start = replica.Clock.GetTimestamp();
        var fenceStart = TimeSpan.FromMilliseconds(300);
        var firedAfter = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timer = replica.Clock.CreateTimer(_ => firedAfter.TrySetResult(replica.Clock.GetElapsedTime(start)), null, fenceStart, Timeout.InfiniteTimeSpan);

        await replica.PauseAsync();
        var reaching = replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend).AsTask();
        await Task.Delay(900);

        Assert.True(replica.IsPaused);
        Assert.False(firedAfter.Task.IsCompleted);
        Assert.False(reaching.IsCompleted);

        await replica.ResumeAsync();

        var seen = await firedAfter.Task.WaitAsync(Limit);
        await reaching.WaitAsync(Limit);
        Assert.True(seen >= TimeSpan.FromMilliseconds(850), $"fired at {seen}");
        Assert.False(replica.IsPaused);
    }

    [Fact]
    public async Task Pausing_for_a_duration_resumes_on_its_own()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", Spec());

        await replica.PauseForAsync(TimeSpan.FromMilliseconds(200));

        Assert.False(replica.IsPaused);
        Assert.True(replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Stopping_a_paused_replica_resumes_it_first()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var replica = await run.StartReplicaAsync("r1", Spec());
        await replica.PauseAsync();

        await replica.StopAsync().WaitAsync(Limit);

        Assert.False(replica.IsPaused);
    }
}
