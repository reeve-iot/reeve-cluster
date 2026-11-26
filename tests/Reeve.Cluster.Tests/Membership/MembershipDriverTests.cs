using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Membership;

public sealed class MembershipDriverTests : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly SignallingTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly ScriptedMemberStore store = new();
    private readonly FakeLifetime lifetime = new();
    private readonly ScriptedLoad load = new();
    private readonly LocalPhaseGate gate;
    private readonly RenewalLoop loop;
    private readonly MembershipDriver driver;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly CancellationTokenSource stopping = new();
    private Task? run;

    public MembershipDriverTests()
    {
        var options = Options.Create(MembershipTestOptions.Default());
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        load.Gate = gate;
        loop = new RenewalLoop(gate, store, options, time, NoFaultPoints.Instance, events, NullLogger<RenewalLoop>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, options, time);
        driver = new MembershipDriver(
            gate, store, loop, load, new NoMemberActivation(),
            new DrainCoordinator(gate, [], store, options, time, NoFaultPoints.Instance, events, NullLogger<DrainCoordinator>.Instance),
            new MemberIdentity(Guid.NewGuid(), "1.2.3", MemberIdentity.CurrentProtocol, []),
            capabilities,
            lifetime, options, time, NoFaultPoints.Instance, NullLogger<MembershipDriver>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        store.ReleaseAll();
        await StopAsync();
        driver.Dispose();
        gate.Dispose();
    }

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private static Task OffContext(Func<Task> test) => Task.Run(test);

    private void Start() => run = driver.RunAsync(stopping.Token);

    private async Task StopAsync()
    {
        await stopping.CancelAsync();
        if (run is not null)
        {
            await run.WaitAsync(Wait);
        }

        await driver.StopAsync(CancellationToken.None);
    }

    private async Task AwaitPhase(ReplicaPhase phase)
    {
        while (true)
        {
            var changed = gate.NextPhaseChange;
            if (gate.Phase == phase)
            {
                return;
            }

            await changed.WaitAsync(Wait);
        }
    }

    [Fact]
    public async Task M01_M04_the_join_starts_renewal_and_the_load_moves_the_replica_to_CatchingUp()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            Assert.Equal(5, gate.Epoch);
            Assert.Equal(1, gate.JoinedTimeline);
            var request = Assert.Single(store.JoinRequests);
            Assert.Equal("r1", request.ReplicaId);
            Assert.Equal("1.2.3", request.Identity.Version);

            time.Advance(S(3));
            Assert.Equal(5, Assert.Single(store.Attempts).Epoch);
        });
    }

    [Fact]
    public async Task The_join_carries_the_capabilities_set_before_it()
    {
        await OffContext(async () =>
        {
            capabilities.AddUnitKind("widget");
            await capabilities.SetAsync(["gpu"]);
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            Assert.Equal(["gpu", "unit:widget"], Assert.Single(store.JoinRequests).Identity.Capabilities);
        });
    }

    [Fact]
    public async Task M02_a_refused_join_retries_every_RenewInterval_with_a_named_readiness_reason_then_joins()
    {
        await OffContext(async () =>
        {
            var joins = System.Threading.Channels.Channel.CreateUnbounded<PendingJoin>();
            store.PendingJoins = joins;

            Start();
            Read(joins).Answer(null);
            Assert.Equal(ReplicaPhase.Starting, gate.Phase);
            Assert.Equal(MembershipDriver.JoinRefused, Assert.Single(gate.Readiness.Current).Code);

            time.Advance(S(3) - TimeSpan.FromTicks(1));
            Assert.False(joins.Reader.TryRead(out _));
            time.Advance(TimeSpan.FromTicks(1));
            Read(joins).Answer(null);
            Assert.Equal(ReplicaPhase.Starting, gate.Phase);

            time.Advance(S(3));
            Read(joins).Answer(new JoinAnswer(9, 1));
            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Equal(9, gate.Epoch);
            Assert.Empty(gate.Readiness.Current);
        });
    }

    [Theory]
    [InlineData("FenceStart")]
    [InlineData("FatalFault")]
    [InlineData("ZeroRowRenewal")]
    [InlineData("Timeline")]
    public async Task SIMP1_every_rejoin_runs_the_full_load_whatever_triggered_the_fence_with_a_cursor_that_is_still_valid(string trigger)
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1), _ => new JoinAnswer(6, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);
            load.Cursor = new Reeve.Cluster.Core.Cursor(42, Guid.NewGuid());

            gate.RequestFence(new FenceRequest(Enum.Parse<FenceTrigger>(trigger)));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            await AwaitPhase(ReplicaPhase.CatchingUp);

            Assert.Equal(6, gate.Epoch);
            Assert.Equal([5L, 6L], load.Loads);
        });
    }

    [Fact]
    public async Task A_first_join_writes_the_empty_cursor_and_a_rejoin_the_cursor_its_kept_state_stands_for()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1), _ => new JoinAnswer(6, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            var token = Guid.NewGuid();
            load.Cursor = new Reeve.Cluster.Core.Cursor(42, token);
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            await AwaitPhase(ReplicaPhase.CatchingUp);

            Assert.Equal(0, store.JoinRequests[0].AppliedSeq);
            Assert.Null(store.JoinRequests[0].AppliedToken);
            Assert.Equal(42, store.JoinRequests[1].AppliedSeq);
            Assert.Equal(token, store.JoinRequests[1].AppliedToken);
        });
    }

    [Fact]
    public async Task The_start_up_order_StartingAsync_returns_only_once_the_first_load_is_done()
    {
        await OffContext(async () =>
        {
            var loads = new TaskCompletionSource<LoadOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
            load.Pending = loads.Task;
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            var starting = driver.StartingAsync(CancellationToken.None);
            await AwaitPhase(ReplicaPhase.Loading);
            Assert.False(starting.IsCompleted);
            Assert.False(driver.FirstLoad.IsCompleted);

            loads.SetResult(LoadOutcome.Loaded);
            await starting.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
            Assert.True(driver.FirstLoad.IsCompleted);
        });
    }

    [Fact]
    public async Task The_start_up_order_StartingAsync_returns_when_the_process_exits_before_any_load()
    {
        await OffContext(async () =>
        {
            gate.ReportFatalFault("disk full");

            await driver.StartingAsync(CancellationToken.None).WaitAsync(Wait);

            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            Assert.False(driver.FirstLoad.IsCompleted);
            await lifetime.Stopped.WaitAsync(Wait);
        });
    }

    [Fact]
    public async Task The_applier_loop_starts_only_once_the_gate_has_applied_the_load()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            Assert.Equal([(5L, ReplicaPhase.CatchingUp)], load.Applying);
        });
    }

    [Fact]
    public async Task M14_a_stop_during_the_first_load_that_ends_the_hosts_start_writes_draining_once()
    {
        await OffContext(async () =>
        {
            using var start = new CancellationTokenSource();
            load.Pending = Task.Delay(Timeout.Infinite, start.Token).ContinueWith(_ => LoadOutcome.Failed, TaskScheduler.Default);
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            var starting = driver.StartingAsync(start.Token);
            await AwaitPhase(ReplicaPhase.Loading);
            await start.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => starting.WaitAsync(Wait));
            Assert.Equal([5L], store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            Assert.True(load.Stopped);

            await StopAsync();
            Assert.Equal([5L], store.DrainingWrites);
        });
    }

    [Fact]
    public async Task The_hosts_stop_stops_the_applier_for_good()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            await StopAsync();

            Assert.True(load.Stopped);
        });
    }

    [Theory]
    [InlineData(5)]
    [InlineData(4)]
    public async Task A_rejoin_given_an_epoch_at_or_below_the_one_this_process_held_ends_the_process(long again)
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1), _ => new JoinAnswer(again, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            gate.RequestFence(new FenceRequest(FenceTrigger.ZeroRowRenewal));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            await AwaitPhase(ReplicaPhase.Exited);

            Assert.Equal([5L], load.Loads);
            await lifetime.Stopped.WaitAsync(Wait);
        });
    }

    [Fact]
    public async Task S1_a_fatal_fault_in_Starting_ends_the_process()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => null);
            Start();
            Assert.Single(store.JoinRequests);

            gate.ReportFatalFault("the key store is unreadable");

            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            await lifetime.Stopped.WaitAsync(Wait);
        });
    }

    [Fact]
    public async Task S1_a_fatal_fault_reported_before_the_driver_runs_still_ends_the_process()
    {
        await OffContext(async () =>
        {
            gate.ReportFatalFault("no config");

            Start();
            await run!.WaitAsync(Wait);

            await lifetime.Stopped.WaitAsync(Wait);
            Assert.Empty(store.JoinRequests);
        });
    }

    [Fact]
    public async Task S1_S2_a_fence_marked_to_exit_ends_the_process_never_a_rejoin()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            gate.ReportFatalFault("disk full");
            gate.CompleteFence();

            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            await lifetime.Stopped.WaitAsync(Wait);
            Assert.Single(store.JoinRequests);
        });
    }

    [Fact]
    public async Task M14_a_graceful_stop_in_CatchingUp_writes_draining_for_its_epoch_and_exits()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);

            await StopAsync();

            Assert.Equal([5L], store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            Assert.False(gate.TryGetRenewal(out _));
        });
    }

    [Fact]
    public async Task The_stop_order_the_drain_from_Serving_runs_in_StoppingAsync_and_StopAsync_then_only_stops_the_applier()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);
            gate.Apply(MemberTrigger.ActivationCommitted);
            gate.Apply(MemberTrigger.AppliedToActivationHead);
            Assert.Equal(ReplicaPhase.Serving, gate.Phase);

            await driver.StoppingAsync(CancellationToken.None).WaitAsync(Wait);

            Assert.Equal([5L], store.ServingSinceClears);
            Assert.Equal([5L], store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
            Assert.False(load.Stopped);

            await StopAsync();
            Assert.Equal([5L], store.DrainingWrites);
            Assert.True(load.Stopped);
        });
    }

    [Fact]
    public async Task M15_a_graceful_stop_in_Activated_writes_draining_for_its_epoch_and_exits()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);
            gate.Apply(MemberTrigger.ActivationCommitted);

            await StopAsync();

            Assert.Empty(store.ServingSinceClears);
            Assert.Equal([5L], store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        });
    }

    [Fact]
    public async Task M16_a_graceful_stop_in_Starting_exits_with_no_row_write()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => null);
            Start();
            Assert.Single(store.JoinRequests);

            await StopAsync();

            Assert.Empty(store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Exited, gate.Phase);
        });
    }

    [Fact]
    public async Task M16_a_join_that_fails_after_the_stop_began_ends_the_driver_as_a_stop_not_a_fault()
    {
        await OffContext(async () =>
        {
            var joins = System.Threading.Channels.Channel.CreateUnbounded<PendingJoin>();
            store.PendingJoins = joins;
            Start();
            var join = Read(joins);

            await stopping.CancelAsync();
            join.Fail(new System.Net.Sockets.SocketException((int)System.Net.Sockets.SocketError.ConnectionRefused));

            await run!.WaitAsync(Wait);
            Assert.True(run.IsCompletedSuccessfully);
        });
    }

    [Fact]
    public async Task M16_a_load_that_fails_after_the_stop_began_ends_the_driver_as_a_stop_not_a_fault()
    {
        await OffContext(async () =>
        {
            var pending = new TaskCompletionSource<LoadOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
            load.Pending = pending.Task;
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await load.CallsStarted.Reader.ReadAsync().AsTask().WaitAsync(Wait);

            await stopping.CancelAsync();
            pending.SetException(new InvalidOperationException("relation \"reeve_cluster.member\" does not exist"));

            await run!.WaitAsync(Wait);
            Assert.True(run.IsCompletedSuccessfully);
        });
    }

    [Fact]
    public async Task M16_a_graceful_stop_in_Fencing_writes_nothing_and_leaves_the_fence_to_exit_at_step_6()
    {
        await OffContext(async () =>
        {
            store.ScriptJoins(_ => new JoinAnswer(5, 1));
            Start();
            await AwaitPhase(ReplicaPhase.CatchingUp);
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));

            await StopAsync();

            Assert.Empty(store.DrainingWrites);
            Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
            Assert.True(gate.ExitAfterFence);
        });
    }

    [Fact]
    public async Task M02_a_join_the_database_does_not_answer_retries_every_RenewInterval_with_its_own_reason()
    {
        await OffContext(async () =>
        {
            var joins = System.Threading.Channels.Channel.CreateUnbounded<PendingJoin>();
            store.PendingJoins = joins;

            Start();
            Read(joins).Fail(new TimeoutException("network"));
            Assert.Equal(ReplicaPhase.Starting, gate.Phase);
            Assert.Equal(MembershipDriver.JoinUnanswered, Assert.Single(gate.Readiness.Current).Code);

            time.Advance(S(3));
            Read(joins).Answer(null);
            Assert.Equal(MembershipDriver.JoinRefused, Assert.Single(gate.Readiness.Current).Code);

            time.Advance(S(3));
            Read(joins).Answer(new JoinAnswer(9, 1));
            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Empty(gate.Readiness.Current);
        });
    }

    [Fact]
    public async Task M04_a_failed_load_stays_in_Loading_and_retries_after_RenewInterval()
    {
        await OffContext(async () =>
        {
            load.Outcomes.Enqueue(LoadOutcome.Failed);
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            Start();
            Assert.Equal(ReplicaPhase.Loading, gate.Phase);
            Assert.Equal(1, load.Calls);

            time.Advance(S(3));
            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Equal(2, load.Calls);
        });
    }

    [Fact]
    public async Task M04_failed_loads_retry_with_a_backoff_that_doubles_from_RenewInterval()
    {
        await OffContext(async () =>
        {
            load.Outcomes.Enqueue(LoadOutcome.Failed);
            store.ScriptJoins(_ => new JoinAnswer(5, 1));

            Start();
            Assert.Equal(1, load.Calls);

            var second = new TaskCompletionSource<LoadOutcome>();
            load.Pending = second.Task;
            time.Advance(S(3));
            await Until(() => load.Calls == 2);
            var retry = time.DelayArmed(S(6));
            second.SetResult(LoadOutcome.Failed);
            await retry.WaitAsync(Wait);

            time.Advance(S(6) - TimeSpan.FromMilliseconds(1));
            Assert.Equal(2, load.Calls);
            Assert.Equal(ReplicaPhase.Loading, gate.Phase);

            time.Advance(TimeSpan.FromMilliseconds(1));
            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Equal(3, load.Calls);
        });
    }

    [Fact]
    public async Task M04_a_rejoin_permitted_during_the_loads_backoff_joins_without_waiting_for_it()
    {
        await OffContext(async () =>
        {
            load.Outcomes.Enqueue(LoadOutcome.Failed);
            store.ScriptJoins(_ => new JoinAnswer(5, 1), _ => new JoinAnswer(6, 1));

            Start();
            Assert.Equal(1, load.Calls);

            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);

            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Equal(6, gate.Epoch);
            Assert.Equal([5L, 6L], load.Loads);
        });
    }

    [Fact]
    public async Task M04_a_new_epochs_first_failed_load_retries_after_RenewInterval_whatever_the_last_epoch_failed()
    {
        await OffContext(async () =>
        {
            load.Outcomes.Enqueue(LoadOutcome.Failed);
            load.Outcomes.Enqueue(LoadOutcome.Failed);
            store.ScriptJoins(_ => new JoinAnswer(5, 1), _ => new JoinAnswer(6, 1));

            Start();
            time.Advance(S(3));
            await Until(() => load.Calls == 2);

            var third = new TaskCompletionSource<LoadOutcome>();
            load.Pending = third.Task;
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            await Until(() => load.Calls == 3);
            Assert.Equal(6, gate.Epoch);

            var retry = time.DelayArmed(S(3));
            third.SetResult(LoadOutcome.Failed);
            await retry.WaitAsync(Wait);
            time.Advance(S(3) - TimeSpan.FromMilliseconds(1));
            Assert.Equal(3, load.Calls);
            time.Advance(TimeSpan.FromMilliseconds(1));
            await AwaitPhase(ReplicaPhase.CatchingUp);
            Assert.Equal([5L, 5L, 6L, 6L], load.Loads);
        });
    }

    private static async Task Until(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not hold in time");
            await Task.Delay(10);
        }
    }

    private static PendingJoin Read(System.Threading.Channels.Channel<PendingJoin> joins)
    {
        Assert.True(joins.Reader.TryRead(out var join), "the driver has not joined");
        return join!;
    }

}

internal sealed class FakeLifetime : IHostApplicationLifetime
{
    private readonly TaskCompletionSource stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Stopped => stopped.Task;

    public CancellationToken ApplicationStarted => CancellationToken.None;

    public CancellationToken ApplicationStopping => CancellationToken.None;

    public CancellationToken ApplicationStopped => CancellationToken.None;

    public void StopApplication() => stopped.TrySetResult();
}

internal sealed class ScriptedLoad : IMemberLoad
{
    public System.Threading.Channels.Channel<int> CallsStarted { get; } = System.Threading.Channels.Channel.CreateUnbounded<int>();
    public System.Collections.Concurrent.ConcurrentQueue<LoadOutcome> Outcomes { get; } = new();

    public int Calls { get; private set; }

    public Reeve.Cluster.Core.Cursor Cursor { get; set; } = Reeve.Cluster.Core.Cursor.Empty;

    public PublishedPass? LastPublished => null;

    public long LastPassEntries => 0;

    public bool Stopped { get; private set; }

    public void StopForGood() => Stopped = true;

    public System.Collections.Concurrent.ConcurrentQueue<(long Epoch, ReplicaPhase Phase)> Applying { get; } = new();

    public LocalPhaseGate? Gate { get; set; }

    public void StartApplying(long epoch) => Applying.Enqueue((epoch, Gate!.Phase));

    public Task<LoadOutcome>? Pending { get; set; }

    public System.Collections.Concurrent.ConcurrentQueue<long> Loads { get; } = new();

    public Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken)
    {
        Calls++;
        CallsStarted.Writer.TryWrite(Calls);
        Loads.Enqueue(epoch);
        if (Pending is { } pending)
        {
            Pending = null;
            return pending;
        }

        return Task.FromResult(Outcomes.TryDequeue(out var outcome) ? outcome : LoadOutcome.Loaded);
    }
}
