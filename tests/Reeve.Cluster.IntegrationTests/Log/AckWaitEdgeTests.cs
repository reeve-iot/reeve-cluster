using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class AckWaitCancelTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan AckTimeout = TimeSpan.FromHours(1);

    private static readonly TimeSpan InTime = TimeSpan.FromSeconds(20);

    private readonly PostgresFixture postgres;

    public AckWaitCancelTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_wait_set_read_whose_connect_a_database_that_does_not_answer_holds_does_not_delay_the_answer_past_AckTimeout()
    {
        var server = new NpgsqlConnectionStringBuilder(postgres.ConnectionString);
        await using var proxy = new HoldingProxy(server.Host!, server.Port);
        var time = new FakeTimeProvider();
        await using var rig = await AckRig.CreateAsync(postgres, AckTimeout, time: time);
        var data = new ClusterDataSource(new ClusterOptions { ConnectionString = Proxied(rig.Skip.ConnectionString, proxy) });
        var lines = new System.Collections.Concurrent.ConcurrentQueue<string>();
        using var logging = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Debug).AddProvider(new HarnessLoggerProvider("r1", lines.Enqueue, LogLevel.Debug)));
        var ack = NewAck(rig, rig.Log.Gate, data, AckTimeout, logging.CreateLogger<ChangeAck>());
        try
        {
            var seq = await rig.WriteAsync();
            await rig.LocalAtAsync(seq);
            await rig.MemberAsync("r2");

            var started = time.GetUtcNow();
            var wait = ack.WaitAppliedAsync(seq);
            await Poll.UntilAsync(() => rig.EventsOf(LogActions.AckBegin).Count > 0, "the wait's first read");
            Assert.False(wait.IsCompleted, "The wait answered before the database stopped answering.");
            proxy.Hold();

            await Poll.UntilAsync(
                () =>
                {
                    time.Advance(TimeSpan.FromMilliseconds(100));
                    return proxy.BytesHeld || proxy.AcceptedWhileHeld > 0;
                },
                "a read at the held database; the wait logged: " + string.Join(" | ", lines),
                TimeSpan.FromSeconds(30));

            time.Advance(started + AckTimeout - time.GetUtcNow());
            var returned = await Task.WhenAny(wait, Task.Delay(InTime)) == wait;
            var opened = proxy.AcceptedWhileHeld;
            proxy.Release();

            Assert.True(
                returned,
                $"No answer within {InTime.TotalSeconds:F0} s of AckTimeout on the injected clock; connections opened to the database while it did not answer, "
                + $"the cancel request's among them: {opened}.");
            Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), await wait);
        }
        finally
        {
            proxy.Release();
            ack.Dispose();
            await data.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_record_whose_commit_a_database_that_does_not_answer_holds_does_not_delay_the_answer_past_AckTimeout()
    {
        var server = new NpgsqlConnectionStringBuilder(postgres.ConnectionString);
        await using var proxy = new HoldingProxy(server.Host!, server.Port);

        var time = new FakeTimeProvider();
        await using var rig = await AckRig.CreateAsync(postgres, AckTimeout, connectionString: cs => Proxied(cs, proxy), time: time);
        try
        {
            var seq = await rig.WriteAsync();
            await rig.LocalAtAsync(seq);
            using var stall = rig.Skip.Faults.Observe(LogFaultPoints.RecordBeforeCommit, _ =>
            {
                proxy.Hold();
                return ValueTask.CompletedTask;
            });

            var started = time.GetUtcNow();
            var wait = rig.Ack.WaitAppliedAsync(seq);
            await Poll.UntilAsync(() => proxy.BytesHeld, "the record's COMMIT held at the proxy");
            Assert.False(wait.IsCompleted, "The wait answered before the commit was held.");

            time.Advance(started + AckTimeout - time.GetUtcNow());
            var returned = await Task.WhenAny(wait, Task.Delay(InTime)) == wait;
            var opened = proxy.AcceptedWhileHeld;
            proxy.Release();

            Assert.True(
                returned,
                $"No answer within {InTime.TotalSeconds:F0} s of AckTimeout on the injected clock; connections opened to the database while it did not answer, "
                + $"the cancel request's among them: {opened}.");
            Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), await wait);
            Assert.Empty(rig.EventsOf(LogActions.AckDone));
        }
        finally
        {
            proxy.Release();
        }
    }

    internal static string Proxied(string connectionString, HoldingProxy proxy) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Host = "127.0.0.1", Port = proxy.Port, Timeout = 60 }.ConnectionString;

    internal static ChangeAck NewAck(AckRig rig, IPhaseGate gate, ClusterDataSource data, TimeSpan ackTimeout, ILogger<ChangeAck>? logger = null) =>
        new(
            rig.Log.Scopes, rig.Log.Refusal, gate, rig.Local, data, rig.Skip.Recorder, rig.Channel,
            Options.Create(new ClusterOptions
            {
                ConnectionString = rig.Skip.ConnectionString,
                ReplicaId = "r1",
                AckTimeout = ackTimeout,
                AckPollInterval = TimeSpan.FromMilliseconds(100),
            }),
            rig.Skip.Time, rig.Skip.Faults, rig.Skip.Events, logger ?? NullLogger<ChangeAck>.Instance);
}

public sealed class AckWaitEdgeTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Answer = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Pending = TimeSpan.FromMilliseconds(600);

    private readonly PostgresFixture postgres;

    public AckWaitEdgeTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task AckDone_is_recorded_when_the_call_returns_Acknowledged()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        const int calls = 200;
        var misses = 0;
        for (var i = 0; i < calls; i++)
        {
            var seq = await rig.WriteAsync();
            await rig.LocalAtAsync(seq);
            var before = rig.EventsOf(LogActions.AckDone).Count;
            var outcome = await rig.Ack.WaitAppliedAsync(seq);
            var after = rig.EventsOf(LogActions.AckDone).Count;
            Assert.IsType<AckOutcome.Acknowledged>(outcome);
            if (after != before + 1)
            {
                misses++;
            }
        }

        Assert.True(misses == 0, $"AckDone was not yet recorded when the call returned Acknowledged in {misses} of {calls} calls.");
    }

    [Fact]
    public async Task A_wait_held_before_its_first_wait_set_read_has_no_AckBegin_and_goes_on_when_released()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.AckBeforeWaitSetRead);

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await hold.WaitReachedAsync(timeout: Answer);
        await Task.Delay(300);
        Assert.False(wait.IsCompleted);
        Assert.Empty(rig.EventsOf(LogActions.AckBegin));
        hold.Release();

        Assert.Equal(AckOutcome.Acknowledged.None, await wait.WaitAsync(Answer));
        Assert.Single(rig.EventsOf(LogActions.AckBegin));
    }

    [Fact]
    public async Task A_round_that_fails_at_its_fault_point_is_retried_at_the_next_wake_up()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var fail = rig.Skip.Faults.Throw(LogFaultPoints.AckBeforeWaitSetRead, () => new InvalidOperationException("the injected failure of the first round"));

        Assert.Equal(AckOutcome.Acknowledged.None, await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.True(rig.Skip.Faults.Reached(LogFaultPoints.AckBeforeWaitSetRead) >= 2);
    }

    [Fact]
    public async Task A_wait_whose_every_read_fails_ends_at_AckTimeout_after_an_AckBegin()
    {
        await using var rig = await AckRig.CreateAsync(postgres, TimeSpan.FromSeconds(2));
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var fail = rig.Skip.Faults.Throw(
            LogFaultPoints.AckBeforeWaitSetRead, () => new InvalidOperationException("the injected failure of every round"), times: int.MaxValue);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.Single(rig.EventsOf(LogActions.AckTimeout));
        Assert.Single(rig.EventsOf(LogActions.AckBegin));
    }

    [Fact]
    public async Task A_fence_while_the_wait_is_held_between_its_count_and_its_record_answers_Unconfirmed_Fenced_and_runs_no_record()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.AckBetweenCountAndRecord);

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await hold.WaitReachedAsync(timeout: Answer);
        Assert.False(wait.IsCompleted);
        rig.Log.Gate.MoveTo(ReplicaPhase.Fencing);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Fenced), await wait.WaitAsync(Answer));
        hold.Release();
        await Task.Delay(Pending);
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
        Assert.Empty(rig.EventsOf(LogActions.AckDone));
        Assert.Equal(0L, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_fence_while_the_record_is_held_before_its_commit_answers_Unconfirmed_Fenced_and_the_record_counts_for_nothing()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RecordBeforeCommit);

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await hold.WaitReachedAsync(timeout: Answer);
        Assert.Single(rig.EventsOf(LogActions.AckRecord));
        rig.Log.Gate.MoveTo(ReplicaPhase.Fencing);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Fenced), await wait.WaitAsync(Answer));
        await rig.Skip.WaitUntilNoSessionIsIdleInATransactionAsync();
        Assert.Equal(0L, await rig.AckedSeqAsync());
        Assert.Empty(rig.EventsOf(LogActions.AckDone));
    }

    [Fact]
    public async Task A_local_cursor_that_names_no_row_holds_the_wait_until_AckTimeout()
    {
        await using var rig = await AckRig.CreateAsync(postgres, TimeSpan.FromSeconds(2));
        var seq = await rig.WriteAsync();
        rig.Local.Current = new Gen(seq, Guid.NewGuid());

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
        Assert.Equal(0L, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_local_cursor_below_the_seq_holds_the_wait_until_the_local_applier_reaches_it()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var first = await rig.WriteAsync();
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(first);

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await AssertPendingAsync(wait);
        await rig.LocalAtAsync(seq);

        Assert.Equal(AckOutcome.Acknowledged.None, await wait.WaitAsync(Answer));
        Assert.Equal(seq, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_transition_to_Serving_between_the_subscription_and_the_phase_read_does_not_end_the_wait()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var gate = new TransitionOnFirstRead(ReplicaPhase.Activated, ReplicaPhase.Serving);
        using var ack = AckWaitCancelTests.NewAck(rig, gate, rig.Data, TimeSpan.FromSeconds(20));
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);

        Assert.Equal(AckOutcome.Acknowledged.None, await ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.True(gate.Raised, "The gate never raised its transition.");
    }

    [Fact]
    public async Task An_opted_out_scope_waits_for_a_seq_its_tracker_does_not_hold()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        rig.Ack.AcknowledgeAtCommit();

        Assert.Empty(rig.Ack.Seqs);
        Assert.Equal(AckOutcome.Acknowledged.None, await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
    }

    private static async Task AssertPendingAsync(Task wait)
    {
        await Task.WhenAny(wait, Task.Delay(Pending));
        Assert.False(wait.IsCompleted, "The wait answered.");
    }

    private sealed class Gen(long head, Guid token) : IGeneration
    {
        public long Head => head;

        public Guid? HeadToken => token;

        public object Get(string aggregateType) => throw new NotSupportedException();

        public CancellationToken Superseded(string aggregateType) => CancellationToken.None;

        public bool TryActivate(string aggregateType, Action activate) => throw new NotSupportedException();
    }

    private sealed class TransitionOnFirstRead(ReplicaPhase from, ReplicaPhase to) : IPhaseGate
    {
        private readonly List<Action<PhaseEvent>> handlers = [];
        private int armed = 1;
        private int phase = (int)from;

        public bool Raised => Volatile.Read(ref armed) == 0;

        public ReplicaPhase Phase
        {
            get
            {
                if (Interlocked.Exchange(ref armed, 0) == 1)
                {
                    Volatile.Write(ref phase, (int)to);
                    Action<PhaseEvent>[] copy;
                    lock (handlers)
                    {
                        copy = [.. handlers];
                    }

                    foreach (var handler in copy)
                    {
                        handler(new PhaseEvent(PhaseEventKind.Transition, from, to));
                    }
                }

                return (ReplicaPhase)Volatile.Read(ref phase);
            }
        }

        public long? FenceStartTimestamp => null;

        public bool ExitAfterFence => false;

        public IReadinessReasons Readiness => throw new NotSupportedException();

        public IDisposable Subscribe(Action<PhaseEvent> handler)
        {
            lock (handlers)
            {
                handlers.Add(handler);
            }

            return new Nothing();
        }

        public ServingCheckResult CheckServing() => throw new NotSupportedException();

        public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

        public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

        public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

        public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        private sealed class Nothing : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
