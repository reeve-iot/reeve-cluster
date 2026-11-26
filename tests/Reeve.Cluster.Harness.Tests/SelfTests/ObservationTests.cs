using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Membership;

namespace Harness.SelfTests;

public sealed class ObservationTests
{
    private const string ConnectionString = "Host=localhost;Username=u;Database=d";

    private sealed class StubGate : IPhaseGate
    {
        private readonly List<Action<PhaseEvent>> handlers = [];

        public ReplicaPhase Phase => ReplicaPhase.Starting;

        public long? FenceStartTimestamp => null;

        public IReadinessReasons Readiness => throw new NotSupportedException();

        public bool ExitAfterFence => false;

        public int Subscribers => handlers.Count;

        public void Raise(PhaseEvent phaseEvent)
        {
            foreach (var handler in handlers.ToArray())
            {
                handler(phaseEvent);
            }
        }

        public ServingCheckResult CheckServing() => throw new NotSupportedException();

        public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

        public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

        public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

        public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable Subscribe(Action<PhaseEvent> handler)
        {
            handlers.Add(handler);
            return new Subscription(() => handlers.Remove(handler));
        }

        private sealed class Subscription(Action dispose) : IDisposable
        {
            public void Dispose() => dispose();
        }
    }

    [Fact]
    public async Task The_phase_timeline_records_what_a_replicas_gate_raises_in_the_runs_order()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var gate = new StubGate();
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(gate),
        });

        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));
        run.Events.Record("r1", new ModelEvent(MembershipActions.Join), replica.Clock);
        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Loading, ReplicaPhase.CatchingUp));
        gate.Raise(new PhaseEvent(PhaseEventKind.DrainStep4, ReplicaPhase.Draining, ReplicaPhase.Draining));

        Assert.Equal([ReplicaPhase.Loading, ReplicaPhase.CatchingUp], run.Phases.Path("r1"));
        var all = run.Phases.Of("r1");
        Assert.Equal(3, all.Count);
        Assert.True(all[0].Sequence < run.Events.Snapshot()[0].Sequence);
        Assert.True(run.Events.Snapshot()[0].Sequence < all[1].Sequence);
    }

    [Fact]
    public async Task A_replica_whose_container_has_no_gate_has_an_empty_timeline()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });

        await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services =>
            {
                services.RemoveAll<IPhaseGate>();
                services.RemoveAll<IFenceParticipant>();
                services.RemoveAll<IHostedService>();
            },
        });

        Assert.Empty(run.Phases.Snapshot());
    }

    [Fact]
    public async Task A_stopped_replica_stops_recording_its_gate()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var gate = new StubGate();
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(gate),
        });
        Assert.Equal(2, gate.Subscribers);

        await replica.DisposeAsync();

        Assert.Equal(0, gate.Subscribers);
    }

    private static ModelEvent Attempt(string action, string type, long pass, int attempt) =>
        new(action, new Dictionary<string, object?>
        {
            [ObservationKeys.Pass] = pass,
            [ObservationKeys.Attempt] = attempt,
            [ObservationKeys.Type] = type,
        });

    private static ModelEvent Publish(long generation, long head, params string[] dirty) =>
        new(LogActions.Publish, new Dictionary<string, object?>
        {
            [ObservationKeys.Generation] = generation,
            [ObservationKeys.Head] = head,
            [ObservationKeys.Dirty] = dirty,
        });

    [Fact]
    public void The_event_views_read_the_generation_each_request_read_and_the_attempt_each_publication_came_from()
    {
        var log = new ModelEventLog(new RunSequence());
        var clock = TimeProvider.System;
        log.Record("r1", Attempt(LogActions.AttemptOK, "widget", 1, 1), clock);
        log.Record("r1", Attempt(LogActions.AttemptOK, "gadget", 1, 1), clock);
        log.Record("r1", Publish(1, 12, "widget", "gadget"), clock);
        log.Record("r1", Attempt(LogActions.AttemptOK, "widget", 2, 2), clock);
        log.Record("r1", Publish(2, 15, "widget"), clock);
        log.Record("r1", new ModelEvent(LogActions.QRead, new Dictionary<string, object?>
        {
            [ObservationKeys.Request] = "q1",
            [ObservationKeys.Generation] = 2,
            [ObservationKeys.Head] = 15L,
        }), clock);
        log.Record("r2", new ModelEvent(LogActions.CursorWrite), clock);

        var publications = log.Publications();
        Assert.Equal([1L, 2L], publications.Select(p => p.Generation));
        Assert.Equal(new AttemptRef(1, 1), publications[0].Attempts["widget"]);
        Assert.Equal(new AttemptRef(2, 2), publications[1].Attempts["widget"]);
        Assert.Equal(new AttemptRef(1, 1), publications[1].Attempts["gadget"]);
        Assert.True(publications[1].Dirty.SetEquals(["widget"]));
        var read = Assert.Single(log.GenerationsRead());
        Assert.Equal(("q1", 2L, 15L), (read.Request, read.Generation, read.Head));
        Assert.Equal(["AttemptOK", "AttemptOK", "Publish", "AttemptOK", "Publish", "QRead"], log.Actions("r1"));
        Assert.Equal([LogActions.CursorWrite], log.Actions("r2"));
    }

    [Fact]
    public void A_late_attempt_that_returned_after_its_timeout_is_never_the_source_of_a_publication()
    {
        var log = new ModelEventLog(new RunSequence());
        var clock = TimeProvider.System;
        log.Record("r1", Attempt(LogActions.AttemptOK, "widget", 1, 1), clock);
        log.Record("r1", Attempt(LogActions.AttemptHang, "widget", 2, 1), clock);
        log.Record("r1", Publish(1, 5, "widget"), clock);
        log.Record("r1", Attempt(LogActions.LateReturn, "widget", 2, 1), clock);
        log.Record("r1", Publish(2, 6), clock);

        var publications = log.Publications();

        Assert.All(publications, p => Assert.Equal(new AttemptRef(1, 1), p.Attempts["widget"]));
    }

    [Fact]
    public void A_publication_of_a_type_no_attempt_returned_for_is_refused()
    {
        var log = new ModelEventLog(new RunSequence());
        log.Record("r1", Publish(1, 5, "widget"), TimeProvider.System);

        var refusal = Assert.Throws<ObservationException>(() => log.Publications());

        Assert.Contains("widget", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_view_refuses_an_event_that_lacks_a_key_instead_of_finding_nothing()
    {
        var log = new ModelEventLog(new RunSequence());
        log.Record("r1", new ModelEvent(LogActions.Publish, new Dictionary<string, object?> { ["gen"] = 3 }), TimeProvider.System);

        var refusal = Assert.Throws<ObservationException>(() => log.Publications());

        Assert.Contains("Publish", refusal.Message, StringComparison.Ordinal);
        Assert.Contains(ObservationKeys.Dirty, refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_view_refuses_an_event_that_carries_a_key_as_another_type_and_one_with_no_state_at_all()
    {
        var wrongType = new ModelEventLog(new RunSequence());
        wrongType.Record("r1", new ModelEvent(LogActions.Publish, new Dictionary<string, object?>
        {
            [ObservationKeys.Generation] = 3L,
            [ObservationKeys.Head] = "twelve",
            [ObservationKeys.Dirty] = Array.Empty<string>(),
        }), TimeProvider.System);
        var noState = new ModelEventLog(new RunSequence());
        noState.Record("r1", new ModelEvent(LogActions.QRead), TimeProvider.System);

        Assert.Throws<ObservationException>(() => wrongType.Publications());
        Assert.Throws<ObservationException>(() => noState.GenerationsRead());
    }

    [Fact]
    public void The_wait_sets_waits_counted_are_read_from_their_events()
    {
        var log = new ModelEventLog(new RunSequence());
        log.Record("r1", new ModelEvent(LogActions.AckBegin, new Dictionary<string, object?>
        {
            [ObservationKeys.Request] = "w1",
            [ObservationKeys.AwaitedSeq] = 7L,
            [ObservationKeys.WaitSet] = new[] { "r2", "r3" },
        }), TimeProvider.System);

        var counted = Assert.Single(log.WaitSets());

        Assert.Equal(7, counted.AwaitedSeq);
        Assert.True(counted.Members.SetEquals(["r2", "r3"]));
        log.Record("r1", new ModelEvent(LogActions.AckDone), TimeProvider.System);
        Assert.Throws<ObservationException>(() => log.WaitSets());
    }

    [Fact]
    public void The_log_keeps_a_snapshot_of_what_an_emitter_hands_over_and_may_reuse()
    {
        var log = new ModelEventLog(new RunSequence());
        var waitSet = new List<string> { "r2" };
        var dirty = new List<string> { "widget" };
        log.Record("r1", new ModelEvent(LogActions.AckBegin, new Dictionary<string, object?>
        {
            [ObservationKeys.Request] = "w1",
            [ObservationKeys.AwaitedSeq] = 7L,
            [ObservationKeys.WaitSet] = waitSet,
        }), TimeProvider.System);
        log.Record("r1", Attempt(LogActions.AttemptOK, "widget", 1, 1), TimeProvider.System);
        log.Record("r1", new ModelEvent(LogActions.Publish, new Dictionary<string, object?>
        {
            [ObservationKeys.Generation] = 1L,
            [ObservationKeys.Head] = 1L,
            [ObservationKeys.Dirty] = dirty,
        }), TimeProvider.System);

        waitSet.Add("r3");
        dirty.Add("gadget");

        Assert.True(Assert.Single(log.WaitSets()).Members.SetEquals(["r2"]));
        Assert.True(Assert.Single(log.Publications()).Dirty.SetEquals(["widget"]));
    }

    [Fact]
    public async Task The_timeline_reports_a_transition_that_does_not_start_where_the_previous_one_ended()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var gate = new StubGate();
        await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(gate),
        });

        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));
        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Loading, ReplicaPhase.CatchingUp));
        Assert.Empty(run.Phases.Discontinuities);

        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Serving, ReplicaPhase.Fencing));

        Assert.Single(run.Phases.Discontinuities);
    }

    [Fact]
    public async Task A_process_started_again_under_the_same_id_begins_at_starting_without_a_break_in_the_timeline()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var firstGate = new StubGate();
        var first = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(firstGate),
        });
        firstGate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));
        await first.CrashAsync();
        var secondGate = new StubGate();
        await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(secondGate),
        });

        secondGate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));

        Assert.Empty(run.Phases.Discontinuities);
        Assert.Equal([ReplicaPhase.Loading, ReplicaPhase.Loading], run.Phases.Path("r1"));
    }

    [Fact]
    public async Task A_crashed_replicas_phase_changes_are_not_recorded()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { });
        var gate = new StubGate();
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(gate),
        });
        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));

        await replica.CrashAsync();
        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Loading, ReplicaPhase.Fencing));

        Assert.Equal([ReplicaPhase.Loading], run.Phases.Path("r1"));
    }

    [Fact]
    public async Task The_report_of_a_run_starts_with_its_seed_and_lists_arrivals_events_and_phases_in_order()
    {
        var lines = new List<string>();
        await using var run = await HarnessRun.BeginAsync(lines.Add, seed: 4242);
        var gate = new StubGate();
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec
        {
            ConnectionString = ConnectionString,
            ConfigureServices = services => services.AddSingleton<IPhaseGate>(gate),
        });

        await replica.Faults.ReachAsync(MembershipFaultPoints.RenewSend);
        gate.Raise(new PhaseEvent(PhaseEventKind.Transition, ReplicaPhase.Starting, ReplicaPhase.Loading));
        run.Events.Record("r1", new ModelEvent(MembershipActions.RenewSend), replica.Clock);
        var report = run.Report().Split(Environment.NewLine).Where(l => !l.Contains("membership.join", StringComparison.Ordinal)).ToArray();

        Assert.Contains("4242", report[0], StringComparison.Ordinal);
        Assert.Contains("reached membership.renew.send", report[1], StringComparison.Ordinal);
        Assert.Contains("phase Starting -> Loading", report[2], StringComparison.Ordinal);
        Assert.Contains("event RenewSend", report[3], StringComparison.Ordinal);
        Assert.Equal("harness seed: 4242 (replay with REEVE_HARNESS_SEED=4242)", lines[0]);
    }
}
