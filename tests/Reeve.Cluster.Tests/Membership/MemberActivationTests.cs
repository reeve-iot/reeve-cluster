using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Reeve.Cluster.Wakeup;

namespace Unit.Membership;

public sealed class MemberActivationTests : IDisposable
{
    private const long Epoch = 7;
    private static readonly Task Now = Task.CompletedTask;
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly ScriptedActivationStore store = new();
    private readonly ScriptedLoad load = new();
    private readonly InProcessWakeupChannel channel = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly LocalPhaseGate gate;
    private readonly MemberActivation activation;
    private readonly MemberIdentity identity = new(Guid.NewGuid(), "1.4.0", MemberIdentity.CurrentProtocol, []);

    public MemberActivationTests()
    {
        var options = Options.Create(MembershipTestOptions.Default());
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        activation = new MemberActivation(gate, store, load, identity, channel, options, time, NoFaultPoints.Instance, NullLogger<MemberActivation>.Instance, NoUpgradePathGuard.Instance);
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Apply(MemberTrigger.StateLoaded).To);
    }

    public void Dispose()
    {
        activation.Dispose();
        gate.Dispose();
    }

    private Task RoundAsync(long epoch = Epoch) => activation.RunAsync(gate.Phase, epoch, Now, CancellationToken.None).WaitAsync(Wait);

    private static ActivationAnswer Answer(ActivationOutcome outcome, long head = 12, int floor = 0, string? versionFloor = null) =>
        new(outcome, head, floor, versionFloor, FloorsRead: true);

    private async Task ActivateAtAsync(long head)
    {
        load.LastPublished = new PublishedPass(new Cursor(head - 1, Guid.NewGuid()), 5, Full: true);
        store.Backlog = new Backlog(true, 1);
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed, head));
        await RoundAsync();
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    [Fact]
    public async Task M06_more_entries_after_the_cursor_than_the_last_pass_applied_runs_no_transaction()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 2, Full: false);
        store.Backlog = new Backlog(true, 3);

        await RoundAsync();

        Assert.Equal((10L, 2L), (store.BacklogReads.Single().Cursor.Seq, store.BacklogReads.Single().Threshold));
        Assert.Equal(0, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
    }

    [Fact]
    public async Task M06_the_first_activation_after_a_full_load_counts_against_the_loads_entry_count()
    {
        load.LastPublished = new PublishedPass(new Cursor(40, Guid.NewGuid()), 40, Full: true);
        store.Backlog = new Backlog(true, 40);
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed, 80));

        await RoundAsync();

        Assert.Equal(40L, store.BacklogReads.Single().Threshold);
        Assert.Equal(1, store.Activations);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    [Fact]
    public async Task M06_after_a_pass_that_stalled_the_threshold_is_0_not_the_last_published_passs_count()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: true);
        load.Stalled = 0;
        store.Backlog = new Backlog(true, 1);

        await RoundAsync();

        Assert.Equal((10L, 0L), (store.BacklogReads.Single().Cursor.Seq, store.BacklogReads.Single().Threshold));
        Assert.Equal(0, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
    }

    [Fact]
    public async Task M06_an_invalid_cursor_is_not_caught_up()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(false, 0);

        await RoundAsync();

        Assert.Equal(0, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
    }

    [Fact]
    public async Task M06_before_any_pass_published_nothing_is_read()
    {
        load.LastPublished = null;

        await RoundAsync();

        Assert.Empty(store.BacklogReads);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
    }

    [Fact]
    public async Task M06_a_committed_activation_enters_Activated_with_its_head_and_emits_Activate()
    {
        await ActivateAtAsync(12);

        Assert.Equal(new ActivationRequest("r1", Epoch, "1.4.0", MemberIdentity.CurrentProtocol), store.Requests.Single());
        Assert.Equal(12, activation.Current!.Head);
        Assert.Null(activation.Current.ServingSince);
        var activate = Assert.Single(events.Of(MembershipActions.Activate));
        Assert.Equal(Epoch, activate.State!["epoch"]);
        Assert.Equal(12L, activate.State!["head"]);
        Assert.Equal(ServingCheckOutcome.NotServing, gate.CheckServing().Outcome);
    }

    [Fact]
    public async Task M06_the_phase_moves_to_Activated_only_if_it_is_still_CatchingUp_when_the_commit_returns()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.During = () => gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed));

        await RoundAsync();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.False(store.MayCommitAfterCall, "the commit predicate must refuse once the phase left CatchingUp");
        Assert.Empty(events.Of(MembershipActions.Activate));
        Assert.Null(activation.Current);
    }

    [Fact]
    public async Task M06_a_commit_that_returns_after_a_rejoin_changes_nothing_in_the_next_epoch()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.During = () =>
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            gate.Joined(Epoch + 1, 1, gate.Now);
            gate.Apply(MemberTrigger.StateLoaded);
        };
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed));

        await RoundAsync();

        Assert.Equal((ReplicaPhase.CatchingUp, (long?)(Epoch + 1)), (gate.Phase, gate.Epoch));
        Assert.False(store.MayCommitAfterCall);
        Assert.Empty(events.Of(MembershipActions.Activate));
    }

    [Fact]
    public void The_gate_applies_an_epochs_trigger_only_to_that_epoch()
    {
        Assert.True(gate.Apply(MemberTrigger.ActivationCommitted, Epoch - 1).IsIgnored);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Equal(ReplicaPhase.Activated, gate.Apply(MemberTrigger.ActivationCommitted, Epoch).To);
    }

    [Theory]
    [InlineData("RefusedByVersionFloor", ActivationRules.VersionFloorReason)]
    [InlineData("RefusedByProtocolFloor", ActivationRules.ProtocolFloorReason)]
    public async Task I14_a_floors_refusal_stays_CatchingUp_with_its_named_reason_and_a_later_activation_clears_it(string outcome, string reason)
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.Answers.Enqueue(Answer(Enum.Parse<ActivationOutcome>(outcome), floor: 9, versionFloor: "9.0.0"));

        await RoundAsync();

        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        var raised = Assert.Single(gate.Readiness.Current, r => r.Code == reason);
        if (reason == ActivationRules.VersionFloorReason)
        {
            Assert.Contains("'9.0.0'", raised.Description, StringComparison.Ordinal);
            Assert.Contains("'1.4.0'", raised.Description, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("protocol floor is 9;", raised.Description, StringComparison.Ordinal);
            Assert.Contains($"speaks protocol {MemberIdentity.CurrentProtocol}.", raised.Description, StringComparison.Ordinal);
        }

        Assert.Equal((9, "9.0.0"), (activation.Floors!.ProtocolFloor, activation.Floors.VersionFloor));
        Assert.Equal(ServingCheckOutcome.NotServing, gate.CheckServing().Outcome);

        store.Answers.Enqueue(Answer(ActivationOutcome.Committed));
        await RoundAsync();

        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.DoesNotContain(gate.Readiness.Current, r => r.Code == reason);
    }

    [Theory]
    [InlineData("Abandoned")]
    [InlineData("LeftCatchingUp")]
    public async Task An_abandoned_activation_or_one_that_left_CatchingUp_stays_CatchingUp_with_no_reason(string outcome)
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.Answers.Enqueue(Answer(ActivationOutcome.Abandoned, floor: 3, versionFloor: "3.0.0"));
        await RoundAsync();
        store.Answers.Enqueue(Answer(Enum.Parse<ActivationOutcome>(outcome)));

        await RoundAsync();

        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(gate.Readiness.Current);
        Assert.Empty(events.Of(MembershipActions.Activate));
    }

    [Theory]
    [InlineData("cancelled")]
    [InlineData("connection-failed")]
    public async Task SIMP8_an_activation_whose_commit_does_not_answer_fences_the_replica_once_and_nothing_admits_after_it(string how)
    {
        var fences = new List<FenceRequest>();
        using var subscription = gate.OnFenceRequested(fences.Add);
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.Answers.Enqueue(Answer(ActivationOutcome.Abandoned, floor: 3, versionFloor: "3.0.0"));
        await RoundAsync();
        store.Throws = how == "cancelled" ? new OperationCanceledException("the wait for the standby was cancelled") : new IOException("connection reset");
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed));

        await RoundAsync();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        var fence = Assert.Single(fences);
        Assert.Equal(FenceTrigger.UnansweredActivation, fence.Trigger);
        Assert.Equal("unanswered-activation", FenceTriggerNames.Of(fence.Trigger));
        Assert.Equal((Epoch, ReplicaPhase.CatchingUp), (gate.CurrentFence!.Epoch, gate.CurrentFence.From));
        Assert.Single(events.Of(MembershipActions.FenceAny));

        store.Throws = null;
        await RoundAsync();
        Assert.Equal(2, store.Activations);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Single(fences);
        Assert.Empty(events.Of(MembershipActions.Activate));
        Assert.Null(activation.Current);
        Assert.Empty(gate.Readiness.Current);

        Assert.Equal((3, "3.0.0"), (activation.Floors!.ProtocolFloor, activation.Floors.VersionFloor));
    }

    [Fact]
    public async Task SIMP8_a_graceful_stop_during_the_commits_wait_raises_no_fence_and_the_cancel_propagates()
    {
        var fences = new List<FenceRequest>();
        using var subscription = gate.OnFenceRequested(fences.Add);
        using var stop = new CancellationTokenSource();
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.During = stop.Cancel;
        store.Throws = new OperationCanceledException(stop.Token);

        var error = await Record.ExceptionAsync(() => activation.RunAsync(gate.Phase, Epoch, Now, stop.Token).WaitAsync(Wait));

        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(1, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        Assert.Empty(fences);
        Assert.Empty(events.Of(MembershipActions.FenceAny));
    }

    [Fact]
    public async Task SIMP8_an_unanswered_activation_during_a_fence_from_another_trigger_does_not_fence_again()
    {
        var fences = new List<FenceRequest>();
        using var subscription = gate.OnFenceRequested(fences.Add);
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.During = () => gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "fence start came first"));
        store.Throws = new OperationCanceledException("no answer");

        await RoundAsync();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.FenceStart, Assert.Single(fences).Trigger);
        Assert.Equal(FenceTrigger.FenceStart, gate.CurrentFence!.Request.Trigger);
        Assert.False(gate.ExitAfterFence);
        Assert.Empty(events.Of(MembershipActions.FenceAny));
    }

    [Fact]
    public async Task SIMP8_an_activation_that_answers_nothing_after_a_rejoin_does_not_fence_the_next_epoch()
    {
        var fences = new List<FenceRequest>();
        using var subscription = gate.OnFenceRequested(fences.Add);
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.During = () =>
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));
            gate.CompleteFence();
            gate.Apply(MemberTrigger.RejoinPermitted);
            gate.Joined(Epoch + 1, 1, gate.Now);
            gate.Apply(MemberTrigger.StateLoaded);
        };
        store.Throws = new IOException("connection reset");

        await RoundAsync();

        Assert.Equal((ReplicaPhase.CatchingUp, (long?)(Epoch + 1)), (gate.Phase, gate.Epoch));
        Assert.Equal(FenceTrigger.FatalFault, Assert.Single(fences).Trigger);
        Assert.Null(gate.CurrentFence);
    }

    [Fact]
    public async Task A_refused_round_waits_RenewInterval_before_the_next_attempt()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.Answers.Enqueue(Answer(ActivationOutcome.RefusedByVersionFloor, versionFloor: "9.0.0"));
        var never = new TaskCompletionSource().Task;

        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        await channel.PublishAsync(new Wakeup(WakeupKind.Applied, 11));
        time.Advance(MembershipTestOptions.Default().RenewInterval - TimeSpan.FromTicks(1));
        Assert.False(round.IsCompleted, "a refused attempt is retried after RenewInterval, not sooner");

        time.Advance(TimeSpan.FromTicks(1));
        await round.WaitAsync(Wait);
        Assert.Equal(1, store.Activations);
    }

    [Fact]
    public async Task A_round_after_a_consumed_applied_notice_waits_for_a_new_one_or_the_poll()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 0, Full: false);
        store.Backlog = new Backlog(true, 1);
        var never = new TaskCompletionSource().Task;
        var first = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        await channel.PublishAsync(new Wakeup(WakeupKind.Applied, 11));
        await first.WaitAsync(Wait);

        var second = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        Assert.False(second.IsCompleted, "the notice the first round consumed must not end the second");

        time.Advance(MembershipTestOptions.Default().ApplyPollInterval);
        await second.WaitAsync(Wait);
    }

    [Fact]
    public async Task One_floor_refusal_adds_1_to_the_refusals_counter_with_its_reason()
    {
        using var listener = new RefusalListener();
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 5, Full: false);
        store.Backlog = new Backlog(true, 0);
        store.Answers.Enqueue(Answer(ActivationOutcome.RefusedByProtocolFloor, floor: 9));

        await RoundAsync();

        Assert.Equal([(1L, ActivationRules.ProtocolFloorReason)], listener.Measurements());
        Assert.Equal("{refusal}", listener.Unit);
        Assert.Contains("attempts", listener.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task M07_Activated_waits_until_the_applied_cursor_reaches_the_head_read_at_activation()
    {
        await ActivateAtAsync(12);

        load.Cursor = new Cursor(11, Guid.NewGuid());
        await RoundAsync();
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Empty(store.ServingWrites);
        Assert.Equal(ServingCheckOutcome.NotServing, gate.CheckServing().Outcome);

        load.Cursor = new Cursor(12, Guid.NewGuid());
        await RoundAsync();
        Assert.Equal(("r1", Epoch, load.Cursor), store.ServingWrites.Single());
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Equal(ServingCheckOutcome.Serving, gate.CheckServing().Outcome);
        Assert.NotNull(activation.Current!.ServingSince);
        var serving = Assert.Single(events.Of(MembershipActions.EnterServing));
        Assert.Equal((Epoch, 12L, 12L), (serving.State!["epoch"], serving.State!["head"], serving.State!["act"]));
    }

    [Fact]
    public async Task M07_a_serving_since_write_that_throws_stays_Activated_and_the_next_round_enters_Serving()
    {
        await ActivateAtAsync(12);
        load.Cursor = new Cursor(12, Guid.NewGuid());
        store.ServingThrows = 1;

        await RoundAsync();
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Empty(events.Of(MembershipActions.EnterServing));
        Assert.Null(activation.Current!.ServingSince);

        await RoundAsync();
        Assert.Equal(2, store.ServingWrites.Count);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Single(events.Of(MembershipActions.EnterServing));
    }

    [Fact]
    public async Task M07_a_serving_since_write_that_touches_no_row_stays_Activated()
    {
        await ActivateAtAsync(12);
        load.Cursor = new Cursor(12, Guid.NewGuid());
        store.ServingAnswer = false;

        await RoundAsync();

        Assert.Single(store.ServingWrites);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.Empty(events.Of(MembershipActions.EnterServing));
    }

    [Fact]
    public async Task An_applied_notice_ends_a_rounds_wait()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 0, Full: false);
        store.Backlog = new Backlog(true, 1);
        var never = new TaskCompletionSource().Task;
        var round = activation.RunAsync(ReplicaPhase.CatchingUp, Epoch, never, CancellationToken.None);
        Assert.False(round.IsCompleted, "the round waits for a notice, a change of phase or ApplyPollInterval");

        await channel.PublishAsync(new Wakeup(WakeupKind.Applied, 11));
        await round.WaitAsync(Wait);
    }

    [Fact]
    public async Task The_status_sections_report_the_phase_the_activation_head_and_the_floors_read()
    {
        await ActivateAtAsync(12);

        var phase = await new PhaseStatusSection(gate, activation).CollectAsync(CancellationToken.None);
        var floors = await new FloorStatusSection(activation, identity).CollectAsync(CancellationToken.None);

        Assert.Equal(PhaseStatusSection.Name, phase.Name);
        Assert.Equal("Activated", phase.Values["phase"]);
        Assert.Equal(Epoch, phase.Values["epoch"]);
        Assert.Equal(12L, phase.Values["activatedAtSeq"]);
        Assert.Equal(false, phase.Values["ready"]);
        Assert.Equal(FloorStatusSection.Name, floors.Name);
        Assert.Equal(0, floors.Values["protocolFloor"]);
        Assert.Null(floors.Values["versionFloor"]);
        Assert.Equal("1.4.0", floors.Values["version"]);
        Assert.Equal(MemberIdentity.CurrentProtocol, floors.Values["protocol"]);
    }

    private MemberActivation GuardedActivation(ScriptedGuard guard) => new(
        gate, store, load, identity, channel, Options.Create(MembershipTestOptions.Default()), time, NoFaultPoints.Instance,
        NullLogger<MemberActivation>.Instance, guard);

    private void CaughtUp()
    {
        load.LastPublished = new PublishedPass(new Cursor(11, Guid.NewGuid()), 5, Full: true);
        store.Backlog = new Backlog(true, 1);
    }

    private Task GuardedRoundAsync(MemberActivation guarded) => guarded.RunAsync(gate.Phase, Epoch, Now, CancellationToken.None).WaitAsync(Wait);

    [Fact]
    public async Task E_an_active_member_that_speaks_less_than_this_release_requires_keeps_it_joining_and_raises_the_reason()
    {
        CaughtUp();
        var guard = new ScriptedGuard { Blocker = new OlderActiveMember("old", 1) };
        using var guarded = GuardedActivation(guard);

        await GuardedRoundAsync(guarded);

        Assert.Equal(0, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
        var reason = Assert.Single(gate.Readiness.Current, r => r.Code == UpgradePathRules.Reason);
        Assert.Contains("'old'", reason.Description, StringComparison.Ordinal);
        Assert.Contains("protocol 1", reason.Description, StringComparison.Ordinal);
        Assert.Equal([MembershipTestOptions.Default().ReplicaId!], guard.Asked);
    }

    [Fact]
    public async Task E_when_the_older_member_is_gone_the_next_round_activates_and_the_reason_is_cleared()
    {
        CaughtUp();
        var guard = new ScriptedGuard { Blocker = new OlderActiveMember("old", 1) };
        using var guarded = GuardedActivation(guard);
        await GuardedRoundAsync(guarded);
        guard.Blocker = null;
        store.Answers.Enqueue(Answer(ActivationOutcome.Committed, 12));

        await GuardedRoundAsync(guarded);

        Assert.Equal(1, store.Activations);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
        Assert.DoesNotContain(gate.Readiness.Current, r => r.Code == UpgradePathRules.Reason);
    }

    [Fact]
    public async Task E_a_guard_that_got_no_answer_keeps_the_member_joining_and_runs_no_transaction()
    {
        CaughtUp();
        using var guarded = GuardedActivation(new ScriptedGuard { Throws = new TimeoutException("no answer") });

        await GuardedRoundAsync(guarded);

        Assert.Equal(0, store.Activations);
        Assert.Equal(ReplicaPhase.CatchingUp, gate.Phase);
    }

    [Fact]
    public async Task E_the_guard_is_asked_only_once_the_member_is_caught_up()
    {
        load.LastPublished = new PublishedPass(new Cursor(10, Guid.NewGuid()), 2, Full: false);
        store.Backlog = new Backlog(true, 3);
        var guard = new ScriptedGuard();
        using var guarded = GuardedActivation(guard);

        await GuardedRoundAsync(guarded);

        Assert.Empty(guard.Asked);
    }

    [Fact]
    public async Task E_a_change_of_phase_clears_the_guards_reason()
    {
        CaughtUp();
        using var guarded = GuardedActivation(new ScriptedGuard { Blocker = new OlderActiveMember("old", 1) });
        await GuardedRoundAsync(guarded);
        Assert.Contains(gate.Readiness.Current, r => r.Code == UpgradePathRules.Reason);

        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"), Epoch);

        Assert.DoesNotContain(gate.Readiness.Current, r => r.Code == UpgradePathRules.Reason);
    }

    [Fact]
    public async Task The_floor_status_section_reports_who_moved_a_floor_last()
    {
        load.LastPublished = new PublishedPass(new Cursor(11, Guid.NewGuid()), 5, Full: true);
        store.Backlog = new Backlog(true, 1);
        var movedAt = new DateTimeOffset(2026, 10, 30, 12, 0, 0, TimeSpan.Zero);
        store.Answers.Enqueue(new ActivationAnswer(ActivationOutcome.Committed, 12, 2, "1.2.0", FloorsRead: true, new FloorMove("lower:alice", movedAt)));
        await RoundAsync();

        var floors = await new FloorStatusSection(activation, identity).CollectAsync(CancellationToken.None);

        Assert.Equal(("lower:alice", movedAt), (floors.Values["movedBy"], floors.Values["movedAt"]));
    }

    private sealed class ScriptedGuard : IUpgradePathGuard
    {
        public OlderActiveMember? Blocker { get; set; }

        public Exception? Throws { get; set; }

        public List<string> Asked { get; } = [];

        public int MinimumPeerProtocol => 2;

        public Task<OlderActiveMember?> FindBlockerAsync(string replicaId, CancellationToken cancellationToken)
        {
            Asked.Add(replicaId);
            return Throws is { } exception ? Task.FromException<OlderActiveMember?>(exception) : Task.FromResult(Blocker);
        }
    }

    private sealed class ScriptedLoad : IMemberLoad
    {
        public Cursor Cursor { get; set; } = Cursor.Empty;

        public PublishedPass? LastPublished { get; set; }

        public long? Stalled { get; set; }

        public long LastPassEntries => Stalled ?? LastPublished?.Entries ?? 0;

        public Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken) => Task.FromResult(LoadOutcome.Loaded);

        public void StartApplying(long epoch)
        {
        }

        public void StopForGood()
        {
        }
    }

    private sealed class ScriptedActivationStore : IActivationStore
    {
        public Backlog Backlog { get; set; }

        public System.Collections.Concurrent.ConcurrentQueue<ActivationAnswer> Answers { get; } = new();

        public List<(Cursor Cursor, long Threshold)> BacklogReads { get; } = [];

        public List<ActivationRequest> Requests { get; } = [];

        public List<(string ReplicaId, long Epoch, Cursor Applied)> ServingWrites { get; } = [];

        public int Activations => Requests.Count;

        public Action? During { get; set; }

        public Exception? Throws { get; set; }

        public bool MayCommitAfterCall { get; private set; }

        public bool ServingAnswer { get; set; } = true;

        public int ServingThrows { get; set; }

        public Task<Backlog> ReadBacklogAsync(Cursor cursor, long threshold, CancellationToken cancellationToken)
        {
            BacklogReads.Add((cursor, threshold));
            return Task.FromResult(Backlog);
        }

        public Task<ActivationAnswer> ActivateAsync(ActivationRequest request, Func<bool> mayCommit, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            During?.Invoke();
            MayCommitAfterCall = mayCommit();
            if (Throws is { } exception)
            {
                throw exception;
            }

            return Task.FromResult(Answers.TryDequeue(out var answer) ? answer : new ActivationAnswer(ActivationOutcome.Abandoned, 0, 0, null, true));
        }

        public Task<bool> SetServingSinceAsync(string replicaId, long epoch, Cursor applied, CancellationToken cancellationToken)
        {
            ServingWrites.Add((replicaId, epoch, applied));
            if (ServingThrows > 0)
            {
                ServingThrows--;
                throw new TimeoutException("no answer");
            }

            return Task.FromResult(ServingAnswer);
        }
    }

    private sealed class RefusalListener : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly List<(long, string)> measurements = [];

        public RefusalListener()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == ClusterDiagnostics.Name && instrument.Name == MemberActivation.RefusalsName)
                {
                    Unit = instrument.Unit;
                    Description = instrument.Description;
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "reason")
                    {
                        lock (measurements)
                        {
                            measurements.Add((value, (string)tag.Value!));
                        }
                    }
                }
            });
            listener.Start();
        }

        public string? Unit { get; private set; }

        public string? Description { get; private set; }

        public IReadOnlyList<(long, string)> Measurements()
        {
            lock (measurements)
            {
                return [.. measurements];
            }
        }

        public void Dispose() => listener.Dispose();
    }
}
