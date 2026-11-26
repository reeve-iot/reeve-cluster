using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Generations;
using Unit.Membership;
using Unit.Testing;

namespace Unit.Owned;

public sealed class UnitParticipantTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly ClusterScopeSource scopes = new(new FenceSteps());
    private readonly GenerationStore generations = Stores.Create("config");
    private readonly HeldPoint effect = new(OwnedFaultPoints.EnsureOwnerBeforeEffect);
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly UnitRuntime runtime;

    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);

    public UnitParticipantTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new Unit.Membership.RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        UnitKind[] registered =
        [
            new("widget", typeof(RecordingUnit), new UnitKindOptions()),
            new("sweep", typeof(RecordingUnit), new UnitKindOptions { Convergent = true }),
        ];
        runtime = new UnitRuntime(
            registered, services, rows, gate, scopes, generations, effect, capabilities, wakeups, new NoTenureFence(), Options.Create(options), time,
            NullLogger<UnitRuntime>.Instance);
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        effect.Release();
        fenceTimer.Release();
        runtime.Dispose();
        gate.Dispose();
    }

    private async Task<Dictionary<string, (CancellationToken Stop, OwnedContext Context)>> StartAsync(params HeldRow[] held)
    {
        rows.Held = held;
        await runtime.PassAsync(CancellationToken.None);
        var started = new Dictionary<string, (CancellationToken, OwnedContext)>();
        foreach (var _ in held)
        {
            var next = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            started[next.Key] = (next.Stop, next.Context);
        }

        return started;
    }

    private static HeldRow Widget(string key, long token, bool requested = false) => new("widget", key, token, requested);

    private static HeldRow Sweep(string key, long token) => new("sweep", key, token, false);

    private IReadOnlyList<(string Kind, string Key, string Owner, long Epoch, long Token)> Released()
    {
        var released = new List<(string, string, string, long, long)>();
        while (rows.Releases.Reader.TryRead(out var release))
        {
            released.Add(release);
        }

        return released;
    }

    [Fact]
    public async Task EnsureOwner_passes_while_the_unit_serves_and_throws_after_its_release_request()
    {
        var started = await StartAsync(Widget("a", 10));
        started["a"].Context.EnsureOwner();

        rows.Held = [Widget("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.Throws<OwnershipLostException>(started["a"].Context.EnsureOwner);
    }

    [Fact]
    public async Task Fence_step_0_cancels_every_stop_token_convergent_ones_included_and_EnsureOwner_throws()
    {
        var started = await StartAsync(Widget("a", 10), Sweep("s", 20));

        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(started["a"].Stop.IsCancellationRequested);
        Assert.True(started["s"].Stop.IsCancellationRequested);
        Assert.Throws<OwnershipLostException>(started["a"].Context.EnsureOwner);

        Assert.Empty(unit.LostKeys);
    }

    [Fact]
    public async Task I1_a_process_paused_across_fence_start_fails_EnsureOwner_and_the_check_fences()
    {
        var started = await StartAsync(Widget("a", 10));

        fenceTimer.Arm();
        time.Advance(options.LeaseTtl);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        Assert.Throws<OwnershipLostException>(started["a"].Context.EnsureOwner);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.True(started["a"].Stop.IsCancellationRequested);
    }

    [Fact]
    public async Task A_fenced_start_carries_the_tenure_the_joined_timeline_and_FencedTransactionTimeout()
    {
        var started = await StartAsync(Widget("a", 10));

        var start = ((IFencedOwner)started["a"].Context).Start();

        Assert.Equal(new FencedStart("widget", "a", 10, 1, (int)options.FencedTransactionTimeout.TotalMilliseconds), start);
    }

    [Fact]
    public async Task BeginFencedTransaction_refuses_a_tenure_being_stopped_before_it_touches_the_connection()
    {
        var started = await StartAsync(Widget("a", 10));
        rows.Held = [Widget("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);

        await using var closed = new Npgsql.NpgsqlConnection();
        await Assert.ThrowsAsync<OwnershipLostException>(() => started["a"].Context.BeginFencedTransactionAsync(closed));
    }

    [Fact]
    public async Task I32_an_assert_owner_refused_on_another_timeline_raises_the_timeline_trigger_of_the_tenures_join()
    {
        var started = await StartAsync(Widget("a", 10));

        ((IFencedOwner)started["a"].Context).OnOtherTimeline();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.Timeline, gate.CurrentFence?.Request.Trigger);
        Assert.True(started["a"].Stop.IsCancellationRequested);
    }

    [Fact]
    public async Task A_fenced_start_whose_EnsureOwner_passed_before_a_fence_and_a_rejoin_is_refused()
    {
        var started = await StartAsync(Widget("a", 10));
        var owner = (IFencedOwner)started["a"].Context;

        effect.Arm();
        var start = Task.Run(owner.Start);
        await effect.Reached.WaitAsync(Wait);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);
        Assert.True(gate.Joined(Epoch + 1, 2, gate.Now));
        effect.Release();

        await Assert.ThrowsAsync<OwnershipLostException>(() => start.WaitAsync(Wait));
    }

    [Fact]
    public void I32_a_refusal_on_another_timeline_under_an_earlier_epoch_fences_nothing()
    {
        Assert.False(gate.TryGetTimeline(Epoch - 1, out _));
        Assert.True(gate.TryGetTimeline(Epoch, out var timeline));
        Assert.Equal(1, timeline);

        gate.OnOtherTimeline(Epoch - 1);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Fact]
    public async Task Fence_step_5_runs_OnLostAsync_of_every_unit_and_releases_nothing()
    {
        await StartAsync(Widget("a", 10), Sweep("s", 20));
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        await runtime.StopAllAsync(drain: false, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(["a", "s"], unit.LostKeys.Order());
        Assert.Empty(Released());
    }

    [Fact]
    public async Task Fence_step_5_waits_for_a_hung_OnLostAsync_until_StopBudget()
    {
        await StartAsync(Widget("a", 10));
        unit.HangLost("a");
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        var step = runtime.StopAllAsync(drain: false, CancellationToken.None);

        await unit.LostStarted("a").WaitAsync(Wait);
        time.Advance(options.StopBudget - TimeSpan.FromMilliseconds(1));
        Assert.False(step.IsCompleted);

        time.Advance(TimeSpan.FromMilliseconds(1));
        await step.WaitAsync(Wait);
        Assert.Empty(Released());
    }

    [Fact]
    public async Task Fence_step_5_does_not_stop_again_a_unit_whose_release_is_finishing_and_waits_for_its_release()
    {
        await StartAsync(Widget("a", 10));
        unit.HangLost("a");
        rows.Held = [Widget("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);

        await unit.LostStarted("a").WaitAsync(Wait);
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        var step = runtime.StopAllAsync(drain: false, CancellationToken.None);
        Assert.False(step.IsCompleted);
        time.Advance(options.StopBudget);
        await step.WaitAsync(Wait);

        Assert.Equal(["a"], unit.LostKeys);
        Assert.Equal(10L, Assert.Single(Released()).Token);
    }

    [Fact]
    public async Task Drain_step_2_stops_every_unit_and_releases_only_the_non_convergent_rows()
    {
        var started = await StartAsync(Widget("a", 10), Sweep("s", 20));
        gate.Apply(MemberTrigger.GracefulStop);
        Assert.Equal(ReplicaPhase.Draining, gate.Phase);

        Assert.Throws<OwnershipLostException>(started["a"].Context.EnsureOwner);

        await runtime.StopAllAsync(drain: true, CancellationToken.None).WaitAsync(Wait);

        Assert.True(started["a"].Stop.IsCancellationRequested);
        Assert.True(started["s"].Stop.IsCancellationRequested);
        Assert.Equal(["a", "s"], unit.LostKeys.Order());
        Assert.Equal([("widget", "a", "r1", Epoch, 10L)], Released());
    }

    [Fact]
    public async Task A_convergent_unit_gets_no_token_and_cannot_use_EnsureOwner_or_a_fenced_transaction()
    {
        var started = await StartAsync(Sweep("s", 20));
        var context = started["s"].Context;

        Assert.Contains("convergent", Assert.Throws<InvalidOperationException>(() => context.Token).Message, StringComparison.Ordinal);
        Assert.Contains("convergent", Assert.Throws<InvalidOperationException>(context.EnsureOwner).Message, StringComparison.Ordinal);

        await using var closed = new Npgsql.NpgsqlConnection();
        var fenced = await Assert.ThrowsAsync<InvalidOperationException>(() => context.BeginFencedTransactionAsync(closed));
        Assert.Contains("convergent", fenced.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_poll_judges_a_convergent_units_row_by_its_token_though_the_unit_gets_none()
    {
        var started = await StartAsync(Sweep("s", 20), Sweep("t", 21), Sweep("u", 22));

        rows.Held = [new HeldRow("sweep", "s", 20, true), Sweep("t", 23)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.All(started.Values, s => Assert.True(s.Stop.IsCancellationRequested));
        foreach (var key in new[] { "s", "t", "u" })
        {
            await unit.LostReturned(key).WaitAsync(Wait);
        }

        var released = new[]
        {
            await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait),
            await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait),
        };
        Assert.Equal([("sweep", "s", "r1", Epoch, 20L), ("sweep", "t", "r1", Epoch, 23L)], released.Order());
        Assert.Empty(Released());
    }

    [Fact]
    public async Task The_fault_point_holds_a_unit_after_EnsureOwner_passed_and_is_not_reached_by_one_that_throws()
    {
        var started = await StartAsync(Widget("a", 10), Widget("b", 11));
        rows.Held = [Widget("a", 10), Widget("b", 11, requested: true)];
        await runtime.PassAsync(CancellationToken.None);
        effect.Arm();

        await Assert.ThrowsAsync<OwnershipLostException>(() => Task.Run(started["b"].Context.EnsureOwner).WaitAsync(Wait));
        Assert.False(effect.Reached.IsCompleted);

        var held = Task.Run(started["a"].Context.EnsureOwner);
        await effect.Reached.WaitAsync(Wait);
        Assert.False(held.IsCompleted);
        effect.Release();
        await held.WaitAsync(Wait);
    }

    [Fact]
    public async Task Configuration_pins_the_current_generation_once_per_unit_of_work()
    {
        var started = await StartAsync(Widget("a", 10));
        var context = started["a"].Context;
        var first = await generations.PublishAsync(1, null, ("config", "one"));

        Assert.Throws<InvalidOperationException>(() => context.Configuration);
        using (scopes.BeginUnitOfWork())
        {
            Assert.Same(first, context.Configuration);
            await generations.PublishAsync(2, null, ("config", "two"));
            Assert.Same(first, context.Configuration);
        }

        using (scopes.BeginUnitOfWork())
        {
            Assert.Equal(2, context.Configuration.Head);
        }
    }

    [Fact]
    public async Task Configuration_is_not_pinned_once_the_unit_is_being_stopped()
    {
        var started = await StartAsync(Widget("a", 10));
        await generations.PublishAsync(1, null, ("config", "one"));
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        using var scope = scopes.BeginUnitOfWork();
        Assert.Throws<OwnershipLostException>(() => started["a"].Context.Configuration);
        Assert.Null(scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task No_unit_starts_after_fence_step_0()
    {
        rows.Held = [Widget("a", 10)];
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));

        await runtime.PassAsync(CancellationToken.None);
        await runtime.StopAllAsync(drain: false, CancellationToken.None).WaitAsync(Wait);

        Assert.False(unit.Acquired.Reader.TryRead(out _));
        Assert.Empty(rows.Reads);
    }

    [Fact]
    public void The_container_registers_the_fence_step_5_and_drain_step_2_participants()
    {
        var services = new ServiceCollection().AddClusterOwned();

        Assert.Contains(services, d => d.ServiceType == typeof(IFenceParticipant) && d.ImplementationType == typeof(UnitFenceParticipant));
        Assert.Contains(services, d => d.ServiceType == typeof(IDrainParticipant) && d.ImplementationType == typeof(UnitDrainParticipant));
        Assert.Equal(FenceStep.StopUnits, new UnitFenceParticipant(runtime).Step);
        Assert.Equal(DrainStep.ReleaseUnits, new UnitDrainParticipant(runtime).Step);
    }
}
