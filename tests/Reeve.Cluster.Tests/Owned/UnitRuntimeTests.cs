using System.Collections.Concurrent;
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
using Unit.Membership;
using Unit.Testing;

namespace Unit.Owned;

public sealed class UnitRuntimeTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly List<UnitRuntime> runtimes = [];

    private readonly Unit.Generations.HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);

    public UnitRuntimeTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        fenceTimer.Release();
        foreach (var runtime in runtimes)
        {
            runtime.Dispose();
        }

        gate.Dispose();
    }

    private UnitRuntime Runtime(params string[] kinds) => Runtime(new UnitKindOptions(), kinds);

    private UnitRuntime Runtime(UnitKindOptions declaration, params string[] kinds)
    {
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var registered = (kinds.Length == 0 ? ["widget"] : kinds).Select(k => new UnitKind(k, typeof(RecordingUnit), declaration));
        var runtime = new UnitRuntime(
            registered, services, rows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, wakeups, new NoTenureFence(), Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        runtimes.Add(runtime);
        return runtime;
    }

    private UnitRuntime FencedRuntime(ITenureFence fence)
    {
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        UnitKind[] registered =
        [
            new("widget", typeof(RecordingUnit), new UnitKindOptions { RedisFence = true }),
            new("plain", typeof(RecordingUnit), new UnitKindOptions()),
        ];
        var runtime = new UnitRuntime(
            registered, services, rows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, wakeups, fence, Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        runtimes.Add(runtime);
        return runtime;
    }

    private void ToActivated()
    {
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    private void ToServing()
    {
        ToActivated();
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private static HeldRow Row(string key, long token, bool requested = false) => new("widget", key, token, requested);

    [Fact]
    public void Registering_a_kind_advertises_its_capability()
    {
        Runtime("widget", "scheduler");

        Assert.Equal(["unit:scheduler", "unit:widget"], capabilities.Current);
    }

    [Fact]
    public async Task A_RedisFence_kind_starts_only_after_its_install_raise_returned_and_another_kind_is_not_raised()
    {
        var fence = new HeldTenureFence();
        var runtime = FencedRuntime(fence);
        ToServing();
        rows.Held = [Row("a", 10), new HeldRow("plain", "b", 11, false)];

        await runtime.PassAsync(CancellationToken.None);

        var raised = await fence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("widget", "a", 10L), (raised.Kind, raised.Key, raised.Token));
        var first = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal("plain", first.Kind);
        Assert.False(unit.Acquired.Reader.TryRead(out _));

        fence.Release();
        var second = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Same(raised, second.Context);
        Assert.False(fence.Acquiring.Reader.TryRead(out _));
    }

    [Fact]
    public async Task S1_a_tenure_is_not_running_for_Ask_or_commands_and_is_not_announced_until_its_install_raise_returned()
    {
        var fence = new HeldTenureFence();
        var runtime = FencedRuntime(fence);
        var announced = 0;
        runtime.TenureStarted += () => Interlocked.Increment(ref announced);
        ToServing();
        rows.Held = [Row("a", 10)];

        await runtime.PassAsync(CancellationToken.None);
        var raised = await fence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        Assert.True(runtime.Holds("widget", "a"));
        Assert.False(runtime.TryGetRunning("widget", "a", 10, out _, out _));
        Assert.False(runtime.IsStopping("widget", "a", 10));
        Assert.Empty(runtime.RunningOf(["widget"]));
        Assert.Equal(0, Volatile.Read(ref announced));

        fence.Release();
        var acquired = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        Assert.Same(raised, acquired.Context);
        Assert.True(runtime.TryGetRunning("widget", "a", 10, out var stopped, out var owner));
        Assert.False(stopped.IsCancellationRequested);
        Assert.Same(acquired.Context, owner);
        var listed = Assert.Single(runtime.RunningOf(["widget"]));
        Assert.Same(acquired.Context, listed.Context);
        Assert.Equal(1, Volatile.Read(ref announced));
    }

    [Fact]
    public async Task S1_a_tenure_whose_stop_began_before_its_install_raise_returned_is_never_running_and_is_not_announced()
    {
        var fence = new HeldTenureFence();
        var runtime = FencedRuntime(fence);
        var announced = 0;
        runtime.TenureStarted += () => Interlocked.Increment(ref announced);
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        await fence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        unit.HangLost("a");
        rows.Held = [Row("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.LostStarted("a").WaitAsync(Wait);
        Assert.True(runtime.IsStopping("widget", "a", 10));

        fence.Release();
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        Assert.True(runtime.IsStopping("widget", "a", 10));
        Assert.False(runtime.TryGetRunning("widget", "a", 10, out _, out _));
        Assert.Empty(runtime.RunningOf(["widget"]));
        Assert.Equal(0, Volatile.Read(ref announced));
    }

    [Fact]
    public async Task S1_a_kind_without_RedisFence_is_running_as_soon_as_its_unit_is_called()
    {
        var runtime = FencedRuntime(new HeldTenureFence());
        ToServing();
        rows.Held = [new HeldRow("plain", "b", 11, false)];

        await runtime.PassAsync(CancellationToken.None);
        var acquired = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        Assert.True(runtime.TryGetRunning("plain", "b", 11, out _, out var owner));
        Assert.Same(acquired.Context, owner);
        Assert.Single(runtime.RunningOf(["plain"]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Fence_step_5_hands_the_RedisFence_tenures_to_the_fence_and_a_drain_does_not(bool drain)
    {
        var fence = new HeldTenureFence();
        fence.Release();
        var runtime = FencedRuntime(fence);
        ToServing();
        rows.Held = [Row("a", 10), new HeldRow("plain", "b", 11, false)];
        await runtime.PassAsync(CancellationToken.None);
        var raised = await fence.Acquiring.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await runtime.StopAllAsync(drain, CancellationToken.None).WaitAsync(Wait);

        if (drain)
        {
            Assert.False(fence.Fenced.Reader.TryRead(out _));
        }
        else
        {
            Assert.Same(raised, await fence.Fenced.Reader.ReadAsync().AsTask().WaitAsync(Wait));
            Assert.False(fence.Fenced.Reader.TryRead(out _));
        }
    }

    [Fact]
    public void A_convergent_kind_with_RedisFence_is_refused()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddClusterUnit<RecordingUnit>("sweep", k => { k.Convergent = true; k.RedisFence = true; }));
        Assert.DoesNotContain(services, d => d.ImplementationInstance is UnitKind);
    }

    [Fact]
    public async Task A_held_row_starts_its_unit_once_with_its_tenure()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];

        await runtime.PassAsync(CancellationToken.None);
        await runtime.PassAsync(CancellationToken.None);

        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("widget", "a", 10L), (started.Kind, started.Key, started.Token));
        Assert.False(unit.Acquired.Reader.TryRead(out _));
        Assert.Equal([("r1", Epoch)], rows.Reads.Distinct());
    }

    [Fact]
    public async Task Nothing_is_read_or_started_before_Serving_and_entering_Serving_polls()
    {
        var runtime = Runtime();
        ToActivated();
        rows.Held = [Row("a", 10)];
        await runtime.StartAsync(CancellationToken.None);

        await PollsAsync(runtime, 1);
        time.Advance(options.PlacementInterval);
        await PollsAsync(runtime, 2);
        Assert.Empty(rows.Reads);

        gate.Apply(MemberTrigger.AppliedToActivationHead);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal("a", started.Key);
        await runtime.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task The_owned_section_lists_the_held_tenures_and_the_release_grace()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10), Row("b", 11)];
        await runtime.StartAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        var section = await new OwnedStatusSection(runtime, Options.Create(options)).CollectAsync(CancellationToken.None);

        var units = Assert.IsType<List<Dictionary<string, object?>>>(section.Values["units"]);
        Assert.Equal([("widget", "a", 10L, true), ("widget", "b", 11L, true)], units.Select(u => ((string)u["kind"]!, (string)u["key"]!, (long)u["token"]!, (bool)u["running"]!)));
        Assert.Equal(options.ReleaseGrace.TotalSeconds, section.Values["releaseGraceSeconds"]);
        await runtime.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task An_owner_nudge_wakes_the_poll_only_when_it_is_for_this_replica()
    {
        var runtime = Runtime();

        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.OwnerNudge, "r2"));
        Assert.False(runtime.NudgePending);

        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.OwnerNudge, "r1"));
        Assert.True(runtime.NudgePending);
    }

    [Fact]
    public async Task An_owner_nudge_polls_at_once()
    {
        var runtime = Runtime();
        ToServing();
        await runtime.StartAsync(CancellationToken.None);
        await rows.WaitForReadsAsync(1).WaitAsync(Wait);
        rows.Held = [Row("a", 10)];

        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.OwnerNudge, "r1"));

        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal("a", started.Key);
        await runtime.StopAsync(CancellationToken.None);
    }

    private static async Task PollsAsync(UnitRuntime runtime, int count)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (runtime.Polls < count)
        {
            await Task.Delay(10, bound.Token);
        }
    }

    [Fact]
    public async Task A_release_request_stops_the_unit_and_releases_after_OnLostAsync_returned()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        rows.Held = [Row("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.True(started.Stop.IsCancellationRequested);
        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("widget", "a", "r1", Epoch, 10L), release);
        Assert.Equal(["a"], unit.LostKeys);
    }

    [Fact]
    public async Task A_release_waits_for_OnLostAsync_until_StopBudget()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        unit.HangLost("a");

        rows.Held = [Row("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);

        time.Advance(options.StopBudget - TimeSpan.FromMilliseconds(1));
        Assert.False(rows.Releases.Reader.TryRead(out _));

        time.Advance(TimeSpan.FromMilliseconds(1));
        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(10L, release.Token);
    }

    [Fact]
    public async Task I26_a_new_assignment_while_OnLostAsync_has_not_returned_is_released_at_once()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");
        rows.Held = [Row("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);
        time.Advance(options.StopBudget);
        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);

        rows.Held = [Row("a", 11)];
        await runtime.PassAsync(CancellationToken.None);
        Assert.Equal(11L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.False(unit.Acquired.Reader.TryRead(out _));

        unit.ReleaseLost("a");
        await unit.LostReturned("a").WaitAsync(Wait);
        rows.Held = [Row("a", 12)];
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return unit.Acquired.Reader.TryRead(out var next) && next.Token == 12;
        }).WaitAsync(Wait);
    }

    [Fact]
    public async Task A_row_no_longer_this_tenures_stops_the_unit_as_lost_and_releases_nothing()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10), Row("b", 20)];
        await runtime.PassAsync(CancellationToken.None);
        var first = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var second = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var a = first.Key == "a" ? first : second;
        var b = first.Key == "b" ? first : second;

        rows.Held = [Row("b", 21)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.True(a.Stop.IsCancellationRequested);
        Assert.True(b.Stop.IsCancellationRequested);
        await unit.LostReturned("a").WaitAsync(Wait);
        await unit.LostReturned("b").WaitAsync(Wait);

        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("b", 21L), (release.Key, release.Token));
        Assert.False(rows.Releases.Reader.TryRead(out _));
    }

    [Fact]
    public async Task A_row_with_its_request_before_it_started_is_released_and_not_started()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10, requested: true)];

        await runtime.PassAsync(CancellationToken.None);

        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task An_owner_that_stops_passing_its_kinds_filter_stops_the_unit_and_releases_its_row()
    {
        var runtime = Runtime(new UnitKindOptions { Eligibility = EligibilityFilter.Any.Forbid("degraded-mode") });
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await capabilities.SetAsync(["degraded-mode"]);
        await runtime.PassAsync(CancellationToken.None);

        Assert.True(started.Stop.IsCancellationRequested);
        Assert.Throws<OwnershipLostException>(started.Context.EnsureOwner);
        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("widget", "a", "r1", Epoch, 10L), release);
        Assert.Equal(["a"], unit.LostKeys);
    }

    [Fact]
    public async Task A_row_of_a_new_tenure_read_over_a_given_up_guard_is_released_at_once_and_never_started()
    {
        var runtime = Runtime(new UnitKindOptions { Eligibility = EligibilityFilter.Any.Forbid("degraded-mode") });
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        rows.FailReleases = true;
        await capabilities.SetAsync(["degraded-mode"]);
        await runtime.PassAsync(CancellationToken.None);
        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        await unit.LostReturned("a").WaitAsync(Wait);

        rows.Held = [Row("a", 11)];
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return rows.Releases.Reader.TryRead(out var next) && next.Token == 11;
        }).WaitAsync(Wait);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task A_row_assigned_to_an_owner_that_fails_the_filter_is_released_at_once_and_never_started()
    {
        var runtime = Runtime(new UnitKindOptions { Eligibility = EligibilityFilter.Any.Require("scheduler-host") });
        ToServing();
        rows.Held = [Row("a", 10)];

        await runtime.PassAsync(CancellationToken.None);

        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
        Assert.Empty(unit.LostKeys);
    }

    [Fact]
    public async Task A_row_of_a_kind_not_registered_here_is_released()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [new HeldRow("gizmo", "a", 10, false)];

        await runtime.PassAsync(CancellationToken.None);

        Assert.Equal(("gizmo", "a"), await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait) is var r ? (r.Kind, r.Key) : default);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task A_release_that_got_no_answer_is_sent_again_by_the_next_poll()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");
        rows.FailReleases = true;
        rows.Held = [Row("a", 10, requested: true)];
        await runtime.PassAsync(CancellationToken.None);
        time.Advance(options.StopBudget);
        await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        rows.FailReleases = false;
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return rows.Releases.Reader.TryRead(out var again) && again.Token == 10;
        }).WaitAsync(Wait);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task I1_a_unit_does_not_start_once_fence_start_passed_and_its_serving_check_fences()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];

        fenceTimer.Arm();
        time.Advance(options.LeaseTtl);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(unit.Acquired.Reader.TryRead(out _));
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task A_failed_read_changes_nothing()
    {
        var runtime = Runtime();
        ToServing();
        rows.Held = [Row("a", 10)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        rows.FailReads = true;
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(started.Stop.IsCancellationRequested);
        Assert.Empty(unit.LostKeys);
    }
}

internal sealed class ScriptedUnitRows : IUnitRows
{
    private readonly ConcurrentQueue<(string, long)> reads = new();
    private readonly SemaphoreSlim read = new(0);

    public IReadOnlyList<HeldRow> Held { get; set; } = [];

    public bool FailReads { get; set; }

    public bool FailReleases { get; set; }

    public bool ReleaseMatches { get; set; } = true;

    public IReadOnlyList<OrphanedRow> Orphaned { get; set; } = [];

    public ConcurrentQueue<IReadOnlyList<string>> OrphanScans { get; } = new();

    public System.Threading.Channels.Channel<(string Kind, string Key, OwnedRow Expected, string Owner, long Epoch)> Takes { get; } =
        System.Threading.Channels.Channel.CreateUnbounded<(string, string, OwnedRow, string, long)>();

    public IReadOnlyList<(string Owner, long Epoch)> Reads => [.. reads];

    public System.Threading.Channels.Channel<(string Kind, string Key, string Owner, long Epoch, long Token)> Releases { get; } =
        System.Threading.Channels.Channel.CreateUnbounded<(string, string, string, long, long)>();

    public async Task WaitForReadsAsync(int count)
    {
        for (var i = 0; i < count; i++)
        {
            await read.WaitAsync();
        }
    }

    public async Task UntilAsync(Func<Task<bool>> step)
    {
        while (!await step())
        {
            await Task.Delay(10);
        }
    }

    public Task<IReadOnlyList<HeldRow>> ReadHeldAsync(string owner, long epoch, CancellationToken cancellationToken)
    {
        reads.Enqueue((owner, epoch));
        read.Release();
        return FailReads ? Task.FromException<IReadOnlyList<HeldRow>>(new TimeoutException("no answer")) : Task.FromResult(Held);
    }

    public Task<bool> ReleaseAsync(string kind, string key, string owner, long epoch, long token, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Releases.Writer.TryWrite((kind, key, owner, epoch, token));
        return FailReleases ? Task.FromException<bool>(new TimeoutException("no answer")) : Task.FromResult(ReleaseMatches);
    }

    public Task<IReadOnlyList<OrphanedRow>> ReadOrphanedAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken)
    {
        OrphanScans.Enqueue(kinds);
        return Task.FromResult<IReadOnlyList<OrphanedRow>>([.. Orphaned.Where(r => kinds.Contains(r.Kind))]);
    }

    public Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken)
    {
        Takes.Writer.TryWrite((kind, key, expected, owner, epoch));
        Held = [.. Held, new HeldRow(kind, key, expected.Token + 1, false)];
        return Task.FromResult<long?>(expected.Token + 1);
    }
}

internal sealed class HeldTenureFence : ITenureFence
{
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public System.Threading.Channels.Channel<OwnedContext> Acquiring { get; } = System.Threading.Channels.Channel.CreateUnbounded<OwnedContext>();

    public System.Threading.Channels.Channel<OwnedContext> Fenced { get; } = System.Threading.Channels.Channel.CreateUnbounded<OwnedContext>();

    public void Release() => released.TrySetResult();

    public Task OnFencedAsync(OwnedContext tenure, CancellationToken cancellationToken)
    {
        Fenced.Writer.TryWrite(tenure);
        return Task.CompletedTask;
    }

    public async Task OnAcquiringAsync(OwnedContext tenure, CancellationToken stop)
    {
        Acquiring.Writer.TryWrite(tenure);
        await released.Task;
    }
}

internal sealed class RecordingUnit : IOwnedUnit
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> hung = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> entered = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> returned = new();
    private readonly ConcurrentQueue<string> lost = new();

    public System.Threading.Channels.Channel<(string Kind, string Key, long Token, CancellationToken Stop, OwnedContext Context)> Acquired { get; } =
        System.Threading.Channels.Channel.CreateUnbounded<(string, string, long, CancellationToken, OwnedContext)>();

    public IReadOnlyList<string> LostKeys => [.. lost];

    public Action? OnAcquiring { get; set; }

    public void HangLost(string key) => hung[key] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public void ReleaseLost(string key)
    {
        if (hung.TryRemove(key, out var gate))
        {
            gate.TrySetResult();
        }
    }

    public void ReleaseAll()
    {
        foreach (var key in hung.Keys)
        {
            ReleaseLost(key);
        }
    }

    public Task LostStarted(string key) => Entered(key).Task;

    public Task LostReturned(string key) => Returned(key).Task;

    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        OnAcquiring?.Invoke();
        Acquired.Writer.TryWrite((context.Kind, context.Key, TokenOf(context), stop, context));
        return Task.Delay(Timeout.Infinite, stop);
    }

    public async Task OnLostAsync(OwnedContext context)
    {
        lost.Enqueue(context.Key);
        Entered(context.Key).TrySetResult();
        if (hung.TryGetValue(context.Key, out var gate))
        {
            await gate.Task;
        }

        Returned(context.Key).TrySetResult();
    }

    private static long TokenOf(OwnedContext context)
    {
        try
        {
            return context.Token;
        }
        catch (InvalidOperationException e) when (e.Message.Contains("convergent", StringComparison.Ordinal))
        {
            return 0;
        }
    }

    private TaskCompletionSource Entered(string key) =>
        entered.GetOrAdd(key, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    private TaskCompletionSource Returned(string key) =>
        returned.GetOrAdd(key, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}
