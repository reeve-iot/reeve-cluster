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

public sealed class UnitContextScopeTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly FenceSteps steps = new();
    private readonly ClusterScopeSource scopes;
    private readonly CommitRefusal refusal;
    private readonly GenerationStore store = Stores.Create("config");
    private readonly RendezvousGenerations rendezvous;
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly ScopeProbeUnit probe;
    private readonly List<IDisposable> built = [];

    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);

    public UnitContextScopeTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new Unit.Membership.RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
        scopes = new ClusterScopeSource(steps);
        refusal = new CommitRefusal(gate, steps);
        rendezvous = new RendezvousGenerations(store);
        probe = new ScopeProbeUnit(scopes, refusal);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        fenceTimer.Release();
        for (var i = built.Count - 1; i >= 0; i--)
        {
            built[i].Dispose();
        }

        refusal.Dispose();
        gate.Dispose();
    }

    private UnitRuntime ServingRuntime(IGenerationSource generations, Type unitType)
    {
        var services = new ServiceCollection().AddSingleton(unit).AddSingleton(probe).BuildServiceProvider();
        built.Add(services);
        UnitKind[] registered = [new("widget", unitType, new UnitKindOptions())];
        var runtime = new UnitRuntime(
            registered, services, rows, gate, scopes, generations, NoFaultPoints.Instance, capabilities, wakeups, new NoTenureFence(), Options.Create(options), time,
            NullLogger<UnitRuntime>.Instance);
        built.Add(runtime);
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        return runtime;
    }

    private async Task<OwnedContext> StartRecordingAsync(UnitRuntime runtime)
    {
        rows.Held = [new HeldRow("widget", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        return (await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Context;
    }

    private async Task StartProbeAsync(UnitRuntime runtime)
    {
        rows.Held = [new HeldRow("widget", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        await probe.Acquired.WaitAsync(Wait);
    }

    [Fact]
    public async Task Two_first_reads_of_Configuration_in_one_unit_of_work_answer_the_same_generation()
    {
        var runtime = ServingRuntime(rendezvous, typeof(RecordingUnit));
        var context = await StartRecordingAsync(runtime);
        rendezvous.Answers = [await store.PublishAsync(1, null, ("config", "one")), await store.PublishAsync(2, null, ("config", "two"))];

        IGeneration first;
        IGeneration second;
        IGeneration? pinned;
        using (scopes.BeginUnitOfWork())
        {
            var a = Task.Factory.StartNew(() => context.Configuration, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var b = Task.Factory.StartNew(() => context.Configuration, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            first = await a.WaitAsync(Wait);
            second = await b.WaitAsync(Wait);
            pinned = scopes.CurrentWriter!.PinnedGeneration;
        }

        Assert.True(rendezvous.Met, "the two reads did not meet at Current: the test proves nothing");
        Assert.Same(first, second);
        Assert.Same(pinned, first);
        Assert.Contains(first, rendezvous.Answers);
    }

    [Fact]
    public async Task Configuration_is_not_pinned_once_a_release_request_stopped_the_unit_while_the_replica_serves()
    {
        var runtime = ServingRuntime(store, typeof(RecordingUnit));
        var context = await StartRecordingAsync(runtime);
        await store.PublishAsync(1, null, ("config", "one"));

        rows.Held = [new HeldRow("widget", "a", 10, true)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        using var scope = scopes.BeginUnitOfWork();
        Assert.Throws<OwnershipLostException>(() => context.Configuration);
        Assert.Null(scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task In_a_units_scope_TryPin_and_then_Configuration_answer_the_same_generation()
    {
        var runtime = ServingRuntime(store, typeof(RecordingUnit));
        var context = await StartRecordingAsync(runtime);
        var generations = new ClusterGenerations(store, gate, scopes);
        await store.PublishAsync(1, null, ("config", "one"));

        using var scope = scopes.BeginUnitOfWork();
        var pinned = generations.TryPin();
        await store.PublishAsync(2, null, ("config", "two"));

        Assert.NotNull(pinned);
        Assert.Same(pinned, context.Configuration);
        Assert.Same(pinned, scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task Configuration_inside_TryActivate_is_refused_before_the_owner_check()
    {
        var runtime = ServingRuntime(store, typeof(RecordingUnit));
        var context = await StartRecordingAsync(runtime);
        var generation = await store.PublishAsync(1, null, ("config", "one"));
        fenceTimer.Arm();
        time.Advance(options.LeaseTtl);

        using var scope = scopes.BeginUnitOfWork();
        Exception? thrown = null;
        Assert.True(generation.TryActivate("config", () => thrown = Record.Exception(() => context.Configuration)));

        Assert.IsType<InvalidOperationException>(thrown);
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Fact]
    public async Task Configuration_reads_the_generation_before_the_owner_check()
    {
        fenceTimer.Arm();
        var advance = false;
        var reads = new AtRead(store, () =>
        {
            if (advance)
            {
                time.Advance(options.LeaseTtl);
            }
        });
        var runtime = ServingRuntime(reads, typeof(RecordingUnit));
        var context = await StartRecordingAsync(runtime);
        await store.PublishAsync(1, null, ("config", "one"));

        using var scope = scopes.BeginUnitOfWork();
        advance = true;
        Assert.Throws<OwnershipLostException>(() => context.Configuration);
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task OnLostAsync_runs_outside_the_scope_of_the_flow_that_finishes_the_stop()
    {
        var runtime = ServingRuntime(store, typeof(ScopeProbeUnit));
        await StartProbeAsync(runtime);

        (ClusterScopeKind? Kind, bool Refused) seen;
        using (scopes.Begin(ClusterScopeKind.ClientRequest))
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "test"));
            refusal.Start();
            var step = runtime.StopAllAsync(drain: false, CancellationToken.None);
            seen = await probe.Lost.WaitAsync(Wait);
            await step.WaitAsync(Wait);
        }

        Assert.Equal(((ClusterScopeKind?)null, false), seen);
    }

    [Fact]
    public async Task A_fence_begun_by_a_client_requests_serving_check_runs_OnLostAsync_outside_the_requests_scope()
    {
        var runtime = ServingRuntime(store, typeof(ScopeProbeUnit));
        await StartProbeAsync(runtime);
        using var coordinator = new FenceCoordinator(
            gate,
            [new UnitFenceParticipant(runtime), new AppendRefusalParticipant(refusal)],
            new ScriptedFenceStore(),
            new FencePassConnection(),
            Options.Create(options),
            time,
            NoFaultPoints.Instance,
            NoModelEvents.Instance,
            NullLogger<FenceCoordinator>.Instance);

        fenceTimer.Arm();
        time.Advance(options.LeaseTtl);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        (ClusterScopeKind? Kind, bool Refused) seen;
        using (scopes.Begin(ClusterScopeKind.ClientRequest))
        {
            Assert.False(gate.CheckServing().IsServing);
            seen = await probe.Lost.WaitAsync(Wait);
        }

        Assert.Equal(((ClusterScopeKind?)null, false), seen);
    }

    private sealed class AtRead(IGenerationSource inner, Action atRead) : IGenerationSource
    {
        public IGeneration? Current
        {
            get
            {
                atRead();
                return inner.Current;
            }
        }

        public void EnsureNotInsideTheSwap() => inner.EnsureNotInsideTheSwap();
    }

    private sealed class RendezvousGenerations(IGenerationSource inner) : IGenerationSource
    {
        private readonly ManualResetEventSlim second = new();
        private int reads;

        public bool Met => second.IsSet;

        public IGeneration[]? Answers { get; set; }

        public IGeneration? Current
        {
            get
            {
                var read = Interlocked.Increment(ref reads);
                if (read == 1)
                {
                    second.Wait(Bounds.Safety);
                }
                else
                {
                    second.Set();
                }

                return Answers is { } answers ? answers[Math.Min(read, 2) - 1] : inner.Current;
            }
        }

        public void EnsureNotInsideTheSwap() => inner.EnsureNotInsideTheSwap();
    }

    private sealed class ScopeProbeUnit(ClusterScopeSource source, CommitRefusal commits) : IOwnedUnit
    {
        private readonly TaskCompletionSource acquired = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<(ClusterScopeKind? Kind, bool Refused)> lost = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Acquired => acquired.Task;

        public Task<(ClusterScopeKind? Kind, bool Refused)> Lost => lost.Task;

        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            acquired.TrySetResult();
            return Task.Delay(Timeout.Infinite, stop);
        }

        public Task OnLostAsync(OwnedContext context)
        {
            var scope = source.CurrentWriter;
            lost.TrySetResult((scope?.Kind, commits.Refuses(scope)));
            return Task.CompletedTask;
        }
    }
}
