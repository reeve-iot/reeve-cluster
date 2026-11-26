using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Generations;
using Unit.Testing;

namespace Unit.Log;

public sealed class ClusterGenerationsTests : IDisposable
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private static readonly TimeSpan PastFenceStart = TimeSpan.FromSeconds(23);

    private readonly CountingTimeProvider time = new();
    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);
    private readonly LocalPhaseGate gate;
    private readonly GenerationStore store = Stores.Create("a");
    private readonly ClusterScopeSource scopes = new(new FenceSteps());
    private readonly ClusterGenerations generations;

    public ClusterGenerationsTests()
    {
        fenceTimer.Arm();
        gate = new LocalPhaseGate(Timing, time, NoModelEvents.Instance, fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        generations = new ClusterGenerations(store, gate, scopes);
    }

    public void Dispose()
    {
        fenceTimer.Release();
        gate.Dispose();
    }

    private void ToActivated(long epoch = 1)
    {
        Assert.True(gate.Joined(epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    private void ToServing(long epoch = 1)
    {
        ToActivated(epoch);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private Task<IGeneration> PublishAsync(long head) => store.PublishAsync(head, null, ("a", $"a{head}"));

    private IClusterScopeHandle Begin(ClusterScopeKind kind) => kind switch
    {
        ClusterScopeKind.UnitOfWork => scopes.BeginUnitOfWork(),
        ClusterScopeKind.Background => scopes.BeginBackground(),
        _ => scopes.Begin(kind),
    };

    [Theory]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    public async Task A_scope_that_has_pinned_answers_its_pin_without_a_check(ClusterScopeKind kind)
    {
        ToServing();
        var first = await PublishAsync(1);
        using var scope = Begin(kind);
        Assert.Same(first, generations.TryPin());

        await PublishAsync(2);
        time.Advance(PastFenceStart);

        Assert.Same(first, generations.TryPin());
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Theory]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    public async Task An_unpinned_scope_on_a_serving_replica_pins_the_current_generation(ClusterScopeKind kind)
    {
        ToServing();
        await PublishAsync(1);
        var current = await PublishAsync(2);
        using var scope = Begin(kind);

        Assert.Same(current, generations.TryPin());
        Assert.Same(current, scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task An_unpinned_scope_on_a_replica_that_does_not_serve_answers_null_without_a_pin()
    {
        await PublishAsync(1);
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);
        using var scope = scopes.BeginBackground();

        Assert.Null(generations.TryPin());
        Assert.Null(scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task A_check_that_finds_fence_start_passed_answers_null_and_starts_the_fence()
    {
        ToServing();
        await PublishAsync(1);
        time.Advance(PastFenceStart);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        using var scope = scopes.BeginUnitOfWork();

        Assert.Null(generations.TryPin());
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Theory]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.ClientRequest)]
    public async Task Fence_start_passed_at_the_read_refuses_at_the_check_after_it(ClusterScopeKind kind)
    {
        ToServing();
        await PublishAsync(1);
        var reads = new AtRead(store, () => time.Advance(PastFenceStart));
        var reading = new ClusterGenerations(reads, gate, scopes);
        using var scope = Begin(kind);

        Assert.Null(reading.TryPin());
        Assert.Equal(1, reads.Reads);
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Theory]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.ClientRequest)]
    public async Task Serving_entered_at_the_read_answers_null_because_the_check_before_the_read_refused(ClusterScopeKind kind)
    {
        ToActivated();
        await PublishAsync(1);
        var reads = new AtRead(store, () => gate.Apply(MemberTrigger.AppliedToActivationHead));
        var reading = new ClusterGenerations(reads, gate, scopes);
        using var scope = Begin(kind);

        Assert.Null(reading.TryPin());
        Assert.Equal(0, reads.Reads);
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Activated, gate.Phase);
    }

    [Fact]
    public async Task A_loop_in_Activated_gets_null_and_its_next_action_after_Serving_pins_the_activation_heads_generation()
    {
        ToActivated();
        await PublishAsync(1);
        using (scopes.BeginBackground())
        {
            Assert.Null(generations.TryPin());
        }

        var atHead = await PublishAsync(5);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        using var next = scopes.BeginBackground();
        Assert.Same(atHead, generations.TryPin());
    }

    [Theory]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.ClientRequest)]
    public async Task A_rejoin_between_the_two_checks_answers_null(ClusterScopeKind kind)
    {
        ToServing();
        await PublishAsync(1);
        var reads = new AtRead(store, () =>
        {
            gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
            Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
            gate.Apply(MemberTrigger.RejoinPermitted);
            ToServing(epoch: 2);
        });
        var reading = new ClusterGenerations(reads, gate, scopes);
        using var scope = Begin(kind);

        Assert.Null(reading.TryPin());
        Assert.Equal(1, reads.Reads);
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.True(gate.CheckServing().IsServing);
    }

    [Theory]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.ClientRequest)]
    public void Nothing_published_at_the_read_answers_null_while_the_checks_pass(ClusterScopeKind kind)
    {
        ToServing();
        using var scope = Begin(kind);

        Assert.Null(generations.TryPin());
        Assert.Null(scope.Scope.PinnedGeneration);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    [Theory]
    [InlineData(ClusterScopeKind.ClientRequest)]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    public async Task A_request_scope_that_has_pinned_answers_its_pin_without_a_check_also_through_a_joined_begin(ClusterScopeKind kind)
    {
        var pinned = await PublishAsync(1);
        await PublishAsync(2);
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);
        using var request = scopes.Begin(kind);
        scopes.CurrentWriter!.Pin(pinned);

        Assert.Same(pinned, generations.TryPin());
        using (var joined = scopes.BeginBackground())
        {
            Assert.Same(request.Scope, joined.Scope);
            Assert.Same(pinned, generations.TryPin());
        }
    }

    [Theory]
    [InlineData(ClusterScopeKind.ClientRequest)]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    public async Task An_unpinned_request_scope_gets_the_checked_current_generation_without_a_pin(ClusterScopeKind kind)
    {
        ToServing();
        var first = await PublishAsync(1);
        using var request = scopes.Begin(kind);

        Assert.Same(first, generations.TryPin());
        Assert.Null(request.Scope.PinnedGeneration);

        var second = await PublishAsync(2);
        Assert.Same(second, generations.TryPin());
        Assert.Null(request.Scope.PinnedGeneration);

        time.Advance(PastFenceStart);
        Assert.Null(generations.TryPin());
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task The_applier_scope_throws_also_through_a_joined_begin()
    {
        ToServing();
        await PublishAsync(1);
        using var applier = scopes.Begin(ClusterScopeKind.Applier);

        Assert.Throws<InvalidOperationException>(() => generations.TryPin());
        using (scopes.BeginBackground())
        {
            Assert.Throws<InvalidOperationException>(() => generations.TryPin());
        }

        Assert.Null(applier.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task No_live_scope_throws()
    {
        ToServing();
        await PublishAsync(1);
        Assert.Throws<InvalidOperationException>(() => generations.TryPin());

        var ended = scopes.BeginBackground();
        ended.Dispose();
        Assert.Throws<InvalidOperationException>(() => generations.TryPin());
    }

    [Fact]
    public async Task Inside_TryActivate_TryPin_throws_without_a_check_and_Current_answers_the_activated_generation()
    {
        ToServing();
        var generation = await PublishAsync(1);
        time.Advance(PastFenceStart);
        using var scope = scopes.BeginBackground();

        Exception? thrown = null;
        IGeneration? current = null;
        Assert.True(generation.TryActivate("a", () =>
        {
            thrown = Record.Exception(() => generations.TryPin());
            current = generations.Current;
        }));

        Assert.IsType<InvalidOperationException>(thrown);
        Assert.Same(generation, current);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Null(scope.Scope.PinnedGeneration);
    }

    [Fact]
    public async Task The_pin_ends_with_the_scope_and_the_next_scope_pins_the_newer_generation()
    {
        ToServing();
        var first = await PublishAsync(1);
        var action = scopes.BeginBackground();
        Assert.Same(first, generations.TryPin());
        action.Dispose();
        Assert.Null(action.Scope.PinnedGeneration);

        var second = await PublishAsync(2);
        using var next = scopes.BeginBackground();
        Assert.Same(second, generations.TryPin());
    }

    [Fact]
    public async Task Current_is_the_newest_generation_unchecked_and_unpinned()
    {
        Assert.Null(generations.Current);
        var generation = await PublishAsync(1);
        Assert.Equal(ReplicaPhase.Starting, gate.Phase);
        using var scope = scopes.BeginBackground();

        Assert.Same(generation, generations.Current);
        Assert.Null(scope.Scope.PinnedGeneration);
    }

    private sealed class AtRead(IGenerationSource inner, Action atRead) : IGenerationSource
    {
        public void EnsureNotInsideTheSwap() => inner.EnsureNotInsideTheSwap();

        private int reads;

        public int Reads => Volatile.Read(ref reads);

        public IGeneration? Current
        {
            get
            {
                Interlocked.Increment(ref reads);
                atRead();
                return inner.Current;
            }
        }
    }
}
