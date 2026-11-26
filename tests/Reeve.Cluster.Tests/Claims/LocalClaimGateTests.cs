using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Unit.Claims;

public sealed class LocalClaimGateTests
{
    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

    [Fact]
    public void Serving_claim_gate_returns_the_current_replica_epoch_and_timeline()
    {
        var time = new FakeTimeProvider();
        using var phase = NewServingGate(time, 7, 3);
        var gate = new LocalClaimGate(phase, Options.Create(new ClusterOptions { ReplicaId = "r1" }));

        Assert.Equal(new ClaimSeat("r1", 7, 3), gate.CheckServing());
    }

    [Fact]
    public void Gate_refuses_before_serving_and_when_the_join_epoch_is_missing()
    {
        var time = new FakeTimeProvider();
        using var phase = NewGate(time);
        var gate = new LocalClaimGate(phase, Options.Create(new ClusterOptions { ReplicaId = "r1" }));

        Assert.Null(gate.CheckServing());
        Assert.True(phase.Joined(7, 3, phase.Now));
        phase.Apply(MemberTrigger.StateLoaded);
        Assert.Null(gate.CheckServing());
    }

    [Fact]
    public void Fence_start_passed_is_not_serving_for_claims()
    {
        var time = new FakeTimeProvider();
        using var phase = NewServingGate(time, 7, 3);
        var gate = new LocalClaimGate(phase, Options.Create(new ClusterOptions { ReplicaId = "r1" }));
        time.Advance(TimeSpan.FromSeconds(22));

        Assert.Null(gate.CheckServing());
        Assert.Equal(ReplicaPhase.Fencing, phase.Phase);
    }

    [Fact]
    public void Claims_registration_resolves_the_public_service_and_production_gate()
    {
        var time = new FakeTimeProvider();
        using var phase = NewGate(time);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(time);
        services.AddSingleton<LocalPhaseGate>(phase);
        services.AddSingleton<IClaimRows>(new EmptyRows());
        services.AddSingleton<IOptions<ClusterOptions>>(Options.Create(new ClusterOptions { ReplicaId = "r1" }));
        services.AddSingleton<IFaultPoints>(NoFaultPoints.Instance);
        services.AddClusterClaims();
        using var provider = services.BuildServiceProvider();

        Assert.IsType<LocalClaimGate>(provider.GetRequiredService<IClaimGate>());
        Assert.Same(provider.GetRequiredService<ClusterClaims>(), provider.GetRequiredService<IClusterClaims>());
    }

    private static LocalPhaseGate NewGate(FakeTimeProvider time) =>
        new(Timing, time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);

    private static LocalPhaseGate NewServingGate(FakeTimeProvider time, long epoch, long timeline)
    {
        var gate = NewGate(time);
        Assert.True(gate.Joined(epoch, timeline, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        return gate;
    }

    private sealed class EmptyRows : IClaimRows
    {
        public Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken) => Task.FromResult<Guid?>(null);
        public Task<ClaimRead?> ReadAsync(string key, CancellationToken cancellationToken) => Task.FromResult<ClaimRead?>(null);
        public Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken) => Task.FromResult<Guid?>(null);
        public Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken) => Task.FromResult(false);
    }
}
