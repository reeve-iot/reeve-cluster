using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class CapabilitiesTests : IDisposable
{
    private readonly FakeTimeProvider time = new();
    private readonly RecordingCapabilityStore store = new();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;

    public CapabilitiesTests()
    {
        var options = Options.Create(MembershipTestOptions.Default());
        gate = new LocalPhaseGate(options, time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(store, gate, options, time);
    }

    public void Dispose() => gate.Dispose();

    [Fact]
    public void Any_admits_every_member()
    {
        Assert.True(EligibilityFilter.Any.IsEligible([]));
        Assert.True(EligibilityFilter.Any.IsEligible(["degraded-mode"]));
    }

    [Fact]
    public void A_filter_needs_every_required_capability_and_none_of_the_forbidden_ones()
    {
        var filter = EligibilityFilter.Any.Require("unit:scheduler", "gpu").Forbid("degraded-mode");

        Assert.True(filter.IsEligible(["gpu", "unit:scheduler", "other"]));
        Assert.False(filter.IsEligible(["unit:scheduler"]));
        Assert.False(filter.IsEligible(["gpu", "unit:scheduler", "degraded-mode"]));
    }

    [Fact]
    public void A_filter_is_immutable_and_refuses_an_empty_capability()
    {
        var narrowed = EligibilityFilter.Any.Require("gpu");

        Assert.Empty(EligibilityFilter.Any.Required);
        Assert.Equal(["gpu"], narrowed.Require("gpu").Required);
        Assert.Throws<ArgumentException>(() => narrowed.Forbid(" "));
    }

    [Fact]
    public async Task Current_is_the_applications_set_and_the_unit_kinds_sorted()
    {
        capabilities.AddUnitKind("widget");
        await capabilities.SetAsync(["gpu", "degraded-mode", "gpu"]);

        Assert.Equal(["degraded-mode", "gpu", "unit:widget"], capabilities.Current);
    }

    [Fact]
    public async Task A_set_before_the_join_writes_no_row()
    {
        await capabilities.SetAsync(["gpu"]);

        Assert.Empty(store.Writes);
    }

    [Fact]
    public async Task A_set_on_a_joined_replica_writes_the_row_with_the_whole_set()
    {
        gate.Joined(7, 1, TimeSpan.Zero);
        capabilities.AddUnitKind("widget");

        await capabilities.SetAsync(["gpu"]);

        var write = Assert.Single(store.Writes);
        Assert.Equal(("r1", 7L), (write.ReplicaId, write.Epoch));
        Assert.Equal(["gpu", "unit:widget"], write.Capabilities);
        Assert.Equal(["gpu", "unit:widget"], capabilities.Current);
    }

    [Fact]
    public async Task A_write_that_fails_changes_nothing()
    {
        gate.Joined(7, 1, TimeSpan.Zero);
        await capabilities.SetAsync(["gpu"]);
        store.Failure = new InvalidOperationException("down");

        await Assert.ThrowsAsync<InvalidOperationException>(() => capabilities.SetAsync(["gpu", "degraded-mode"]));

        Assert.Equal(["gpu"], capabilities.Current);
    }

    [Fact]
    public async Task The_application_cannot_set_a_unit_capability()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => capabilities.SetAsync([ClusterCapabilities.ForUnit("widget")]));
    }

    [Fact]
    public async Task A_set_waits_for_a_join_in_progress_and_then_finds_its_epoch()
    {
        using var joining = await capabilities.EnterJoinAsync(CancellationToken.None);

        var set = capabilities.SetAsync(["gpu"]);
        Assert.False(set.IsCompleted);

        gate.Joined(7, 1, TimeSpan.Zero);
        joining.Dispose();
        await set;

        Assert.Single(store.Writes);
    }
}

internal sealed class RecordingCapabilityStore : ICapabilityStore
{
    public List<(string ReplicaId, long Epoch, string[] Capabilities)> Writes { get; } = [];

    public Exception? Failure { get; set; }

    public Task<bool> WriteAsync(string replicaId, long epoch, IReadOnlyList<string> capabilities, CancellationToken cancellationToken)
    {
        if (Failure is { } failure)
        {
            throw failure;
        }

        Writes.Add((replicaId, epoch, [.. capabilities]));
        return Task.FromResult(true);
    }
}
