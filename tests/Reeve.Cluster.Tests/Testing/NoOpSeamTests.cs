using Reeve.Cluster;

namespace Unit.Testing;

public sealed class NoOpSeamTests
{
    [Fact]
    public void A_fault_point_never_holds_in_production()
    {
        var reached = NoFaultPoints.Instance.ReachAsync("any");

        Assert.True(reached.IsCompletedSuccessfully);
    }

    [Fact]
    public void The_model_event_sink_drops_events_in_production()
    {
        NoModelEvents.Instance.Emit(new ModelEvent("any", new Dictionary<string, object?> { ["k"] = 1 }));
    }

    [Fact]
    public void The_meter_carries_the_library_name()
    {
        Assert.Equal("Reeve.Cluster", ClusterDiagnostics.Name);
        Assert.Equal(ClusterDiagnostics.Name, ClusterDiagnostics.Meter.Name);
    }
}
