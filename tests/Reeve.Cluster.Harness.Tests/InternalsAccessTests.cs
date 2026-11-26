using Reeve.Cluster;

namespace Harness.Smoke;

public sealed class InternalsAccessTests
{
    [Fact]
    public async Task The_harness_sees_the_fault_point_and_model_event_seams_through_InternalsVisibleTo()
    {
        IFaultPoints points = NoFaultPoints.Instance;
        IModelEventSink sink = NoModelEvents.Instance;

        await points.ReachAsync("any");
        sink.Emit(new ModelEvent("any"));
    }
}
