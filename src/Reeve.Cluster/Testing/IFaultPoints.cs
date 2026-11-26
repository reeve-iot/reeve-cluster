namespace Reeve.Cluster;

internal interface IFaultPoints
{
    ValueTask ReachAsync(string point, CancellationToken cancellationToken = default);
}

internal sealed class NoFaultPoints : IFaultPoints
{
    public static NoFaultPoints Instance { get; } = new();

    private NoFaultPoints()
    {
    }

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
}
