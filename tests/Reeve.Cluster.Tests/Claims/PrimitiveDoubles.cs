using System.Data.Common;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

internal sealed class ScriptedGate : IClaimGate
{
    public bool Serving { get; set; } = true;

    public int Checks { get; private set; }

    public List<long> OtherTimelines { get; } = [];

    public ClaimSeat? CheckServing()
    {
        Checks++;
        return Serving ? new ClaimSeat("r1", 4, 100) : null;
    }

    public void OnOtherTimeline(long epoch) => OtherTimelines.Add(epoch);
}

internal sealed class HookedPoints : IFaultPoints
{
    private readonly Dictionary<string, Action> hooks = [];

    public void On(string point, Action action) => hooks[point] = action;

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        if (hooks.TryGetValue(point, out var action))
        {
            action();
        }

        return ValueTask.CompletedTask;
    }
}

internal sealed class DbFailure(string message) : DbException(message)
{
}
