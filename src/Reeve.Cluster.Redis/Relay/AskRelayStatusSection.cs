using Reeve.Cluster.Status;

namespace Reeve.Cluster.Redis.Relay;

internal sealed class AskRelayStatusSection(AskRelay relay) : IStatusSectionSource
{
    public const string Name = "ask";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var state = relay.SubscriptionState;
        var values = new Dictionary<string, object?>
        {
            ["subscriptions"] = state switch
            {
                AskSubscriptionState.Holding => "holding",
                AskSubscriptionState.Lost => "lost",
                _ => "not-subscribed",
            },
            ["reachable"] = relay.Reachable,
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}
