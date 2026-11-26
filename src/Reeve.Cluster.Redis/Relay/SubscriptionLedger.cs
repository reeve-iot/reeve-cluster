namespace Reeve.Cluster.Redis.Relay;

internal sealed class SubscriptionLedger
{
    private readonly object gate = new();
    private readonly HashSet<string> subscribed = [];
    private readonly HashSet<string> failedEndpoints = [];

    public void Subscribed(string channel)
    {
        lock (gate)
        {
            subscribed.Add(channel);
        }
    }

    public void Unsubscribed(string channel)
    {
        lock (gate)
        {
            subscribed.Remove(channel);
        }
    }

    public void ConnectionFailed(string endpoint)
    {
        lock (gate)
        {
            failedEndpoints.Add(endpoint);
        }
    }

    public void ConnectionRestored(string endpoint)
    {
        lock (gate)
        {
            failedEndpoints.Remove(endpoint);
        }
    }

    public bool IsSubscribed(string channel)
    {
        lock (gate)
        {
            return failedEndpoints.Count == 0 && subscribed.Contains(channel);
        }
    }
}
