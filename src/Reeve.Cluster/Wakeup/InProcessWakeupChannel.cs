using Microsoft.Extensions.Logging;

namespace Reeve.Cluster.Wakeup;

internal sealed class InProcessWakeupChannel : IWakeupChannel
{
    private readonly object gate = new();
    private readonly ILogger logger;
    private Subscription[] subscriptions = [];

    public InProcessWakeupChannel(ILogger<InProcessWakeupChannel> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        this.logger = logger;
    }

    public ValueTask PublishAsync(Wakeup wakeup, CancellationToken cancellationToken = default)
    {
        foreach (var subscription in Volatile.Read(ref subscriptions))
        {
            if (subscription.Kind != wakeup.Kind)
            {
                continue;
            }

            try
            {
                subscription.Handler(wakeup);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogError(exception, "A subscriber of {Kind} wake-ups threw.", wakeup.Kind);
            }
        }

        return ValueTask.CompletedTask;
    }

    public IDisposable Subscribe(WakeupKind kind, Action<Wakeup> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var subscription = new Subscription(this, kind, handler);
        lock (gate)
        {
            subscriptions = [.. subscriptions, subscription];
        }

        return subscription;
    }

    private void Remove(Subscription subscription)
    {
        lock (gate)
        {
            subscriptions = [.. subscriptions.Where(s => !ReferenceEquals(s, subscription))];
        }
    }

    private sealed class Subscription(InProcessWakeupChannel owner, WakeupKind kind, Action<Wakeup> handler) : IDisposable
    {
        public WakeupKind Kind => kind;

        public Action<Wakeup> Handler => handler;

        public void Dispose() => owner.Remove(this);
    }
}
