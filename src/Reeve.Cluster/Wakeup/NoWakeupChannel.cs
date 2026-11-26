namespace Reeve.Cluster.Wakeup;

internal sealed class NoWakeupChannel : IWakeupChannel
{
    public static NoWakeupChannel Instance { get; } = new();

    private NoWakeupChannel()
    {
    }

    public ValueTask PublishAsync(Wakeup wakeup, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

    public IDisposable Subscribe(WakeupKind kind, Action<Wakeup> handler) => Nothing.Instance;

    private sealed class Nothing : IDisposable
    {
        public static Nothing Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}
