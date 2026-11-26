namespace Reeve.Cluster.Membership;

internal sealed class ReplicaPhaseReader : IReplicaPhase
{
    private readonly IPhaseGate gate;

    public ReplicaPhaseReader(IPhaseGate gate)
    {
        ArgumentNullException.ThrowIfNull(gate);
        this.gate = gate;
    }

    public ReplicaPhase Current => gate.Phase;

    public Task<ReplicaPhase> WaitForChangeAsync(ReplicaPhase seen, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<ReplicaPhase>(cancellationToken);
        }

        var changed = new TaskCompletionSource<ReplicaPhase>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = gate.Subscribe(e =>
        {
            if (e.Kind == PhaseEventKind.Transition && e.To != seen)
            {
                changed.TrySetResult(e.To);
            }
        });

        var phase = gate.Phase;
        if (phase != seen)
        {
            subscription.Dispose();
            return Task.FromResult(phase);
        }

        var registration = cancellationToken.Register(
            static (state, token) => ((TaskCompletionSource<ReplicaPhase>)state!).TrySetCanceled(token),
            changed);

        _ = changed.Task.ContinueWith(
            static (_, state) =>
            {
                var (subscribed, registered) = ((IDisposable, CancellationTokenRegistration))state!;
                registered.Dispose();
                subscribed.Dispose();
            },
            (subscription, registration),
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
        return changed.Task;
    }
}
