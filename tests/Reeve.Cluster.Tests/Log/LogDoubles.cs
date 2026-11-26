using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

namespace Unit.Log;

internal sealed class EventGate : IPhaseGate
{
    private readonly List<Action<PhaseEvent>> handlers = [];

    public ReplicaPhase Phase { get; private set; } = ReplicaPhase.Serving;

    public long? FenceStartTimestamp => null;

    public bool ExitAfterFence => false;

    public IReadinessReasons Readiness => throw new NotSupportedException();

    public int Subscribers => handlers.Count;

    public void MoveTo(ReplicaPhase to)
    {
        var from = Phase;
        Phase = to;
        foreach (var handler in handlers.ToList())
        {
            handler(new PhaseEvent(PhaseEventKind.Transition, from, to));
        }
    }

    public void RaiseDrainStep4()
    {
        foreach (var handler in handlers.ToList())
        {
            handler(new PhaseEvent(PhaseEventKind.DrainStep4, Phase, Phase));
        }
    }

    public IDisposable Subscribe(Action<PhaseEvent> handler)
    {
        handlers.Add(handler);
        return new Unsubscribe(() => handlers.Remove(handler));
    }

    public ServingCheckResult CheckServing() => throw new NotSupportedException();

    public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

    public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

    public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

    public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

    public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

    private sealed class Unsubscribe(Action action) : IDisposable
    {
        public void Dispose() => action();
    }
}

internal sealed class FakeTransaction : DbTransaction
{
    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

    protected override DbConnection? DbConnection => null;

    public override void Commit()
    {
    }

    public override void Rollback()
    {
    }
}

internal sealed class RecordingChannel : IWakeupChannel
{
    private readonly ConcurrentQueue<Wakeup> published = new();

    public IReadOnlyList<Wakeup> Published => [.. published];

    public ValueTask PublishAsync(Wakeup wakeup, CancellationToken cancellationToken = default)
    {
        published.Enqueue(wakeup);
        return ValueTask.CompletedTask;
    }

    public IDisposable Subscribe(WakeupKind kind, Action<Wakeup> handler) => throw new NotSupportedException();
}
