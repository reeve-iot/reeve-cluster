using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Harness.Observation;

internal sealed record PhaseChange(long Sequence, string Replica, PhaseEventKind Kind, ReplicaPhase From, ReplicaPhase To, long Timestamp, DateTimeOffset At);

internal sealed class PhaseTimeline(RunSequence sequence)
{
    private readonly object gate = new();
    private readonly List<PhaseChange> changes = [];
    private readonly List<string> breaks = [];

    public IDisposable Attach(string replica, IPhaseGate phaseGate, TimeProvider clock, Func<bool>? isAlive = null)
    {
        ArgumentNullException.ThrowIfNull(phaseGate);

        PhaseChange? previous = null;
        return phaseGate.Subscribe(e =>
        {
            if (isAlive is not null && !isAlive())
            {
                return;
            }

            lock (gate)
            {
                if (e.Kind == PhaseEventKind.Transition && previous is not null && previous.To != e.From)
                {
                    breaks.Add($"{replica}: {previous.From} -> {previous.To}, then {e.From} -> {e.To}");
                }

                var change = new PhaseChange(sequence.Next(), replica, e.Kind, e.From, e.To, clock.GetTimestamp(), clock.GetUtcNow());
                changes.Add(change);
                if (e.Kind == PhaseEventKind.Transition)
                {
                    previous = change;
                }
            }
        });
    }

    public IReadOnlyList<string> Discontinuities
    {
        get
        {
            lock (gate)
            {
                return [.. breaks];
            }
        }
    }

    public IReadOnlyList<PhaseChange> Snapshot()
    {
        lock (gate)
        {
            return [.. changes];
        }
    }

    public IReadOnlyList<PhaseChange> Of(string replica) => [.. Snapshot().Where(c => c.Replica == replica)];

    public IReadOnlyList<ReplicaPhase> Path(string replica) =>
        [.. Of(replica).Where(c => c.Kind == PhaseEventKind.Transition).Select(c => c.To)];
}
