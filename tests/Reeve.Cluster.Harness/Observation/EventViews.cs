using System.Globalization;
using Reeve.Cluster.Harness.Faults;

namespace Reeve.Cluster.Harness.Observation;

internal sealed record GenerationRead(long Sequence, string Replica, string Request, long Generation, long Head);

internal readonly record struct AttemptRef(long Pass, long Attempt);

internal sealed record Publication(long Sequence, string Replica, long Generation, long Head, IReadOnlySet<string> Dirty, IReadOnlyDictionary<string, AttemptRef> Attempts);

internal sealed record CountedWaitSet(long Sequence, string Replica, string Action, string Request, long AwaitedSeq, IReadOnlySet<string> Members);

internal sealed class ObservationException(string message) : InvalidOperationException(message);

internal static class EventViews
{
    public static IReadOnlyList<RecordedEvent> Of(this ModelEventLog log, string replica) =>
        [.. log.Snapshot().Where(e => e.Replica == replica)];

    public static IReadOnlyList<string> Actions(this ModelEventLog log, string replica) => [.. log.Of(replica).Select(e => e.Action)];

    public static IReadOnlyList<GenerationRead> GenerationsRead(this ModelEventLog log) =>
        [.. log.Snapshot()
            .Where(e => e.Action == LogActions.QRead)
            .Select(e => new GenerationRead(
                e.Sequence,
                e.Replica,
                Require<string>(e, ObservationKeys.Request),
                RequireLong(e, ObservationKeys.Generation),
                RequireLong(e, ObservationKeys.Head)))];

    public static IReadOnlyList<Publication> Publications(this ModelEventLog log)
    {
        var publications = new List<Publication>();
        var sources = new Dictionary<string, Dictionary<string, AttemptRef>>(StringComparer.Ordinal);
        foreach (var e in log.Snapshot())
        {
            var perType = sources.TryGetValue(e.Replica, out var known) ? known : sources[e.Replica] = new Dictionary<string, AttemptRef>(StringComparer.Ordinal);
            if (e.Action == LogActions.AttemptOK)
            {
                perType[Require<string>(e, ObservationKeys.Type)] = new AttemptRef(RequireLong(e, ObservationKeys.Pass), RequireLong(e, ObservationKeys.Attempt));
            }
            else if (e.Action == LogActions.Publish)
            {
                var dirty = Require<IEnumerable<string>>(e, ObservationKeys.Dirty).ToHashSet(StringComparer.Ordinal);
                foreach (var type in dirty.Where(t => !perType.ContainsKey(t)))
                {
                    throw new ObservationException($"Publish event #{e.Sequence} of replica {e.Replica} recomputed '{type}', but no AttemptOK of that type came before it on the replica.");
                }

                publications.Add(new Publication(
                    e.Sequence,
                    e.Replica,
                    RequireLong(e, ObservationKeys.Generation),
                    RequireLong(e, ObservationKeys.Head),
                    dirty,
                    new Dictionary<string, AttemptRef>(perType)));
            }
        }

        return publications;
    }

    public static IReadOnlyList<CountedWaitSet> WaitSets(this ModelEventLog log) =>
        [.. log.Snapshot()
            .Where(e => e.Action is LogActions.AckBegin or LogActions.AckDone)
            .Select(e => new CountedWaitSet(
                e.Sequence,
                e.Replica,
                e.Action,
                Require<string>(e, ObservationKeys.Request),
                RequireLong(e, ObservationKeys.AwaitedSeq),
                Require<IEnumerable<string>>(e, ObservationKeys.WaitSet).ToHashSet(StringComparer.Ordinal)))];

    private static T Require<T>(RecordedEvent e, string key)
    {
        if (e.State is null || !e.State.TryGetValue(key, out var value))
        {
            throw new ObservationException($"{e.Action} event #{e.Sequence} of replica {e.Replica} carries no '{key}': the code that emits it must use the keys of ObservationKeys.");
        }

        return value is T typed
            ? typed
            : throw new ObservationException($"{e.Action} event #{e.Sequence} of replica {e.Replica} carries '{key}' as {value?.GetType().Name ?? "null"}, not {typeof(T).Name}.");
    }

    private static long RequireLong(RecordedEvent e, string key)
    {
        var value = Require<object>(e, key);
        return value is int or long
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : throw new ObservationException($"{e.Action} event #{e.Sequence} of replica {e.Replica} carries '{key}' as {value.GetType().Name}, not an integer.");
    }
}
