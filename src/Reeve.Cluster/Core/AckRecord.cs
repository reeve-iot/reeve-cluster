namespace Reeve.Cluster.Core;

internal readonly record struct SkipRange(string AggregateType, long FromSeq, long? UntilSeq);

internal static class SkipRead
{
    public static bool Names(SkipRange range, long seq, long bound) =>
        range.FromSeq <= bound && (range.UntilSeq is not { } until || until > seq);

    public static IReadOnlyList<string> NamedTypes(IEnumerable<SkipRange> ranges, long seq, long bound) =>
        ranges
            .Where(range => Names(range, seq, bound))
            .Select(range => range.AggregateType)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();
}

internal readonly record struct RecordTimeout(int? Milliseconds)
{
    public const int MaxMilliseconds = int.MaxValue;

    public bool RecordRuns => Milliseconds is not null;

    public static AckOutcome.Unconfirmed TimedOut { get; } = new(UnconfirmedReason.Timeout);

    public static RecordTimeout FromRemaining(TimeSpan remaining)
    {
        if (remaining <= TimeSpan.Zero)
        {
            return new RecordTimeout(null);
        }

        var whole = remaining.Ticks / TimeSpan.TicksPerMillisecond;
        var milliseconds = remaining.Ticks % TimeSpan.TicksPerMillisecond == 0 ? whole : whole + 1;
        return new RecordTimeout((int)Math.Min(Math.Max(milliseconds, 1), MaxMilliseconds));
    }
}
