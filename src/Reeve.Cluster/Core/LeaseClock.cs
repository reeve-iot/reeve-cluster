namespace Reeve.Cluster.Core;

internal static class LeaseClock
{
    public static TimeSpan? AdvanceDeadline(TimeSpan? current, TimeSpan t0, LeaseTiming timing)
    {
        var candidate = t0 + timing.LeaseTtl - timing.SafetyMargin;

        return current is { } c && c > candidate ? c : candidate;
    }

    public static TimeSpan FenceStart(TimeSpan deadline, LeaseTiming timing) => deadline - timing.StopBudget;
}
