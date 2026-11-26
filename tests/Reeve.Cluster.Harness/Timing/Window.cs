using System.Globalization;
using System.Runtime.CompilerServices;
using Xunit.Sdk;

namespace Reeve.Cluster.Harness.Timing;

internal readonly record struct Window(string What, TimeSpan Earliest, TimeSpan Latest)
{
    public TimeSpan Width => Latest - Earliest;

    public bool Contains(TimeSpan offset) => offset >= Earliest && offset <= Latest;

    public Window OnClockOfRate(double rate) =>
        this with { Earliest = TimeSpan.FromTicks(Math.Max(0, (Earliest / rate - TimerGranularity).Ticks)), Latest = Latest / rate };

    private static readonly TimeSpan TimerGranularity = TimeSpan.FromMilliseconds(50);

    public Window Widen(TimeSpan before, TimeSpan after) =>
        this with { Earliest = Earliest - before, Latest = Latest + after };

    public void AssertContains(TimeSpan offset)
    {
        if (!Contains(offset))
        {
            throw new WindowViolationException(this, offset);
        }
    }

    public bool AssertContains(
        TimeSpan offset,
        StallSpan stalls,
        Action<string> log,
        [CallerMemberName] string member = "",
        [CallerFilePath] string file = "")
    {
        ArgumentNullException.ThrowIfNull(log);
        if (Contains(offset))
        {
            return true;
        }

        var stall = stalls.Largest;
        if (offset > Latest && offset - Latest <= stall + StallProbe.Granularity)
        {
            Degraded.Report(
                log,
                string.Create(CultureInfo.InvariantCulture, $"{What} landed {offset.TotalSeconds:0.###} s from its reference instant, {(offset - Latest).TotalSeconds:0.###} s after the window's end ({Latest.TotalSeconds:0.###} s); {stalls.Describe()} accounts for it, so the upper end was not checked"),
                member,
                file);
            return false;
        }

        throw new WindowViolationException(this, offset, stalls.Describe());
    }

    public override string ToString() => string.Create(CultureInfo.InvariantCulture, $"{What}: [{Earliest.TotalSeconds:0.###} s, {Latest.TotalSeconds:0.###} s]");
}

internal sealed class WindowViolationException(Window window, TimeSpan observed, string? stalls = null)
    : XunitException(string.Create(
        CultureInfo.InvariantCulture,
        $"{window.What} landed {observed.TotalSeconds:0.###} s from its reference instant; the profile's window is [{window.Earliest.TotalSeconds:0.###} s, {window.Latest.TotalSeconds:0.###} s].{(stalls is null ? string.Empty : $" The process saw {stalls}, which does not account for it.")}"))
{
    public Window Window { get; } = window;

    public TimeSpan Observed { get; } = observed;
}
