namespace Reeve.Cluster.Harness.Timing;

internal static class TimingWindows
{
    public static readonly TimeSpan DefaultSlack = TimeSpan.FromMilliseconds(750);

    public static Window FenceStart(ClusterOptions options, TimeSpan? timerLateness = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var start = options.LeaseTtl - options.SafetyMargin - options.StopBudget;
        return new Window("fence start", start, start + (timerLateness ?? DefaultSlack));
    }

    public static Window LeaseRelease(ClusterOptions options, TimeSpan? slack = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new Window("lease release (fence step 3)", TimeSpan.Zero, options.RenewTimeout + (slack ?? DefaultSlack));
    }

    public static Window AckAfterCrash(ClusterOptions options, TimeSpan? slack = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new Window(
            "acknowledgement after a crash",
            options.LeaseTtl - options.RenewInterval - options.RenewTimeout,
            options.LeaseTtl + options.AckPollInterval + (slack ?? DefaultSlack));
    }

    public static double SlowestClockRate(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return 1.0 - options.SafetyMargin / (2 * options.LeaseTtl);
    }

    public static TimeSpan GuaranteedOutage(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var latestStart = options.LeaseTtl - options.SafetyMargin - options.StopBudget - options.RenewTimeout;
        var attempts = (long)Math.Floor(latestStart / options.RenewInterval);
        return attempts < 1 ? TimeSpan.Zero : (attempts - 1) * options.RenewInterval;
    }
}

internal enum OutageRegime
{
    RiddenOut,

    Either,

    Fences,
}

internal static class OutageRegimes
{
    public static OutageRegime Of(ClusterOptions options, TimeSpan outage)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (outage <= TimingWindows.GuaranteedOutage(options))
        {
            return OutageRegime.RiddenOut;
        }

        return outage >= options.LeaseTtl - options.SafetyMargin - options.StopBudget ? OutageRegime.Fences : OutageRegime.Either;
    }
}
