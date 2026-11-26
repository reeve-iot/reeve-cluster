using DotNet.Testcontainers.Containers;

namespace Reeve.Cluster.Harness.Stack;

internal sealed class DatabaseFaults(Func<IContainer> database, Action<string> log)
{
    internal DatabaseFaults(IContainer postgres, Action<string> log)
        : this(() => postgres, log)
    {
    }

    public static IReadOnlyList<TimeSpan> Outages { get; } =
    [
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(20),
        TimeSpan.FromSeconds(30),
    ];

    public static IReadOnlyList<TimeSpan> OutagesFor(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var ridden = Timing.TimingWindows.GuaranteedOutage(options);
        var fences = options.LeaseTtl - options.SafetyMargin - options.StopBudget;
        return [ridden, (ridden + fences) / 2, fences];
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        log("database: stop");
        return database().StopAsync(cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        log("database: start");
        return database().StartAsync(cancellationToken);
    }

    public Task PauseAsync(CancellationToken cancellationToken = default)
    {
        log("database: pause");
        return database().PauseAsync(cancellationToken);
    }

    public Task UnpauseAsync(CancellationToken cancellationToken = default)
    {
        log("database: unpause");
        return database().UnpauseAsync(cancellationToken);
    }

    public async Task RestartAsync(TimeSpan downtime, CancellationToken cancellationToken = default)
    {
        await StopAsync(cancellationToken).ConfigureAwait(false);
        await Task.Delay(downtime, cancellationToken).ConfigureAwait(false);
        await StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task PauseForAsync(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await PauseAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await Task.Delay(duration, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await UnpauseAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
