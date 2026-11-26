using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Unit.Status;

namespace Unit.Membership;

public sealed class ReplicationMonitorTests
{
    private static readonly ReplicationFinding NotSynchronous = new(ReplicationOutcome.NotSynchronous, new ReplicationSettings(1, "", "on"));
    private static readonly ReplicationFinding Synchronous = new(ReplicationOutcome.Synchronous, new ReplicationSettings(1, "FIRST 1 (s1)", "on"));
    private static readonly ReplicationFinding Unreadable = new(ReplicationOutcome.Unreadable, default);

    private readonly CapturingLogger<ReplicationMonitor> logger = new();
    private readonly ReplicationMonitor monitor;

    public ReplicationMonitorTests()
    {
        var options = Options.Create(new ClusterOptions { ConnectionString = "Host=unused", ReplicaId = "r1" });
        monitor = new ReplicationMonitor(new ClusterDataSource(options.Value), options, TimeProvider.System, logger);
    }

    private int Warnings => logger.Entries.Count(e => e.Level == LogLevel.Warning);

    private async Task<object?> WarningAsync() => (await monitor.CollectAsync(CancellationToken.None)).Values["warning"];

    [Fact]
    public async Task Not_synchronous_warns_once_after_consecutive_reads_and_a_pass_ends_the_warning()
    {
        for (var i = 1; i < ReplicationCheck.NotSynchronousReads; i++)
        {
            monitor.Apply(NotSynchronous);
        }

        Assert.Equal(0, Warnings);
        Assert.Null(await WarningAsync());
        Assert.Equal(nameof(ReplicationOutcome.NotSynchronous), (await monitor.CollectAsync(CancellationToken.None)).Values["outcome"]);

        monitor.Apply(NotSynchronous);
        monitor.Apply(NotSynchronous);
        Assert.Equal(1, Warnings);
        Assert.Equal(nameof(ReplicationOutcome.NotSynchronous), await WarningAsync());

        monitor.Apply(Synchronous);
        Assert.Null(await WarningAsync());
        Assert.Equal(1, Warnings);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information);
    }

    [Fact]
    public async Task A_pass_between_reads_restarts_the_count()
    {
        for (var i = 1; i < ReplicationCheck.NotSynchronousReads; i++)
        {
            monitor.Apply(NotSynchronous);
        }

        monitor.Apply(Synchronous);
        monitor.Apply(NotSynchronous);

        Assert.Equal(0, Warnings);
        Assert.Null(await WarningAsync());
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task An_unreadable_view_warns_at_once_and_only_once()
    {
        monitor.Apply(Unreadable);
        monitor.Apply(Unreadable);

        Assert.Equal(1, Warnings);
        Assert.Equal(nameof(ReplicationOutcome.Unreadable), await WarningAsync());
    }

    [Fact]
    public async Task An_unreadable_warning_holds_until_not_synchronous_is_confirmed()
    {
        monitor.Apply(Unreadable);
        for (var i = 1; i < ReplicationCheck.NotSynchronousReads; i++)
        {
            monitor.Apply(NotSynchronous);
            Assert.Equal(nameof(ReplicationOutcome.Unreadable), await WarningAsync());
        }

        monitor.Apply(NotSynchronous);

        Assert.Equal(nameof(ReplicationOutcome.NotSynchronous), await WarningAsync());
        Assert.Equal(2, Warnings);
    }
}
