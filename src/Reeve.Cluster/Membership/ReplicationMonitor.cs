using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Membership;

internal sealed class ReplicationMonitor(
    ClusterDataSource dataSource,
    IOptions<ClusterOptions> options,
    TimeProvider time,
    ILogger<ReplicationMonitor> logger) : BackgroundService, IStatusSectionSource
{
    public const string SectionName = "replication";

    private readonly bool enabled = options.Value.CheckSynchronousReplication;
    private readonly TimeSpan interval = options.Value.ReplicationCheckInterval;
    private volatile Checked? last;
    private int notSynchronousReads;

    string IStatusSectionSource.Name => SectionName;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var seen = last;
        var values = new Dictionary<string, object?>
        {
            ["enabled"] = enabled,
            ["outcome"] = seen?.Finding.Outcome.ToString(),
            ["warning"] = seen?.Warned?.ToString(),
            ["walsenders"] = seen?.Finding.Settings.Walsenders,
            ["synchronousStandbyNames"] = seen?.Finding.Settings.StandbyNames,
            ["synchronousCommit"] = seen?.Finding.Settings.SynchronousCommit,
            ["checkedAt"] = seen?.At,
        };
        return ValueTask.FromResult(new StatusSection { Name = SectionName, Values = values });
    }

    internal async Task CheckAsync(CancellationToken cancellationToken)
    {
        ReplicationFinding finding;
        try
        {
            using var timeout = new CancellationTokenSource(interval, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
            finding = ReplicationCheck.Evaluate(await ReadAsync(bound.Token).ConfigureAwait(false));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            finding = new ReplicationFinding(ReplicationOutcome.Unreadable, default);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(ex, "The synchronous-replication check could not read the replication settings; the last result stands.");
            return;
        }

        Apply(finding);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!enabled)
        {
            logger.LogWarning("Cluster:CheckSynchronousReplication is off: the library does not check that a standby is synchronous. The guarantee that an acknowledged commit survives a failover is the operator's to keep.");
            return;
        }

        try
        {
            await CheckAsync(stoppingToken).ConfigureAwait(false);
            using var timer = new PeriodicTimer(interval, time);
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await CheckAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task<ReplicationSettings> ReadAsync(CancellationToken cancellationToken)
    {
        var connection = await dataSource.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var _ = connection.ConfigureAwait(false);
        var command = new NpgsqlCommand(ReplicationCheck.Sql, connection);
        await using var __ = command.ConfigureAwait(false);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var ___ = reader.ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new ReplicationSettings(reader.GetInt32(0), reader.GetString(1), reader.GetString(2));
    }

    internal void Apply(ReplicationFinding finding)
    {
        var warned = last?.Warned;
        notSynchronousReads = finding.Outcome == ReplicationOutcome.NotSynchronous ? notSynchronousReads + 1 : 0;
        var warning = finding.Outcome switch
        {
            ReplicationOutcome.Unreadable => ReplicationOutcome.Unreadable,
            ReplicationOutcome.NotSynchronous when notSynchronousReads >= ReplicationCheck.NotSynchronousReads => ReplicationOutcome.NotSynchronous,
            ReplicationOutcome.NotSynchronous => warned,
            _ => (ReplicationOutcome?)null,
        };

        last = new Checked(finding, warning, time.GetUtcNow());
        if (warning == warned)
        {
            return;
        }

        switch (warning)
        {
            case ReplicationOutcome.Unreadable:
                logger.LogWarning("The synchronous-replication check cannot read pg_stat_replication: the database revoked it from PUBLIC. Whether an acknowledged commit survives a failover is not checked.");
                break;
            case ReplicationOutcome.NotSynchronous:
                logger.LogWarning(
                    "The database has {Walsenders} physical walsender(s), and synchronous_standby_names = '{StandbyNames}' with synchronous_commit = {SynchronousCommit} does not make a commit wait for a standby: an acknowledged commit may be lost in a failover. Configure synchronous replication (for example minSyncReplicas >= 1).",
                    finding.Settings.Walsenders,
                    finding.Settings.StandbyNames,
                    finding.Settings.SynchronousCommit);
                break;
            default:
                logger.LogInformation("The synchronous-replication check passes again ({Outcome}).", finding.Outcome);
                break;
        }
    }

    private sealed record Checked(ReplicationFinding Finding, ReplicationOutcome? Warned, DateTimeOffset At);
}
