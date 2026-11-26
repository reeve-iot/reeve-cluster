using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Log;

internal sealed record OpenSkip(long Id, string AggregateType, long FromSeq, string Reason, string OpenedBy, DateTimeOffset OpenedAt);

internal sealed record OpenSkipsRead(IReadOnlyList<OpenSkip> Skips, DateTimeOffset ReadAt);

internal sealed partial class SkipMonitor(
    ClusterDataSource dataSource, IOptions<ClusterOptions> options, TimeProvider time, ILogger<SkipMonitor> logger)
    : BackgroundService, IStatusSectionSource
{
    public const string SectionName = "skips";

    internal const string Sql = """
        SELECT id, aggregate_type, from_seq, reason, opened_by, opened_at
        FROM reeve_cluster.change_skip WHERE until_seq IS NULL ORDER BY id
        """;

    private readonly TimeSpan interval = options.Value.StatusReportInterval;
    private readonly object sync = new();

    private readonly Dictionary<long, OpenSkip> counted = [];
    private bool disposed;
    private volatile OpenSkipsRead? last;

    public OpenSkipsRead? Last => last;

    string IStatusSectionSource.Name => SectionName;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var seen = last;
        var values = new Dictionary<string, object?>
        {
            ["open"] = seen?.Skips.Select(s => new Dictionary<string, object?>
            {
                ["id"] = s.Id,
                ["aggregateType"] = s.AggregateType,
                ["fromSeq"] = s.FromSeq,
                ["reason"] = s.Reason,
                ["openedBy"] = s.OpenedBy,
                ["openedAt"] = s.OpenedAt,
            }).ToList(),
            ["readAt"] = seen?.ReadAt,
        };
        return ValueTask.FromResult(new StatusSection { Name = SectionName, Values = values });
    }

    internal async Task CheckAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<OpenSkip> skips;
        try
        {
            using var timeout = new CancellationTokenSource(interval, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
            skips = await ReadAsync(bound.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            LogReadFailed(logger, ex);
            return;
        }

        Apply(skips, time.GetUtcNow());
    }

    internal void Apply(IReadOnlyList<OpenSkip> skips, DateTimeOffset readAt)
    {
        List<OpenSkip> opened = [];
        List<OpenSkip> ended = [];
        lock (sync)
        {
            if (disposed)
            {
                return;
            }

            var ids = skips.Select(s => s.Id).ToHashSet();
            foreach (var gone in counted.Values.Where(s => !ids.Contains(s.Id)).ToList())
            {
                counted.Remove(gone.Id);
                ended.Add(gone);
                ClusterDiagnostics.OpenSkips.Add(-1, new TagList { { "aggregate_type", gone.AggregateType } });
            }

            foreach (var skip in skips.Where(s => !counted.ContainsKey(s.Id)))
            {
                counted[skip.Id] = skip;
                opened.Add(skip);
                ClusterDiagnostics.OpenSkips.Add(1, new TagList { { "aggregate_type", skip.AggregateType } });
            }

            last = new OpenSkipsRead(skips, readAt);
        }

        foreach (var skip in opened)
        {
            LogOpen(logger, skip.AggregateType, skip.FromSeq, skip.Id, skip.OpenedBy, skip.Reason);
        }

        foreach (var skip in ended)
        {
            LogEnded(logger, skip.AggregateType, skip.FromSeq, skip.Id);
        }
    }

    public override void Dispose()
    {
        lock (sync)
        {
            if (!disposed)
            {
                disposed = true;

                foreach (var skip in counted.Values)
                {
                    ClusterDiagnostics.OpenSkips.Add(-1, new TagList { { "aggregate_type", skip.AggregateType } });
                }

                counted.Clear();
            }
        }

        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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

    [LoggerMessage(Level = LogLevel.Critical, Message = "A skip is open: the replicas treat {AggregateType} as applied from seq {FromSeq} on (skip {SkipId}, opened by {OpenedBy}: {Reason}). Acknowledgements name the type and G1 and G2 do not cover it until the skip is closed.")]
    private static partial void LogOpen(ILogger logger, string aggregateType, long fromSeq, long skipId, string openedBy, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "The skip {SkipId} of {AggregateType} from seq {FromSeq} is no longer open.")]
    private static partial void LogEnded(ILogger logger, string aggregateType, long fromSeq, long skipId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The open skips could not be read; the last result stands.")]
    private static partial void LogReadFailed(ILogger logger, Exception exception);

    private async Task<IReadOnlyList<OpenSkip>> ReadAsync(CancellationToken cancellationToken)
    {
        var connection = await dataSource.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var _ = connection.ConfigureAwait(false);
        var command = new NpgsqlCommand(Sql, connection);
        await using var __ = command.ConfigureAwait(false);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var ___ = reader.ConfigureAwait(false);
        var skips = new List<OpenSkip>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            skips.Add(new OpenSkip(
                reader.GetInt64(0), reader.GetString(1), reader.GetInt64(2), reader.GetString(3), reader.GetString(4),
                reader.GetFieldValue<DateTimeOffset>(5)));
        }

        return skips;
    }
}
