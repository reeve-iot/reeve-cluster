using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Owned;

internal sealed partial class ReleaseOverdueReport(ILogger<ReleaseOverdueReport> logger) : IStatusSectionSource, IDisposable
{
    public const string SectionName = "releases";

    private readonly object sync = new();
    private readonly Dictionary<(string Kind, string Key, long Token), OverdueRelease> counted = [];
    private bool disposed;
    private volatile Snapshot? last;

    public IReadOnlyList<OverdueRelease> Overdue => last?.Overdue ?? [];

    string IStatusSectionSource.Name => SectionName;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var seen = last;
        var values = new Dictionary<string, object?>
        {
            ["overdue"] = seen?.Overdue.Select(r => new Dictionary<string, object?>
            {
                ["kind"] = r.Kind,
                ["key"] = r.Key,
                ["token"] = r.Token,
                ["owner"] = r.Owner,
                ["requestedAt"] = r.RequestedAt,
            }).ToList(),
            ["readAt"] = seen?.ReadAt,
        };
        return ValueTask.FromResult(new StatusSection { Name = SectionName, Values = values });
    }

    public void Apply(IReadOnlyList<OverdueRelease> overdue, DateTimeOffset readAt)
    {
        ArgumentNullException.ThrowIfNull(overdue);
        List<OverdueRelease> newly = [];
        lock (sync)
        {
            if (disposed)
            {
                return;
            }

            newly.AddRange(Count(overdue));
            last = new Snapshot(overdue, readAt);
        }

        foreach (var request in newly)
        {
            LogOverdue(logger, request.Kind, request.Key, request.Token, request.Owner, request.RequestedAt);
        }
    }

    public void Clear()
    {
        lock (sync)
        {
            Count([]);
            last = null;
        }
    }

    public void Dispose()
    {
        lock (sync)
        {
            Count([]);
            disposed = true;
        }
    }

    private List<OverdueRelease> Count(IReadOnlyList<OverdueRelease> overdue)
    {
        var keys = overdue.Select(r => (r.Kind, r.Key, r.Token)).ToHashSet();
        foreach (var (key, gone) in counted.Where(c => !keys.Contains(c.Key)).ToList())
        {
            counted.Remove(key);
            ClusterDiagnostics.OverdueReleases.Add(-1, new TagList { { "kind", gone.Kind } });
        }

        List<OverdueRelease> newly = [];
        foreach (var request in overdue.Where(r => !counted.ContainsKey((r.Kind, r.Key, r.Token))))
        {
            counted[(request.Kind, request.Key, request.Token)] = request;
            ClusterDiagnostics.OverdueReleases.Add(1, new TagList { { "kind", request.Kind } });
            newly.Add(request);
        }

        return newly;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The release of {Kind}/{Key} tenure {Token} requested from {Owner} at {RequestedAt} is overdue: ReleaseGrace has passed. The request stays.")]
    private static partial void LogOverdue(ILogger logger, string kind, string key, long token, string owner, DateTimeOffset requestedAt);

    private sealed record Snapshot(IReadOnlyList<OverdueRelease> Overdue, DateTimeOffset ReadAt);
}
