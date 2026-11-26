using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.Owned;

namespace Unit.Owned;

public sealed class ReleaseOverdueReportTests
{
    private static readonly DateTimeOffset At = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static OverdueRelease Overdue(string kind, long token) => new(kind, kind, token, "r1", At);

    [Fact]
    public async Task The_section_lists_the_last_read_and_is_empty_once_cleared()
    {
        using var report = new ReleaseOverdueReport(NullLogger<ReleaseOverdueReport>.Instance);
        report.Apply([Overdue("section-k", 10)], At);

        var section = await report.CollectAsync(CancellationToken.None);
        var listed = Assert.IsType<List<Dictionary<string, object?>>>(section.Values["overdue"]);

        Assert.Equal(ReleaseOverdueReport.SectionName, section.Name);
        Assert.Equal(("section-k", "section-k", 10L, "r1", At), ((string)listed[0]["kind"]!, (string)listed[0]["key"]!, (long)listed[0]["token"]!, (string)listed[0]["owner"]!, (DateTimeOffset)listed[0]["requestedAt"]!));
        Assert.Equal(At, section.Values["readAt"]);

        report.Clear();
        Assert.Null((await report.CollectAsync(CancellationToken.None)).Values["overdue"]);
    }

    [Fact]
    public void The_metric_follows_the_reads_and_each_report_takes_back_its_own()
    {
        using var listener = new OverdueListener("metric-k");
        var report = new ReleaseOverdueReport(NullLogger<ReleaseOverdueReport>.Instance);

        report.Apply([Overdue("metric-k", 10)], At);
        report.Apply([Overdue("metric-k", 10)], At);
        Assert.Equal(1, listener.Total);

        report.Apply([Overdue("metric-k", 12)], At);
        Assert.Equal(1, listener.Total);

        report.Clear();
        Assert.Equal(0, listener.Total);

        report.Apply([Overdue("metric-k", 12), Overdue("metric-k", 14)], At);
        Assert.Equal(2, listener.Total);
        report.Dispose();
        Assert.Equal(0, listener.Total);
        report.Apply([Overdue("metric-k", 16)], At);
        Assert.Equal(0, listener.Total);
    }

    private sealed class OverdueListener : IDisposable
    {
        private readonly MeterListener listener = new();
        private long total;

        public OverdueListener(string kind)
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == ClusterDiagnostics.Name && instrument.Name == ClusterDiagnostics.OverdueReleasesName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "kind" && (string?)tag.Value == kind)
                    {
                        Interlocked.Add(ref total, value);
                    }
                }
            });
            listener.Start();
        }

        public long Total => Interlocked.Read(ref total);

        public void Dispose() => listener.Dispose();
    }
}
