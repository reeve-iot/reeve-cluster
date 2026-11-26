using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Unit.Membership;

namespace Unit.Status;

public sealed class StatusReporterTests : IDisposable
{
    private const long Epoch = 7;
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly FakeStatusStore store = new();
    private readonly CapturingLogger<StatusReporter> logger = new();

    public StatusReporterTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
    }

    public void Dispose() => gate.Dispose();

    private StatusReporter Reporter(params IStatusSectionSource[] sources) =>
        new(sources, store, gate, Options.Create(options), time, logger, []);

    private void Join() => Assert.True(gate.Joined(Epoch, 1, gate.Now));

    [Fact]
    public async Task The_reporter_writes_every_source_as_a_section_of_the_gates_epoch_at_start_and_every_StatusReportInterval()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1), new FixedSource("lease", 2));

        await reporter.StartAsync(CancellationToken.None);
        await store.NextAsync(Wait);
        time.Advance(options.StatusReportInterval);
        await store.NextAsync(Wait);
        time.Advance(options.StatusReportInterval - TimeSpan.FromMilliseconds(1));
        Assert.Equal(2, store.Reports.Count);
        time.Advance(TimeSpan.FromMilliseconds(1));
        await store.NextAsync(Wait);
        await reporter.StopAsync(CancellationToken.None);

        Assert.Equal(3, store.Reports.Count);
        Assert.All(store.Reports, r =>
        {
            Assert.Equal(("r1", Epoch), (r.ReplicaId, r.Epoch));
            Assert.Equal(["phase", "lease"], r.Sections.Select(s => s.Name));
        });
        Assert.Equal("""{"n":1}""", store.Reports.First().Sections[0].Json);
        Assert.Equal(TimeSpan.FromSeconds(3), options.StatusReportInterval);
    }

    [Fact]
    public async Task No_report_is_written_while_the_gate_has_no_epoch()
    {
        var reporter = Reporter(new FixedSource("phase", 1));

        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.Empty(store.Reports);
        Join();
        await reporter.ReportOnceAsync(CancellationToken.None);
        Assert.Single(store.Reports);
    }

    [Fact]
    public async Task A_source_that_throws_drops_its_own_section_only_and_is_logged_once()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1), new ThrowingSource(), new FixedSource("lease", 2));

        await reporter.ReportOnceAsync(CancellationToken.None);
        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.All(store.Reports, r => Assert.Equal(["phase", "lease"], r.Sections.Select(s => s.Name)));
        Assert.Equal(2, store.Reports.Count);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(ThrowingSource), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_report_that_fails_is_logged_once_and_the_next_tick_reports_again()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1));
        store.Fault = new InvalidOperationException("the database is down");

        await reporter.ReportOnceAsync(CancellationToken.None);
        await reporter.ReportOnceAsync(CancellationToken.None);
        store.Fault = null;
        await reporter.ReportOnceAsync(CancellationToken.None);

        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Information);
        Assert.Single(store.Reports);
        Assert.Equal(3, store.Calls);
    }

    [Fact]
    public async Task A_report_that_does_not_answer_is_cancelled_at_StatusReportInterval_and_the_loop_goes_on()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1));
        store.Hang = true;

        var report = reporter.ReportOnceAsync(CancellationToken.None);
        await store.NextCallAsync(Wait);
        Assert.False(report.IsCompleted);
        time.Advance(options.StatusReportInterval);
        await report.WaitAsync(Wait);

        Assert.True(store.LastToken.IsCancellationRequested);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task Stopping_the_reporter_cancels_an_inflight_report_and_writes_nothing_after()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1));
        store.Hang = true;
        await reporter.StartAsync(CancellationToken.None);
        await store.NextCallAsync(Wait);

        await reporter.StopAsync(CancellationToken.None).WaitAsync(Wait);
        store.Hang = false;
        time.Advance(options.StatusReportInterval * 3);

        Assert.True(store.LastToken.IsCancellationRequested);
        Assert.Equal(1, store.Calls);
        Assert.Empty(store.Reports);
        Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task A_source_that_throws_a_cancellation_that_is_not_the_hosts_drops_its_own_section_and_does_not_stop_the_host()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1), new CancelledSource(), new FixedSource("lease", 2));

        await reporter.StartAsync(CancellationToken.None);
        await store.NextAsync(Wait);
        time.Advance(options.StatusReportInterval);
        await store.NextAsync(Wait);
        var loop = reporter.ExecuteTask!;
        await reporter.StopAsync(CancellationToken.None);

        Assert.False(loop.IsFaulted);
        Assert.All(store.Reports, r => Assert.Equal(["phase", "lease"], r.Sections.Select(s => s.Name)));
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(CancelledSource), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_source_that_throws_after_the_stop_began_ends_the_reporter_as_a_stop_not_a_fault()
    {
        Join();
        var stopping = new StoppingSource();
        var reporter = Reporter(stopping, new DisposedOnStopSource());
        await reporter.StartAsync(CancellationToken.None);
        await store.NextAsync(Wait);
        var loop = reporter.ExecuteTask!;
        Task? stop = null;
        stopping.OnCollect = () => stop = reporter.StopAsync(CancellationToken.None);

        time.Advance(options.StatusReportInterval);
        await stopping.Stopped.WaitAsync(Wait);
        await stop!.WaitAsync(Wait);

        Assert.True(loop.IsCompletedSuccessfully);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Debug && e.Message.EndsWith(nameof(ObjectDisposedException), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_source_that_does_not_answer_within_the_interval_is_left_out_of_that_report_and_the_others_are_written()
    {
        Join();
        var reporter = Reporter(new FixedSource("phase", 1), new HungSource(), new FixedSource("lease", 2));

        var report = reporter.ReportOnceAsync(CancellationToken.None);
        Assert.False(report.IsCompleted);
        time.Advance(options.StatusReportInterval);
        await report.WaitAsync(Wait);

        Assert.Equal(["phase", "lease"], Assert.Single(store.Reports).Sections.Select(s => s.Name));
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(HungSource), StringComparison.Ordinal));
    }

    [Fact]
    public void Two_sources_of_one_name_are_refused_when_the_reporter_is_built()
    {
        var error = Assert.Throws<ClusterConfigurationException>(() => Reporter(new FixedSource("phase", 1), new FixedSource("phase", 2)));

        Assert.Equal(ClusterConfigurationReason.StatusSectionNameDuplicate, error.Reason);
        Assert.Contains("'phase'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_NUL_in_a_value_is_replaced_so_that_jsonb_takes_the_report()
    {
        Join();
        var reporter = Reporter(new ValueSource("owned", "unit\0key"));

        await reporter.ReportOnceAsync(CancellationToken.None);

        var json = Assert.Single(store.Reports).Sections.Single().Json;
        Assert.DoesNotContain("\\u0000", json, StringComparison.Ordinal);
        Assert.Equal("unit\uFFFDkey", System.Text.Json.JsonDocument.Parse(json).RootElement.GetProperty("v").GetString());
    }

    private sealed class StoppingSource : IStatusSectionSource
    {
        private readonly TaskCompletionSource stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Name => "stopping";

        public Action? OnCollect { get; set; }

        public Task Stopped => stopped.Task;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
        {
            if (OnCollect is { } stop)
            {
                stop();
                stopped.TrySetResult();
            }

            return ValueTask.FromResult(new StatusSection { Name = Name, Values = new Dictionary<string, object?>() });
        }
    }

    private sealed class DisposedOnStopSource : IStatusSectionSource
    {
        public string Name => "disposed";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) => cancellationToken.IsCancellationRequested
            ? throw new ObjectDisposedException("the source's scope")
            : ValueTask.FromResult(new StatusSection { Name = Name, Values = new Dictionary<string, object?>() });
    }

    private sealed class CancelledSource : IStatusSectionSource
    {
        public string Name => "cancelled";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) => throw new OperationCanceledException("a source's own token");
    }

    private sealed class HungSource : IStatusSectionSource
    {
        public string Name => "hung";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) => new(new TaskCompletionSource<StatusSection>().Task);
    }

    private sealed class ValueSource(string name, string value) : IStatusSectionSource
    {
        public string Name => name;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection { Name = name, Values = new Dictionary<string, object?> { ["v"] = value } });
    }

    private sealed class FixedSource(string name, int n) : IStatusSectionSource
    {
        public string Name => name;

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(new StatusSection { Name = name, Values = new Dictionary<string, object?> { ["n"] = n } });
    }

    private sealed class ThrowingSource : IStatusSectionSource
    {
        public string Name => "throwing";

        public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken) => throw new InvalidOperationException("the source is broken");
    }
}

internal sealed class FakeStatusStore : IStatusStore
{
    private readonly System.Threading.Channels.Channel<bool> written = System.Threading.Channels.Channel.CreateUnbounded<bool>();
    private readonly System.Threading.Channels.Channel<bool> called = System.Threading.Channels.Channel.CreateUnbounded<bool>();
    private int calls;

    public ConcurrentQueue<(string ReplicaId, long Epoch, IReadOnlyList<ReportedSection> Sections)> Reports { get; } = new();

    public int Calls => Volatile.Read(ref calls);

    public Exception? Fault { get; set; }

    public bool Hang { get; set; }

    public CancellationToken LastToken { get; private set; }

    public Func<Task<ClusterStatusRead>>? OnRead { get; set; }

    public Func<CancellationToken, Task<ClusterStatusRead>>? OnReadBound { get; set; }

    public async Task NextAsync(TimeSpan wait) => await written.Reader.ReadAsync().AsTask().WaitAsync(wait);

    public async Task NextCallAsync(TimeSpan wait) => await called.Reader.ReadAsync().AsTask().WaitAsync(wait);

    public async Task<int> ReportAsync(string replicaId, long epoch, IReadOnlyList<ReportedSection> sections, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref calls);
        LastToken = cancellationToken;
        called.Writer.TryWrite(true);
        if (Fault is { } fault)
        {
            throw fault;
        }

        if (Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        Reports.Enqueue((replicaId, epoch, sections));
        written.Writer.TryWrite(true);
        return sections.Count;
    }

    public Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken) =>
        OnReadBound is { } bound ? bound(cancellationToken) : OnRead is { } read ? read() : Task.FromResult(new ClusterStatusRead { ReadAt = DateTimeOffset.UnixEpoch, Members = [] });
}

internal sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

    public IReadOnlyList<(LogLevel Level, string Message)> Entries => [.. entries];

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        entries.Enqueue((logLevel, formatter(state, exception) + (exception is null ? string.Empty : " " + exception.GetType().Name)));
}
