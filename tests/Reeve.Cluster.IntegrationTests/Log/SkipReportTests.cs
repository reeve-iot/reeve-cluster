using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class SkipReportTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public SkipReportTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            await item.DisposeAsync();
        }
    }

    [Fact]
    public async Task Management_path_a_container_built_with_AddReeveCluster_and_never_started_opens_refuses_and_closes_a_skip_without_joining()
    {
        var log = await LogRig.CreateAsync(postgres);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddReeveCluster(new ClusterPorts([typeof(ThrowingReconciler)], typeof(NoFenceSink)), o =>
        {
            TestProfile.Apply(o);
            o.ReplicaId = "management";
            o.ConnectionString = log.ConnectionString;
        });
        using var host = builder.Build();
        var skips = host.Services.GetRequiredService<IChangeSkip>();
        long poison;
        await using (var connection = await log.OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            poison = await host.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction, [new ChangeEntry("thrown", "k")]);
            await transaction.CommitAsync();
        }

        var opened = Assert.IsType<SkipOpenResult.Opened>(await skips.OpenAsync(new SkipRequest("thrown", poison, "poison", "cli")));
        Assert.Equal(1L, await log.ScalarAsync<long>($"SELECT count(*) FROM reeve_cluster.change_skip WHERE id = {opened.SkipId} AND until_seq IS NULL"));
        await using (var connection = await log.OpenAsync())
        await using (var command = new Npgsql.NpgsqlCommand($"UPDATE reeve_cluster.change_ack SET acked_seq = {poison}", connection))
        {
            await command.ExecuteNonQueryAsync();
        }

        Assert.Equal(new SkipOpenResult.Refused(poison), await skips.OpenAsync(new SkipRequest("other", poison, "late", "cli")));
        var repair = await skips.CloseAsync(opened.SkipId, "cli");

        Assert.Equal(repair, await log.ScalarAsync<long>("SELECT max(seq) FROM reeve_cluster.change_log"));
        Assert.Equal(1L, await log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_skip"));
        Assert.Equal(0L, await log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.member"));
        Assert.Null(host.Services.GetRequiredService<GenerationStore>().Current);
    }

    [Fact]
    public async Task An_open_skip_is_reported_by_a_critical_log_line_the_metric_and_the_status_section_and_its_end_by_an_information_line()
    {
        var rig = await RigAsync();
        var type = "t" + Guid.NewGuid().ToString("N");
        var logger = new ListLogger();
        using var metric = new OpenSkipsMetric(type);
        var monitor = Monitor(rig.Options, logger);

        var id = await InsertAsync(rig, type, 7);
        await monitor.CheckAsync(CancellationToken.None);

        var skip = Assert.Single(monitor.Last!.Skips);
        Assert.Equal((id, type, 7L, "poison", "ops"), (skip.Id, skip.AggregateType, skip.FromSeq, skip.Reason, skip.OpenedBy));
        var critical = Assert.Single(logger.Entries, e => e.Level == LogLevel.Critical);
        Assert.Contains(type, critical.Message);
        Assert.Contains("from seq 7", critical.Message);
        Assert.Contains("ops", critical.Message);
        Assert.Contains("poison", critical.Message);
        Assert.Equal(1, metric.Sum);

        var section = await monitor.CollectAsync(CancellationToken.None);
        Assert.Equal(SkipMonitor.SectionName, section.Name);
        var row = Assert.Single((IEnumerable<IReadOnlyDictionary<string, object?>>)section.Values["open"]!);
        Assert.Equal(id, row["id"]);
        Assert.Equal(type, row["aggregateType"]);
        Assert.Equal(7L, row["fromSeq"]);
        Assert.Equal("poison", row["reason"]);
        Assert.Equal("ops", row["openedBy"]);
        Assert.IsType<DateTimeOffset>(row["openedAt"]);
        Assert.Equal(monitor.Last.ReadAt, section.Values["readAt"]);

        await monitor.CheckAsync(CancellationToken.None);
        Assert.Single(logger.Entries, e => e.Level == LogLevel.Critical);
        Assert.Equal(1, metric.Sum);

        await rig.ExecuteAsync($"UPDATE reeve_cluster.change_skip SET until_seq = 9, closed_by = 'ops', closed_at = clock_timestamp() WHERE id = {id}");
        await monitor.CheckAsync(CancellationToken.None);

        Assert.Empty(monitor.Last.Skips);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information && e.Message.Contains(type, StringComparison.Ordinal));
        Assert.Equal(0, metric.Sum);
    }

    [Fact]
    public async Task A_failed_read_keeps_the_last_result_and_logs_a_warning_and_a_disposed_monitor_takes_back_its_count()
    {
        var type = "t" + Guid.NewGuid().ToString("N");
        var logger = new ListLogger();
        using var metric = new OpenSkipsMetric(type);
        var down = new ClusterOptions { ConnectionString = "Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1", ReplicaId = "r1" };
        down.StatusReportInterval = TimeSpan.FromSeconds(5);
        var data = new ClusterDataSource(down);
        owned.Add(data);
        var monitor = new SkipMonitor(data, Options.Create(down), TimeProvider.System, new TypedLogger(logger));
        var before = new OpenSkipsRead([new OpenSkip(1, type, 3, "r", "ops", DateTimeOffset.UtcNow)], DateTimeOffset.UtcNow);
        monitor.Apply(before.Skips, before.ReadAt);
        Assert.Equal(1, metric.Sum);

        await monitor.CheckAsync(CancellationToken.None);

        Assert.Same(before.Skips, monitor.Last!.Skips);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning);
        monitor.Dispose();
        Assert.Equal(0, metric.Sum);
    }

    [Fact]
    public async Task Readiness_an_open_skip_is_a_critical_detail_of_the_readiness_check_and_never_changes_its_status()
    {
        var rig = await RigAsync();
        var monitor = Monitor(rig.Options, new ListLogger());
        var check = new ReadinessCheck(rig.Gate, [monitor]);
        var id = await InsertAsync(rig, "a", 4);

        await monitor.CheckAsync(CancellationToken.None);
        var loading = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, loading.Status);
        Assert.Contains("critical: skip open", loading.Description, StringComparison.Ordinal);

        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        var serving = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, serving.Status);
        Assert.Contains("critical: skip open, a from seq 4", serving.Description, StringComparison.Ordinal);
        Assert.Contains(serving.Data, d => d.Key == $"skip:{id}" && ((string)d.Value).Contains("a from seq 4", StringComparison.Ordinal));
        Assert.Equal(HealthStatus.Healthy, (await new ReadinessCheck(rig.Gate, []).CheckHealthAsync(new HealthCheckContext())).Status);
    }

    [Fact]
    public async Task A_read_still_unanswered_when_StatusReportInterval_has_passed_is_cancelled_with_a_warning_and_the_last_result_stands()
    {
        var rig = await RigAsync();
        var logger = new ListLogger();
        var time = new FakeTimeProvider();
        var monitor = Monitor(rig.Options, logger, time, b => b.CommandTimeout = 0);
        await InsertAsync(rig, "a", 4);
        using var giveUp = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await monitor.CheckAsync(CancellationToken.None).WaitAsync(giveUp.Token);
        var before = monitor.Last;
        Assert.Single(before!.Skips);

        await using var holder = await rig.Log.OpenAsync();
        await using var blocker = await holder.BeginTransactionAsync();
        await using (var hold = new Npgsql.NpgsqlCommand("LOCK TABLE reeve_cluster.change_skip IN ACCESS EXCLUSIVE MODE", holder, blocker))
        {
            await hold.ExecuteNonQueryAsync();
        }

        var check = monitor.CheckAsync(CancellationToken.None);
        while (await rig.Log.ScalarAsync<long>("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()") == 0)
        {
            Assert.False(check.IsCompleted);
            giveUp.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, giveUp.Token);
        }

        await Task.Delay(rig.Options.StatusReportInterval * 2, giveUp.Token);
        Assert.False(check.IsCompleted);
        time.Advance(rig.Options.StatusReportInterval);
        await check.WaitAsync(giveUp.Token);

        Assert.Same(before, monitor.Last);
        var warning = Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning);
        Assert.IsAssignableFrom<OperationCanceledException>(warning.Exception);
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Information);
        await blocker.RollbackAsync();
    }

    private static async Task<long> InsertAsync(ApplierRig rig, string type, long from)
    {
        await using var connection = await rig.Log.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            $"INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, reason, opened_by) VALUES ('{type}', {from}, 'poison', 'ops') RETURNING id",
            connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private SkipMonitor Monitor(ClusterOptions options, ListLogger logger, FakeTimeProvider? time = null, Action<Npgsql.NpgsqlConnectionStringBuilder>? tune = null)
    {
        var data = new ClusterDataSource(options, tune ?? (static _ => { }));
        owned.Add(data);
        var monitor = new SkipMonitor(data, Options.Create(options), time ?? new FakeTimeProvider(), new TypedLogger(logger));
        owned.Add(new Disposing(monitor));
        return monitor;
    }

    private async Task<ApplierRig> RigAsync()
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a"]);
        owned.Add(rig);
        await rig.LoadAsync();
        return rig;
    }

    private sealed class ThrowingReconciler : IReconciler
    {
        public string AggregateType => "thrown";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("never runs: the container is not started");
    }

    private sealed class Disposing(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    internal sealed record Entry(LogLevel Level, string Message, Exception? Exception = null);

    internal sealed class ListLogger : ILogger
    {
        private readonly ConcurrentQueue<Entry> entries = new();

        public IReadOnlyList<Entry> Entries => [.. entries];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            entries.Enqueue(new Entry(logLevel, formatter(state, exception), exception));
    }

    private sealed class TypedLogger(ListLogger inner) : ILogger<SkipMonitor>
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }

    private sealed class OpenSkipsMetric : IDisposable
    {
        private readonly MeterListener listener = new();
        private long sum;

        public OpenSkipsMetric(string type)
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == ClusterDiagnostics.Name && instrument.Name == "reeve_cluster.skip.open")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "aggregate_type" && (string?)tag.Value == type)
                    {
                        Interlocked.Add(ref sum, value);
                    }
                }
            });
            listener.Start();
        }

        public long Sum => Interlocked.Read(ref sum);

        public void Dispose() => listener.Dispose();
    }
}
