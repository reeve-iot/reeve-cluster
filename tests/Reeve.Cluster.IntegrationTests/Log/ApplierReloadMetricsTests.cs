using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ApplierReloadMetricsCollection
{
    public const string Name = "ApplierReloadMetrics";
}

[Collection(ApplierReloadMetricsCollection.Name)]
public sealed class ApplierReloadMetricsTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];
    private readonly ListLogger log = new();

    public ApplierReloadMetricsTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var rig in rigs)
        {
            await rig.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_cursor_a_failover_lost_counts_one_reload_and_logs_it_while_the_first_load_and_an_incremental_pass_count_nothing()
    {
        using var reloads = new CounterRecorder(ClusterDiagnostics.InvalidCursorReloads);
        var rig = await RigAsync();
        var survivor = await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("a", "k2"));
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "k3"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal(0, reloads.Sum);
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("names no entry", StringComparison.Ordinal));

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq > {survivor}");
        await rig.WriteAsync(("a", "k4"));
        await rig.WriteAsync(("a", "k5"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.True(rig.Applier.LastPublished!.Full);
        Assert.Equal(1, reloads.Sum);
        var line = Assert.Single(log.Entries, e => e.Message.Contains("names no entry", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Information, line.Level);

        await rig.WriteAsync(("a", "k6"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.False(rig.Applier.LastPublished!.Full);
        Assert.Equal(1, reloads.Sum);
    }

    [Fact]
    public async Task A_rejoins_load_with_a_valid_cursor_reloads_in_full_and_counts_nothing()
    {
        using var reloads = new CounterRecorder(ClusterDiagnostics.InvalidCursorReloads);
        var rig = await RigAsync();
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync();

        rig.Gate.ReportFatalFault("cache corrupt");
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced'");
        rig.Applier.Stop();
        rig.Gate.CompleteFence();
        rig.Gate.Apply(MemberTrigger.RejoinPermitted);
        await rig.JoinAsync();
        Assert.Equal(LoadOutcome.Loaded, await rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None));

        Assert.True(rig.Applier.LastPublished!.Full);
        Assert.Equal(0, reloads.Sum);
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("names no entry", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_rewind_the_poll_finds_with_no_new_write_counts_one_reload()
    {
        using var reloads = new CounterRecorder(ClusterDiagnostics.InvalidCursorReloads);
        var rig = await RigAsync(o => o.ApplyPollInterval = TimeSpan.FromMilliseconds(200));
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync(startLoop: true);
        var seq = await rig.WriteAsync(("a", "k2"));
        await Poll.UntilAsync(() => rig.Store.Current!.Head == seq, "the pass of k2");
        Assert.Equal(0, reloads.Sum);

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq = {seq}");
        var head = await rig.HeadAsync();
        await Poll.UntilAsync(() => rig.Store.Current!.Head == head.Seq, "the reload");

        Assert.Equal(1, reloads.Sum);
        Assert.Single(log.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("names no entry", StringComparison.Ordinal));
    }

    private async Task<ApplierRig> RigAsync(Action<ClusterOptions>? configure = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a"], configure: configure, applierLogger: log);
        rigs.Add(rig);
        return rig;
    }

    private sealed class CounterRecorder : IDisposable
    {
        private readonly MeterListener listener = new();
        private long sum;

        public CounterRecorder(Instrument instrument)
        {
            listener.InstrumentPublished = (published, l) =>
            {
                if (ReferenceEquals(published, instrument))
                {
                    l.EnableMeasurementEvents(published);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref sum, value));
            listener.Start();
        }

        public long Sum => Interlocked.Read(ref sum);

        public void Dispose() => listener.Dispose();
    }

    private sealed class ListLogger : ILogger<Applier>
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries => [.. entries];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            entries.Enqueue((logLevel, formatter(state, exception)));
    }
}
