using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class LoadSizingTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private ApplierRig? rig;

    public LoadSizingTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (rig is not null)
        {
            await rig.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_full_load_that_exceeds_ReconcileTimeout_raises_the_loading_reason_with_the_sizing_rule_and_a_published_load_clears_it()
    {
        var log = new ListLogger();
        rig = await ApplierRig.CreateAsync(postgres, ["a"], configure: o =>
        {
            o.ReconcileTimeout = TimeSpan.FromSeconds(1);
            o.ApplyStallLimit = TimeSpan.FromMinutes(5);
            o.AckTimeout = TimeSpan.FromMinutes(10);
        }, applierLogger: log);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig["a"].Before = (_, _) => held.Task;
        await rig.JoinAsync();

        var failed = await rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(LoadOutcome.Failed, failed);
        var reason = Assert.Single(rig.Gate.Readiness.Current, r => r.Code == Applier.LoadFailedReason);
        Assert.Contains("ReconcileTimeout 00:00:01", reason.Description, StringComparison.Ordinal);
        Assert.Contains("the reconciler of a did not return within ReconcileTimeout", reason.Description, StringComparison.Ordinal);
        Assert.Contains("2 to 3 times the slowest replica's full load of the largest type", reason.Description, StringComparison.Ordinal);

        var error = Assert.Single(log.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("failed after", error.Message, StringComparison.Ordinal);
        Assert.Contains("ReconcileTimeout 00:00:01", error.Message, StringComparison.Ordinal);
        Assert.Contains("the reconciler of a did not return within ReconcileTimeout", error.Message, StringComparison.Ordinal);
        Assert.Contains("2 to 3 times the slowest replica's full load of the largest type", error.Message, StringComparison.Ordinal);

        held.SetResult();
        rig["a"].Before = null;
        var deadline = DateTime.UtcNow + Wait;
        while (await rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None).WaitAsync(Wait) != LoadOutcome.Loaded)
        {
            Assert.True(DateTime.UtcNow < deadline, "no load published");
        }

        Assert.DoesNotContain(rig.Gate.Readiness.Current, r => r.Code == Applier.LoadFailedReason);
    }

    private sealed record Entry(LogLevel Level, string Message);

    private sealed class ListLogger : ILogger<Applier>
    {
        private readonly ConcurrentQueue<Entry> entries = new();

        public IReadOnlyList<Entry> Entries => [.. entries];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            entries.Enqueue(new Entry(logLevel, formatter(state, exception)));
    }
}
