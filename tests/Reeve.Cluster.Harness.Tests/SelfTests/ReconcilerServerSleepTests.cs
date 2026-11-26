using System.Diagnostics;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Stack;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class ReconcilerServerSleepTests(StackFixture fixture)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);
    private readonly ClusterStack stack = fixture.Stack;

    private sealed class WidgetReconciler(ReconcilerScript script) : FaultyReconciler("widget", script);

    private async Task<long> SleepingStatementsAsync()
    {
        await using var observer = await stack.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(86400)%' AND state = 'active'", observer);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task AssertSleepsAsync(HangMode mode)
    {
        var script = new ReconcilerScript(TimeProvider.System);
        using var hang = script.HangNext("widget", mode);
        var reconciler = new WidgetReconciler(script);

        var builder = new NpgsqlConnectionStringBuilder(stack.ObserverPostgresConnectionString) { Pooling = false };
        await using var pass = new NpgsqlConnection(builder.ConnectionString);
        await pass.OpenAsync();
        await using var transaction = await pass.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead);
        var context = new ReconcileContext(
            "widget",
            1,
            1,
            1,
            true,
            null,
            null,
            transaction,
            async cancellationToken =>
            {
                var own = new NpgsqlConnection(builder.ConnectionString);
                await own.OpenAsync(cancellationToken);
                return new OwnSession(own);
            });
        var attempt = reconciler.ReconcileAsync(context, CancellationToken.None);
        await hang.WaitEnteredAsync();

        var waiting = Stopwatch.StartNew();
        while (await SleepingStatementsAsync() == 0 && waiting.Elapsed < Limit)
        {
            await Task.Delay(50);
        }

        Assert.Equal(1, await SleepingStatementsAsync());
        Assert.False(attempt.IsCompleted);

        hang.Release();

        var result = await attempt.WaitAsync(Limit);
        Assert.IsType<ReconciledState>(result.State);
        Assert.Equal(0, await SleepingStatementsAsync());
    }

    [Fact]
    public Task A_reconciler_can_sleep_on_the_server_on_its_own_connection() => AssertSleepsAsync(HangMode.ServerSleepOwnConnection);

    [Fact]
    public Task A_reconciler_can_sleep_on_the_server_on_the_pass_connection() => AssertSleepsAsync(HangMode.ServerSleepPassConnection);

    private sealed class OwnSession(NpgsqlConnection connection) : ISnapshotSession
    {
        public System.Data.Common.DbConnection Connection => connection;

        public System.Data.Common.DbTransaction Transaction => throw new NotSupportedException();

        public ValueTask DisposeAsync() => connection.DisposeAsync();
    }
}
