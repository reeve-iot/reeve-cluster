using Npgsql;
using Reeve.Cluster.Harness.Stack;
using Xunit.Abstractions;

namespace Harness.Failover;

[Collection("harness-failover")]
public sealed class PairStackTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task The_pair_reports_its_standby_as_async_sync_and_quorum_by_mode()
    {
        await using var stack = await StartAsync();
        var pair = stack.Pair;
        Assert.Equal(["standby1 streaming async"], await pair.ReplicationStateAsync());

        await pair.SetModeAsync(StandbyMode.SyncFirst);
        Assert.Equal(["standby1 streaming sync"], await pair.ReplicationStateAsync());
        await pair.SetModeAsync(StandbyMode.SyncAny);
        Assert.Equal(["standby1 streaming quorum"], await pair.ReplicationStateAsync());
        await pair.SetModeAsync(StandbyMode.Async);
        Assert.Equal(["standby1 streaming async"], await pair.ReplicationStateAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_cut_stream_holds_a_synchronous_commit_until_its_wait_is_cancelled_and_the_commit_is_then_visible(bool quorum)
    {
        await using var stack = await StartAsync();
        await stack.Pair.SetModeAsync(quorum ? StandbyMode.SyncAny : StandbyMode.SyncFirst);
        await using var admin = await stack.OpenObserverConnectionAsync();
        await Execute(admin, "CREATE TABLE t (v int)");

        await stack.ReplicationLink.CutAsync();
        await using var writer = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(stack.ObserverPostgresConnectionString) { ApplicationName = "writer" }.ConnectionString);
        await writer.OpenAsync();
        var commit = Execute(writer, "INSERT INTO t VALUES (1)");
        await WaitForSyncWaitAsync(admin, "writer");
        Assert.False(commit.IsCompleted);
        Assert.Equal(0L, await Scalar(admin, "SELECT count(*) FROM t"));

        await Scalar(admin, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'writer' AND wait_event = 'SyncRep'");
        await commit.WaitAsync(Limit);
        Assert.Equal(1L, await Scalar(admin, "SELECT count(*) FROM t"));
    }

    [Fact]
    public async Task A_failover_promotes_the_standby_on_a_new_timeline_and_the_observers_connection_reaches_it()
    {
        await using var stack = await StartAsync();
        await stack.Pair.SetModeAsync(StandbyMode.SyncFirst);
        await using var database = await stack.CreateRunDatabaseAsync();
        await using (var before = await stack.OpenObserverConnectionAsync())
        {
            await Execute(before, "CREATE TABLE t (v int)");
            await Execute(before, "INSERT INTO t VALUES (1)");
            Assert.Equal(1L, await Scalar(before, TimelineSql));
        }

        await stack.FailOverAsync();

        await using var after = await stack.OpenObserverConnectionAsync();
        Assert.Equal(2L, await Scalar(after, TimelineSql));
        Assert.Equal(1L, await Scalar(after, "SELECT count(*) FROM t"));
        await Execute(after, "INSERT INTO t VALUES (2)");
    }

    [Fact]
    public async Task A_switchover_leaves_the_old_primary_streaming_from_the_new_one_in_the_same_mode()
    {
        await using var stack = await StartAsync();
        await stack.Pair.SetModeAsync(StandbyMode.SyncFirst);
        await using (var before = await stack.OpenObserverConnectionAsync())
        {
            await Execute(before, "CREATE TABLE t (v int)");
        }

        await stack.SwitchOverAsync();

        Assert.Equal(["standby1 streaming sync"], await stack.Pair.ReplicationStateAsync());
        await using var after = await stack.OpenObserverConnectionAsync();
        Assert.Equal(2L, await Scalar(after, TimelineSql));
        await Execute(after, "INSERT INTO t VALUES (1)");
        Assert.Equal("1", await stack.Pair.QueryAsync(stack.Pair.Standby!, "SELECT count(*) FROM t"));
    }

    private const string TimelineSql = "SELECT ('x' || lpad(substr(pg_walfile_name(pg_current_wal_insert_lsn()), 1, 8), 16, '0'))::bit(64)::bigint";

    private async Task<ClusterStack> StartAsync()
    {
        var stack = new ClusterStack(output.WriteLine, new ClusterStackOptions { Standby = true });
        try
        {
            await stack.InitializeAsync();
            return stack;
        }
        catch
        {
            await stack.DisposeAsync();
            throw;
        }
    }

    private static async Task WaitForSyncWaitAsync(NpgsqlConnection admin, string application)
    {
        var deadline = DateTime.UtcNow + Limit;
        while (await Scalar(admin, $"SELECT count(*) FROM pg_stat_activity WHERE application_name = '{application}' AND wait_event = 'SyncRep'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "no backend waits for the standby");
            await Task.Delay(50);
        }
    }

    private static async Task Execute(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> Scalar(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync();
        return value is bool b ? (b ? 1 : 0) : Convert.ToInt64(value);
    }
}
