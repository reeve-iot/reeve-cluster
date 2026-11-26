using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using StackExchange.Redis;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class ProxyTests(StackFixture fixture)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(20);
    private readonly ClusterStack stack = fixture.Stack;

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false, Timeout = 30 };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync();
    }

    [Fact]
    public async Task A_replica_reaches_the_database_through_its_own_proxy()
    {
        await using var link = await stack.CreateLinkAsync("via-proxy");

        await using var connection = await OpenAsync(link.PostgresConnectionString);

        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
        Assert.NotEqual(new NpgsqlConnectionStringBuilder(stack.ObserverPostgresConnectionString).Port, new NpgsqlConnectionStringBuilder(link.PostgresConnectionString).Port);
    }

    [Fact]
    public async Task A_cut_proxy_cuts_a_connection_already_open_and_refuses_new_ones_until_restored()
    {
        await using var link = await stack.CreateLinkAsync("cut");
        await using var open = await OpenAsync(link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(open, "SELECT 1"));

        await link.Postgres.CutAsync();

        Assert.True(link.Postgres.IsCut);
        await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync(open, "SELECT 1")).WaitAsync(Limit);
        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(link.PostgresConnectionString)).WaitAsync(Limit);

        await link.Postgres.RestoreAsync();

        await using var again = await OpenAsync(link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(again, "SELECT 1"));
    }

    [Fact]
    public async Task Cutting_one_replica_leaves_the_others_connected()
    {
        await using var cut = await stack.CreateLinkAsync("cut-one");
        await using var other = await stack.CreateLinkAsync("stays");
        await using var connection = await OpenAsync(other.PostgresConnectionString);

        await cut.CutAsync();

        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
    }

    [Fact]
    public async Task A_held_proxy_holds_the_answer_with_the_connection_open_and_delivers_it_on_release()
    {
        await using var link = await stack.CreateLinkAsync("hold");
        await using var connection = await OpenAsync(link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));

        var hold = await link.Postgres.HoldAsync();
        var asking = ScalarAsync(connection, "SELECT 42");
        await Task.Delay(700);

        Assert.False(asking.IsCompleted);

        await hold.ReleaseAsync();

        Assert.Equal(42, await asking.WaitAsync(Limit));
    }

    [Fact]
    public async Task A_request_sent_through_a_proxy_held_downstream_still_reaches_the_server()
    {
        await using var observer = await stack.OpenObserverConnectionAsync();
        await using (var create = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS harness_held (id int)", observer))
        {
            await create.ExecuteNonQueryAsync();
        }

        await using (var clear = new NpgsqlCommand("TRUNCATE harness_held", observer))
        {
            await clear.ExecuteNonQueryAsync();
        }

        await using var link = await stack.CreateLinkAsync("held-down");
        await using var connection = await OpenAsync(link.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");
        var hold = await link.Postgres.HoldAsync(Direction.Downstream);

        var inserting = ScalarAsync(connection, "INSERT INTO harness_held VALUES (1) RETURNING id");
        var rows = 0L;
        var deadline = Stopwatch.StartNew();
        while (rows == 0 && deadline.Elapsed < Limit)
        {
            rows = (long)(await ScalarAsync(observer, "SELECT count(*) FROM harness_held"))!;
            await Task.Delay(50);
        }

        Assert.Equal(1, rows);
        Assert.False(inserting.IsCompleted);

        await hold.ReleaseAsync();

        Assert.Equal(1, await inserting.WaitAsync(Limit));
    }

    [Fact]
    public async Task A_proxy_with_latency_answers_late_and_answers_at_once_when_cleared()
    {
        await using var link = await stack.CreateLinkAsync("slow");
        await using var connection = await OpenAsync(link.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");
        var latency = await link.Postgres.LatencyAsync(TimeSpan.FromSeconds(2), TimeSpan.Zero, Direction.Downstream);

        var slow = Stopwatch.StartNew();
        await ScalarAsync(connection, "SELECT 1");
        var slowTime = slow.Elapsed;

        await latency.RemoveAsync();
        var fastest = TimeSpan.MaxValue;
        for (var i = 0; i < 3; i++)
        {
            var fast = Stopwatch.StartNew();
            await ScalarAsync(connection, "SELECT 1");
            fastest = TimeSpan.FromTicks(Math.Min(fastest.Ticks, fast.Elapsed.Ticks));
        }

        Assert.True(slowTime >= TimeSpan.FromSeconds(1.95), $"took {slowTime}");
        Assert.True(fastest < TimeSpan.FromSeconds(1), $"the fastest of three queries after the toxic was removed took {fastest}");
    }

    [Fact]
    public async Task A_cut_redis_proxy_cuts_redis_and_leaves_postgres_alone()
    {
        await using var link = await stack.CreateLinkAsync("redis-cut");
        var options = new ConfigurationOptions { AbortOnConnectFail = true, ConnectTimeout = 3000 };
        options.EndPoints.Add(link.RedisConnectionString);
        await using (var redis = await ConnectionMultiplexer.ConnectAsync(options))
        {
            await redis.GetDatabase().PingAsync();
        }

        await link.Redis.CutAsync();

        await Assert.ThrowsAnyAsync<Exception>(() => ConnectionMultiplexer.ConnectAsync(options)).WaitAsync(Limit);
        await using var connection = await OpenAsync(link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
    }

    [Fact]
    public async Task A_disposed_link_gives_its_ports_back()
    {
        var first = await stack.CreateLinkAsync("recycled");
        var port = first.Postgres.Port;
        await first.DisposeAsync();

        await using var second = await stack.CreateLinkAsync("recycled-too");

        Assert.Equal(port, second.Postgres.Port);
    }

    [Fact]
    public async Task A_port_a_live_process_may_still_use_is_not_handed_to_another_proxy()
    {
        var first = await stack.CreateLinkAsync("quarantined");
        var port = first.Postgres.Port;
        await first.DisposeAsync(releasePorts: false);

        await using var second = await stack.CreateLinkAsync("next");

        Assert.NotEqual(port, second.Postgres.Port);
        Assert.NotEqual(port, second.Redis.Port);
    }

    [Fact]
    public async Task Two_holds_of_one_proxy_are_released_each_by_its_own_handle()
    {
        await using var link = await stack.CreateLinkAsync("two-holds");
        await using var connection = await OpenAsync(link.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");
        var testsHold = await link.Postgres.HoldAsync();
        var anotherHold = await link.Postgres.HoldAsync();
        var asking = ScalarAsync(connection, "SELECT 9");

        await testsHold.ReleaseAsync();
        await Task.Delay(500);

        Assert.False(asking.IsCompleted);

        await anotherHold.ReleaseAsync();

        Assert.Equal(9, await asking.WaitAsync(Limit));
    }

    [Fact]
    public async Task Releasing_a_hold_twice_does_nothing_the_second_time()
    {
        await using var link = await stack.CreateLinkAsync("twice");
        var hold = await link.Postgres.HoldAsync();

        await hold.ReleaseAsync();
        await hold.ReleaseAsync();

        await using var connection = await OpenAsync(link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
    }

    [Fact]
    public async Task A_run_has_a_database_of_its_own_and_drops_it_when_it_ends()
    {
        string name;
        await using (var first = await HarnessRun.BeginAsync(_ => { }, stack))
        {
            await using var second = await HarnessRun.BeginAsync(_ => { }, stack);
            await using var viaObserver = await first.OpenObserverConnectionAsync();
            name = (string)(await ScalarAsync(viaObserver, "SELECT current_database()"))!;
            await using var other = await second.OpenObserverConnectionAsync();

            Assert.NotEqual(name, (string)(await ScalarAsync(other, "SELECT current_database()"))!);
            Assert.Equal(name, first.Database!.Name);
        }

        await using var observer = await stack.OpenObserverConnectionAsync();
        Assert.Equal(0L, await ScalarAsync(observer, $"SELECT count(*) FROM pg_database WHERE datname = '{name}'"));
    }

    [Fact]
    public async Task A_replica_of_a_run_connects_to_the_runs_database_with_driver_timeouts_above_any_pause()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var replica = await run.StartReplicaAsync("r1");

        var connectionString = new NpgsqlConnectionStringBuilder(replica.Options.ConnectionString);

        Assert.Equal(run.Database!.Name, connectionString.Database);
        Assert.Equal(300, connectionString.CommandTimeout);
        Assert.Equal(300, connectionString.Timeout);
    }

    [Fact]
    public async Task A_replicas_container_connects_through_the_replicas_proxy_and_loses_it_when_the_proxy_is_cut()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var replica = await run.StartReplicaAsync("r1");
        var dataSource = replica.Services.GetRequiredService<Reeve.Cluster.Core.ClusterDataSource>().DataSource;
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
        }

        await replica.Link!.Postgres.CutAsync();
        NpgsqlConnection.ClearAllPools();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var connection = await dataSource.OpenConnectionAsync();
            await ScalarAsync(connection, "SELECT 1");
        }).WaitAsync(Limit);
    }
}
