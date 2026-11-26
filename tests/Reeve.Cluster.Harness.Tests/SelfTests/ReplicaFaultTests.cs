using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class ReplicaFaultTests(StackFixture fixture)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);
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
    public async Task Pausing_a_replica_whole_holds_its_network_and_resuming_it_delivers_what_was_held()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var paused = await run.StartReplicaAsync("paused");
        var other = await run.StartReplicaAsync("other");
        await using var connection = await OpenAsync(paused.Link!.PostgresConnectionString);
        await using var bystander = await OpenAsync(other.Link!.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");

        await paused.PauseAsync();
        var asking = ScalarAsync(connection, "SELECT 5");
        await Task.Delay(700);

        Assert.False(asking.IsCompleted);
        Assert.Equal(1, await ScalarAsync(bystander, "SELECT 1"));

        await paused.ResumeAsync();

        Assert.Equal(5, await asking.WaitAsync(Limit));
    }

    [Fact]
    public async Task A_crashed_replica_is_cut_off_for_good_and_its_id_starts_again_on_proxies_of_its_own()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var first = await run.StartReplicaAsync("phoenix");
        var deadAddress = first.Link!.PostgresConnectionString;

        await first.CrashAsync();

        Assert.True(first.Link.Postgres.IsCut);
        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(deadAddress)).WaitAsync(Limit);

        var second = await run.StartReplicaAsync("phoenix");

        Assert.NotSame(first, second);
        Assert.Same(second, run.Get("phoenix"));
        Assert.NotEqual(deadAddress, second.Link!.PostgresConnectionString);
        Assert.NotEqual(first.Link.Postgres.Port, second.Link.Postgres.Port);
        await using var connection = await OpenAsync(second.Link.PostgresConnectionString);
        Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));

        Assert.True(first.Link.Postgres.IsCut);
        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(deadAddress)).WaitAsync(Limit);
    }

    [Fact]
    public async Task Pausing_twice_is_one_pause_and_resuming_an_unpaused_replica_does_nothing()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var replica = await run.StartReplicaAsync("twice");
        await using var connection = await OpenAsync(replica.Link!.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");

        await replica.ResumeAsync();
        await replica.PauseAsync();
        await replica.PauseAsync();
        var asking = ScalarAsync(connection, "SELECT 3");
        await Task.Delay(500);

        Assert.False(asking.IsCompleted);

        await replica.ResumeAsync();

        Assert.Equal(3, await asking.WaitAsync(Limit));
    }

    [Fact]
    public async Task A_hold_the_test_put_on_a_replicas_proxy_survives_the_replicas_pause_and_resume()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var replica = await run.StartReplicaAsync("held");
        await using var connection = await OpenAsync(replica.Link!.PostgresConnectionString);
        await ScalarAsync(connection, "SELECT 1");
        var testsHold = await replica.Link.Postgres.HoldAsync();
        var asking = ScalarAsync(connection, "SELECT 4");

        await replica.PauseAsync();
        await replica.ResumeAsync();
        await Task.Delay(500);

        Assert.False(asking.IsCompleted);

        await testsHold.ReleaseAsync();

        Assert.Equal(4, await asking.WaitAsync(Limit));
    }

    [Fact]
    public async Task A_failing_stop_in_one_replica_does_not_leave_the_others_or_their_proxies_behind()
    {
        string[] connectionStrings;
        var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var failing = await run.StartReplicaAsync("failing", new ReplicaSpec { ConfigureServices = s => s.AddHostedService<FailingStop>() });
        var other = await run.StartReplicaAsync("other");
        connectionStrings = [failing.Link!.PostgresConnectionString, other.Link!.PostgresConnectionString];

        await Assert.ThrowsAnyAsync<Exception>(async () => await run.DisposeAsync().AsTask()).WaitAsync(Limit);
        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(connectionStrings[0])).WaitAsync(Limit);
        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(connectionStrings[1])).WaitAsync(Limit);
        Assert.False(other.IsRunning);
    }

    private sealed class FailingStop : Microsoft.Extensions.Hosting.IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => throw new InvalidOperationException("this stop fails");
    }

    [Fact]
    public async Task Disposing_the_run_stops_its_replicas_and_removes_their_proxies()
    {
        string connectionString;
        await using (var run = await HarnessRun.BeginAsync(_ => { }, stack))
        {
            var replica = await run.StartReplicaAsync("transient");
            connectionString = replica.Link!.PostgresConnectionString;
            await using var connection = await OpenAsync(connectionString);
            Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
        }

        await Assert.ThrowsAnyAsync<Exception>(() => OpenAsync(connectionString)).WaitAsync(Limit);
    }

    [Fact]
    public async Task Several_replicas_run_in_one_process_against_one_database()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);

        var replicas = await run.StartReplicasAsync(3);

        Assert.Equal(3, replicas.Select(r => r.Link!.Postgres.Port).Distinct().Count());
        foreach (var replica in replicas)
        {
            await using var connection = await OpenAsync(replica.Link!.PostgresConnectionString);
            Assert.Equal(1, await ScalarAsync(connection, "SELECT 1"));
        }
    }
}
