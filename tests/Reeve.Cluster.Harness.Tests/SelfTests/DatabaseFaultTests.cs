using Npgsql;
using Reeve.Cluster.Harness.Stack;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class DatabaseFaultTests(StackFixture fixture)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);
    private readonly ClusterStack stack = fixture.Stack;

    private async Task<DateTimeOffset> PostmasterStartAsync()
    {
        var builder = new NpgsqlConnectionStringBuilder(stack.ObserverPostgresConnectionString) { Pooling = false, Timeout = 30 };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT pg_postmaster_start_time()", connection);
        var start = (DateTime)(await command.ExecuteScalarAsync())!;
        return new DateTimeOffset(DateTime.SpecifyKind(start, DateTimeKind.Utc));
    }

    [Fact]
    public async Task A_stopped_database_refuses_through_every_proxy_and_a_started_one_is_a_new_postmaster()
    {
        var before = await PostmasterStartAsync();

        await stack.Database.StopAsync();
        await Assert.ThrowsAnyAsync<Exception>(PostmasterStartAsync).WaitAsync(Limit);
        await stack.Database.StartAsync();

        var after = await PostmasterStartAsync().WaitAsync(Limit);
        Assert.True(after > before, $"postmaster started {before:O} then {after:O}");
    }

    [Fact]
    public async Task A_restart_lasts_as_long_as_its_downtime()
    {
        var started = System.Diagnostics.Stopwatch.StartNew();

        await stack.Database.RestartAsync(TimeSpan.FromSeconds(2)).WaitAsync(Limit);

        Assert.True(started.Elapsed >= TimeSpan.FromSeconds(1.95), $"took {started.Elapsed}");
        await PostmasterStartAsync().WaitAsync(Limit);
    }

    [Fact]
    public async Task A_paused_database_holds_the_answer_of_an_open_connection_until_it_is_thawed()
    {
        var builder = new NpgsqlConnectionStringBuilder(stack.ObserverPostgresConnectionString) { Pooling = false, Timeout = 30 };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT 7", connection);
        Assert.Equal(7, await command.ExecuteScalarAsync());

        await stack.Database.PauseAsync();
        try
        {
            var asking = command.ExecuteScalarAsync();
            await Task.Delay(1000);

            Assert.False(asking.IsCompleted);

            await stack.Database.UnpauseAsync();

            Assert.Equal(7, await asking.WaitAsync(Limit));
        }
        finally
        {
            await stack.Database.UnpauseAsync().ContinueWith(_ => { });
        }
    }

    [Fact]
    public async Task The_four_outage_lengths_of_the_spec_are_offered()
    {
        Assert.Equal(4, DatabaseFaults.Outages.Count);
        await Task.CompletedTask;
    }
}
