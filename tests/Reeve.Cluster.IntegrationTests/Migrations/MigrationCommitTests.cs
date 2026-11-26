using System.Diagnostics;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Migrations;
using Xunit.Abstractions;

namespace Int.Migrations;

public sealed class MigrationCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan NoAnswer = TimeSpan.FromSeconds(5);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;

    public MigrationCommitTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SetStandbyAsync(string.Empty);

    private async Task<string> DatabaseAsync()
    {
        var created = await postgres.CreateDatabaseAsync();
        await using (var context = new ClusterMigrationsDbContext(ClusterMigrationsDesignTimeFactory.Options(created)))
        await using (var connection = new NpgsqlConnection(created))
        {
            await connection.OpenAsync();
            await using var create = new NpgsqlCommand("CREATE SCHEMA IF NOT EXISTS reeve_cluster; " + context.GetService<IHistoryRepository>().GetCreateIfNotExistsScript(), connection);
            await create.ExecuteNonQueryAsync();
        }

        var cs = new NpgsqlConnectionStringBuilder(created) { CommandTimeout = 1 };
        return cs.ConnectionString;
    }

    private static ClusterMigrationRunner Runner(string connectionString) => ClusterRunners.For(connectionString);

    private static List<string> Shipped() => typeof(ClusterMigrationsDbContext).Assembly.GetTypes()
        .Select(t => t.GetCustomAttribute<MigrationAttribute>()?.Id)
        .Where(id => id is not null)
        .Order(StringComparer.Ordinal)
        .ToList()!;

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_runners_wait_for_the_standby()
    {
        var cs = await DatabaseAsync();
        await StallTheStandbyAsync(cs);

        var run = Runner(cs).MigrateAsync();

        await AssertNoAnswerAsync(run, cs);
        var result = await run.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.NotEmpty(result.AppliedMigrations);
    }

    [Fact]
    public async Task I33_a_run_whose_wait_for_the_standby_its_callers_token_cancels_throws_although_the_history_was_written()
    {
        var cs = await DatabaseAsync();
        await StallTheStandbyAsync(cs);
        using var cancel = new CancellationTokenSource();

        var run = Runner(cs).MigrateAsync(cancel.Token);
        await UntilTheCommitWaitsAsync(cs);
        var clock = Stopwatch.StartNew();
        cancel.CancelAfter(TimeSpan.FromSeconds(2));
        var error = await Record.ExceptionAsync(() => run.WaitAsync(TimeSpan.FromSeconds(30)));

        output.WriteLine($"the run threw {error?.GetType().Name} after {clock.Elapsed}: {error?.Message}");
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.True(clock.Elapsed >= TimeSpan.FromSeconds(1.9), $"The run ended after {clock.Elapsed}, before its token.");
        Assert.Equal(1L, await HistoryCountAsync(cs));

        await SetStandbyAsync(string.Empty);
        var shipped = Shipped();
        var next = await Runner(cs).MigrateAsync();
        Assert.Equal(shipped.Skip(1), next.AppliedMigrations);
        Assert.Equal(shipped[^1], next.LastMigration);
        Assert.Equal(shipped.Count, await HistoryCountAsync(cs));
    }

    [Fact]
    public async Task A_statement_before_the_commit_stays_bounded_by_the_connection_strings_Command_Timeout()
    {
        var cs = await DatabaseAsync();
        await using var holder = new NpgsqlConnection(cs);
        await holder.OpenAsync();
        await using (var hold = new NpgsqlCommand("SELECT pg_advisory_lock(@key)", holder))
        {
            hold.Parameters.AddWithValue("key", ClusterMigrationRunner.AdvisoryLockKey);
            await hold.ExecuteNonQueryAsync();
        }

        var clock = Stopwatch.StartNew();
        var error = await Record.ExceptionAsync(() => Runner(cs).MigrateAsync().WaitAsync(TimeSpan.FromSeconds(30)));

        output.WriteLine($"the run threw {error?.GetType().Name} after {clock.Elapsed}: {error?.Message}");
        var npgsql = Assert.IsType<NpgsqlException>(error);
        Assert.IsType<TimeoutException>(npgsql.InnerException);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), $"The run ended after {clock.Elapsed}.");
    }

    private async Task AssertNoAnswerAsync(Task call, string cs)
    {
        await UntilTheCommitWaitsAsync(cs);
        var ended = await Task.WhenAny(call, Task.Delay(NoAnswer));
        output.WriteLine(ended == call ? $"the call ended: {call.Status}, {call.Exception?.GetBaseException().Message}" : $"no answer within {NoAnswer}");
        var waiting = await SyncStandby.WaitersAsync(postgres, new NpgsqlConnectionStringBuilder(cs).Database!);
        await SetStandbyAsync(string.Empty);
        Assert.NotSame(call, ended);
        Assert.Equal(1, waiting);
    }

    private Task UntilTheCommitWaitsAsync(string cs) => SyncStandby.UntilAWaiterAsync(postgres, new NpgsqlConnectionStringBuilder(cs).Database!);

    private async Task<long> HistoryCountAsync(string cs)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var exists = new NpgsqlCommand("SELECT to_regclass('reeve_cluster.\"__ClusterMigrationsHistory\"') IS NOT NULL", connection);
        if (!(bool)(await exists.ExecuteScalarAsync())!)
        {
            return 0;
        }

        await using var command = new NpgsqlCommand("SELECT count(*) FROM reeve_cluster.\"__ClusterMigrationsHistory\"", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task StallTheStandbyAsync(string cs)
    {
        await using (var admin = await postgres.OpenConnectionAsync())
        {
            await using var alter = new NpgsqlCommand($"ALTER DATABASE \"{new NpgsqlConnectionStringBuilder(cs).Database}\" SET synchronous_commit = on", admin);
            await alter.ExecuteNonQueryAsync();
        }

        await SyncStandby.StallAsync(postgres);
    }

    private Task SetStandbyAsync(string names) => SyncStandby.SetAsync(postgres, names);
}
