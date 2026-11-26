using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Redis;
using StackExchange.Redis;

namespace Reeve.Cluster.Harness.Stimuli;

internal sealed class RunBackup : IAsyncDisposable
{
    private readonly HarnessRun run;
    private int disposed;

    internal RunBackup(HarnessRun run, string name)
    {
        this.run = run;
        Name = name;
    }

    public string Name { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            await HarnessOperations.DropDatabaseAsync(run, Name).ConfigureAwait(false);
        }
    }
}

internal static partial class HarnessOperations
{
    public static async Task<RunBackup> TakeBackup(HarnessRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        var name = "bk_" + Guid.NewGuid().ToString("N");
        await CopyDatabaseAsync(run, template: run.Database!.Name, name).ConfigureAwait(false);
        return new RunBackup(run, name);
    }

    public static async Task RestoreDatabase(HarnessRun run, RunBackup backup)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(backup);
        var database = run.Database!.Name;
        await DropDatabaseAsync(run, database).ConfigureAwait(false);
        await CopyDatabaseAsync(run, template: backup.Name, database).ConfigureAwait(false);
        await ClearRedisKeysAsync(run).ConfigureAwait(false);
        run.RecordEnvironment(null, EnvironmentActions.Restore);
    }

    internal static async Task DropDatabaseAsync(HarnessRun run, string name)
    {
        await using var admin = await run.Stack!.OpenObserverConnectionAsync().ConfigureAwait(false);
        await EndSessionsAsync(admin, name).ConfigureAwait(false);
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {name} WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task CopyDatabaseAsync(HarnessRun run, string template, string name)
    {
        await using var admin = await run.Stack!.OpenObserverConnectionAsync().ConfigureAwait(false);
        await EndSessionsAsync(admin, template).ConfigureAwait(false);
        await using var create = new NpgsqlCommand($"CREATE DATABASE {name} TEMPLATE {template}", admin);
        await create.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task EndSessionsAsync(NpgsqlConnection admin, string database)
    {
        await using var end = new NpgsqlCommand("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", admin)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = database } },
        };
        await end.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task ClearRedisKeysAsync(HarnessRun run)
    {
        var options = ConfigurationOptions.Parse(run.Stack!.ObserverRedisConnectionString);
        options.AbortOnConnectFail = false;
        await using var redis = await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
        await ClusterRedisRestore.ClearOwnedKeysAsync(redis, run.RedisKeyPrefix).ConfigureAwait(false);
    }
}
