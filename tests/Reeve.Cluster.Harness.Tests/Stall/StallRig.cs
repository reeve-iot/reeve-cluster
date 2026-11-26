using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Timing;

namespace Harness.Stall;

internal sealed class StallRig : IAsyncDisposable
{
    public const string Widget = "widget";
    public const string CreateTable = "CREATE TABLE IF NOT EXISTS stall_item (id bigserial PRIMARY KEY, value text NOT NULL)";
    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private StallRig(HarnessRun run) => Run = run;

    public static void Profile(ClusterOptions options)
    {
        TestProfile.ApplyStallTolerant(options);
        options.ApplyStallLimit = TimeSpan.FromSeconds(2);
    }

    public HarnessRun Run { get; }

    public static async Task<StallRig> CreateAsync(StackFixture stack, Action<string> log)
    {
        var run = await HarnessRun.BeginAsync(log, stack.Stack);
        try
        {
            await run.MigrateAsync();
            await using var connection = await run.OpenObserverConnectionAsync();
            await using var command = new NpgsqlCommand(CreateTable, connection);
            await command.ExecuteNonQueryAsync();
            return new StallRig(run);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public static ReplicaSpec Scripted(Action<ClusterOptions>? configure = null) => new()
    {
        Profile = Profile,
        Configure = configure,
        Reconcilers = [typeof(ScriptedWidget)],
        Redis = true,
    };

    public static ReplicaSpec Pooled(PoolPlan plan, int poolSize = 2, Action<ClusterOptions>? configure = null) => new()
    {
        Profile = Profile,
        Configure = configure,
        Reconcilers = [typeof(PooledWidget)],
        Redis = true,
        ConfigureServices = services =>
        {
            services.AddSingleton(plan);
            services.AddSingleton(sp => new AppPool(sp.GetRequiredService<IOptions<ClusterOptions>>().Value.ConnectionString!, poolSize, plan.CommandTimeoutSeconds));
        },
    };

    public async Task<Replica> StartAsync(string id, ReplicaSpec spec)
    {
        var replica = await Run.PrepareAsync(id, spec);
        await replica.StartAsync();
        await replica.Started!.WaitAsync(Limit);
        return replica;
    }

    public static async Task<long> UnitWriteAsync(Replica replica, string value)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        await using var connection = new NpgsqlConnection(replica.Options.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = new NpgsqlCommand("INSERT INTO stall_item (value) VALUES ($1)", connection, transaction))
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = value });
            await command.ExecuteNonQueryAsync();
        }

        var seq = await replica.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction, [new ChangeEntry(Widget, value)]);
        await transaction.CommitAsync();
        return seq;
    }

    public static Task<AckOutcome> AckFromAnotherScopeAsync(Replica replica, long seq) => Task.Run(async () =>
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        return await replica.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq);
    });

    public ValueTask DisposeAsync() => Run.DisposeAsync();
}

internal sealed class ScriptedWidget(ReconcilerScript script) : FaultyReconciler(StallRig.Widget, script);

internal enum PoolHang
{
    None,

    Await,

    BlockThread,

    ServerSleep,

    PassConnection,

    BoundedSleep,
}

internal sealed class PoolPlan
{
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int entered;

    public PoolHang Hang { get; set; }

    public int CommandTimeoutSeconds { get; init; }

    public int Entered => Volatile.Read(ref entered);

    public Task Released => released.Task;

    public void Release() => released.TrySetResult();

    public void Enter() => Interlocked.Increment(ref entered);
}

internal sealed class AppPool(string connectionString, int size, int commandTimeoutSeconds) : IAsyncDisposable
{
    public NpgsqlDataSource Source { get; } = new NpgsqlDataSourceBuilder(new NpgsqlConnectionStringBuilder(connectionString)
    {
        MaxPoolSize = size,
        Timeout = 3,
        CommandTimeout = commandTimeoutSeconds,
        ApplicationName = "stall-app-pool",
    }.ConnectionString).Build();

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed class PooledWidget(PoolPlan plan, AppPool pool) : IReconciler
{
    public string AggregateType => StallRig.Widget;

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var hang = context.FullLoad ? PoolHang.None : plan.Hang;
        await using var connection = await pool.Source.OpenConnectionAsync(CancellationToken.None);
        switch (hang)
        {
            case PoolHang.Await:
                plan.Enter();
                await plan.Released;
                break;
            case PoolHang.BlockThread:
                plan.Enter();
                plan.Released.GetAwaiter().GetResult();
                break;
            case PoolHang.ServerSleep or PoolHang.BoundedSleep:
                plan.Enter();
                await SleepAsync(connection, null, hang == PoolHang.ServerSleep ? 0 : null);
                break;
            case PoolHang.PassConnection:
                plan.Enter();
                await SleepAsync((NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction, 0);
                break;
        }

        await using var count = new NpgsqlCommand("SELECT count(*) FROM stall_item", connection);
        return new ReconcileResult((long)(await count.ExecuteScalarAsync(cancellationToken))!);
    }

    private static async Task SleepAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, int? commandTimeout)
    {
        await using var sleep = new NpgsqlCommand("SELECT pg_sleep(3600)", connection, transaction);
        if (commandTimeout is { } t)
        {
            sleep.CommandTimeout = t;
        }

        await sleep.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
