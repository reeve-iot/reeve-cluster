using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Harness.Placer;

internal sealed class PlacerRig : IAsyncDisposable
{
    public const string Kind = "widget";

    public const string WidgetHost = "widget-host";

    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(90);

    private PlacerRig(HarnessRun run) => Run = run;

    public HarnessRun Run { get; }

    public UnitLog Log { get; } = new();

    public static async Task<PlacerRig> CreateAsync(StackFixture stack, Action<string> log)
    {
        var run = await HarnessRun.BeginAsync(log, stack.Stack);
        try
        {
            await run.MigrateAsync();
            await using var connection = await run.OpenObserverConnectionAsync();
            await using var command = new NpgsqlCommand(
                """
                CREATE TABLE placer_history (id boolean PRIMARY KEY, n bigint NOT NULL);
                INSERT INTO placer_history VALUES (true, 0);
                CREATE TABLE placer_effect (n bigint PRIMARY KEY, tenure bigint NOT NULL, replica text NOT NULL);
                """,
                connection);
            await command.ExecuteNonQueryAsync();
            return new PlacerRig(run);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public ReplicaSpec Spec(WriterPlan? writers = null, bool hosting = false) => new()
    {
        ConfigureServices = services =>
        {
            services.AddSingleton(Log);
            services.AddSingleton(writers ?? new WriterPlan(0, TimeSpan.Zero));
            services.AddClusterUnit<WidgetUnit>(Kind, o => o.Eligibility = hosting ? EligibilityFilter.Any.Require(WidgetHost) : EligibilityFilter.Any);
        },
    };

    public async Task<Replica> StartAsync(string id, ReplicaSpec spec)
    {
        var replica = await Run.PrepareAsync(id, spec);
        await replica.StartAsync();
        await replica.Started!.WaitAsync(Limit);
        await ServingAsync(replica);
        return replica;
    }

    public static Task ServingAsync(Replica replica) =>
        UntilAsync(() => Task.FromResult(replica.Services.GetRequiredService<LocalPhaseGate>().Phase == ReplicaPhase.Serving), $"{replica.Id} serving");

    public async Task<bool> RefusedAsync(long token)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT reeve_cluster.assert_owner('widget', 'widget', @token, reeve_cluster.server_timeline())", connection);
        command.Parameters.AddWithValue("token", token);
        try
        {
            await command.ExecuteNonQueryAsync();
            return false;
        }
        catch (PostgresException e) when (e.SqlState == "RC001")
        {
            return true;
        }
    }

    public static Task HostWidgetsAsync(Replica replica) =>
        replica.Services.GetRequiredService<IClusterCapabilities>().SetAsync([WidgetHost]);

    public static Task UnhostWidgetsAsync(Replica replica) =>
        replica.Services.GetRequiredService<IClusterCapabilities>().SetAsync([]);

    public static PlacerUnit Placer(Replica replica) => replica.Services.GetRequiredService<PlacerUnit>();

    public static ClusterOptions Options(Replica replica) => replica.Services.GetRequiredService<IOptions<ClusterOptions>>().Value;

    public async Task<(string? Owner, long Token)?> RowAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT owner_replica, token FROM reeve_cluster.owned WHERE kind = 'widget' AND key = 'widget'", connection);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? (reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetInt64(1)) : null;
    }

    public async Task<string?> PlacerOwnerAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT owner_replica FROM reeve_cluster.owned WHERE kind = 'placer' AND key = 'placer'", connection);
        return await command.ExecuteScalarAsync() as string;
    }

    public async Task<bool> LeaseGoneAsync(string replica)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT lease_expires_at <= clock_timestamp() FROM reeve_cluster.member WHERE replica_id = @id", connection);
        command.Parameters.AddWithValue("id", replica);
        return await command.ExecuteScalarAsync() is true;
    }

    public async Task<IReadOnlyList<(long N, long Tenure, string Replica)>> EffectsAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT n, tenure, replica FROM placer_effect ORDER BY n", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var effects = new List<(long, long, string)>();
        while (await reader.ReadAsync())
        {
            effects.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2)));
        }

        return effects;
    }

    public static async Task<TimeSpan> UntilAsync(Func<Task<bool>> condition, string what, Stopwatch? clock = null)
    {
        var deadline = Stopwatch.StartNew();
        while (!await condition())
        {
            if (deadline.Elapsed > Limit)
            {
                throw new TimeoutException($"Not within {Limit}: {what}.");
            }

            await Task.Delay(50);
        }

        return clock?.Elapsed ?? deadline.Elapsed;
    }

    public ValueTask DisposeAsync() => Run.DisposeAsync();
}

internal sealed record UnitStart(string Replica, string Key, long Token, long At);

internal sealed class UnitLog
{
    private readonly ConcurrentQueue<UnitStart> starts = new();
    private int refused;

    public IReadOnlyList<UnitStart> Starts => [.. starts];

    public int Refused => Volatile.Read(ref refused);

    public void Started(UnitStart start) => starts.Enqueue(start);

    public void Refuse() => Interlocked.Increment(ref refused);

    public Task<UnitStart> StartOfAsync(string replica) => StartAsync(s => s.Replica == replica, $"a start on {replica}");

    public async Task<UnitStart> StartAsync(Func<UnitStart, bool> match, string what)
    {
        UnitStart? found = null;
        await PlacerRig.UntilAsync(() => Task.FromResult((found = starts.FirstOrDefault(match)) is not null), what);
        return found!;
    }
}

internal sealed record WriterPlan(int Writers, TimeSpan Hold);

internal sealed class WidgetUnit(UnitLog log, WriterPlan plan, IOptions<ClusterOptions> options) : IOwnedUnit
{
    public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        log.Started(new UnitStart(options.Value.ReplicaId!, context.Key, context.Token, Stopwatch.GetTimestamp()));
        var writers = Enumerable.Range(0, plan.Writers).Select(i => WriteAsync(context, plan.Hold * i / Math.Max(1, plan.Writers), stop)).ToList();
        try
        {
            await Task.WhenAll([.. writers, Task.Delay(Timeout.Infinite, stop)]);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
    }

    public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

    private async Task WriteAsync(OwnedContext context, TimeSpan stagger, CancellationToken stop)
    {
        await Task.Delay(stagger, stop);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await using var connection = new NpgsqlConnection(options.Value.ConnectionString);
                await connection.OpenAsync(stop);
                await using var fenced = await context.BeginFencedTransactionAsync(connection, cancellationToken: stop);
                await using (var hold = new NpgsqlCommand($"SELECT pg_sleep({plan.Hold.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)})", connection, fenced.Transaction))
                {
                    await hold.ExecuteNonQueryAsync(stop);
                }

                await using (var effect = new NpgsqlCommand(
                    """
                    WITH next AS (UPDATE placer_history SET n = n + 1 RETURNING n)
                    INSERT INTO placer_effect SELECT n, @tenure, @replica FROM next
                    """,
                    connection,
                    fenced.Transaction))
                {
                    effect.Parameters.AddWithValue("tenure", context.Token);
                    effect.Parameters.AddWithValue("replica", options.Value.ReplicaId!);
                    await effect.ExecuteNonQueryAsync(stop);
                }

                await fenced.CommitAsync(stop);
            }
            catch (OwnershipLostException)
            {
                log.Refuse();
                return;
            }
        }
    }
}
