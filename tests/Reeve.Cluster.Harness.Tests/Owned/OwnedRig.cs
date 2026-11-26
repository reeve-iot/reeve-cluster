using System.Collections.Concurrent;
using System.Globalization;
using Harness.Failover;
using Harness.Placer;
using Harness.Slice;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using StackExchange.Redis;

namespace Harness.Owned;

internal sealed class OwnedRig : IAsyncDisposable
{
    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(120);

    private const string Tables =
        """
        CREATE TABLE placer_history (id boolean PRIMARY KEY, n bigint NOT NULL);
        INSERT INTO placer_history VALUES (true, 0);
        CREATE TABLE placer_effect (n bigint PRIMARY KEY, tenure bigint NOT NULL, replica text NOT NULL);
        """;

    private readonly FailoverRig? failover;
    private ConnectionMultiplexer? redis;

    private OwnedRig(HarnessRun run, FailoverRig? failover)
    {
        Run = run;
        this.failover = failover;
    }

    public HarnessRun Run { get; }

    public OwnedLog Log { get; } = new();

    public FailoverRig Failover => failover ?? throw new InvalidOperationException("This run is on the shared stack: it has no standby.");

    public static async Task<OwnedRig> CreateAsync(StackFixture stack, Action<string> log)
    {
        var run = await HarnessRun.BeginAsync(log, stack.Stack);
        try
        {
            await run.MigrateAsync();
            await CreateTablesAsync(run);
            return new OwnedRig(run, null);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public static async Task<OwnedRig> CreateWithStandbyAsync(Action<string> log)
    {
        var rig = await FailoverRig.CreateAsync(log);
        try
        {
            await CreateTablesAsync(rig.Run);
            return new OwnedRig(rig.Run, rig);
        }
        catch
        {
            await rig.DisposeAsync();
            throw;
        }
    }

    private static async Task CreateTablesAsync(HarnessRun run)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(Tables, connection);
        await command.ExecuteNonQueryAsync();
    }

    public ReplicaSpec Spec(
        Action<IServiceCollection> units,
        Action<ClusterOptions>? configure = null,
        bool redis = false,
        IReadOnlyList<Type>? reconcilers = null,
        Action<ClusterOptions>? profile = null) => new()
        {
            Profile = profile,
            Configure = configure,
            Redis = redis,
            Reconcilers = reconcilers ?? [],
            ConfigureServices = services =>
            {
                services.AddSingleton(Log);
                units(services);
            },
        };

    public async Task<Replica> StartAsync(string id, ReplicaSpec spec)
    {
        var replica = await Run.PrepareAsync(id, spec);
        await replica.StartAsync();
        await replica.Started!.WaitAsync(Limit);
        await PlacerRig.ServingAsync(replica);
        return replica;
    }

    public static Task UntilAsync(Func<Task<bool>> condition, string what) => SliceRig.UntilAsync(condition, what, Limit);

    public async Task AllServingAsync()
    {
        foreach (var replica in Run.Replicas.Where(r => r.IsRunning))
        {
            await PlacerRig.ServingAsync(replica);
        }
    }

    public async Task<OwnedStart> AwaitTenureAsync(string kind, string key, long above, int effects)
    {
        OwnedStart? latest = null;
        await UntilAsync(
            async () =>
            {
                latest = Log.Starts.Where(s => s.Kind == kind && s.Key == key && s.Token > above).MaxBy(s => s.Token);
                return latest is not null && (await EffectsAsync()).Count(e => e.Tenure == latest.Token) >= effects;
            },
            $"{effects} effects of a tenure of {kind}/{key} above {above}");
        return latest!;
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

    public async Task<(string? Owner, long? Epoch, long Token)?> RowAsync(string kind, string key)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT owner_replica, owner_epoch, token FROM reeve_cluster.owned WHERE kind = @kind AND key = @key", connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync()
            ? (reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2))
            : null;
    }

    public async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync();
        return value is bool b ? (b ? 1 : 0) : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<string?> AssertOwnerStateAsync(string kind, string key, long token, long timeline)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT reeve_cluster.assert_owner(@kind, @key, @token, @timeline)", connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("token", token);
        command.Parameters.AddWithValue("timeline", timeline);
        try
        {
            await command.ExecuteNonQueryAsync();
            return null;
        }
        catch (PostgresException e)
        {
            return e.SqlState;
        }
    }

    public async Task<IDatabase> RedisAsync()
    {
        redis ??= await ConnectionMultiplexer.ConnectAsync(Run.Stack!.ObserverRedisConnectionString);
        return redis.GetDatabase();
    }

    public async Task<IReadOnlyList<long>> RedisLogAsync(OwnedStart start) =>
        [.. (await (await RedisAsync()).ListRangeAsync(OwnedWriter.RedisLogKey(start.RedisTag!))).Select(v => (long)v)];

    public static long TimelineOf(long token) => token >> 40;

    public static long SequenceValueOf(long token) => token & ((1L << 40) - 1);

    public static void AssertNoInterleave(IReadOnlyList<long> tenures, string what)
    {
        var seen = new HashSet<long>();
        long? previous = null;
        foreach (var tenure in tenures)
        {
            if (tenure == previous)
            {
                continue;
            }

            Assert.True(seen.Add(tenure), $"{what}: tenure {tenure} wrote again after tenure {previous} had written: {string.Join(", ", tenures)}");
            Assert.True(previous is null || tenure > previous, $"{what}: tenure {tenure} wrote after tenure {previous}, which is above it: {string.Join(", ", tenures)}");
            previous = tenure;
        }
    }

    public async ValueTask DisposeAsync()
    {
        redis?.Dispose();
        if (failover is not null)
        {
            await failover.DisposeAsync();
        }
        else
        {
            await Run.DisposeAsync();
        }
    }
}

internal sealed record OwnedStart(string Replica, string Kind, string Key, long Token, OwnedContext Context, string? RedisTag);

internal sealed class OwnedLog
{
    private readonly ConcurrentQueue<OwnedStart> starts = new();

    public IReadOnlyList<OwnedStart> Starts => [.. starts];

    public void Started(OwnedStart start) => starts.Enqueue(start);

    private readonly ConcurrentQueue<(long Token, OwnershipLostException Error)> refused = new();

    public IReadOnlyList<(long Token, OwnershipLostException Error)> Refusals => [.. refused];

    public void Refused(long token, OwnershipLostException error) => refused.Enqueue((token, error));

    public async Task<OwnedStart> StartAsync(Func<OwnedStart, bool> match, string what)
    {
        OwnedStart? found = null;
        await OwnedRig.UntilAsync(() => Task.FromResult((found = starts.FirstOrDefault(match)) is not null), what);
        return found!;
    }
}

internal sealed record OwnedPlan(int Writers, TimeSpan Hold, bool Redis = false, bool IgnoreStop = false);

internal sealed class OwnedWriter(OwnedLog log, OwnedPlan plan, IOptions<ClusterOptions> options) : IOwnedUnit
{
    private const string EffectSql =
        """
        WITH next AS (UPDATE placer_history SET n = n + 1 RETURNING n)
        INSERT INTO placer_effect SELECT n, @tenure, @replica FROM next
        """;

    public static string RedisLogKey(string hashTag) => $"{hashTag}:log";

    public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        var tag = plan.Redis ? context.RedisHashTag() : null;
        log.Started(new OwnedStart(options.Value.ReplicaId!, context.Kind, context.Key, context.Token, context, tag));
        var writers = Enumerable.Range(0, plan.Writers).Select(i => WriteAsync(context, tag, plan.Hold * i / Math.Max(1, plan.Writers), stop)).ToList();
        try
        {
            await Task.WhenAll([.. writers, Task.Delay(Timeout.Infinite, stop)]);
        }
        catch (Exception) when (stop.IsCancellationRequested)
        {
        }
    }

    public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

    private async Task WriteAsync(OwnedContext context, string? tag, TimeSpan stagger, CancellationToken stop)
    {
        await Task.Delay(stagger, stop);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await using (var connection = new NpgsqlConnection(options.Value.ConnectionString))
                {
                    await connection.OpenAsync(stop);
                    await using var fenced = await context.BeginFencedTransactionAsync(connection, cancellationToken: plan.IgnoreStop ? CancellationToken.None : stop);
                    await using (var hold = new NpgsqlCommand($"SELECT pg_sleep({plan.Hold.TotalSeconds.ToString(CultureInfo.InvariantCulture)})", connection, fenced.Transaction))
                    {
                        await hold.ExecuteNonQueryAsync(stop);
                    }

                    await using (var effect = new NpgsqlCommand(EffectSql, connection, fenced.Transaction))
                    {
                        effect.Parameters.AddWithValue("tenure", context.Token);
                        effect.Parameters.AddWithValue("replica", options.Value.ReplicaId!);
                        await effect.ExecuteNonQueryAsync(stop);
                    }

                    await fenced.CommitAsync(stop);
                }

                if (plan.Redis)
                {
                    await context.FenceRedisAsync(t => _ = t.ListRightPushAsync(RedisLogKey(tag!), context.Token), stop);
                }
            }
            catch (OwnershipLostException e)
            {
                log.Refused(context.Token, e);
                return;
            }
            catch (RedisFenceRefusedException)
            {
                await Task.Delay(50, stop);
            }
            catch (Exception) when (!stop.IsCancellationRequested)
            {
                await Task.Delay(100, stop);
            }
        }
    }
}
