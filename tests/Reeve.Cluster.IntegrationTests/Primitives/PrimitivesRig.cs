using System.Collections.Concurrent;
using System.Text;
using Int.Claims;
using Int.Owned;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;

namespace Int.Primitives;

internal sealed class PrimitivesRig : IAsyncDisposable
{
    private readonly OwnedDataSource data;

    private PrimitivesRig(ClaimsRig claims)
    {
        Claims = claims;
        data = new OwnedDataSource(claims.Options);
        Flights = new FlightStore(data, claims.Options);
        Budgets = new BudgetStore(data, claims.Options);
        Counters = new CounterStore(data, claims.Options);
    }

    public ClaimsRig Claims { get; }

    public FlightStore Flights { get; }

    public BudgetStore Budgets { get; }

    public CounterStore Counters { get; }

    public ClaimScopes Scopes { get; } = new();

    public ConcurrentQueue<string> Computations { get; } = new();

    public static async Task<PrimitivesRig> CreateAsync(PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null) =>
        new(await ClaimsRig.CreateAsync(postgres, fencedTransactionTimeout, time));

    public static PrimitivesRig Open(OwnedDatabase db, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null) =>
        new(ClaimsRig.Open(db, fencedTransactionTimeout, time));

    public ClusterSingleFlight FlightOf(Replica replica) =>
        new(Flights, replica, Scopes, Claims.Options, Claims.Time, replica.Points, NullLogger<ClusterSingleFlight>.Instance);

    public ClusterBudget BudgetOf(Replica replica) =>
        new(Budgets, replica, Claims.Options, Claims.Time, replica.Points, NullLogger<ClusterBudget>.Instance);

    public ClusterCounter CounterOf() =>
        new(Counters, Claims.Options, Claims.Time, NullLogger<ClusterCounter>.Instance);

    public Func<CancellationToken, Task<byte[]>> Compute(string key, string value, Func<Task>? during = null) => async _ =>
    {
        Computations.Enqueue(key);
        if (during is not null)
        {
            await during();
        }

        return Bytes(value);
    };

    public Task<SingleFlightResult> RunAsync(Replica replica, string key, string value, TimeSpan? claimTtl = null, TimeSpan? resultTtl = null, Func<Task>? during = null) =>
        FlightOf(replica).RunAsync(key, Compute(key, value, during), claimTtl ?? TimeSpan.FromMinutes(1), resultTtl ?? TimeSpan.FromMinutes(1));

    public static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    public static string Text(SingleFlightResult result) => result.TryGetValue(out var value) ? Encoding.UTF8.GetString(value!) : $"<{result.GetType().Name}>";

    public async Task<FlightRow?> FlightRowAsync(string key)
    {
        await using var connection = await Claims.Db.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT holder_replica, holder_epoch, token, result, result_expires_at > clock_timestamp(), expires_at > clock_timestamp(),
                   reeve_cluster.claim_valid(key, token)
            FROM reeve_cluster.claim WHERE key = @key
            """,
            connection);
        command.Parameters.AddWithValue("key", FlightKeys.Store(key));
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync()
            ? new FlightRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.IsDBNull(3) ? null : Encoding.UTF8.GetString((byte[])reader.GetValue(3)),
                !reader.IsDBNull(4) && reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6))
            : null;
    }

    public Task ExpireResultAsync(string key) =>
        Claims.ExecAsync("UPDATE reeve_cluster.claim SET result_expires_at = clock_timestamp() - interval '1 millisecond' WHERE key = @key", ("key", FlightKeys.Store(key)));

    public Task ExpireFlightClaimAsync(string key) => Claims.ExpireClaimAsync(FlightKeys.Store(key));

    public Task<long> BudgetIdsAsync(string key) =>
        Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget_id WHERE key = @key", ("key", key));

    public Task<long> BudgetUsedAsync(string key) =>
        Claims.ScalarAsync<long>("SELECT COALESCE(sum(used), 0) FROM reeve_cluster.budget WHERE key = @key", ("key", key));

    public Task<long> BudgetWindowsAsync(string key) =>
        Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget WHERE key = @key", ("key", key));

    public Task<DateTime> WindowStartAsync(long seconds) =>
        Claims.ScalarAsync<DateTime>(
            "SELECT to_timestamp(floor(extract(epoch FROM clock_timestamp()) / @seconds) * @seconds) AT TIME ZONE 'UTC'", ("seconds", seconds));

    public Task<DateTime> StoredWindowStartAsync(string key) =>
        Claims.ScalarAsync<DateTime>("SELECT min(window_start) AT TIME ZONE 'UTC' FROM reeve_cluster.budget WHERE key = @key", ("key", key));

    public async Task<long?> CounterValueAsync(string key)
    {
        await using var connection = await Claims.Db.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT value FROM reeve_cluster.counter WHERE key = @key", connection);
        command.Parameters.AddWithValue("key", key);
        return await command.ExecuteScalarAsync() as long?;
    }

    public async ValueTask DisposeAsync()
    {
        await data.DisposeAsync();
        await Claims.DisposeAsync();
    }
}

internal sealed record FlightRow(string Holder, long Epoch, Guid Token, string? Result, bool ResultLive, bool ClaimLive, bool Valid);
