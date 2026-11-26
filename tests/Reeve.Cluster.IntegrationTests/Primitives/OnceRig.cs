using System.Collections.Concurrent;
using Int.Claims;
using Int.Owned;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;

namespace Int.Primitives;

internal sealed class OnceRig : IAsyncDisposable
{
    private static readonly IServiceScopeFactory Scopes = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
    private readonly OwnedDataSource data;

    private OnceRig(ClaimsRig claims)
    {
        Claims = claims;
        data = new OwnedDataSource(claims.Options);
        Store = new OnceStore(data, claims.Options);
        Effects = new OnceEffects(
            [new OnceEffectRegistration("send", (_, key, token) => RunAsync("send", key, token)), new OnceEffectRegistration("mail", (_, key, token) => RunAsync("mail", key, token))]);
    }

    public ClaimsRig Claims { get; }

    public OnceStore Store { get; }

    public OnceEffects Effects { get; }

    public ConcurrentQueue<(string Effect, string Key)> Ran { get; } = new();

    public Func<string, CancellationToken, Task>? Body { get; set; }

    public static async Task<OnceRig> CreateAsync(PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null) =>
        new(await ClaimsRig.CreateAsync(postgres, fencedTransactionTimeout, time));

    public static OnceRig Open(OwnedDatabase db, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null) =>
        new(ClaimsRig.Open(db, fencedTransactionTimeout, time));

    public ClusterOnce OnceOf(Replica replica) =>
        new(Store, replica, Effects, Scopes, Claims.Options, Claims.Time, replica.Points, NullLogger<ClusterOnce>.Instance);

    public OnceSweeper SweeperOf(Replica replica, int passLimit = OnceSweeper.PassLimit, OnceEffects? effects = null) =>
        new(Store, replica, effects ?? Effects, Scopes, Claims.Options, Claims.Time, replica.Points, NullLogger<OnceSweeper>.Instance, passLimit);

    public Task ExpireAsync(string key) => Claims.ExpireClaimAsync(OnceKeys.Store(key));

    public async Task<Guid> SeedAsync(
        string key, string effect, string mode, string holder, long epoch, bool expired = false, bool completed = false, string ttl = "1 minute")
    {
        var token = Guid.NewGuid();
        key = OnceKeys.Store(key);
        await Claims.ExecAsync(
            $"""
            INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at, once_mode, once_effect, once_ttl, completed_at)
            VALUES (@key, @holder, @epoch, @token, clock_timestamp() + interval '{(expired ? "-1 second" : "1 minute")}', @mode, @effect, interval '{ttl}',
                    CASE WHEN @completed THEN clock_timestamp() END)
            """,
            ("key", key), ("holder", holder), ("epoch", epoch), ("token", token), ("mode", mode), ("effect", effect), ("completed", completed));
        return token;
    }

    public async Task<OnceRow?> RowAsync(string key)
    {
        key = OnceKeys.Store(key);
        await using var connection = await Claims.Db.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT holder_replica, holder_epoch, token, once_mode, once_effect, completed_at IS NOT NULL, reeve_cluster.claim_valid(key, token) FROM reeve_cluster.claim WHERE key = @key",
            connection);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync()
            ? new OnceRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetBoolean(5), reader.GetBoolean(6))
            : null;
    }

    public async ValueTask DisposeAsync()
    {
        await data.DisposeAsync();
        await Claims.DisposeAsync();
    }

    private async Task RunAsync(string effect, string key, CancellationToken token)
    {
        Ran.Enqueue((effect, key));
        if (Body is { } body)
        {
            await body(key, token);
        }
    }
}

internal sealed record OnceRow(string Holder, long Epoch, Guid Token, string? Mode, string? Effect, bool Completed, bool Valid);
