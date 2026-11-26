using System.Collections.Concurrent;
using Int.Owned;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;

namespace Int.Claims;

internal sealed class ClaimsRig : IAsyncDisposable
{
    public static readonly TimeSpan Calm = TimeSpan.FromSeconds(30);

    private readonly OwnedDataSource data;

    private ClaimsRig(OwnedDatabase db, OwnedDataSource data, IOptions<ClusterOptions> options, TimeProvider time)
    {
        Db = db;
        this.data = data;
        Options = options;
        Time = time;
        Store = new ClaimStore(data, options);
        Retention = new RetentionStore(data, options);
    }

    public OwnedDatabase Db { get; }

    public IOptions<ClusterOptions> Options { get; }

    public TimeProvider Time { get; }

    public ClaimStore Store { get; }

    public RetentionStore Retention { get; }

    public static async Task<ClaimsRig> CreateAsync(
        PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null, TimeSpan? retention = null)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        return Open(db, fencedTransactionTimeout, time, retention);
    }

    public static ClaimsRig Open(OwnedDatabase db, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null, TimeSpan? retention = null)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new ClusterOptions
        {
            ConnectionString = db.ConnectionString,
            FencedTransactionTimeout = fencedTransactionTimeout ?? Calm,
            OwnedRowRetention = retention ?? TimeSpan.FromHours(24),
        });
        return new ClaimsRig(db, new OwnedDataSource(options), options, time ?? TimeProvider.System);
    }

    public async Task<Replica> AddReplicaAsync(string id, string lease = "1 minute")
    {
        var epoch = await Db.AddMemberAsync(id, "active", lease);
        return new Replica(this, id, epoch);
    }

    public async Task ExecAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, sql, parameters);
    }

    public async Task<T> ScalarAsync<T>(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await Db.OpenAsync();
        return await OwnedDatabase.ScalarAsync<T>(connection, sql, parameters);
    }

    public async Task<ClaimRow?> RowAsync(string key)
    {
        await using var connection = await Db.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT holder_replica, holder_epoch, token, expires_at > clock_timestamp() FROM reeve_cluster.claim WHERE key = @key", connection);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? new ClaimRow(reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.GetBoolean(3)) : null;
    }

    public Task<long> CountAsync() => ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.claim");

    public Task<bool> ValidAsync(string key, Guid token) =>
        ScalarAsync<bool>("SELECT reeve_cluster.claim_valid(@key, @token)", ("key", key), ("token", token));

    public Task SetLeaseAsync(string replica, string lease) =>
        ExecAsync($"UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '{lease}' WHERE replica_id = @replica", ("replica", replica));

    public Task EndLeaseAsync(string replica) => SetLeaseAsync(replica, "-1 millisecond");

    public Task SetStateAsync(string replica, string state) =>
        ExecAsync("UPDATE reeve_cluster.member SET state = @state WHERE replica_id = @replica", ("replica", replica), ("state", state));

    public Task<bool> CanLockAsync(string key, string mode) => Db.CanLockAsync("claim", "key = @key", mode, ("key", key));

    public Task ExpireClaimAsync(string key) =>
        ExecAsync("UPDATE reeve_cluster.claim SET expires_at = clock_timestamp() - interval '1 millisecond' WHERE key = @key", ("key", key));

    public Task UntilInvalidAsync(string key, Guid token) =>
        Poll.UntilAsync(async () => !await ValidAsync(key, token), $"the claim of {key} to end");

    public async ValueTask DisposeAsync()
    {
        await data.DisposeAsync();
    }

    internal ClusterClaims ClaimsOf(Replica replica) =>
        new(Store, replica, Options, Time, replica.Points, NullLogger<ClusterClaims>.Instance);
}

internal sealed record ClaimRow(string Holder, long Epoch, Guid Token, bool Valid);

internal sealed class Replica : IClaimGate
{
    private readonly ConcurrentQueue<long> otherTimelines = new();
    private int checks;

    internal Replica(ClaimsRig rig, string id, long epoch)
    {
        Id = id;
        Epoch = epoch;
        Timeline = rig.Db.Timeline;
        Claims = rig.ClaimsOf(this);
    }

    public string Id { get; }

    public long Epoch { get; }

    public long Timeline { get; set; }

    public volatile bool Serving = true;

    public HoldingPoints Points { get; } = new();

    public ClusterClaims Claims { get; }

    public int Checks => Volatile.Read(ref checks);

    public IReadOnlyCollection<long> OtherTimelines => otherTimelines.ToArray();

    public ClaimSeat? CheckServing()
    {
        Interlocked.Increment(ref checks);
        return Serving ? new ClaimSeat(Id, Epoch, Timeline) : null;
    }

    public void OnOtherTimeline(long epoch) => otherTimelines.Enqueue(epoch);

    public async Task<Guid> WinAsync(string key, TimeSpan? ttl = null)
    {
        var result = await Claims.ClaimAsync(key, ttl ?? TimeSpan.FromMinutes(1));
        return Assert.IsType<ClaimResult.Won>(result).Token;
    }
}

internal sealed class HoldingPoints : IFaultPoints
{
    private readonly ConcurrentDictionary<string, Hold> holds = new(StringComparer.Ordinal);

    public Hold Hold(FaultPoint point) => holds[point.Name] = new Hold();

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default) =>
        holds.TryGetValue(point, out var hold) ? hold.ArriveAsync(cancellationToken) : ValueTask.CompletedTask;
}

internal sealed class Hold
{
    private readonly TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Arrived => arrived.Task;

    public void Release() => released.TrySetResult();

    public async ValueTask ArriveAsync(CancellationToken cancellationToken)
    {
        arrived.TrySetResult();
        await released.Task.WaitAsync(cancellationToken);
    }
}
