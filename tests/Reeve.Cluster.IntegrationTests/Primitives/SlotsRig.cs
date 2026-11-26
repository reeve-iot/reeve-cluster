using System.Collections.Concurrent;
using Int.Claims;
using Int.Owned;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Slots;

namespace Int.Primitives;

internal sealed class SlotsRig : IAsyncDisposable
{
    private readonly OwnedDataSource data;

    private SlotsRig(ClaimsRig claims)
    {
        Claims = claims;
        data = new OwnedDataSource(claims.Options);
        Store = new SlotStore(data, claims.Options);
    }

    public ClaimsRig Claims { get; }

    public OwnedDatabase Db => Claims.Db;

    public SlotStore Store { get; }

    public static async Task<SlotsRig> CreateAsync(PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null, TimeProvider? time = null) =>
        new(await ClaimsRig.CreateAsync(postgres, fencedTransactionTimeout, time));

    public async Task<SlotReplica> AddReplicaAsync(string id, string state = "active", string lease = "1 minute")
    {
        var epoch = await Db.AddMemberAsync(id, state, lease);
        return new SlotReplica(this, id, epoch);
    }

    public async Task<SlotRow?> RowAsync(string key, string id)
    {
        await using var connection = await Db.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT holder_replica, holder_epoch, token, expires_at > clock_timestamp(), reeve_cluster.slot_valid(key, id, token)
            FROM reeve_cluster.slot WHERE key = @key AND id = @id
            """,
            connection);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync()
            ? new SlotRow(reader.GetString(0), reader.GetInt64(1), reader.GetGuid(2), reader.GetBoolean(3), reader.GetBoolean(4))
            : null;
    }

    public Task<long> CountAsync(string key) =>
        Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.slot WHERE key = @key", ("key", key));

    public Task<long> ValidCountAsync(string key) =>
        Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.slot WHERE key = @key AND reeve_cluster.slot_valid(key, id, token)", ("key", key));

    public Task<bool> ValidAsync(string key, string id, Guid token) =>
        Claims.ScalarAsync<bool>("SELECT reeve_cluster.slot_valid(@key, @id, @token)", ("key", key), ("id", id), ("token", token));

    public Task ExpireAsync(string key, string id) =>
        Claims.ExecAsync("UPDATE reeve_cluster.slot SET expires_at = clock_timestamp() - interval '1 millisecond' WHERE key = @key AND id = @id", ("key", key), ("id", id));

    public Task SetTtlAsync(string key, string id, string interval) =>
        Claims.ExecAsync($"UPDATE reeve_cluster.slot SET expires_at = clock_timestamp() + interval '{interval}' WHERE key = @key AND id = @id", ("key", key), ("id", id));

    public Task<bool> CanLockAsync(string key, string id, string mode) =>
        Db.CanLockAsync("slot", "key = @key AND id = @id", mode, ("key", key), ("id", id));

    public Task FenceMemberAsync(string replica) => Claims.SetStateAsync(replica, "fenced");

    public Task<long> RejoinAsync(string replica) =>
        Claims.ScalarAsync<long>(
            "UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch'), state = 'active', lease_expires_at = clock_timestamp() + interval '1 minute' WHERE replica_id = @replica RETURNING epoch",
            ("replica", replica));

    public async ValueTask DisposeAsync()
    {
        await data.DisposeAsync();
        await Claims.DisposeAsync();
    }
}

internal sealed record SlotRow(string Holder, long Epoch, Guid Token, bool Live, bool Valid);

internal sealed class SlotReplica : ISlotGate
{
    private readonly ConcurrentQueue<long> otherTimelines = new();

    internal SlotReplica(SlotsRig rig, string id, long epoch)
    {
        Rig = rig;
        Id = id;
        Epoch = epoch;
        Timeline = rig.Db.Timeline;
        Slots = new ClusterSlots(
            rig.Store, this, new SlotRegistry(rig.Claims.Time), new SlotScopes(), rig.Claims.Options, rig.Claims.Time, Points,
            NullLogger<ClusterSlots>.Instance, NoSlotObserver.Instance);
    }

    public SlotsRig Rig { get; }

    public string Id { get; }

    public long Epoch { get; }

    public long Timeline { get; set; }

    public volatile bool Serving = true;

    public volatile bool Holder = true;

    public HoldingPoints Points { get; } = new();

    public ClusterSlots Slots { get; }

    public IReadOnlyCollection<long> OtherTimelines => otherTimelines.ToArray();

    public ClaimSeat? CheckServing() => Serving ? new ClaimSeat(Id, Epoch, Timeline) : null;

    public ClaimSeat? CheckHolder(long grantedEpoch) => Holder && grantedEpoch == Epoch ? new ClaimSeat(Id, Epoch, Timeline) : null;

    public void OnOtherTimeline(long epoch) => otherTimelines.Enqueue(epoch);

    public async Task<Guid> TakeAsync(string key, string id, int limit = 1, TimeSpan? ttl = null)
    {
        var result = await Slots.AcquireAsync(key, id, limit, ttl ?? TimeSpan.FromMinutes(1));
        return Assert.IsType<SlotAcquireResult.Acquired>(result).Token;
    }

    public Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit = 1, TimeSpan? ttl = null) =>
        Slots.AcquireAsync(key, id, limit, ttl ?? TimeSpan.FromMinutes(1));

    public Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan? ttl = null) =>
        Slots.RenewAsync(key, id, token, ttl ?? TimeSpan.FromMinutes(1));
}
