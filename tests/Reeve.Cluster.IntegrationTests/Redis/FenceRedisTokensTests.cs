using Int.Owned;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis.Fence;
using StackExchange.Redis;
using static Int.Owned.OwnedDatabase;

namespace Int.Redis;

public sealed class FenceRedisTokensTests : IClassFixture<RedisFixture>, IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LongTtl = TimeSpan.FromMinutes(5);

    private readonly RedisFixture redis;
    private readonly PostgresFixture postgres;
    private readonly string prefix = $"k{Guid.NewGuid():N}:";
    private ConnectionMultiplexer direct = null!;

    public FenceRedisTokensTests(RedisFixture redis, PostgresFixture postgres)
    {
        this.redis = redis;
        this.postgres = postgres;
    }

    private IDatabase Db => direct.GetDatabase();

    public async Task InitializeAsync() => direct = await redis.ConnectAsync();

    public async Task DisposeAsync() => await direct.DisposeAsync();

    [Fact]
    public async Task A_fenced_Redis_write_delayed_across_a_freeing_a_recreation_and_a_new_ownership_is_refused()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var staleToken = await db.CreateOwnedAsync("unit", "k", "a");
        var points = new PointHold(OwnedFaultPoints.RedisBeforeExec.Name);
        var staleOwner = new FencedOwner("unit", "k", staleToken, db.Timeline);
        var stale = new TenureFence(Db, staleOwner, points, prefix, LongTtl);
        RedisKey state = stale.HashTag + ":position";
        Assert.Equal(RaiseOutcome.Installed, await stale.RaiseAsync(CancellationToken.None));

        points.Arm();
        var write = stale.WriteAsync(t => _ = t.StringSetAsync(state, "stale"), CancellationToken.None);
        await points.Reached.WaitAsync(Wait);

        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k'");
            await ExecAsync(connection, "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k'");
        }

        var currentToken = await db.CreateOwnedAsync("unit", "k", "a");
        Assert.True(currentToken > staleToken, $"{currentToken} > {staleToken}");
        var current = new TenureFence(Db, new FencedOwner("unit", "k", currentToken, db.Timeline), NoFaultPoints.Instance, prefix, LongTtl);
        Assert.Equal(RaiseOutcome.Installed, await current.RaiseAsync(CancellationToken.None));
        points.Release();

        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => write.WaitAsync(Wait));
        Assert.False(await Db.KeyExistsAsync(state));
        Assert.Equal(currentToken, (long)await Db.StringGetAsync(stale.HighWaterKey));

        await using var pg = await db.OpenAsync();
        await Assert.ThrowsAsync<OwnershipLostException>(() => staleOwner.BeginFencedTransactionAsync(pg));
    }
}
