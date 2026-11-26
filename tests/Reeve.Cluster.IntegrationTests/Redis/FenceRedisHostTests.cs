using System.Threading.Channels;
using Int.Owned;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis.Fence;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class FenceRedisHostTests : IClassFixture<PostgresFixture>, IClassFixture<RedisFixture>, IAsyncLifetime
{
    private const string Kind = "consumer";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly RedisFixture redis;
    private readonly string prefix = $"h{Guid.NewGuid():N}:";
    private readonly FencedUnit unit = new();
    private OwnedDatabase db = null!;
    private ConnectionMultiplexer multiplexer = null!;
    private IHost host = null!;

    public FenceRedisHostTests(PostgresFixture postgres, RedisFixture redis)
    {
        this.postgres = postgres;
        this.redis = redis;
    }

    private OwnedStore Store => host.Services.GetRequiredService<OwnedStore>();

    private LocalPhaseGate Gate => host.Services.GetRequiredService<LocalPhaseGate>();

    private IDatabase Db => multiplexer.GetDatabase();

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        multiplexer = await redis.ConnectAsync();
        unit.Redis = multiplexer;
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);
        builder.Services.AddSingleton<IConnectionMultiplexer>(multiplexer);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = prefix);
        builder.Services.AddClusterKeyedResource<FencedUnit>(Kind, o =>
        {
            o.Policy = AdoptionPolicy.None;
            o.RedisFence = true;
        });
        host = builder.Build();
        await host.StartAsync().WaitAsync(Wait);
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var changed = Gate.NextPhaseChange;
            if (Gate.Phase == ReplicaPhase.Serving)
            {
                break;
            }

            await changed.WaitAsync(bound.Token);
        }
    }

    public async Task DisposeAsync()
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(bound.Token);
        host.Dispose();
        await multiplexer.DisposeAsync();
    }

    private RedisKey FenceKey(string key) => TenureFence.HashTagOf(prefix, Kind, key) + ":fence";

    private RedisKey HighWaterKey(string key) => TenureFence.HashTagOf(prefix, Kind, key) + ":hw";

    private RedisKey State(string key) => TenureFence.HashTagOf(prefix, Kind, key) + ":position";

    private async Task<long> AssignAsync(string key)
    {
        var epoch = Gate.Epoch!.Value;
        var read = await Store.ReadAsync(Kind, key, CancellationToken.None);
        var token = read.Row is { } row
            ? await Store.TakeAsync(Kind, key, row, "r1", epoch, CancellationToken.None)
            : await Store.InsertAsync(Kind, key, "r1", epoch, CancellationToken.None);
        return token ?? throw new InvalidOperationException($"{Kind}/{key} could not be assigned");
    }

    private async Task<FencedUnit.Start> StartedAsync() => await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

    private async Task AwaitFenceAsync(string key, long token)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (await Db.StringGetAsync(FenceKey(key)) is var value && (value.IsNull || (long)value != token))
        {
            await Task.Delay(50, bound.Token);
        }
    }

    private static async Task AwaitRaisedAsync(OwnedContext context)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (RedisTenureFences.Of(context) is not { Raised: true })
        {
            await Task.Delay(20, bound.Token);
        }
    }

    private async Task AwaitRenewalsAsync(int count)
    {
        var seen = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = Gate.OnRenewed(_ =>
        {
            if (Interlocked.Increment(ref seen) >= count)
            {
                done.TrySetResult();
            }
        });
        await done.Task.WaitAsync(Wait);
    }

    [Fact]
    public async Task The_install_raise_lands_before_OnAcquiredAsync_and_FenceRedis_writes_under_the_tenure()
    {
        var token = await AssignAsync("a");

        var started = await StartedAsync();

        Assert.Equal(token, started.Context.Token);
        Assert.Equal(token, started.FenceAtStart);
        Assert.Equal(token, (long)await Db.StringGetAsync(HighWaterKey("a")));
        Assert.Equal(TenureFence.HashTagOf(prefix, Kind, "a"), started.Context.RedisHashTag());
        Assert.True(await started.Context.FenceRedisAsync(t => _ = t.StringSetAsync(State("a"), "p1")));
        Assert.Equal("p1", (string?)await Db.StringGetAsync(State("a")));
    }

    [Fact]
    public async Task A_renewal_refreshes_the_fence_and_installs_a_lost_fence_key_again()
    {
        var token = await AssignAsync("a");
        var started = await StartedAsync();

        await Db.KeyDeleteAsync(FenceKey("a"));

        await AwaitFenceAsync("a", token);
        await AwaitRaisedAsync(started.Context);
        Assert.True(await started.Context.FenceRedisAsync(t => _ = t.StringSetAsync(State("a"), "back")));
    }

    [Fact]
    public async Task A_stopped_tenure_is_refreshed_no_more()
    {
        var tokenA = await AssignAsync("a");
        var a = await StartedAsync();
        var tokenB = await AssignAsync("b");
        await StartedAsync();
        Assert.True(await Store.RequestReleaseAsync(Kind, "a", tokenA, CancellationToken.None));
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (a.Stop.Register(() => stopped.TrySetResult()))
        {
            await stopped.Task.WaitAsync(Wait);
        }

        await AwaitRenewalsAsync(1);
        await Db.KeyDeleteAsync(FenceKey("b"));
        await AwaitFenceAsync("b", tokenB);
        await Db.KeyDeleteAsync([FenceKey("a"), FenceKey("b")]);

        await AwaitFenceAsync("b", tokenB);
        await Db.KeyDeleteAsync(FenceKey("b"));
        await AwaitRenewalsAsync(1);
        await AwaitFenceAsync("b", tokenB);

        Assert.False(await Db.KeyExistsAsync(FenceKey("a")));
        await Assert.ThrowsAsync<OwnershipLostException>(() => a.Context.FenceRedisAsync(t => _ = t.StringSetAsync(State("a"), "stopped")));
    }

    [Fact]
    public async Task A_new_owners_FenceRedis_refuses_while_a_higher_token_is_recorded_its_fenced_PostgreSQL_write_proceeds_and_the_refresh_installs_once_the_keys_are_cleared()
    {
        await Db.StringSetAsync(HighWaterKey("a"), long.MaxValue);
        var token = await AssignAsync("a");
        var started = await StartedAsync();

        Assert.Null(started.FenceAtStart);
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() => started.Context.FenceRedisAsync(t => _ = t.StringSetAsync(State("a"), "refused")));
        Assert.False(await Db.KeyExistsAsync(State("a")));

        await using (var connection = new NpgsqlConnection(db.ConnectionString))
        {
            await connection.OpenAsync();
            await using var fenced = await started.Context.BeginFencedTransactionAsync(connection);
            await fenced.CommitAsync();
        }

        await Db.KeyDeleteAsync([HighWaterKey("a"), FenceKey("a")]);
        await AwaitFenceAsync("a", token);
        await AwaitRaisedAsync(started.Context);
        Assert.True(await started.Context.FenceRedisAsync(t => _ = t.StringSetAsync(State("a"), "installed")));
    }

    internal sealed class FencedUnit : IOwnedUnit
    {
        public IConnectionMultiplexer Redis { get; set; } = null!;

        public Channel<Start> Acquired { get; } = Channel.CreateUnbounded<Start>();

        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            var fence = await Redis.GetDatabase().StringGetAsync(context.RedisHashTag() + ":fence");
            Acquired.Writer.TryWrite(new Start(context, stop, fence.IsNull ? null : (long)fence));
            await Task.Delay(Timeout.Infinite, stop);
        }

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

        internal sealed record Start(OwnedContext Context, CancellationToken Stop, long? FenceAtStart);
    }
}
