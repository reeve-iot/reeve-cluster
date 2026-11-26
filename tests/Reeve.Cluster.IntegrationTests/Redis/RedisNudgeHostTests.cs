using Int.Owned;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Wakeup;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class RedisNudgeHostTests : IClassFixture<PostgresFixture>, IClassFixture<RedisFixture>, IAsyncLifetime
{
    private const string Kind = "consumer";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly RedisFixture redis;
    private readonly string prefix = $"n{Guid.NewGuid():N}:";
    private readonly ProbeUnit unit = new();
    private readonly List<IAsyncDisposable> owned = [];
    private OwnedDatabase db = null!;
    private IHost? host;

    public RedisNudgeHostTests(PostgresFixture postgres, RedisFixture redis)
    {
        this.postgres = postgres;
        this.redis = redis;
    }

    private OwnedStore Store => host!.Services.GetRequiredService<OwnedStore>();

    private UnitRuntime Runtime => host!.Services.GetRequiredService<UnitRuntime>();

    public async Task InitializeAsync() => db = await OwnedDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        if (host is not null)
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(bound.Token);
            host.Dispose();
        }

        foreach (var disposable in owned)
        {
            await disposable.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_nudge_from_another_replica_starts_an_assigned_unit_and_stops_one_whose_release_is_requested()
    {
        await StartHostAsync(await ConnectAsync(redis.ConnectionString), TimeSpan.FromMinutes(10));
        using var other = new RedisWakeupChannel(
            await ConnectAsync(redis.ConnectionString),
            Options.Create(new ClusterRedisOptions { KeyPrefix = prefix }),
            NullLogger<RedisWakeupChannel>.Instance);

        var token = (await Store.InsertAsync(Kind, "k", "r1", Epoch, CancellationToken.None))!.Value;
        await NudgeUntilAsync(other, () => unit.Acquired.Reader.TryPeek(out _));
        Assert.Equal(token, (await unit.Acquired.Reader.ReadAsync()).Token);

        Assert.True(await Store.RequestReleaseAsync(Kind, "k", token, CancellationToken.None));
        await NudgeUntilAsync(other, () => unit.LostReturned("k").IsCompleted);
    }

    [Fact]
    public async Task With_redis_cut_the_poll_alone_starts_an_assigned_unit_and_stops_one_whose_release_is_requested()
    {
        await StartHostAsync(await ConnectAsync("127.0.0.1:1"), TimeSpan.FromSeconds(1));

        var token = (await Store.InsertAsync(Kind, "k", "r1", Epoch, CancellationToken.None))!.Value;
        Assert.Equal(token, (await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);

        Assert.True(await Store.RequestReleaseAsync(Kind, "k", token, CancellationToken.None));
        await unit.LostReturned("k").WaitAsync(Wait);
    }

    private long Epoch => host!.Services.GetRequiredService<LocalPhaseGate>().Epoch!.Value;

    private async Task<IConnectionMultiplexer> ConnectAsync(string connectionString)
    {
        var options = ConfigurationOptions.Parse(connectionString);
        options.AbortOnConnectFail = false;
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        owned.Add(multiplexer);
        return multiplexer;
    }

    private async Task StartHostAsync(IConnectionMultiplexer multiplexer, TimeSpan placementInterval)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);
        builder.Services.AddSingleton(multiplexer);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            o.PlacementInterval = placementInterval;
        });
        builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = prefix);
        builder.Services.AddClusterKeyedResource<ProbeUnit>(Kind, o => o.Policy = AdoptionPolicy.None);
        host = builder.Build();
        await host.StartAsync().WaitAsync(Wait);

        using var bound = new CancellationTokenSource(Wait);
        while (!(Runtime.Holds(Placement.PlacerKind, Placement.PlacerKind) && Runtime.Holds(FreeRowSweep.Kind, FreeRowSweep.Kind)))
        {
            await Task.Delay(50, bound.Token);
        }
    }

    private static async Task NudgeUntilAsync(RedisWakeupChannel channel, Func<bool> reached)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (!reached())
        {
            await channel.PublishAsync(Wakeup.Nudge(WakeupKind.OwnerNudge, "r1"));
            await Task.Delay(200, bound.Token);
        }
    }
}
