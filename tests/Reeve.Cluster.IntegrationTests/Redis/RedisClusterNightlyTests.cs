using System.Net;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Int.Fixtures;
using Int.Owned;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Fence;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class RedisClusterNightlyTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    [NightlyFact]
    [Trait("Category", "Nightly")]
    public async Task On_a_redis_cluster_the_raise_the_tagged_write_and_the_delete_address_one_slot_and_an_untagged_write_is_refused()
    {
        await using var container = new ContainerBuilder(RedisFixture.Image)
            .WithPortBinding(6379, true)
            .WithCommand("redis-server", "--cluster-enabled", "yes")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("redis-cli", "ping"))
            .Build();
        await container.StartAsync();
        var endPoint = await AnnounceAsync(container);
        await using var redis = await ConnectionMultiplexer.ConnectAsync(endPoint);
        Assert.Equal(ServerType.Cluster, redis.GetServers().Single().ServerType);
        UnitKind[] fenced = [new("consumer", typeof(object), new UnitKindOptions { RedisFence = true })];
        await new RedisStartupCheck(redis, fenced, [], Options.Create(new ClusterRedisOptions()), NullLogger<RedisStartupCheck>.Instance).StartingAsync(CancellationToken.None);

        var fence = new TenureFence(redis.GetDatabase(), new FencedOwner("consumer", "k", 7, timeline: 1), NoFaultPoints.Instance, "reeve:", TimeSpan.FromMinutes(5));
        Assert.Equal(RaiseOutcome.Installed, await fence.RaiseAsync(CancellationToken.None));
        var state = fence.HashTag + ":position";
        Assert.True(await fence.WriteAsync(t => _ = t.StringSetAsync(state, "1"), CancellationToken.None));
        Assert.Equal("1", (string?)await redis.GetDatabase().StringGetAsync(state));

        await Assert.ThrowsAsync<RedisCommandException>(() => fence.WriteAsync(t => _ = t.StringSetAsync("position", "2"), CancellationToken.None));
        Assert.False(await redis.GetDatabase().KeyExistsAsync("position"));

        Assert.True(await fence.DeleteOwnAsync(CancellationToken.None));
        Assert.False(await redis.GetDatabase().KeyExistsAsync(fence.FenceKey));
        Assert.Equal(7, (long)await redis.GetDatabase().StringGetAsync(fence.HighWaterKey));
    }

    private static async Task<string> AnnounceAsync(IContainer container)
    {
        var ip = (await Dns.GetHostAddressesAsync(container.Hostname)).First(a => a.AddressFamily == AddressFamily.InterNetwork).ToString();
        var port = container.GetMappedPublicPort(6379).ToString(System.Globalization.CultureInfo.InvariantCulture);
        await CliAsync(container, "CONFIG", "SET", "cluster-announce-ip", ip);
        await CliAsync(container, "CONFIG", "SET", "cluster-announce-port", port);
        await CliAsync(container, "CLUSTER", "ADDSLOTSRANGE", "0", "16383");
        using var bound = new CancellationTokenSource(Bound);
        while (!(await CliAsync(container, "CLUSTER", "INFO")).Contains("cluster_state:ok", StringComparison.Ordinal))
        {
            await Task.Delay(100, bound.Token);
        }

        return ip + ":" + port;
    }

    private static async Task<string> CliAsync(IContainer container, params string[] command)
    {
        var result = await container.ExecAsync(["redis-cli", .. command]);
        Assert.True(result.ExitCode == 0 && !result.Stdout.StartsWith("ERR", StringComparison.Ordinal), $"redis-cli {string.Join(' ', command)}: {result.Stdout}{result.Stderr}");
        return result.Stdout;
    }
}
