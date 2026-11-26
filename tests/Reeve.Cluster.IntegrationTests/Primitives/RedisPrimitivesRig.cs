using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using Int.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using Reeve.Cluster.Slots;
using StackExchange.Redis;

namespace Int.Primitives;

internal sealed class CutProxy : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly CancellationTokenSource stopping = new();
    private readonly object gate = new();
    private readonly List<TcpClient> open = [];
    private bool cut;

    public CutProxy(string target)
    {
        var endpoint = target.Split(',')[0];
        var colon = endpoint.LastIndexOf(':');
        targetHost = endpoint[..colon];
        targetPort = int.Parse(endpoint[(colon + 1)..], System.Globalization.CultureInfo.InvariantCulture);
        listener.Start();
        _ = Task.Run(AcceptLoopAsync);
    }

    public string Endpoint => $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";

    public void Cut()
    {
        TcpClient[] dropped;
        lock (gate)
        {
            cut = true;
            dropped = [.. open];
            open.Clear();
        }

        foreach (var client in dropped)
        {
            client.Dispose();
        }
    }

    public void Restore()
    {
        lock (gate)
        {
            cut = false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        Cut();
        await stopping.CancelAsync();
        listener.Stop();
        stopping.Dispose();
    }

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stopping.Token);
                _ = Task.Run(() => ServeAsync(client));
            }
        }
        catch (Exception) when (stopping.IsCancellationRequested)
        {
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        var server = new TcpClient { NoDelay = true };
        try
        {
            lock (gate)
            {
                if (cut)
                {
                    client.Dispose();
                    return;
                }

                open.Add(client);
                open.Add(server);
            }

            client.NoDelay = true;
            await server.ConnectAsync(targetHost, targetPort, stopping.Token);
            var up = client.GetStream().CopyToAsync(server.GetStream(), stopping.Token);
            var down = server.GetStream().CopyToAsync(client.GetStream(), stopping.Token);
            await Task.WhenAny(up, down);
        }
        catch (Exception)
        {
        }
        finally
        {
            client.Dispose();
            server.Dispose();
        }
    }
}

[CollectionDefinition(Name)]
public sealed class RedisPolicyMetricCollection
{
    public const string Name = "redis-policy-metric";
}

internal sealed class RedisReplica : ISlotGate, IAsyncDisposable
{
    private readonly RedisPrimitivesRig rig;
    private readonly ConnectionMultiplexer connection;

    private RedisReplica(RedisPrimitivesRig rig, string id, ConnectionMultiplexer connection)
    {
        this.rig = rig;
        Id = id;
        this.connection = connection;
    }

    public string Id { get; }

    public volatile bool Serving = true;

    public static async Task<RedisReplica> ConnectAsync(RedisPrimitivesRig rig, string id)
    {
        var options = ConfigurationOptions.Parse(rig.Endpoint);
        options.AbortOnConnectFail = false;
        options.AsyncTimeout = 2000;
        options.SyncTimeout = 2000;
        options.ConnectTimeout = 2000;
        options.ConnectRetry = 1;
        return new RedisReplica(rig, id, await ConnectionMultiplexer.ConnectAsync(options));
    }

    public volatile bool Holder = true;

    public ClaimSeat? CheckServing() => Serving ? new ClaimSeat(Id, 1, 1) : null;

    public ClaimSeat? CheckHolder(long grantedEpoch) => Holder && grantedEpoch == 1 ? new ClaimSeat(Id, 1, 1) : null;

    public void OnOtherTimeline(long epoch)
    {
    }

    public IDatabase Database => connection.GetDatabase();

    public RedisClaims Claims(OnRedisUnavailable policy) => new(new RedisClaimStore(Database, rig.Prefix), this, policy);

    public RedisSingleFlight Flight(OnRedisUnavailable policy) => new(new RedisClaimStore(Database, rig.Prefix), this, policy, TimeProvider.System);

    public RedisSlots Slots(OnRedisUnavailable policy, IRedisSlotRows? rows = null) =>
        new(
            rows ?? new RedisSlotStore(Database, rig.Prefix), this, new SlotRegistry(TimeProvider.System), policy,
            Options.Create(new ClusterOptions { SafetyMargin = TimeSpan.FromMilliseconds(100) }), TimeProvider.System, NullLogger<RedisSlots>.Instance,
            NoSlotObserver.Instance);

    public IRedisSlotRows SlotStore() => new RedisSlotStore(Database, rig.Prefix);

    public RedisBudget Budget(OnRedisUnavailable policy) => new(new RedisBudgetStore(Database, rig.Prefix), this, policy, Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisBudget>.Instance);

    public RedisCounter Counter() => new(new RedisCounterStore(Database, rig.Prefix));

    public async ValueTask DisposeAsync() => await connection.DisposeAsync();
}

internal sealed class RedisPrimitivesRig : IAsyncDisposable
{
    private readonly ConnectionMultiplexer direct;
    private readonly List<RedisReplica> replicas = [];

    private RedisPrimitivesRig(ConnectionMultiplexer direct, MultiHoldProxy hold, CutProxy cut)
    {
        this.direct = direct;
        Hold = hold;
        CutOff = cut;
    }

    public string Prefix { get; } = $"p{Guid.NewGuid():N}:";

    public string Endpoint => CutOff.Endpoint;

    public IDatabase Direct => direct.GetDatabase();

    public MultiHoldProxy Hold { get; }

    public CutProxy CutOff { get; }

    public static async Task<RedisPrimitivesRig> CreateAsync(RedisFixture redis)
    {
        var hold = new MultiHoldProxy(redis.ConnectionString);
        var cut = new CutProxy(hold.Endpoint);
        return new RedisPrimitivesRig(await redis.ConnectAsync(), hold, cut);
    }

    public async Task<RedisReplica> AddReplicaAsync(string id)
    {
        var replica = await RedisReplica.ConnectAsync(this, id);
        replicas.Add(replica);
        return replica;
    }

    public string ClaimTokenKey(string key) => RedisPrimitiveKeys.ClaimHashTag(Prefix, key) + ":token";

    public string SlotHeldKey(string key) => RedisPrimitiveKeys.SlotHashTag(Prefix, key) + ":held";

    public string SlotIdKey(string key, string id) => RedisPrimitiveKeys.SlotHashTag(Prefix, key) + ":id:" + id;

    public string FlightTokenKey(string key) => RedisPrimitiveKeys.FlightHashTag(Prefix, key) + ":token";

    public string FlightResultKey(string key) => RedisPrimitiveKeys.FlightHashTag(Prefix, key) + ":result";

    public async ValueTask DisposeAsync()
    {
        foreach (var replica in replicas)
        {
            await replica.DisposeAsync();
        }

        await CutOff.DisposeAsync();
        await Hold.DisposeAsync();
        await direct.DisposeAsync();
    }
}

internal static class FlightAnswer
{
    public static void Is<T>(byte[] expected, SingleFlightResult actual)
        where T : SingleFlightResult
    {
        Assert.IsType<T>(actual);
        Assert.True(actual.TryGetValue(out var value));
        Assert.Equal(expected, value);
    }
}

internal sealed class PolicyUseListener : IDisposable
{
    private readonly MeterListener listener = new();
    private readonly object sync = new();
    private readonly Dictionary<(string Primitive, string Policy), long> uses = [];

    public PolicyUseListener()
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == RedisDiagnostics.PolicyUsesName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? primitive = null, policy = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "primitive")
                {
                    primitive = (string?)tag.Value;
                }
                else if (tag.Key == "policy")
                {
                    policy = (string?)tag.Value;
                }
            }

            lock (sync)
            {
                uses[(primitive!, policy!)] = uses.GetValueOrDefault((primitive!, policy!)) + value;
            }
        });
        listener.Start();
    }

    public long Count(string primitive, string policy)
    {
        lock (sync)
        {
            return uses.GetValueOrDefault((primitive, policy));
        }
    }

    public long Total()
    {
        lock (sync)
        {
            return uses.Values.Sum();
        }
    }

    public void Dispose() => listener.Dispose();
}
