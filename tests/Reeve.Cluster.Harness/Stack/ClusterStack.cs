using System.Diagnostics;
using System.Net.Http;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;

namespace Reeve.Cluster.Harness.Stack;

internal sealed record ClusterStackOptions
{
    public bool Standby { get; init; }
}

internal sealed class ClusterStack : IAsyncLifetime
{
    public const string ToxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0";

    private const string PostgresAlias = "reeve-pg";
    private const string ToxiproxyAlias = "reeve-toxiproxy";
    private const string RedisAlias = "reeve-redis";
    private const int AdminPort = 8474;
    private const int FirstProxyPort = 18000;
    private const int MaxProxies = 96;

    private readonly object gate = new();
    private readonly SortedSet<int> freePorts = [.. Enumerable.Range(FirstProxyPort, MaxProxies)];
    private readonly Dictionary<string, int> portOfProxy = new(StringComparer.Ordinal);
    private readonly Action<string> log;
    private readonly ClusterStackOptions options;
    private readonly List<FaultProxy> postgresProxies = [];
    private INetwork? network;
    private PostgreSqlContainer? postgres;
    private PostgresPair? pair;
    private FaultProxy? replication;
    private RedisContainer? redis;
    private IContainer? toxiproxy;
    private HttpClient? admin;
    private ToxiproxyClient? client;
    private FaultProxy? observerPostgres;
    private FaultProxy? observerRedis;
    private NpgsqlConnectionStringBuilder? postgresTemplate;

    public ClusterStack(Action<string>? log = null, ClusterStackOptions? options = null)
    {
        this.log = log ?? (_ => { });
        this.options = options ?? new ClusterStackOptions();
    }

    public PostgresPair Pair => pair ?? throw new InvalidOperationException("This stack has a single PostgreSQL: build it with ClusterStackOptions.Standby.");

    public FaultProxy ReplicationLink => replication ?? throw new InvalidOperationException("This stack has no standby.");

    private string PostgresUpstream => pair?.Primary.Upstream ?? $"{PostgresAlias}:5432";

    public DatabaseFaults Database { get; private set; } = null!;

    public string ObserverPostgresConnectionString { get; private set; } = null!;

    public string ObserverRedisConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        network = new NetworkBuilder().Build();
        await network.CreateAsync().ConfigureAwait(false);

        if (!options.Standby)
        {
            postgres = new PostgreSqlBuilder(PostgresFixture.Image)
                .WithNetwork(network)
                .WithNetworkAliases(PostgresAlias)
                .Build();
        }

        redis = new RedisBuilder(RedisFixture.Image)
            .WithNetwork(network)
            .WithNetworkAliases(RedisAlias)
            .Build();

        var proxyPorts = Enumerable.Range(FirstProxyPort, MaxProxies).ToList();
        var builder = new ContainerBuilder(ToxiproxyImage)
            .WithNetwork(network)
            .WithNetworkAliases(ToxiproxyAlias)
            .WithPortBinding(AdminPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPath("/version").ForPort(AdminPort)));
        foreach (var port in proxyPorts)
        {
            builder = builder.WithPortBinding(port, true);
        }

        toxiproxy = builder.Build();

        var primary = options.Standby
            ? Task.Run(async () => pair = await PostgresPair.StartPrimaryAsync(network, PostgresFixture.Image, this.log).ConfigureAwait(false))
            : postgres!.StartAsync();
        await Task.WhenAll(primary, redis.StartAsync(), toxiproxy.StartAsync()).ConfigureAwait(false);

        admin = new HttpClient { BaseAddress = new Uri($"http://{toxiproxy.Hostname}:{toxiproxy.GetMappedPublicPort(AdminPort)}") };
        client = new ToxiproxyClient(admin, this.log);
        postgresTemplate = postgres is not null
            ? new NpgsqlConnectionStringBuilder(postgres.GetConnectionString())
            : new NpgsqlConnectionStringBuilder { Username = "postgres", Password = "postgres", Database = "postgres", Host = "unused", Port = 5432 };
        Database = new DatabaseFaults(() => postgres ?? pair!.Primary.Container, log);

        if (pair is not null)
        {
            replication = await CreateProxyAsync("replication", PostgresUpstream).ConfigureAwait(false);
            await pair.AddStandbyAsync($"{ToxiproxyAlias}:{PortOf(replication)}").ConfigureAwait(false);
        }

        observerPostgres = await CreatePostgresProxyAsync("observer-postgres").ConfigureAwait(false);
        observerRedis = await CreateProxyAsync("observer-redis", $"{RedisAlias}:6379").ConfigureAwait(false);
        ObserverPostgresConnectionString = new NpgsqlConnectionStringBuilder(PostgresConnectionString(observerPostgres)) { Pooling = false }.ConnectionString;
        ObserverRedisConnectionString = $"{observerRedis.Host}:{observerRedis.Port}";
    }

    public async Task DisposeAsync()
    {
        admin?.Dispose();
        foreach (var container in new IAsyncDisposable?[] { toxiproxy, redis, postgres, pair })
        {
            if (container is not null)
            {
                await container.DisposeAsync().ConfigureAwait(false);
            }
        }

        if (network is not null)
        {
            await network.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async Task<ReplicaLink> CreateLinkAsync(string name, RunDatabase? database = null)
    {
        var pg = await CreatePostgresProxyAsync($"{name}-postgres").ConfigureAwait(false);
        FaultProxy rd;
        try
        {
            rd = await CreateProxyAsync($"{name}-redis", $"{RedisAlias}:6379").ConfigureAwait(false);
        }
        catch
        {
            await pg.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return new ReplicaLink(name, pg, rd, PostgresConnectionString(pg, database?.Name), $"{rd.Host}:{rd.Port}");
    }

    public async Task<RunDatabase> CreateRunDatabaseAsync(CancellationToken cancellationToken = default)
    {
        var name = "run_" + Guid.NewGuid().ToString("N");
        await using (var admin = await OpenObserverConnectionAsync(cancellationToken).ConfigureAwait(false))
        {
            await using var create = new NpgsqlCommand($"CREATE DATABASE {name}", admin);
            await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var observer = new NpgsqlConnectionStringBuilder(ObserverPostgresConnectionString) { Database = name, Pooling = false }.ConnectionString;
        return new RunDatabase(name, observer, DropAsync);
    }

    private async Task DropAsync(string name)
    {
        await using var admin = await OpenObserverConnectionAsync().ConfigureAwait(false);
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {name} WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task<NpgsqlConnection> OpenObserverConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new NpgsqlConnection(ObserverPostgresConnectionString);
        await StallProbe.Shared.OpenNotingStallAsync(connection, "observer connection open", cancellationToken).ConfigureAwait(false);
        return connection;
    }

    public async Task<TimeSpan> FailOverAsync(CancellationToken cancellationToken = default)
    {
        var p = Pair;
        await CutPostgresAsync(cancellationToken).ConfigureAwait(false);
        await p.CrashPrimaryAsync(cancellationToken).ConfigureAwait(false);
        await p.PromoteAsync(cancellationToken).ConfigureAwait(false);
        return await RepointAndRestoreAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<TimeSpan> RepointAndRestoreAsync(CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        await RepointPostgresAsync(cancellationToken).ConfigureAwait(false);
        await RestorePostgresAsync(cancellationToken).ConfigureAwait(false);
        log($"database: repoint and restore took {clock.Elapsed.TotalSeconds:F1} s");
        return clock.Elapsed;
    }

    public async Task CutPostgresAsync(CancellationToken cancellationToken = default)
    {
        foreach (var proxy in LivePostgresProxies())
        {
            await proxy.CutAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task RestorePostgresAsync(CancellationToken cancellationToken = default)
    {
        foreach (var proxy in LivePostgresProxies().Where(p => p.IsCut))
        {
            await proxy.RestoreAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private FaultProxy[] LivePostgresProxies()
    {
        lock (gate)
        {
            return [.. postgresProxies.Where(p => !p.IsDisposed)];
        }
    }

    public async Task<TimeSpan> SwitchOverAsync(CancellationToken cancellationToken = default)
    {
        var took = TimeSpan.Zero;
        await CutPostgresAsync(cancellationToken).ConfigureAwait(false);
        await Pair.SwitchoverAsync(async token => took = await RepointAndRestoreAsync(token).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
        return took;
    }

    public async Task RepointPostgresAsync(CancellationToken cancellationToken = default)
    {
        var upstream = PostgresUpstream;
        foreach (var proxy in LivePostgresProxies())
        {
            await proxy.RepointAsync(upstream, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<FaultProxy> CreatePostgresProxyAsync(string name)
    {
        var proxy = await CreateProxyAsync(name, PostgresUpstream).ConfigureAwait(false);
        lock (gate)
        {
            postgresProxies.Add(proxy);
        }

        return proxy;
    }

    private int PortOf(FaultProxy proxy)
    {
        lock (gate)
        {
            return portOfProxy[proxy.Name];
        }
    }

    public async Task<FaultProxy> CreateProxyAsync(string name, string upstream)
    {
        int port;
        lock (gate)
        {
            if (freePorts.Count == 0)
            {
                throw new InvalidOperationException($"The stack publishes {MaxProxies} proxy ports and all are in use.");
            }

            port = freePorts.Min;
            freePorts.Remove(port);
            portOfProxy[name] = port;
        }

        try
        {
            await client!.CreateProxyAsync(name, $"0.0.0.0:{port}", upstream).ConfigureAwait(false);
        }
        catch
        {
            Free(name);
            throw;
        }

        return new FaultProxy(client!, name, toxiproxy!.Hostname, toxiproxy.GetMappedPublicPort(port), upstream, Free);
    }

    private string PostgresConnectionString(FaultProxy proxy, string? database = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(postgresTemplate!.ConnectionString)
        {
            Host = proxy.Host,
            Port = proxy.Port,
        };
        if (database is not null)
        {
            builder.Database = database;
        }

        return builder.ConnectionString;
    }

    private void Free(string name)
    {
        lock (gate)
        {
            if (portOfProxy.Remove(name, out var port))
            {
                freePorts.Add(port);
            }
        }
    }
}

internal sealed class RunDatabase : IAsyncDisposable
{
    private readonly Func<string, Task> drop;
    private int dropped;

    internal RunDatabase(string name, string observerConnectionString, Func<string, Task> drop)
    {
        Name = name;
        ObserverConnectionString = observerConnectionString;
        this.drop = drop;
    }

    public string Name { get; }

    public string ObserverConnectionString { get; }

    public async Task<NpgsqlConnection> OpenObserverConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new NpgsqlConnection(ObserverConnectionString);
        await StallProbe.Shared.OpenNotingStallAsync(connection, "observer connection open", cancellationToken).ConfigureAwait(false);
        return connection;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref dropped, 1) == 0)
        {
            await drop(Name).ConfigureAwait(false);
        }
    }
}
