using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;
using Unit.Ask;

namespace Unit.Redis.Relay;

public sealed class RelayRegistrationTests
{
    private sealed class Handler : IAskHandler
    {
        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class AskingHandler(IClusterAsk ask) : IAskHandler
    {
        public IClusterAsk Ask { get; } = ask;

        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class StubLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }

    private static void Required(ClusterOptions o)
    {
        o.ReplicaId = "replica-1";
        o.ConnectionString = "Host=db;Database=app";
        o.ReconcileTimeout = TimeSpan.FromSeconds(10);
    }

    private static ConnectionMultiplexer Offline()
    {
        var options = new ConfigurationOptions { AbortOnConnectFail = false, ConnectTimeout = 100, ConnectRetry = 1 };
        options.EndPoints.Add("127.0.0.1:1");
        return ConnectionMultiplexer.Connect(options);
    }

    private static ServiceCollection Build(bool redisFirst, IConnectionMultiplexer redis)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(redis);
        services.AddSingleton<IAskWire>(new FakeWire());
        if (redisFirst)
        {
            services.AddReeveClusterRedis();
        }

        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddClusterMemberAskHandler<Handler>();
        if (!redisFirst)
        {
            services.AddReeveClusterRedis();
        }

        return services;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_Redis_package_makes_the_relay_the_transport_whichever_package_is_added_first(bool redisFirst)
    {
        await using var redis = Offline();
        await using var provider = Build(redisFirst, redis).BuildServiceProvider();

        Assert.IsType<AskRelay>(provider.GetRequiredService<IAskTransport>());
        Assert.Same(provider.GetRequiredService<AskRelay>(), provider.GetRequiredService<IAskTransport>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void The_relay_is_started_by_a_hosted_service_that_runs_before_the_membership_driver(bool redisFirst)
    {
        using var redis = Offline();
        var services = Build(redisFirst, redis);

        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).Select(d => d.ImplementationType?.Name).ToList();
        var driver = hosted.FindIndex(n => n == "MembershipDriver");

        Assert.Contains("AskRelayStartup", hosted);
        Assert.True(driver < 0 || hosted.IndexOf("AskRelayStartup") < driver, string.Join(", ", hosted));
        Assert.Single(hosted, n => n == "AskRelayStartup");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_relay_is_registered_as_a_status_section_source_whichever_package_is_added_first(bool redisFirst)
    {
        await using var redis = Offline();
        var services = Build(redisFirst, redis);
        services.AddSingleton<IHostApplicationLifetime, StubLifetime>();
        await using var provider = services.BuildServiceProvider();

        var names = provider.GetServices<Reeve.Cluster.IStatusSectionSource>().Select(s => s.Name).ToList();

        Assert.Single(names, n => n == AskRelayStatusSection.Name);
        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void Adding_the_Redis_package_twice_registers_the_relay_once()
    {
        using var redis = Offline();
        var services = Build(true, redis);
        services.AddReeveClusterRedis();

        Assert.Single(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(AskRelayStartup));
        Assert.Single(services, d => d.ServiceType == typeof(AskRelay));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task With_Ask_off_the_Redis_package_registers_no_relay_whichever_package_is_added_first(bool redisFirst)
    {
        await using var redis = Offline();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConnectionMultiplexer>(redis);
        services.AddSingleton<IHostApplicationLifetime, StubLifetime>();
        if (redisFirst)
        {
            services.AddReeveClusterRedis();
        }

        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), ClusterAreas.OwnedWork | ClusterAreas.Commands, Required);
        services.AddReeveClusterRedis();

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(AskRelay) || d.ServiceType == typeof(IAskTransport) || d.ServiceType == typeof(IAskWire));
        Assert.DoesNotContain(services, d => d.ImplementationType == typeof(AskRelayStartup) || d.ImplementationType == typeof(AskRelayStatusSection));
        await using var provider = services.BuildServiceProvider();
        Assert.DoesNotContain(provider.GetServices<Reeve.Cluster.IStatusSectionSource>(), s => s.Name == AskRelayStatusSection.Name);
        Assert.NotEmpty(provider.GetServices<IHostedService>());
    }

    [Theory]
    [InlineData(true, ClusterAreas.All)]
    [InlineData(false, ClusterAreas.All)]
    [InlineData(true, ClusterAreas.OwnedWork)]
    [InlineData(false, ClusterAreas.OwnedWork)]
    public async Task A_keyed_registration_of_the_application_does_not_break_the_relay_registration(bool redisFirst, ClusterAreas areas)
    {
        await using var redis = Offline();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConnectionMultiplexer>(redis);
        services.AddKeyedSingleton<object>("k", new object());
        if (redisFirst)
        {
            services.AddReeveClusterRedis();
        }

        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), areas, Required);
        services.AddKeyedSingleton<object>("j", new object());
        services.AddReeveClusterRedis();

        Assert.Equal(areas.HasFlag(ClusterAreas.Ask), services.Any(d => d.ServiceType == typeof(AskRelay)));
        Assert.Equal(2, services.Count(d => d.IsKeyedService));
    }

    [Fact]
    public async Task A_handler_that_takes_IClusterAsk_resolves_with_the_relay_as_the_transport()
    {
        using var redis = Offline();
        var services = Build(true, redis);
        services.AddClusterAskHandler<AskingHandler>("scheduler");
        await using var provider = services.BuildServiceProvider();
        var startup = Assert.Single(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType?.Name == "AskStartup");

        await Task.Run(() =>
        {
            _ = provider.GetRequiredService<AskRelay>();
            _ = ActivatorUtilities.CreateInstance(provider, startup.ImplementationType!);
        }).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.IsType<AskRelay>(provider.GetRequiredService<IAskTransport>());
        Assert.Same(provider.GetRequiredService<IClusterAsk>(), provider.GetRequiredService<AskingHandler>().Ask);
    }
}
