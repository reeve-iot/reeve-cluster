using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Ask;

namespace Unit.Ask;

public sealed class AskRegistrationTests
{
    private sealed class Handler : IAskHandler
    {
        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) =>
            Task.FromResult<ReadOnlyMemory<byte>>(request.Payload);
    }

    private sealed class OtherHandler : IAskHandler
    {
        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) =>
            Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class CountedHandler : IAskHandler
    {
        public static int Constructed;

        public CountedHandler() => Interlocked.Increment(ref Constructed);

        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) =>
            Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private static void Required(ClusterOptions o)
    {
        o.ReplicaId = "replica-1";
        o.ConnectionString = "Host=db;Database=app";
        o.ReconcileTimeout = TimeSpan.FromSeconds(10);
    }

    private static ServiceCollection Services() => new();

    [Fact]
    public async Task The_registration_resolves_the_Ask_client_with_the_default_transport_and_its_handlers()
    {
        var services = Services().AddLogging();
        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddClusterAskHandler<Handler>("scheduler");
        services.AddClusterMemberAskHandler<OtherHandler>();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<ClusterAsk>(provider.GetRequiredService<IClusterAsk>());
        Assert.IsType<NoAskTransport>(provider.GetRequiredService<IAskTransport>());
        var handlers = provider.GetRequiredService<AskHandlers>();
        Assert.IsType<Handler>(handlers.For("scheduler"));
        Assert.IsType<OtherHandler>(handlers.Member);
        Assert.Null(handlers.For("other"));
    }

    [Fact]
    public async Task The_handlers_are_built_with_the_hosted_services_and_not_by_the_first_request()
    {
        var services = Services().AddLogging();
        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddClusterAskHandler<CountedHandler>("scheduler");
        var starter = Assert.Single(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType?.Name == "AskStartup");
        await using var provider = services.BuildServiceProvider();
        var before = Volatile.Read(ref CountedHandler.Constructed);

        _ = ActivatorUtilities.CreateInstance(provider, starter.ImplementationType!);

        Assert.Equal(before + 1, Volatile.Read(ref CountedHandler.Constructed));
    }

    [Fact]
    public void A_second_handler_for_one_kind_or_a_second_member_handler_is_refused()
    {
        var services = Services();
        services.AddClusterAskHandler<Handler>("scheduler");
        services.AddClusterMemberAskHandler<Handler>();

        Assert.Throws<ArgumentException>(() => services.AddClusterAskHandler<OtherHandler>("scheduler"));
        Assert.Throws<ArgumentException>(() => services.AddClusterMemberAskHandler<OtherHandler>());
    }

    [Fact]
    public async Task A_request_to_another_process_is_not_taken_while_no_transport_is_registered()
    {
        var result = await new NoAskTransport().DeliverAsync(new AskTarget("b", Guid.NewGuid()), new AskDelivery(null, null, null, default), TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.Equal(DeliveryKind.NotTaken, result.Kind);
    }
}
