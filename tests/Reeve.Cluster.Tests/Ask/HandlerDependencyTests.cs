using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Commands;

namespace Unit.Ask;

public sealed class HandlerDependencyTests
{
    private sealed class AskerHandler(IClusterAsk ask) : IAskHandler
    {
        public IClusterAsk Ask { get; } = ask;

        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class AskerOfCommandsHandler(IClusterCommands commands) : IAskHandler
    {
        public IClusterCommands Commands { get; } = commands;

        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class SenderHandler(IClusterCommands commands) : IDurableCommandHandler
    {
        public IClusterCommands Commands { get; } = commands;

        public Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class SenderOfAsksHandler(IClusterAsk ask) : IDurableCommandHandler
    {
        public IClusterAsk Ask { get; } = ask;

        public Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class ShimUnit : IOwnedUnit
    {
        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => Task.Delay(Timeout.Infinite, stop);

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }

    private sealed class Lifetime : IHostApplicationLifetime
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

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostApplicationLifetime, Lifetime>();
        services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddClusterUnit<ShimUnit>("scheduler");
        return services;
    }

    private static void BuildLikeTheHost(IServiceCollection services, ServiceProvider provider, params Type[] clients)
    {
        var startup = Assert.Single(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType?.Name == "AskStartup");
        Assert.NotNull(ActivatorUtilities.CreateInstance(provider, startup.ImplementationType!));
        Assert.NotNull(provider.GetRequiredService<CommandPickup>());
        foreach (var client in clients)
        {
            Assert.NotNull(provider.GetRequiredService(client));
        }
    }

    private static async Task ResolvesAsync(IServiceCollection services, ServiceProvider provider, params Type[] clients) =>
        await Task.Run(() => BuildLikeTheHost(services, provider, clients)).WaitAsync(TimeSpan.FromSeconds(30));

    [Fact]
    public async Task An_Ask_handler_that_takes_IClusterAsk_resolves()
    {
        var services = Services();
        services.AddClusterAskHandler<AskerHandler>("scheduler");
        await using var provider = services.BuildServiceProvider();

        await ResolvesAsync(services, provider, typeof(IClusterAsk), typeof(AskerHandler));

        Assert.Same(provider.GetRequiredService<IClusterAsk>(), provider.GetRequiredService<AskerHandler>().Ask);
    }

    [Fact]
    public async Task A_command_handler_that_takes_IClusterCommands_resolves()
    {
        var services = Services();
        services.AddClusterCommandHandler<SenderHandler>("scheduler", InDoubtPolicy.ReRun);
        await using var provider = services.BuildServiceProvider();

        await ResolvesAsync(services, provider, typeof(IClusterCommands), typeof(SenderHandler));

        Assert.Same(provider.GetRequiredService<IClusterCommands>(), provider.GetRequiredService<SenderHandler>().Commands);
    }

    [Fact]
    public async Task An_Ask_handler_that_takes_IClusterCommands_beside_a_command_handler_that_takes_IClusterAsk_resolves()
    {
        var services = Services();
        services.AddClusterAskHandler<AskerOfCommandsHandler>("scheduler");
        services.AddClusterCommandHandler<SenderOfAsksHandler>("scheduler", InDoubtPolicy.ReRun);
        await using var provider = services.BuildServiceProvider();

        await ResolvesAsync(services, provider, typeof(IClusterAsk), typeof(IClusterCommands), typeof(AskerOfCommandsHandler), typeof(SenderOfAsksHandler));

        Assert.Same(provider.GetRequiredService<IClusterCommands>(), provider.GetRequiredService<AskerOfCommandsHandler>().Commands);
        Assert.Same(provider.GetRequiredService<IClusterAsk>(), provider.GetRequiredService<SenderOfAsksHandler>().Ask);
    }
}
