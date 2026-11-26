using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Reeve.Cluster.AspNetCore;

internal sealed class HubFenceSinkCheck(IServiceProvider services, IEnumerable<EndpointDataSource> endpointDataSources) : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken)
    {
        var hasHub = endpointDataSources
            .SelectMany(source => source.Endpoints)
            .Any(endpoint => endpoint.Metadata.GetMetadata<HubMetadata>() is not null);
        if (hasHub && services.GetService<IFenceSink>() is NoFenceSink)
        {
            throw new ClusterConfigurationException(
                ClusterConfigurationReason.HubFilterWithoutFenceSink,
                "A SignalR hub is mapped with ClusterHubFilter, but IFenceSink is NoFenceSink; register a fence sink for connections that stay open past a request.");
        }

        return Task.CompletedTask;
    }

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
