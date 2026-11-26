using Microsoft.Extensions.Hosting;

namespace Reeve.Cluster.Redis.Relay;

internal sealed class AskRelayStartup(AskRelay relay) : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        relay.Start();
        return Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken) => await relay.DisposeAsync().ConfigureAwait(false);

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
