using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class HubFenceSinkCheckTests
{
    private sealed class ProbeHub : Hub;

    private sealed class FenceSink : IFenceSink
    {
        public ValueTask AbortConnectionsAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task A_mapped_hub_with_no_fence_sink_fails_start()
    {
        await using var app = Build(mapHub: true, new NoFenceSink());
        var exception = await Assert.ThrowsAsync<ClusterConfigurationException>(() => app.StartAsync());
        Assert.Equal(ClusterConfigurationReason.HubFilterWithoutFenceSink, exception.Reason);
    }

    [Fact]
    public async Task A_rest_only_host_with_no_fence_sink_starts()
    {
        await using var app = Build(mapHub: false, new NoFenceSink());
        await app.StartAsync();
    }

    [Fact]
    public async Task A_mapped_hub_with_a_fence_sink_starts()
    {
        await using var app = Build(mapHub: true, new FenceSink());
        await app.StartAsync();
    }


    [Fact]
    public async Task A_generic_host_with_a_mapped_hub_and_no_fence_sink_fails_start()
    {
        using var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.ConfigureServices(services =>
                {
                    services.AddReeveClusterAspNetCore();
                    services.AddSingleton<IFenceSink, NoFenceSink>();
                    services.AddSignalR();
                });
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapHub<ProbeHub>("/hub"));
                });
            })
            .Build();

        var exception = await Assert.ThrowsAsync<ClusterConfigurationException>(() => host.StartAsync());
        Assert.Equal(ClusterConfigurationReason.HubFilterWithoutFenceSink, exception.Reason);
    }

    private static WebApplication Build(bool mapHub, IFenceSink fenceSink)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddReeveClusterAspNetCore();
        builder.Services.AddReeveClusterAspNetCore();
        builder.Services.AddSingleton(fenceSink);
        builder.Services.AddSignalR();
        var app = builder.Build();
        Assert.Single(app.Services.GetServices<IHostedService>().OfType<HubFenceSinkCheck>());
        app.MapGet("/", () => "ok");
        if (mapHub)
        {
            app.MapHub<ProbeHub>("/hub");
        }

        return app;
    }
}
