using Harness.Slice;
using Harness.Stall;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Membership;

namespace Harness.Replay.Membership;

internal static class ReplayApp
{
    public const string Widget = StallRig.Widget;

    public static ReplicaSpec Spec(Action<ClusterOptions>? configure = null, double clockRate = 1.0, int protocol = MemberIdentity.CurrentProtocol) => new()
    {
        Configure = o =>
        {
            o.ActionTimeBudget = TimeSpan.FromSeconds(5);
            o.ProxyReadTimeout = TimeSpan.FromSeconds(120);
            configure?.Invoke(o);
        },
        Reconcilers = [typeof(ScriptedWidget)],
        FenceSink = typeof(AspNetCoreFenceSink),
        Redis = true,
        ClockRate = clockRate,
        ConfigureServices = services =>
        {
            if (protocol != MemberIdentity.CurrentProtocol)
            {
                services.RemoveAll<MemberIdentity>();
                services.AddSingleton(MemberIdentity.ForThisProcess() with { Protocol = protocol });
            }

            services.AddRouting();
            services.AddHealthChecks().AddReeveClusterReadiness();
            services.AddReeveClusterAspNetCore();
        },
        ConfigureWebApp = Map,
    };

    private static void Map(WebApplication app)
    {
        app.UseReeveCluster();

        app.MapGet("/ping", (IClusterScopeAccessor scopes) => Results.Ok(new { head = scopes.Current?.PinnedGeneration?.Head }));
        app.MapPut("/items/{id}", async (string id, string value, Reeve.Cluster.IChangeLog log, IOptions<ClusterOptions> options, HttpContext http) =>
            Results.Ok(await SliceApp.WriteAsync(log, options.Value, Widget, id, value, http.RequestAborted))).AcknowledgeWrites();
        app.MapReeveClusterSkips("/skips").AllowAnonymous();
        app.MapHealthChecks("/ready").AllowWhenFenced().SkipReadIndex();
    }
}
