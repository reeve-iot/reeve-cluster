using Harness.Slice;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;

namespace Harness.Failover;

internal static class FailoverApp
{
    public const string Widget = "widget";

    public const string CreateTables = SliceApp.CreateTable + "; CREATE TABLE IF NOT EXISTS harness_captured (id int PRIMARY KEY, label text NOT NULL)";

    public static IReadOnlyDictionary<string, string> StateOf(Replica replica) =>
        replica.Services.GetRequiredService<Reeve.Cluster.Log.GenerationStore>().Current!.Get<Dictionary<string, string>>(Widget);

    public static long GenerationIdOf(Replica replica) =>
        ((Reeve.Cluster.Log.Generation)replica.Services.GetRequiredService<Reeve.Cluster.Log.GenerationStore>().Current!).Id;

    public static long HeadOf(Replica replica) =>
        replica.Services.GetRequiredService<Reeve.Cluster.Log.GenerationStore>().Current!.Head;

    public static ReplicaSpec Spec(Action<ClusterOptions>? profile = null, Action<ClusterOptions>? configure = null, bool redis = true) => new()
    {
        Profile = profile,
        Configure = o =>
        {
            o.ActionTimeBudget = TimeSpan.FromSeconds(5);
            o.ProxyReadTimeout = TimeSpan.FromSeconds(120);
            configure?.Invoke(o);
        },
        Reconcilers = [typeof(FailoverWidget)],
        FenceSink = typeof(AspNetCoreFenceSink),
        Redis = redis,
        ConfigureServices = services =>
        {
            services.AddRouting();
            services.AddHealthChecks().AddReeveClusterReadiness();
            services.AddReeveClusterAspNetCore();
            HarnessCapture.Register(services);
        },
        ConfigureWebApp = Map,
    };

    private static void Map(WebApplication app)
    {
        app.UseReeveCluster();
        app.MapGet("/items/{id}", (string id, IClusterScopeAccessor scopes) =>
        {
            var generation = scopes.Current?.PinnedGeneration ?? throw new InvalidOperationException("The request pinned no generation.");
            return generation.Get<Dictionary<string, string>>(Widget).TryGetValue($"s:{id}", out var value)
                ? Results.Ok(new ItemView(id, value, generation.Head, null))
                : Results.NotFound();
        });
        app.MapPut("/items/{id}", async (string id, string value, Reeve.Cluster.IChangeLog log, IOptions<ClusterOptions> options, HttpContext http) =>
            Results.Ok(await SliceApp.WriteAsync(log, options.Value, Widget, id, value, http.RequestAborted))).AcknowledgeWrites();
        app.MapHealthChecks("/ready").AllowWhenFenced().SkipReadIndex();
    }
}

internal sealed class FailoverWidget : Reeve.Cluster.IReconciler
{
    public string AggregateType => FailoverApp.Widget;

    public async Task<Reeve.Cluster.ReconcileResult> ReconcileAsync(Reeve.Cluster.ReconcileContext context, CancellationToken cancellationToken)
    {
        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (prefix, sql) in new[]
        {
            ("s", "SELECT id, value FROM slice_item WHERE type = 'widget'"),
            ("c", "SELECT id::text, label FROM harness_captured"),
        })
        {
            await using var command = new NpgsqlCommand(sql, (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                state[$"{prefix}:{reader.GetString(0)}"] = reader.GetString(1);
            }
        }

        return new Reeve.Cluster.ReconcileResult(state);
    }
}
