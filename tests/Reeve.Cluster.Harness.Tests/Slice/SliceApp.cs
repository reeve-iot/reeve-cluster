using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Membership;

namespace Harness.Slice;

public sealed record ItemView(string Id, string? Value, long? Head, long? Seq);

public sealed class SliceCalls
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> calls = new();

    public IReadOnlyList<string> All => [.. calls];

    internal void Record(string call) => calls.Enqueue(call);
}

public sealed class SliceActivations
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> activations = new();

    public IReadOnlyList<string> All => [.. activations];

    internal void Record(string activation) => activations.Enqueue(activation);
}

internal abstract class SliceReconciler(string aggregateType, SliceActivations activations) : IReconciler
{
    public string AggregateType { get; } = aggregateType;

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var items = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT id, value FROM slice_item WHERE type = $1",
            (NpgsqlConnection)context.Connection,
            (NpgsqlTransaction)context.Transaction)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = AggregateType } },
        };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items[reader.GetString(0)] = reader.GetString(1);
        }

        var read = string.Join(",", items.OrderBy(i => i.Key, StringComparer.Ordinal).Select(i => $"{i.Key}={i.Value}"));
        return new ReconcileResult(items)
        {
            Deferred = (generation, _) =>
            {
                generation.TryActivate(AggregateType, () => activations.Record($"{AggregateType}@{generation.Head}: {read}"));
                return Task.CompletedTask;
            },
        };
    }
}

internal sealed class WidgetReconciler(SliceActivations activations) : SliceReconciler(SliceApp.Widget, activations);

internal sealed class GadgetReconciler(SliceActivations activations) : SliceReconciler(SliceApp.Gadget, activations);

internal sealed class SliceHub(IClusterScopeAccessor scopes, IChangeLog log, IOptions<ClusterOptions> options, SliceCalls calls) : Hub
{
    public ItemView Get(string type, string id)
    {
        calls.Record($"hub-get {type}/{id}");
        return SliceApp.Read(scopes, type, id) ?? new ItemView(id, null, scopes.Current?.PinnedGeneration?.Head, null);
    }

    public Task<ItemView> Put(string type, string id, string value)
    {
        calls.Record($"hub-put {type}/{id}");
        return SliceApp.WriteAsync(log, options.Value, type, id, value, Context.ConnectionAborted);
    }
}

internal static class SliceApp
{
    public const string Widget = "widget";
    public const string Gadget = "gadget";

    public const string CreateTable = "CREATE TABLE IF NOT EXISTS slice_item (type text NOT NULL, id text NOT NULL, value text NOT NULL, PRIMARY KEY (type, id))";

    public static ReplicaSpec Spec(Action<ClusterOptions>? profile = null, Action<ClusterOptions>? configure = null, bool redis = true, double clockRate = 1.0) => new()
    {
        ClockRate = clockRate,
        Profile = profile,
        Configure = o =>
        {
            o.ActionTimeBudget = TimeSpan.FromSeconds(5);
            o.ProxyReadTimeout = TimeSpan.FromSeconds(120);
            configure?.Invoke(o);
        },
        Reconcilers = [typeof(WidgetReconciler), typeof(GadgetReconciler)],
        FenceSink = typeof(AspNetCoreFenceSink),
        Redis = redis,
        ConfigureServices = services =>
        {
            services.AddSingleton<SliceCalls>();
            services.AddSingleton<SliceActivations>();
            services.AddRouting();
            services.AddSignalR();
            services.AddHealthChecks().AddReeveClusterReadiness();
            services.AddReeveClusterAspNetCore();
        },
        ConfigureWebApp = Map,
    };

    private static void Map(WebApplication app)
    {
        app.UseReeveCluster();
        app.MapGet("/items/{type}/{id}", (string type, string id, IClusterScopeAccessor scopes, SliceCalls calls) =>
        {
            calls.Record($"get {type}/{id}");
            return Read(scopes, type, id) is { } item ? Results.Ok(item) : Results.NotFound(new ItemView(id, null, scopes.Current?.PinnedGeneration?.Head, null));
        });
        app.MapPut("/items/{type}/{id}", async (string type, string id, string value, IChangeLog log, IOptions<ClusterOptions> options, SliceCalls calls, HttpContext http) =>
        {
            calls.Record($"put {type}/{id}");
            return Results.Ok(await WriteAsync(log, options.Value, type, id, value, http.RequestAborted));
        }).AcknowledgeWrites();

        app.MapPut("/optout/{type}/{id}", async (string type, string id, string value, IChangeLog log, IChangeAck ack, IOptions<ClusterOptions> options, SliceCalls calls, HttpContext http) =>
        {
            calls.Record($"optout {type}/{id}");
            ack.AcknowledgeAtCommit();
            return Results.Ok(await WriteAsync(log, options.Value, type, id, value, http.RequestAborted));
        }).AcknowledgeWrites();

        app.MapGet("/exempt", (IClusterScopeAccessor scopes, IPhaseGate gate, SliceCalls calls) =>
        {
            calls.Record("exempt");
            return Results.Ok(new { kind = scopes.Current?.Kind.ToString(), phase = gate.Phase.ToString() });
        }).AllowWhenFenced();
        app.MapHealthChecks("/ready").AllowWhenFenced().SkipReadIndex();
        app.MapHub<SliceHub>("/hub");
    }

    public static ItemView? Read(IClusterScopeAccessor scopes, string type, string id)
    {
        var generation = scopes.Current?.PinnedGeneration ?? throw new InvalidOperationException("The request pinned no generation.");
        return generation.Get<Dictionary<string, string>>(type).TryGetValue(id, out var value) ? new ItemView(id, value, generation.Head, null) : null;
    }

    public static async Task<ItemView> WriteAsync(IChangeLog log, ClusterOptions options, string type, string id, string value, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(options.ConnectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var command = new NpgsqlCommand(
            "INSERT INTO slice_item (type, id, value) VALUES ($1, $2, $3) ON CONFLICT (type, id) DO UPDATE SET value = $3",
            connection,
            transaction)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = type }, new NpgsqlParameter<string> { TypedValue = id }, new NpgsqlParameter<string> { TypedValue = value } },
        })
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var seq = await log.AppendAsync(transaction, [new ChangeEntry(type, id)], cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new ItemView(id, value, null, seq);
    }
}
