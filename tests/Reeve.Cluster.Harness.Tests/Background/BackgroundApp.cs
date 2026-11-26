using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Harness.Slice;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;

namespace Harness.Background;

internal sealed record Act(long Timestamp, int Write);

internal sealed class ActLog
{
    private readonly ConcurrentQueue<Act> acts = new();

    public IReadOnlyList<Act> All => [.. acts];

    internal void Add(Act act) => acts.Enqueue(act);
}

internal sealed record Component(IGeneration Generation, int Write);

internal sealed class ComponentHost(ActLog log) : BackgroundService
{
    private volatile Component? installed;

    public void Install(Component component) => installed = component;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (installed is { } component)
            {
                component.Generation.TryActivate(SliceApp.Widget, () => log.Add(new Act(Stopwatch.GetTimestamp(), component.Write)));
            }

            try
            {
                await Task.Delay(1, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}

internal sealed class BackgroundReconciler(ComponentHost host) : IReconciler
{
    public static readonly TimeSpan StartTime = TimeSpan.FromMilliseconds(40);

    public string AggregateType => SliceApp.Widget;

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var items = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT id, value FROM slice_item WHERE type = $1",
            (NpgsqlConnection)context.Connection,
            (NpgsqlTransaction)context.Transaction)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = SliceApp.Widget } },
        };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items[reader.GetString(0)] = reader.GetString(1);
        }

        var write = items.TryGetValue("a", out var value) ? int.Parse(value.AsSpan(1)) : -1;
        return new ReconcileResult(items)
        {
            Deferred = async (generation, superseded) =>
            {
                try
                {
                    await Task.Delay(StartTime, superseded).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                generation.TryActivate(SliceApp.Widget, () => host.Install(new Component(generation, write)));
            },
        };
    }
}

internal static class BackgroundApp
{
    public const string AnsweredAtHeader = "X-Answered-At";

    public static ReplicaSpec Spec()
    {
        var slice = SliceApp.Spec();
        return new ReplicaSpec
        {
            ClockRate = slice.ClockRate,
            Profile = slice.Profile,
            Configure = slice.Configure,
            Reconcilers = [typeof(BackgroundReconciler)],
            FenceSink = slice.FenceSink,
            Redis = slice.Redis,
            ConfigureServices = services =>
            {
                slice.ConfigureServices!(services);
                services.AddSingleton<ActLog>();
                services.AddSingleton<ComponentHost>();
                services.AddHostedService(provider => provider.GetRequiredService<ComponentHost>());
            },
            ConfigureWebApp = app =>
            {
                app.Use((context, next) =>
                {
                    context.Response.OnStarting(() =>
                    {
                        context.Response.Headers[AnsweredAtHeader] = Stopwatch.GetTimestamp().ToString(System.Globalization.CultureInfo.InvariantCulture);
                        return Task.CompletedTask;
                    });
                    return next(context);
                });
                slice.ConfigureWebApp!(app);
            },
        };
    }
}
