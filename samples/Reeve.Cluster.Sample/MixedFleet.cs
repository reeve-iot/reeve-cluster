using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Redis;
using StackExchange.Redis;

namespace Reeve.Cluster.Sample;

internal static class MixedFleet
{
    public static readonly string[] PlacedKinds = ["mixed-single", "mixed-single-2", "mixed-single-3", "mixed-single-4", "mixed-single-5", "mixed-single-6"];
    public const string KeyedKind = "mixed-keyed";

    public static void AddServices(WebApplicationBuilder builder)
    {
        var redis = builder.Configuration["Sample:RedisConnectionString"] ?? throw new InvalidOperationException("Sample:RedisConnectionString is required.");
        var prefix = builder.Configuration["Sample:RedisKeyPrefix"] ?? "reeve:";
        var options = ConfigurationOptions.Parse(redis);
        options.AbortOnConnectFail = false;
        builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(options));
        builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = prefix);
        builder.Services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);
        builder.Services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);

        foreach (var kind in PlacedKinds)
        {
            builder.Services.AddClusterUnit<SingleUnit>(kind);
        }

        builder.Services.AddClusterKeyedResource<KeyedUnit>(KeyedKind, o => o.Policy = AdoptionPolicy.AnyEligible);
        builder.Services.AddClusterAskHandler<KeyedAskHandler>(KeyedKind);
        builder.Services.AddClusterMemberAskHandler<MemberAskHandler>();
    }

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/mixed").AllowWhenFenced().SkipReadIndex();

        group.MapGet("/whoami", (Microsoft.Extensions.Options.IOptions<ClusterOptions> options) =>
            Results.Ok(new { replicaId = options.Value.ReplicaId, version = Version, library = LibraryVersion }));

        group.MapGet("/flag/{name}", (string name, IClusterScopeAccessor scopes) =>
            scopes.Current!.PinnedGeneration!.Get<FlagState>("flag").Flags.TryGetValue(name, out var enabled)
                ? Results.Ok(new { name, enabled })
                : Results.NotFound());
        group.MapGet("/config", (IClusterScopeAccessor scopes) => ConfigView.Of(scopes));

        group.MapGet("/units", () => Results.Ok(Tenures.Select(t => new { kind = t.Key.Kind, key = t.Key.Key, token = t.Value })));

        group.MapPost("/keyed/{key}", async (string key, IKeyedResources resources, CancellationToken cancellationToken) =>
            Results.Ok(new { outcome = (await resources.CreateAsync(KeyedKind, key, cancellationToken)).ToString() }));

        group.MapPost("/ask/member/{target}", async (string target, HttpRequest request, IClusterAsk ask, CancellationToken cancellationToken) =>
            Answer(await ask.AskMemberAsync(target, await ReadAsync(request), TimeSpan.FromSeconds(30), cancellationToken)));

        group.MapPost("/ask/key/{key}", async (string key, HttpRequest request, IClusterAsk ask, CancellationToken cancellationToken) =>
            Answer(await ask.AskAsync(KeyedKind, key, await ReadAsync(request), TimeSpan.FromSeconds(30), cancellationToken)));

        group.MapPost("/claim/{store}/{key}", async (string store, string key, IClusterClaims pg, IRedisClaims redis, CancellationToken cancellationToken) =>
        {
            var result = store == "redis"
                ? await redis.ClaimAsync(key, TimeSpan.FromMinutes(5), cancellationToken)
                : await pg.ClaimAsync(key, TimeSpan.FromMinutes(5), cancellationToken);
            return Results.Ok(result switch
            {
                ClaimResult.Won won => new { result = "won", holder = (string?)null, token = (Guid?)won.Token },
                ClaimResult.Lost lost => new { result = "lost", holder = (string?)lost.Holder, token = (Guid?)null },
                _ => new { result = result.GetType().Name.ToLowerInvariant(), holder = (string?)null, token = (Guid?)null },
            });
        });

        group.MapDelete("/claim/{store}/{key}/{token:guid}", async (string store, string key, Guid token, IClusterClaims pg, IRedisClaims redis, CancellationToken cancellationToken) =>
            Results.Ok(new { released = store == "redis" ? await redis.ReleaseAsync(key, token, cancellationToken) : await pg.ReleaseAsync(key, token, cancellationToken) }));

        group.MapPost("/slot/{store}/{key}/{id}/{limit:int}", async (string store, string key, string id, int limit, IClusterSlots pg, IRedisSlots redis, CancellationToken cancellationToken) =>
        {
            var result = store == "redis"
                ? await redis.AcquireAsync(key, id, limit, TimeSpan.FromMinutes(5), cancellationToken)
                : await pg.AcquireAsync(key, id, limit, TimeSpan.FromMinutes(5), cancellationToken);
            return Results.Ok(result switch
            {
                SlotAcquireResult.Acquired acquired => new { result = "acquired", token = (Guid?)acquired.Token },
                _ => new { result = result.GetType().Name.ToLowerInvariant(), token = (Guid?)null },
            });
        });

        group.MapDelete("/slot/{store}/{key}/{id}/{token:guid}", async (string store, string key, string id, Guid token, IClusterSlots pg, IRedisSlots redis, CancellationToken cancellationToken) =>
            Results.Ok(new { released = store == "redis" ? await redis.ReleaseAsync(key, id, token, cancellationToken) : await pg.ReleaseAsync(key, id, token, cancellationToken) }));

        group.MapPost("/gate/require/{version}", async (string version, IClusterVersionGate gate, CancellationToken cancellationToken) =>
            Results.Ok(new { raised = await gate.RequireVersionAsync(version, cancellationToken) }));
        group.MapGet("/gate/at-least/{version}", async (string version, IClusterVersionGate gate, CancellationToken cancellationToken) =>
            Results.Ok(new { atLeast = await gate.AllMembersAtLeastAsync(version, cancellationToken) }));
    }

    internal static readonly ConcurrentDictionary<(string Kind, string Key), long> Tenures = new();

    internal static string Version => InformationalVersionOf(Assembly.GetEntryAssembly());

    internal static string LibraryVersion => InformationalVersionOf(typeof(ClusterOptions).Assembly);

    private static string InformationalVersionOf(Assembly? assembly) =>
        assembly?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";

    internal static Task Acquired(OwnedContext context)
    {
        Tenures[(context.Kind, context.Key)] = context.Token;
        Console.WriteLine($"harness: acquired {context.Kind}/{context.Key}");
        return Task.CompletedTask;
    }

    internal static Task Lost(OwnedContext context)
    {
        Tenures.TryRemove((context.Kind, context.Key), out _);
        return Task.CompletedTask;
    }

    private static async Task<byte[]> ReadAsync(HttpRequest request)
    {
        using var buffer = new MemoryStream();
        await request.Body.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static IResult Answer(AskResult result) => Results.Ok(new
    {
        outcome = result.Outcome.ToString(),
        response = result.Outcome == AskOutcome.Answered && !result.Response.IsEmpty ? Encoding.UTF8.GetString(result.Response.Span) : null,
        error = result.Error,
    });

    internal static ReadOnlyMemory<byte> Reply(string? replicaId, ReadOnlyMemory<byte> payload) =>
        Encoding.UTF8.GetBytes($"{replicaId}|{Version}|{Encoding.UTF8.GetString(payload.Span)}");
}

public sealed class SingleUnit : IOwnedUnit
{
    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => MixedFleet.Acquired(context);

    public Task OnLostAsync(OwnedContext context) => MixedFleet.Lost(context);
}

public sealed class KeyedUnit : IOwnedUnit
{
    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => MixedFleet.Acquired(context);

    public Task OnLostAsync(OwnedContext context) => MixedFleet.Lost(context);
}

public sealed class KeyedAskHandler(Microsoft.Extensions.Options.IOptions<ClusterOptions> options) : IAskHandler
{
    public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) =>
        Task.FromResult(MixedFleet.Reply(options.Value.ReplicaId, request.Payload));
}

public sealed class MemberAskHandler(Microsoft.Extensions.Options.IOptions<ClusterOptions> options) : IAskHandler
{
    public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) =>
        Task.FromResult(MixedFleet.Reply(options.Value.ReplicaId, request.Payload));
}
