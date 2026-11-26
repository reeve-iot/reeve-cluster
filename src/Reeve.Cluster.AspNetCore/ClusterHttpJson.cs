using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using JsonOptions = Microsoft.AspNetCore.Http.Json.JsonOptions;
using ProblemDetails = Microsoft.AspNetCore.Mvc.ProblemDetails;

namespace Reeve.Cluster.AspNetCore;

internal sealed record SkipOpenedBody(long skipId);

internal sealed record SkipClosedBody(long untilSeq);

internal sealed record SkipOpenBody(string? AggregateType, long? FromSeq, string? Reason, string? OpenedBy);

internal sealed record SkipCloseBody(string? ClosedBy);

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(SkipOpenedBody))]
[JsonSerializable(typeof(SkipClosedBody))]
[JsonSerializable(typeof(SkipOpenBody))]
[JsonSerializable(typeof(SkipCloseBody))]
[JsonSerializable(typeof(ProblemDetails))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(DateTimeOffset))]
internal sealed partial class ClusterHttpJsonContext : JsonSerializerContext;

internal sealed class ClusterHttpJsonSetup : IPostConfigureOptions<JsonOptions>
{
    public void PostConfigure(string? name, JsonOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var serializer = options.SerializerOptions;
        if (!serializer.IsReadOnly && serializer.TypeInfoResolver is not null && !serializer.TypeInfoResolverChain.Contains(ClusterHttpJsonContext.Default))
        {
            serializer.TypeInfoResolverChain.Add(ClusterHttpJsonContext.Default);
        }
    }
}

internal static class ClusterHttpJson
{
    private static readonly JsonSerializerOptions Unconfigured = CreateUnconfigured();

    public static JsonTypeInfo<T> Info<T>(HttpContext context) =>
        (JsonTypeInfo<T>)ApplicationOptions(context).GetTypeInfo(typeof(T));

    private static JsonSerializerOptions ApplicationOptions(HttpContext context) =>
        context.RequestServices?.GetService<IOptions<JsonOptions>>()?.Value.SerializerOptions ?? Unconfigured;

    private static JsonSerializerOptions CreateUnconfigured()
    {
        var options = new JsonOptions();
        new ClusterHttpJsonSetup().PostConfigure(Options.DefaultName, options);
        return options.SerializerOptions;
    }
}
