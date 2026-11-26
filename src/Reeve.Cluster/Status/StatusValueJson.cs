using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Reeve.Cluster.Status;

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(IReadOnlyDictionary<string, object?>))]
[JsonSerializable(typeof(Dictionary<string, object?>))]
[JsonSerializable(typeof(List<Dictionary<string, object?>>))]
[JsonSerializable(typeof(List<object?>))]
[JsonSerializable(typeof(object?[]))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(decimal))]
[JsonSerializable(typeof(Guid))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(TimeSpan))]
[JsonSerializable(typeof(JsonElement))]
internal sealed partial class StatusValueJsonContext : JsonSerializerContext;

internal static class StatusValueJson
{
    private const string FallbackGuard =
        "Reached only when JsonSerializer.IsReflectionEnabledByDefault holds; trimmed and NativeAOT publishes turn it off.";

    public static JsonSerializerOptions Options { get; } = Create(JsonSerializer.IsReflectionEnabledByDefault);

    public static string Serialize(IReadOnlyDictionary<string, object?> values) =>
        JsonSerializer.Serialize(values, (JsonTypeInfo<IReadOnlyDictionary<string, object?>>)Options.GetTypeInfo(typeof(IReadOnlyDictionary<string, object?>)));

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = FallbackGuard)]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = FallbackGuard)]
    internal static JsonSerializerOptions Create(bool reflectionFallback)
    {
        var options = new JsonSerializerOptions { Converters = { new NulSafeStringConverter() } };
        options.TypeInfoResolverChain.Add(StatusValueJsonContext.Default);
        if (reflectionFallback && JsonSerializer.IsReflectionEnabledByDefault)
        {
            options.TypeInfoResolverChain.Add(new DefaultJsonTypeInfoResolver());
        }

        options.MakeReadOnly();
        return options;
    }
}
