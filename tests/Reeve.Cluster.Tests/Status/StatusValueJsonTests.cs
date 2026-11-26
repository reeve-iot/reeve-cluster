using System.Text.Json;
using Reeve.Cluster.Status;

namespace Unit.Status;

public sealed class StatusValueJsonTests
{
    private static readonly JsonSerializerOptions Reflection = new() { Converters = { new NulSafeStringConverter() } };

    private static readonly JsonSerializerOptions GeneratedOnly = StatusValueJson.Create(reflectionFallback: false);

    private static IReadOnlyDictionary<string, object?> LibraryValues() => new Dictionary<string, object?>
    {
        ["epoch"] = (long?)7,
        ["absent"] = null,
        ["timeline"] = 3,
        ["protocol"] = (int?)2,
        ["ready"] = true,
        ["stalled"] = false,
        ["leaseRemainingSeconds"] = 0.1 + 0.2,
        ["leaseTtlSeconds"] = 15d,
        ["cursorToken"] = (Guid?)Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff"),
        ["servingSince"] = (DateTimeOffset?)new DateTimeOffset(2026, 11, 9, 12, 30, 15, 123, TimeSpan.FromHours(1)).AddTicks(4567),
        ["phase"] = "Serving",
        ["nul\0key"] = "nul\0value",
        ["escaped"] = "za\u017C\u00F3\u0142\u0107 <tag> & \"quote\" \u2028 \uD83D\uDE00",
        ["readinessReasons"] = new List<Dictionary<string, object?>>
        {
            new() { ["code"] = "join_unanswered", ["description"] = "no\0answer" },
            new() { ["code"] = "other", ["description"] = null },
        },
        ["open"] = new List<Dictionary<string, object?>>(),
        ["nested"] = new Dictionary<string, object?> { ["fromSeq"] = 9L, ["openedAt"] = DateTimeOffset.UnixEpoch },
    };

    [Fact]
    public void The_library_values_are_written_byte_for_byte_as_the_reflection_serializer_wrote_them()
    {
        var values = LibraryValues();

        Assert.Equal(JsonSerializer.Serialize(values, Reflection), StatusValueJson.Serialize(values));
    }

    [Fact]
    public void The_library_values_are_written_without_reflection_and_byte_for_byte_the_same()
    {
        var values = LibraryValues();

        Assert.Equal(
            JsonSerializer.Serialize(values, Reflection),
            JsonSerializer.Serialize(values, GeneratedOnly.GetTypeInfo(typeof(IReadOnlyDictionary<string, object?>))));
    }

    [Fact]
    public void An_application_value_of_its_own_type_is_written_by_reflection_as_before()
    {
        IReadOnlyDictionary<string, object?> values = new Dictionary<string, object?> { ["probe"] = new Probe("p\0", [1, 2]) };

        Assert.Equal(JsonSerializer.Serialize(values, Reflection), StatusValueJson.Serialize(values));
    }

    [Fact]
    public void An_application_value_of_its_own_type_cannot_be_written_without_reflection()
    {
        IReadOnlyDictionary<string, object?> values = new Dictionary<string, object?> { ["probe"] = new Probe("p", [1]) };

        Assert.Throws<NotSupportedException>(() => JsonSerializer.Serialize(values, GeneratedOnly.GetTypeInfo(typeof(IReadOnlyDictionary<string, object?>))));
    }

    private sealed record Probe(string Name, int[] Counts);
}
