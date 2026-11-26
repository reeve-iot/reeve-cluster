using System.Text.Json;
using System.Text.Json.Serialization;

namespace Reeve.Cluster.AspNetCore;

internal sealed record StatusBody(DateTimeOffset ReadAt, IEnumerable<StatusMemberBody> Members)
{
    public static StatusBody Of(ClusterStatusRead read) => new(
        read.ReadAt,
        read.Members.Select(m => new StatusMemberBody(
            m.ReplicaId,
            m.Epoch,
            m.State,
            m.LeaseExpiresAt,
            m.LeaseValid,
            m.AppliedSeq,
            m.ServingSince,
            m.Version,
            m.Protocol,
            m.Sections.Select(s => new StatusSectionBody(s.Name, s.Age.TotalSeconds, s.Values)),
            m.Fences.Select(f => new StatusFenceBody(f.Epoch, f.Trigger, f.FencedAt, f.Span.TotalSeconds)))));
}

internal sealed record StatusMemberBody(
    string ReplicaId,
    long Epoch,
    string State,
    DateTimeOffset LeaseExpiresAt,
    bool LeaseValid,
    long AppliedSeq,
    DateTimeOffset? ServingSince,
    string Version,
    int Protocol,
    IEnumerable<StatusSectionBody> Sections,
    IEnumerable<StatusFenceBody> Fences);

internal sealed record StatusSectionBody(string Name, double AgeSeconds, JsonElement Values);

internal sealed record StatusFenceBody(long Epoch, string Trigger, DateTimeOffset FencedAt, double SpanSeconds);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(StatusBody))]
internal sealed partial class ClusterStatusJsonContext : JsonSerializerContext;
