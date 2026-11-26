namespace Reeve.Cluster;

/// <summary>One section of a replica's status report: a name and its values.</summary>
/// <remarks>The type is a record with init-only properties, so a minor release may add a property.</remarks>
public sealed record StatusSection
{
    /// <summary>The section's name. It must be the <see cref="IStatusSectionSource.Name"/> of the source that collected it, or the section is dropped for that tick.</summary>
    public required string Name { get; init; }

    /// <summary>
    /// What the section reports. The values must serialise to JSON with <c>System.Text.Json</c>; a string's NUL characters are replaced by U+FFFD.
    /// Under trimming or NativeAOT a value must be a <see cref="string"/>, <see cref="bool"/>, <see cref="int"/>, <see cref="long"/>, <see cref="double"/>,
    /// <see cref="decimal"/>, <see cref="Guid"/>, <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="TimeSpan"/> or <see cref="System.Text.Json.JsonElement"/>;
    /// a <c>Dictionary&lt;string, object?&gt;</c>, a <c>List&lt;object?&gt;</c>, an <c>object?[]</c> or a <c>List&lt;Dictionary&lt;string, object?&gt;&gt;</c> whose
    /// elements are again of these types; or a <c>List&lt;string&gt;</c>, a <c>string[]</c> or a <c>Dictionary&lt;string, string&gt;</c>.
    /// A section holding another type is left out of the report with a logged warning.
    /// </summary>
    public required IReadOnlyDictionary<string, object?> Values { get; init; }
}

/// <summary>
/// A source of one status section. The library collects every registered source each
/// <c>StatusReportInterval</c> and writes the sections beside its own; <c>AddClusterStatusSection&lt;T&gt;()</c>
/// registers an application's.
/// </summary>
/// <remarks>
/// A source is a singleton, collected once per interval and in sequence with the others, each bounded by the interval: it must return quickly and read its own memory. A collect abandoned after the interval is not cancelled and may still be running when the next starts. A source
/// that throws, is cancelled by something other than the host's stop, or does not answer within <c>StatusReportInterval</c> loses its
/// section for that tick and is logged once until it collects again; its previous row stays, with a growing age. Nothing a decision of the
/// library does waits for a source.
/// </remarks>
public interface IStatusSectionSource
{
    /// <summary>
    /// The name of the section this source reports. It is unique among the registered sources and is none of the library's own
    /// section names (<c>applier</c>, <c>ask</c>, <c>floors</c>, <c>lease</c>, <c>owned</c>, <c>phase</c>, <c>releases</c>, <c>replication</c>,
    /// <c>skips</c>), and it is not null, empty or white space; the host's start is refused with a <see cref="ClusterConfigurationException"/> otherwise.
    /// It is read once, at the host's start.
    /// </summary>
    string Name { get; }

    /// <summary>Collects the section's current values. Must not block on a database that does not answer: a source reads its own memory.</summary>
    /// <param name="cancellationToken">Cancelled when the host stops.</param>
    ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken);
}
