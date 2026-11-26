using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Status;

internal sealed partial class StatusReporter(
    IEnumerable<IStatusSectionSource> sources,
    IStatusStore store,
    LocalPhaseGate gate,
    IOptions<ClusterOptions> options,
    TimeProvider time,
    ILogger<StatusReporter> logger,
    IEnumerable<ApplicationStatusSection> applicationSections) : BackgroundService
{
    private readonly TimeSpan interval = options.Value.StatusReportInterval;
    private readonly string replicaId = options.Value.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    private readonly IReadOnlyList<(IStatusSectionSource Source, string Name)> sources = Check([.. sources], [.. applicationSections]);
    private readonly HashSet<Type> failedSources = [];
    private bool failing;

    internal static readonly IReadOnlySet<string> LibrarySectionNames = new HashSet<string>(StringComparer.Ordinal)
    {
        "applier", "ask", "floors", "lease", "owned", "phase", "releases", "replication", "skips",
    };

    private static IReadOnlyList<(IStatusSectionSource Source, string Name)> Check(IReadOnlyList<IStatusSectionSource> registered, IReadOnlyList<ApplicationStatusSection> application)
    {
        var applicationTypes = application.Select(a => a.Type).ToHashSet();
        var named = registered.Select(s => (Source: s, Name: s.Name, Application: applicationTypes.Any(t => t.IsInstanceOfType(s)))).ToList();
        foreach (var (source, name, _) in named)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ClusterConfigurationException(
                    ClusterConfigurationReason.StatusSectionNameInvalid,
                    $"The status section source {source.GetType().Name} has no name: its Name is null, empty or white space.");
            }
        }

        var reserved = named.Where(n => !n.Application).Select(n => n.Name).Concat(LibrarySectionNames).ToHashSet(StringComparer.Ordinal);
        foreach (var (source, name, _) in named.Where(n => n.Application))
        {
            if (reserved.Contains(name))
            {
                throw new ClusterConfigurationException(
                    ClusterConfigurationReason.StatusSectionNameReserved,
                    $"The application status section '{name}' ({source.GetType().Name}) is named like a section of the library. The library's sections are {string.Join(", ", LibrarySectionNames.Order(StringComparer.Ordinal))}; name an application section otherwise.");
            }
        }

        var duplicate = named.GroupBy(n => n.Name, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        return duplicate is null
            ? [.. named.Select(n => (n.Source, n.Name))]
            : throw new ClusterConfigurationException(
                ClusterConfigurationReason.StatusSectionNameDuplicate,
                $"Two status section sources report the section '{duplicate.Key}': {string.Join(", ", duplicate.Select(n => n.Source.GetType().Name))}. A section name is unique.");
    }

    internal async Task ReportOnceAsync(CancellationToken cancellationToken)
    {
        if (gate.Epoch is not { } epoch)
        {
            return;
        }

        var sections = new List<ReportedSection>();
        foreach (var (source, name) in sources)
        {
            var type = source.GetType();
            try
            {
                var section = await source.CollectAsync(cancellationToken).AsTask().WaitAsync(interval, time, cancellationToken).ConfigureAwait(false);

                if (!string.Equals(section.Name, name, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"The source declares the section '{name}' and collected a section named '{section.Name}'.");
                }

                sections.Add(new ReportedSection(section.Name, StatusValueJson.Serialize(section.Values)));
                failedSources.Remove(type);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
            {
                if (failedSources.Add(type))
                {
                    LogSourceFailed(logger, ex, type.FullName ?? type.Name);
                }
            }
        }

        try
        {
            using var timeout = new CancellationTokenSource(interval, time);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
            await store.ReportAsync(replicaId, epoch, sections, bound.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException && (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested))
        {
            if (!failing)
            {
                failing = true;
                LogReportFailed(logger, ex);
            }

            return;
        }

        if (failing)
        {
            failing = false;
            LogReportRestored(logger);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            using var timer = new PeriodicTimer(interval, time);
            do
            {
                await ReportOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException && stoppingToken.IsCancellationRequested)
        {
            if (ex is not OperationCanceledException)
            {
                LogStoppedOnFailure(logger, ex);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "The status reporter stopped; a call that failed after the stop began is not retried.")]
    private static partial void LogStoppedOnFailure(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The status report could not be written; the next tick tries again. A status record: no decision depends on it.")]
    private static partial void LogReportFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "The status report is written again.")]
    private static partial void LogReportRestored(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The status section source {Source} failed; its section is left out of this report.")]
    private static partial void LogSourceFailed(ILogger logger, Exception exception, string source);
}

internal sealed class NulSafeStringConverter : System.Text.Json.Serialization.JsonConverter<string>
{
    public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString();

    public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options) =>
        writer.WriteStringValue(Safe(value));

    public override string ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString()!;

    public override void WriteAsPropertyName(Utf8JsonWriter writer, string value, JsonSerializerOptions options) =>
        writer.WritePropertyName(Safe(value));

    private static string Safe(string value) => value.Contains('\0', StringComparison.Ordinal) ? value.Replace('\0', '\uFFFD') : value;
}
