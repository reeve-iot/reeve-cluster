using Npgsql;

namespace Reeve.Cluster.Core;

internal static class PrimaryTarget
{
    public static string? Refusal(string connectionString)
    {
        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException ex)
        {
            return $"the connection string does not parse: {ex.Message}";
        }

        var attributes = builder.TargetSessionAttributes?.Trim();
        var hasAttributes = !string.IsNullOrEmpty(attributes);
        var severalHosts = builder.Host?.Contains(',', StringComparison.Ordinal) == true;

        if (!severalHosts)
        {
            return hasAttributes
                ? $"Target Session Attributes={attributes} is only supported with several hosts; remove it, because with one host the library checks on every connection that it reached the primary"
                : null;
        }

        if (!hasAttributes)
        {
            return "several hosts with no Target Session Attributes would let a standby answer; set Target Session Attributes=primary";
        }

        var lower = attributes!.ToLowerInvariant();
        return lower == "primary" || lower == "read-write"
            ? null
            : $"Target Session Attributes={attributes} may reach a standby; with several hosts the library needs primary";
    }
}
