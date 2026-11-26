namespace Reeve.Cluster.AspNetCore;

/// <summary>Settings of the ASP.NET Core package. The timing settings are <see cref="ClusterOptions"/>'s.</summary>
public sealed class ReeveClusterHttpOptions
{
    /// <summary>
    /// The prefix of the response headers the package adds (<c>Reeve-Seq</c>, <c>Reeve-Skipped</c>,
    /// <c>Reeve-Replication</c>); these docs write them with the default, <c>Reeve-</c>. It must be a run of letters, digits
    /// and hyphens that ends in a hyphen, so that it forms a valid header name with each suffix.
    /// </summary>
    public string HeaderPrefix { get; set; } = "Reeve-";

    /// <summary>
    /// The hook for the application's error envelope: writes the body of every error answer the package gives — the three
    /// <c>503</c> refusals of admission, <c>503 write_refused</c>, <c>202 write_unconfirmed</c>, the status endpoint's
    /// <c>503 status_unavailable</c>, and the skip endpoint's <c>409</c>, <c>404</c>, <c>400</c> and <c>403 skip_endpoint_unprotected</c>,
    /// whose members besides the code are in <see cref="ClusterHttpError.Extensions"/>. The status, <c>Retry-After</c> and the
    /// <c>Reeve-</c> headers are already set when it runs; it sets the content type and writes the body. When it is not set the body
    /// is <c>application/problem+json</c> with an <c>errorCode</c> member. Hub invocations answer with the bare code in a
    /// <see cref="Microsoft.AspNetCore.SignalR.HubException"/> and do not reach it.
    /// </summary>
    public Func<Microsoft.AspNetCore.Http.HttpContext, ClusterHttpError, Task>? WriteError { get; set; }

    internal string HeaderName(string suffix) => HeaderPrefix + suffix;

    internal IReadOnlyList<string> Problems()
    {
        var prefix = HeaderPrefix;
        if (string.IsNullOrEmpty(prefix) || prefix[^1] != '-' || !prefix.All(c => char.IsAsciiLetterOrDigit(c) || c == '-'))
        {
            return [$"HeaderPrefix \"{prefix}\" must consist of ASCII letters, digits and hyphens and end in a hyphen."];
        }

        return [];
    }
}

/// <summary>An error answer of the package, as the application's error envelope receives it (<see cref="ReeveClusterHttpOptions.WriteError"/>).</summary>
/// <param name="StatusCode">
/// The status already set on the response: <c>503</c> or <c>202</c>; <c>503</c> from the status endpoint; from the skip endpoint
/// (<see cref="Microsoft.AspNetCore.Builder.ReeveClusterSkipEndpoints"/>) <c>409</c>, <c>404</c>, <c>400</c> or <c>403</c>.
/// </param>
/// <param name="ErrorCode">
/// The code: <c>replica_not_serving</c>, <c>replica_fenced</c>, <c>replica_behind</c> (admission: nothing ran), <c>write_refused</c> (a
/// commit refused at fence step 2: no configuration write took effect, but the handler ran) or <c>write_unconfirmed</c>; from the status
/// endpoint <c>status_unavailable</c>; from the skip endpoint <c>skip_covers_acknowledged</c>, <c>skip_not_found</c>, <c>skip_request_invalid</c>
/// or <c>skip_endpoint_unprotected</c>.
/// </param>
/// <param name="Detail">A sentence for a person: what happened and what the client does next.</param>
public sealed record ClusterHttpError(int StatusCode, string ErrorCode, string Detail)
{
    private static readonly IReadOnlyDictionary<string, object?> None = new Dictionary<string, object?>();

    /// <summary>
    /// Members the answer carries besides the code, named as the body names them: <c>ackedSeq</c> on <c>409
    /// skip_covers_acknowledged</c>. Empty for every other answer. The default body adds them as members.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Extensions { get; init; } = None;
}
