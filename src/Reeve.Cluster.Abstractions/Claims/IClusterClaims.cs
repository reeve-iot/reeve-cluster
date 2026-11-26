namespace Reeve.Cluster;

/// <summary>
/// Claims on PostgreSQL: the first caller wins a key for a time to live. Only a serving replica
/// grants, and a grant is its commit: a grant whose commit the library cancelled before it returned is not a grant.
/// The effect of a winner runs in a claim-fenced transaction (<c>BeginClaimFencedTransactionAsync</c>), which the database refuses
/// once the claim is no longer valid.
/// </summary>
public interface IClusterClaims
{
    /// <summary>
    /// Claims <paramref name="key"/> for <paramref name="ttl"/>. A key whose claim is no longer valid — its time to live has
    /// passed, or its holder's member row is no longer active with a valid lease — is taken over under a new token.
    /// </summary>
    /// <param name="key">The claim's key.</param>
    /// <param name="ttl">How long the claim is valid, on the database's clock.</param>
    /// <param name="cancellationToken">Ends the call; a grant it interrupts has an unknown outcome and the call throws.</param>
    /// <returns><see cref="ClaimResult.Won"/>, <see cref="ClaimResult.Lost"/>, <see cref="ClaimResult.NotServing"/>, or <see cref="ClaimResult.Unknown"/>.</returns>
    /// <exception cref="InvalidOperationException">This process has an open claim-fenced transaction on the key.</exception>
    /// <exception cref="ArgumentException">The key starts with <c>reeve.once:</c>, which belongs to <see cref="IClusterOnce"/>, or with <c>reeve.flight:</c>, which belongs to <see cref="IClusterSingleFlight"/>.</exception>
    /// <remarks>Database statement errors are thrown as-is; for example, a transaction timeout before commit means no grant.</remarks>
    Task<ClaimResult> ClaimAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ends the claim of <paramref name="token"/> on <paramref name="key"/> with a write conditioned on the token, so the key can be
    /// claimed again at once. Refused while this process holds an open claim-fenced transaction on the key: on another connection
    /// the write would wait for that transaction, which waits for it.
    /// </summary>
    /// <param name="key">The claim's key.</param>
    /// <param name="token">The token <see cref="ClaimResult.Won"/> carried.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <returns>Whether the claim was still the token's and was ended.</returns>
    /// <exception cref="InvalidOperationException">This process holds an open claim-fenced transaction on the key.</exception>
    /// <exception cref="ArgumentException">The key starts with <c>reeve.once:</c>, which belongs to <see cref="IClusterOnce"/>, or with <c>reeve.flight:</c>, which belongs to <see cref="IClusterSingleFlight"/>.</exception>
    /// <exception cref="CommitOutcomeUnknownException">The release's database outcome is unknown.</exception>
    Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken = default);
}

/// <summary>
/// What <see cref="IClusterClaims.ClaimAsync"/> answered. Only <see cref="Won"/> is a grant: <see cref="IsWon"/> and
/// <see cref="TryGetToken"/> tell it from the three answers that are not, so a caller that only runs its effect on a win never
/// matches the others.
/// </summary>
public abstract record ClaimResult
{
    private protected ClaimResult()
    {
    }

    /// <summary>Whether the caller holds the claim: this is <see cref="Won"/>.</summary>
    public bool IsWon => this is Won;

    /// <summary>The token of a won claim; <see langword="false"/> for every other answer.</summary>
    /// <param name="token">The token <see cref="Won"/> carries, or <see cref="Guid.Empty"/>.</param>
    public bool TryGetToken(out Guid token)
    {
        token = this is Won won ? won.Token : Guid.Empty;
        return this is Won;
    }

    /// <summary>The caller holds the claim under <see cref="Token"/>, which its effect presents to the database.</summary>
    /// <param name="Token">The random token drawn at this grant.</param>
    public sealed record Won(Guid Token) : ClaimResult;

    /// <summary>The grant did not win; the read before its conditional takeover observed this holder.</summary>
    /// <param name="Holder">
    /// The replica id observed by the grant's read; the claim may have changed since then, and the key may be free: a row that read saw
    /// can be deleted by retention before the takeover, which then matches nothing. Nobody else was granted the key, and a retry can win it.
    /// </param>
    public sealed record Lost(string Holder) : ClaimResult;

    /// <summary>The replica is not serving, so it grants nothing; nothing was written.</summary>
    public sealed record NotServing : ClaimResult;

    /// <summary>
    /// The grant's commit was cancelled by the library before it returned, so it is not a grant whatever the call returned.
    /// Its record may be visible, and then lost in a failover; it expires with its time to live.
    /// </summary>
    public sealed record Unknown : ClaimResult;
}

/// <summary>
/// The database refused a claim: the claim of the token is no longer valid —
/// its time to live has passed, it was taken over, released or deleted, or its holder's lease is gone — or the replica is on
/// another timeline than the one it joined on (which also fences the replica). The effect must not happen. It derives from
/// <see cref="InvalidOperationException"/>: code that catches that and means to tell them apart catches this one first.
/// </summary>
public sealed class ClaimLostException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public ClaimLostException()
        : base("The claim is no longer valid.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public ClaimLostException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and its cause.</summary>
    public ClaimLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
