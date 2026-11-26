namespace Reeve.Cluster;

/// <summary>
/// The creator's side of keyed resources: one of many owned things of one
/// kind, created and ended by the application rather than placed. The kind is registered on this replica with
/// <c>AddClusterKeyedResource</c>; a created key runs there as any owned unit does.
/// </summary>
public interface IKeyedResources
{
    /// <summary>
    /// Creates <paramref name="key"/> of <paramref name="kind"/>, owned by this replica: the insert of an absent row, or the
    /// takeover of a free or orphaned one (a tombstone of an ended resource, or a dead owner's row) under a higher token. The
    /// unit starts at the next own-row poll, which the call nudges. A free row of a keyed resource is taken back only by a
    /// creation: a creator calls this again whenever the resource should exist. A replica that does not pass the kind's
    /// <see cref="UnitKindOptions.Eligibility"/> filter writes nothing and answers <see cref="CreateOutcome.NotEligible"/>, or
    /// <see cref="CreateOutcome.AlreadyOwned"/> for a key that has a valid owner.
    /// </summary>
    /// <param name="kind">A keyed-resource kind registered on this replica.</param>
    /// <param name="key">The resource's key.</param>
    /// <param name="cancellationToken">Ends the call; a statement it interrupts has an unknown outcome, and the next call reads again.</param>
    /// <exception cref="ArgumentException">The kind is not registered on this replica as a keyed resource.</exception>
    Task<CreateOutcome> CreateAsync(string kind, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// The creator's clean-up under the <see cref="AdoptionPolicy.None"/> policy: frees the key's row when its owner's hold is
    /// no longer valid, so that the free-row sweep deletes it after <c>OwnedRowRetention</c>. Returns whether it freed the row;
    /// a row that is absent, free or validly held is left as it is, and so is every row while the replica is not serving.
    /// </summary>
    /// <param name="kind">A keyed-resource kind registered on this replica with the <see cref="AdoptionPolicy.None"/> policy.</param>
    /// <param name="key">The resource's key.</param>
    /// <param name="cancellationToken">Ends the call.</param>
    /// <exception cref="ArgumentException">The kind is not registered on this replica as a keyed resource, or its policy is not <see cref="AdoptionPolicy.None"/>.</exception>
    Task<bool> CleanUpAsync(string kind, string key, CancellationToken cancellationToken = default);
}

/// <summary>What <see cref="IKeyedResources.CreateAsync"/> did.</summary>
public enum CreateOutcome
{
    /// <summary>This replica owns the key under a new token.</summary>
    Created,

    /// <summary>The key has an owner whose hold is valid, this replica included; nothing was written.</summary>
    AlreadyOwned,

    /// <summary>The replica is not serving, so it creates nothing; nothing was written.</summary>
    NotServing,

    /// <summary>
    /// The key's previous tenure on this replica is still stopping (its <c>OnLostAsync</c> has not returned, or its release
    /// got no answer), and a new one would be released at once; nothing was written. Call again once it has.
    /// </summary>
    Stopping,

    /// <summary>
    /// This replica does not pass the kind's <see cref="UnitKindOptions.Eligibility"/> filter, so it may not own the key;
    /// nothing was written. Create the key on an eligible replica.
    /// </summary>
    NotEligible,
}
