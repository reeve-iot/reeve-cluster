using System.Text.Json;

namespace Reeve.Cluster;

/// <summary>
/// The typed read of the status registry: every member row of the cluster with its sections and its fence
/// history, the read the status endpoint (<c>MapReeveClusterStatus</c>) answers with. It is a status record, written best effort by the
/// members: no decision of the library reads it, and a host that decides on it (a roster of the members that are running, say) takes the
/// reading as of <see cref="ClusterStatusRead.ReadAt"/>, not as a fact that holds after.
/// </summary>
/// <remarks>
/// The read goes to PostgreSQL over the pool where request load already queues (the read index's), never over the connections of the
/// activation, the rejoin, the floor raise or the applier's poll. It works on any replica in any phase, and from a container built with
/// <c>AddReeveCluster</c> whose host is never started; it is not refused on a fenced replica and takes no part in the read index.
/// </remarks>
public interface IClusterStatus
{
    /// <summary>
    /// Reads every member row with its sections and fence history. The members are ordered by replica id; the library never deletes a
    /// member row, so a stopped or crashed replica is listed until an operator deletes its row: whether a member is running is for the
    /// caller to decide from <see cref="ClusterMemberStatus.State"/> and <see cref="ClusterMemberStatus.LeaseValid"/>.
    /// </summary>
    /// <param name="cancellationToken">Ends the call; a cancel by this token throws <see cref="OperationCanceledException"/>.</param>
    /// <returns>The read, as of the database's clock.</returns>
    /// <remarks>
    /// A database that cannot be reached, or that fails, throws the exception Npgsql raised, unchanged; the first physical connection can
    /// also throw <see cref="ClusterConfigurationException"/> when the server's configuration is one the library refuses. The read writes nothing.
    /// </remarks>
    /// <exception cref="TimeoutException">
    /// <c>ClusterOptions.ActionTimeBudget</c> elapsed before the database answered. The budget bounds the call when it is set, which the
    /// ASP.NET Core package requires; with no budget configured only <paramref name="cancellationToken"/> and the database's own limits
    /// end it.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// The status registry as one read returned it. The types of a read are records with init-only properties: a 1.x minor release may add a
/// property, and a host builds its own (a test double of <see cref="IClusterStatus"/>) with an object initializer.
/// </summary>
public sealed record ClusterStatusRead
{
    /// <summary>The database's time of the read.</summary>
    public required DateTimeOffset ReadAt { get; init; }

    /// <summary>Every member row, ordered by replica id.</summary>
    public required IReadOnlyList<ClusterMemberStatus> Members { get; init; }
}

/// <summary>One member of a <see cref="ClusterStatusRead"/>: its row, the sections inside their window, and its fence history.</summary>
public sealed record ClusterMemberStatus
{
    /// <summary>The replica.</summary>
    public required string ReplicaId { get; init; }

    /// <summary>The epoch of its row.</summary>
    public required long Epoch { get; init; }

    /// <summary>
    /// The row's state: <c>joining</c>, <c>active</c>, <c>fenced</c> or <c>draining</c>. A 1.x minor release may add values: a caller
    /// treats a value it does not know as unknown.
    /// </summary>
    public required string State { get; init; }

    /// <summary>When the row's lease ends (it ended at the fence, for a fenced row).</summary>
    public required DateTimeOffset LeaseExpiresAt { get; init; }

    /// <summary>Whether the lease is valid, decided by the database in the statement that read the row, on its clock.</summary>
    public required bool LeaseValid { get; init; }

    /// <summary>The cursor the member last wrote to its row.</summary>
    public required long AppliedSeq { get; init; }

    /// <summary>When it entered <c>Serving</c>; <see langword="null"/> when it is not.</summary>
    public required DateTimeOffset? ServingSince { get; init; }

    /// <summary>The version it runs.</summary>
    public required string Version { get; init; }

    /// <summary>The protocol it speaks.</summary>
    public required int Protocol { get; init; }

    /// <summary>Its sections while its lease is valid and for <c>LeaseTtl</c> after it ended; empty past that.</summary>
    public required IReadOnlyList<ClusterStatusSection> Sections { get; init; }

    /// <summary>Its recorded fences, the newest first.</summary>
    public required IReadOnlyList<ClusterFenceRecord> Fences { get; init; }
}

/// <summary>One section of a member as the read returns it.</summary>
public sealed record ClusterStatusSection
{
    /// <summary>The section's name.</summary>
    public required string Name { get; init; }

    /// <summary>
    /// What the member reported. The keys inside are diagnostic and may change in a minor release. A time or a duration inside is as of the
    /// member's write: subtract <see cref="Age"/> to read it now.
    /// </summary>
    public required JsonElement Values { get; init; }

    /// <summary>The time since the member wrote it, on the database's clock.</summary>
    public required TimeSpan Age { get; init; }
}

/// <summary>A fence as the registry records it.</summary>
public sealed record ClusterFenceRecord
{
    /// <summary>The epoch that fenced.</summary>
    public required long Epoch { get; init; }

    /// <summary>The trigger's name. A 1.x minor release may add values: a caller treats a value it does not know as unknown.</summary>
    public required string Trigger { get; init; }

    /// <summary>The time the fence began, on the database's clock.</summary>
    public required DateTimeOffset FencedAt { get; init; }

    /// <summary>The epoch's span, from the join to the fence.</summary>
    public required TimeSpan Span { get; init; }
}
