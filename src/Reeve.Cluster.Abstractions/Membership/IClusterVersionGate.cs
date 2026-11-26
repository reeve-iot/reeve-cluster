namespace Reeve.Cluster;

/// <summary>
/// The version gate: the durable floors a member must meet to activate, and the observations over the
/// members. A feature gates new behaviour on <see cref="RequireProtocolAsync"/> or <see cref="RequireVersionAsync"/>, so a
/// feature says "after release N" once, and the answer is durable: it does not revert when an old binary appears later, because
/// such a member can no longer activate. A raise takes the change-log lock, which every append and every activation waits for.
/// The statements before the commit are bounded by <c>RenewTimeout</c> + 1 s on the server (<c>transaction_timeout</c>): a raise
/// that waits longer for the lock or for the cluster row throws Npgsql's <c>PostgresException</c> (SQLSTATE <c>25P04</c>) and
/// changes nothing, and the caller retries. The commit's wait for the synchronous standby is not under that bound; only the
/// caller's token ends it, and a cancelled commit throws <see cref="OperationCanceledException"/> with an unknown outcome. A raise that waits behind a stalled retention run is bounded in the same way: a stated residual.
/// </summary>
public interface IClusterVersionGate
{
    /// <summary>
    /// Answers <see langword="true"/> when the protocol floor is already at or above <paramref name="protocol"/>, or when it can
    /// raise it: one transaction takes the change-log lock, checks that every member with a valid lease, <c>joining</c> and
    /// <c>active</c>, speaks at least <paramref name="protocol"/>, sets the floor and commits. Otherwise <see langword="false"/>,
    /// and nothing was written.
    /// </summary>
    /// <remarks>
    /// <paramref name="protocol"/> is the library's protocol number. A raise above the protocol of the calling binary is refused,
    /// because it would refuse every member including the caller. With no member holding a lease (a management container, a start
    /// before the first join) the database accepts any floor not above that.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="protocol"/> is negative or above the protocol of the calling binary.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled; the floor may or may not have been raised.</exception>
    Task<bool> RequireProtocolAsync(int protocol, CancellationToken cancellationToken = default);

    /// <summary>
    /// Answers <see langword="true"/> when the version floor is already at or above <paramref name="version"/> by SemVer 2.0
    /// precedence, or when it can raise it: the same transaction as <see cref="RequireProtocolAsync"/>, checking the leased
    /// <c>joining</c> and <c>active</c> members' versions. A member whose version is not SemVer 2.0 is not at or above
    /// <paramref name="version"/>. Answers <see langword="false"/>, and writes nothing, when a member is below, when the stored
    /// floor is not SemVer 2.0, or when the floor was lowered by the management command while the raise ran.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="version"/> is not SemVer 2.0 (the floor the library writes always parses), or is above the version of the calling binary (the raise would refuse the caller too).</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled; the floor may or may not have been raised.</exception>
    Task<bool> RequireVersionAsync(string version, CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether every member with a valid lease, <c>joining</c> or <c>active</c>, speaks at least <paramref name="protocol"/>.
    /// An observation, for status and for decisions that are reversible: it authorises nothing that an old member joining later
    /// could break. A <c>fenced</c> or <c>draining</c> row, or one whose lease has expired, pins nothing. No lock, no write.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="protocol"/> is negative.</exception>
    Task<bool> AllMembersSpeakAsync(int protocol, CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether every member with a valid lease, <c>joining</c> or <c>active</c>, runs at least <paramref name="version"/> by SemVer
    /// 2.0 precedence; a member whose version is not SemVer 2.0 is not at least it. An observation, as
    /// <see cref="AllMembersSpeakAsync"/> is.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="version"/> is not SemVer 2.0.</exception>
    Task<bool> AllMembersAtLeastAsync(string version, CancellationToken cancellationToken = default);
}

/// <summary>
/// How a lowering of a floor ended. A 1.x minor release may add values: a caller treats a value it does not know as an outcome it does not know.
/// </summary>
public enum FloorLowering
{
    /// <summary>The floor was above the value and is now at it; who lowered it is recorded.</summary>
    Lowered,

    /// <summary>The floor was already at or below the value: the command only lowers, and nothing was written.</summary>
    NotAbove,

    /// <summary>The floor moved between the command's read and its write: nothing was written, and the command can be called again.</summary>
    Changed,
}

/// <summary>
/// The management command that lowers a floor: for an operator who has undone what a raise
/// authorised. The library itself never lowers a floor. Like <see cref="IChangeSkip"/> it runs without joining the cluster:
/// resolve it from a container built with <c>AddReeveCluster</c> whose host is never started; the calls need only the database,
/// but the container's options still need a <c>ReplicaId</c>, which no member of the cluster uses. It writes the cluster row directly, outside any lease and without the change-log lock, and records who moved the floor and
/// when, which the status registry shows as each member's last activation read the floors. An activation guards its write on the version floor it read, so a lowering of
/// the version floor that lands during one leaves that activation abandoned, and the member retries; the protocol floor is tested in
/// the activation's own <c>UPDATE</c>, so a lowering of it in that window admits the member. A feature that asks <c>Require…</c> on
/// every call raises the floor again at its next call: lower the floor while no member that asks holds a lease (an older member
/// that is <c>joining</c> holds one, and a newer member's raise is refused while it does), or after the members that ask have stopped.
/// </summary>
public interface IClusterFloorCommand
{
    /// <summary>Lowers the protocol floor to <paramref name="protocol"/>, if it is above it.</summary>
    /// <param name="protocol">The new floor, not negative.</param>
    /// <param name="loweredBy">Who lowers it; recorded.</param>
    /// <param name="cancellationToken">The call's token.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="protocol"/> is negative.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="loweredBy"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="loweredBy"/> is empty or whitespace.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled; the floor may or may not have been lowered.</exception>
    Task<FloorLowering> LowerProtocolFloorAsync(int protocol, string loweredBy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lowers the version floor to <paramref name="version"/> by SemVer 2.0 precedence, if it is above it; <see langword="null"/>
    /// clears it, which admits every version. A stored floor that is not SemVer 2.0 is replaced by any valid value.
    /// </summary>
    /// <param name="version">The new floor: SemVer 2.0, or <see langword="null"/> for none.</param>
    /// <param name="loweredBy">Who lowers it; recorded.</param>
    /// <param name="cancellationToken">The call's token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="loweredBy"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="version"/> is not SemVer 2.0, or <paramref name="loweredBy"/> is empty or whitespace.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled; the floor may or may not have been lowered.</exception>
    Task<FloorLowering> LowerVersionFloorAsync(string? version, string loweredBy, CancellationToken cancellationToken = default);
}
