namespace Reeve.Cluster;

/// <summary>
/// Every setting of the library, with its default. A setting whose default
/// derives from others reads the derived value until it is set, so code reads one effective value. The relations
/// between settings and the required settings are checked by the validator at start-up; a replica with an
/// invalid combination refuses to start. While <see cref="ReconcileTimeout"/> is missing, the settings that derive from it
/// (<see cref="ApplyStallLimit"/>, <see cref="AckTimeout"/> and the computed relation bounds) are computed with it as zero;
/// the validator reports the missing setting and judges no relation that reads it.
/// </summary>
public sealed class ClusterOptions
{
    /// <summary>The configuration section the options bind to.</summary>
    public const string SectionName = "Cluster";

    private TimeSpan? applyStallLimit;
    private TimeSpan? ackTimeout;
    private TimeSpan? applyPollInterval;
    private TimeSpan? replicationCheckInterval;
    private TimeSpan? statusReportInterval;
    private TimeSpan? releaseGrace;

    /// <summary>
    /// Connection string of the database that holds the <c>reeve_cluster</c> schema. Required. The target must be
    /// the primary: the validator refuses a standby or <c>any</c> target session attribute. With one host, set no
    /// attribute at all (Npgsql accepts none but <c>any</c> there, and the library checks every connection reached the
    /// primary); with several hosts, set <c>Target Session Attributes=primary</c>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>The member row's <c>replica_id</c>, supplied by the deployment. Required, with no default.</summary>
    public string? ReplicaId { get; set; }

    /// <summary>Writers' wait after a hard crash, and the database outage the fleet rides out. Default 30 s.</summary>
    public TimeSpan LeaseTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Covers the clock assumptions, together with <see cref="StopBudget"/>. Default 3 s.</summary>
    public TimeSpan SafetyMargin { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Time the replica allows itself to stop serving; more than <c>SafetyMargin / 2</c>. Default 5 s.</summary>
    public TimeSpan StopBudget { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Bound of one renewal attempt; it also bounds the join, the activation transaction (whose server-side bound is
    /// <c>RenewTimeout + 1 s</c>, below the default <see cref="FencedTransactionTimeout"/> with the defaults), the drain's row writes and fence step 3.
    /// Default 3 s, which leaves <see cref="RenewIntervalLimit"/> at 6.33 s with the other defaults.
    /// </summary>
    public TimeSpan RenewTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Renewal period; at most <see cref="RenewIntervalLimit"/>. Default 3 s.</summary>
    public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Bound of one reconciler attempt. Required when the application names a reconciler: no default, the application sets it.
    /// With no reconciler, <c>AddReeveCluster</c> sets 10 seconds when it is not set.
    /// </summary>
    public TimeSpan? ReconcileTimeout { get; set; }

    /// <summary>At least <c>2 × ReconcileTimeout</c>, which is its default.</summary>
    public TimeSpan ApplyStallLimit
    {
        get => applyStallLimit ?? 2 * (ReconcileTimeout ?? TimeSpan.Zero);
        set => applyStallLimit = value;
    }

    /// <summary>
    /// Bound of an acknowledgement wait; must exceed <see cref="AckTimeoutRelationBound"/>. Default is that bound
    /// plus <see cref="AckTimeoutDefaultSlack"/>, derived from the relation so it keeps it for every valid setting.
    /// </summary>
    public TimeSpan AckTimeout
    {
        get => ackTimeout ?? AckTimeoutRelationBound + AckTimeoutDefaultSlack;
        set => ackTimeout = value;
    }

    /// <summary>The wait's poll of the member table. Default 1 s.</summary>
    public TimeSpan AckPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The applier's poll of the log head. Default <see cref="AckPollInterval"/>; enters no validated relation.</summary>
    public TimeSpan ApplyPollInterval
    {
        get => applyPollInterval ?? AckPollInterval;
        set => applyPollInterval = value;
    }

    /// <summary>How often a member runs retention, and the free-row sweep its pass of old free <c>owned</c> rows. Default 60 s.</summary>
    public TimeSpan RetentionInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>How often the synchronous-replication check runs after start-up. Default <see cref="LeaseTtl"/>.</summary>
    public TimeSpan ReplicationCheckInterval
    {
        get => replicationCheckInterval ?? LeaseTtl;
        set => replicationCheckInterval = value;
    }

    /// <summary>How often a member writes its status sections. Default <see cref="RenewInterval"/>.</summary>
    public TimeSpan StatusReportInterval
    {
        get => statusReportInterval ?? RenewInterval;
        set => statusReportInterval = value;
    }

    /// <summary>Bound of the read-index wait; part of the HTTP bound. Default 10 s.</summary>
    public TimeSpan ReadIndexTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The shortest time any balancer, proxy or client in front of a replica waits for a response to start.
    /// Required when the ASP.NET Core package is used.
    /// </summary>
    public TimeSpan? ProxyReadTimeout { get; set; }

    /// <summary>
    /// The longest an endpoint's own work takes; the application's declaration. Required when the ASP.NET Core package is used. The
    /// library enforces it on one read only: <see cref="IClusterStatus.ReadAsync"/> (and so the status endpoint) throws
    /// <see cref="TimeoutException"/> past it; it bounds nothing else.
    /// </summary>
    public TimeSpan? ActionTimeBudget { get; set; }

    /// <summary>Bound of a fenced transaction (B). Default 5 s.</summary>
    public TimeSpan FencedTransactionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The placer's pass (B). Default 2 s.</summary>
    public TimeSpan PlacementInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Must equal <see cref="ReleaseGraceRelationValue"/>, which is its default; a different explicit value
    /// is refused by the validator.
    /// </summary>
    public TimeSpan ReleaseGrace
    {
        get => releaseGrace ?? ReleaseGraceRelationValue;
        set => releaseGrace = value;
    }

    /// <summary>No rebalance within it after a membership change (B). Default 30 s.</summary>
    public TimeSpan StabilisationWindow { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>A freed <c>owned</c> row is deleted after it. Default 24 h.</summary>
    public TimeSpan OwnedRowRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How long a once-run's record is kept after its time to live passed, at most 3650 days. Default
    /// <see langword="null"/>: no once-record is deleted. Deleting a record lets its key run again, so "once" then holds for
    /// the call's <c>ttl</c> plus this, and a record of an in-doubt <c>AtMostOnce</c> run goes with the rest. The record of an
    /// <c>AtLeastOnce</c> run that is not complete is kept whatever its age, since its repeat is owed. With the default every once-run
    /// leaves a row for ever: set this when the keys are many.
    /// </summary>
    public TimeSpan? OnceRetention { get; set; }

    /// <summary>
    /// How long a counter may go without an increment before it is deleted, on PostgreSQL by a convergent sweep and on Redis by the key's
    /// expiry, which every increment renews; at most 3650 days. A deleted counter starts again at zero, so the default,
    /// <see langword="null"/>, deletes none. On Redis, unsetting the option does not remove an expiry already set on a live counter, and an
    /// increment without the option does not renew it (a replica without the option): the counter still expires, and
    /// starts at zero, when its last increment under the option is older than the retention. To stop that, set the option to 3650 days or
    /// <c>PERSIST</c> the counter keys. A Redis key with an expiry can be evicted under a <c>volatile-*</c> memory policy: use
    /// <c>noeviction</c> with this option.
    /// </summary>
    public TimeSpan? CounterRetention { get; set; }

    /// <summary>
    /// How old a <c>pending</c> durable command must be, counted from <c>created_at</c>, before the expire sweep may expire it.
    /// One value for the whole fleet. At least the failover bound of every registered kind that
    /// declares an in-doubt policy plus <see cref="PlacementInterval"/>; start-up refuses a lower value and names the one needed.
    /// Default 10 min.
    /// </summary>
    public TimeSpan CommandPendingTtl { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>A <c>done</c>, <c>in_doubt</c> or <c>expired</c> durable command row is deleted by a convergent sweep after it, counted from <c>completed_at</c>. Fleet-wide. Default 24 h.</summary>
    public TimeSpan CommandRetention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Keys per type above which a pass runs the reconciler without hints (A). Default 1000.</summary>
    public int HintLimit { get; set; } = 1000;

    /// <summary>
    /// The wait before a fenced replica rejoins, when the fenced epoch's span lasted less than <see cref="LeaseTtl"/>, counted
    /// from the fence. It applies within one process: a new process joins at once. Default 60 s.
    /// </summary>
    public TimeSpan RejoinBackoff { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// <c>Cluster:CheckSynchronousReplication</c>. On by default: the library warns, in its log and its status section, when the
    /// database has a physical standby and its settings do not make a commit wait for one. The
    /// check never fails readiness.
    /// </summary>
    public bool CheckSynchronousReplication { get; set; } = true;

    /// <summary>The 2 s the <see cref="AckTimeout"/> default adds to the relation's bound.</summary>
    public static TimeSpan AckTimeoutDefaultSlack { get; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// <c>max(LeaseTtl, ReconcileTimeout + ApplyStallLimit + RenewTimeout) + AckPollInterval</c>: <see cref="AckTimeout"/>
    /// must be strictly greater.
    /// </summary>
    public TimeSpan AckTimeoutRelationBound =>
        TimeSpan.FromTicks(Math.Max(
            LeaseTtl.Ticks,
            ((ReconcileTimeout ?? TimeSpan.Zero) + ApplyStallLimit + RenewTimeout).Ticks)) + AckPollInterval;

    /// <summary><c>2 × StopBudget + PlacementInterval</c>, the value the timing relations require of <see cref="ReleaseGrace"/>.</summary>
    public TimeSpan ReleaseGraceRelationValue => (2 * StopBudget) + PlacementInterval;

    /// <summary><c>(LeaseTtl − SafetyMargin − StopBudget − RenewTimeout) / 3</c> in whole ticks, truncated toward zero: the largest valid <see cref="RenewInterval"/>.</summary>
    public TimeSpan RenewIntervalLimit => TimeSpan.FromTicks((LeaseTtl - SafetyMargin - StopBudget - RenewTimeout).Ticks / 3);
}
