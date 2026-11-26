namespace Reeve.Cluster;

/// <summary>Why the library refused the deployment's configuration, an application registration at start-up, or a write, as a stable name.</summary>
public enum ClusterConfigurationReason
{
    /// <summary>
    /// <c>transaction_timeout</c> is not 0, or <c>statement_timeout</c> is not 0, on a connection the library opens or fences.
    /// A non-zero session <c>transaction_timeout</c> arms at <c>BEGIN</c>, and a <c>SET LOCAL</c> inside the
    /// transaction does not re-arm it, so the session's value rather than the library's would bound the transaction.
    /// </summary>
    TransactionTimeoutNotZero,

    /// <summary>The server has no <c>transaction_timeout</c> setting: PostgreSQL 18 or later is required.</summary>
    PostgreSqlTooOld,

    /// <summary>The connection reached a standby; reads go to the primary, always.</summary>
    NotPrimary,

    /// <summary>
    /// A kind declares <c>RedisFence</c> and a Redis primary the Redis package is connected to runs a version below 6.2.5, whose
    /// <c>EXEC</c> may land after a watched key's TTL has passed.
    /// </summary>
    RedisTooOld,

    /// <summary>
    /// The <c>maxmemory-policy</c> is not <c>noeviction</c> where a kind declares <c>RedisFence</c> or a Redis claim, single-flight, budget, counter or slot is registered; the policy could not be read; or
    /// a server the check's connection listed could not be reached; or no primary was connected.
    /// </summary>
    RedisEvictionPolicy,

    /// <summary>
    /// A reconciler's <c>DependsOn</c> is null, or names a type that is null, empty or that no registered reconciler owns.
    /// </summary>
    ReconcilerDependencyUnknown,

    /// <summary>
    /// A reconciler depends on a registered type that is not declared before it: itself, a later one, and so every cycle.
    /// </summary>
    ReconcilerDependencyNotEarlier,

    /// <summary>
    /// A Redis claim, single-flight or budget was registered with a value that is not an <c>OnRedisUnavailable</c> policy, such as
    /// <c>default</c>. The policy is required and is never assumed.
    /// </summary>
    RedisPolicyRequired,

    /// <summary>
    /// Two status section sources report one section name, application sources among them. The section
    /// name is unique within a replica's report.
    /// </summary>
    StatusSectionNameDuplicate,

    /// <summary>
    /// An application status section (<c>AddClusterStatusSection</c>) is named like one of the library's own sections: <c>applier</c>,
    /// <c>ask</c>, <c>floors</c>, <c>lease</c>, <c>owned</c>, <c>phase</c>, <c>releases</c>, <c>replication</c> or <c>skips</c>.
    /// </summary>
    StatusSectionNameReserved,

    /// <summary>A status section source's <c>Name</c> is null, empty or white space.</summary>
    StatusSectionNameInvalid,

    /// <summary>A connection the library opens or fences has a non-zero <c>statement_timeout</c>, which can cancel an unreplicated commit.</summary>
    StatementTimeoutNotZero,

    /// <summary>A mapped aggregate type has no registered reconciler, so replicas can serve stale data.</summary>
    AggregateTypeWithoutReconciler,

    /// <summary>The entry assembly's informational version is not SemVer 2.0.</summary>
    VersionNotSemVer,

    /// <summary>A SignalR hub is mapped while the fence sink is <see cref="NoFenceSink"/>, so a fenced replica could keep serving.</summary>
    HubFilterWithoutFenceSink,

    /// <summary>A mapped entity was written inside an HTTP request outside the live cluster scope opened by <c>UseReeveCluster()</c>.</summary>
    WriteOutsideClusterScope,

    /// <summary>A registered context could not be checked or has mapped entities without a cluster capture interceptor.</summary>
    ContextWithoutCapture,
}

/// <summary>The library refused to start, use a connection, or save because the deployment's configuration or the application's registration breaks one of its assumptions.</summary>
public sealed class ClusterConfigurationException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="reason"/>.</summary>
    public ClusterConfigurationException(ClusterConfigurationReason reason, string message)
        : base(message)
    {
        Reason = reason;
    }

    /// <summary>Creates the exception for <paramref name="reason"/> with the failure that caused the refusal.</summary>
    public ClusterConfigurationException(ClusterConfigurationReason reason, string message, Exception innerException)
        : base(message, innerException)
    {
        ArgumentNullException.ThrowIfNull(innerException);
        Reason = reason;
    }

    /// <summary>The named reason.</summary>
    public ClusterConfigurationReason Reason { get; }
}
