namespace Reeve.Cluster;

/// <summary>
/// The local phase of a replica: what the replica itself acts on (readiness, the fence middleware, the read
/// index, whether its units run). Never confused with <see cref="MemberRowState"/>, which peers act on.
/// </summary>
public enum ReplicaPhase
{
    /// <summary>No row of its own yet.</summary>
    Starting,

    /// <summary>First load or reload.</summary>
    Loading,

    /// <summary>Applying the log to its head before activation.</summary>
    CatchingUp,

    /// <summary>Row <c>active</c>, applying up to the activation head.</summary>
    Activated,

    /// <summary>The one phase open to non-exempt client traffic.</summary>
    Serving,

    /// <summary>Fence steps 0–5.</summary>
    Fencing,

    /// <summary>Fence step 6 done.</summary>
    Fenced,

    /// <summary>Graceful stop, drain steps 1–4.</summary>
    Draining,

    /// <summary>The process is leaving.</summary>
    Exited,
}

/// <summary>
/// The state of a member row in the database, which peers act on: a writer's wait set, the placer, <c>Ask</c>
/// resolution, the version gate. Stored in <c>reeve_cluster.member.state</c> as the lower-case name.
/// </summary>
public enum MemberRowState
{
    /// <summary>Joined, loading or catching up; in no wait set.</summary>
    Joining,

    /// <summary>Activated; counted in the wait set while its lease is valid.</summary>
    Active,

    /// <summary>Released its lease at fence step 3.</summary>
    Fenced,

    /// <summary>Wrote <c>draining</c> at drain step 4.</summary>
    Draining,
}
