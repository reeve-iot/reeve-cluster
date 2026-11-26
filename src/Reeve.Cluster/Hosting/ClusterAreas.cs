namespace Reeve.Cluster;

/// <summary>
/// The optional areas <c>AddReeveCluster</c> registers. Membership, the change log, the read index and the migrations are
/// always registered. <see cref="Primitives"/>, <see cref="Ask"/> and <see cref="Commands"/> each imply
/// <see cref="OwnedWork"/>, which they run on. A service of an area that is off is not registered: resolving it is a
/// dependency-injection error, and a registration that needs the area (a unit kind, a handler, a once effect, a Redis
/// primitive) refuses the start with a message that names the area. An area turned off on every replica leaves its rows
/// in the database unswept.
/// </summary>
[Flags]
public enum ClusterAreas
{
    /// <summary>No optional area: membership, the change log and the read index only.</summary>
    None = 0,

    /// <summary>
    /// Owned work: the unit runtime, unit kinds and keyed resources (<c>AddClusterUnit</c>, <c>AddClusterKeyedResource</c>),
    /// the placer and the free-row sweep.
    /// </summary>
    OwnedWork = 1,

    /// <summary>
    /// The coordination primitives: claims, once-runs, single-flight, budgets, counters and held slots, and the Redis
    /// primitives. Implies <see cref="OwnedWork"/>.
    /// </summary>
    Primitives = 2,

    /// <summary><c>IClusterAsk</c> and its handlers, and the Redis relay where the Redis package is added. Implies <see cref="OwnedWork"/>.</summary>
    Ask = 4,

    /// <summary><c>IClusterCommands</c>, durable command handlers, the pickup and the command sweep. Implies <see cref="OwnedWork"/>.</summary>
    Commands = 8,

    /// <summary>Every area: what <c>AddReeveCluster</c> registers when no areas are given.</summary>
    All = OwnedWork | Primitives | Ask | Commands,
}
