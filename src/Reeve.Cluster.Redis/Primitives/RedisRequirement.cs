namespace Reeve.Cluster.Redis.Primitives;

internal sealed record RedisRequirement(string Primitive, bool NeedsVersionFloor = true, bool RefusesUnreachable = false);
