namespace Reeve.Cluster;

/// <summary>
/// The Redis fence refused a fenced write (<c>FenceRedis</c>): this tenure's raise has not succeeded,
/// or the fence key no longer holds its token — a successor installed, or the key expired or was evicted before the next
/// refresh installed it again. Nothing was written. The first is transient and the second may be final for the tenure; a unit
/// reacts the same way to both: it stops writing to Redis, and tries again after a renewal while it still owns the key (each
/// renewal raises again).
/// </summary>
public sealed class RedisFenceRefusedException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public RedisFenceRefusedException()
        : base("The Redis fence refused the write.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public RedisFenceRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and its cause.</summary>
    public RedisFenceRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
