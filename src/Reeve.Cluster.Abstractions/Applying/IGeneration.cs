namespace Reeve.Cluster;

/// <summary>
/// An immutable set of the derived state of every aggregate type, built from one database snapshot and standing for
/// the log head visible in it. One generation is current; publishing the next replaces it in one atomic swap.
/// A reader pins one and reads everything from it.
/// </summary>
public interface IGeneration
{
    /// <summary>The log head, <c>Hs</c>, this generation stands for: it reflects every entry at or below it that no open skip covers, and none above.</summary>
    long Head { get; }

    /// <summary>The <c>entry_token</c> of the entry at <see cref="Head"/>; <see langword="null"/> for an empty log.</summary>
    Guid? HeadToken { get; }

    /// <summary>The derived state of <paramref name="aggregateType"/> in this generation.</summary>
    /// <exception cref="KeyNotFoundException">No reconciler of that type is registered.</exception>
    object Get(string aggregateType);

    /// <summary>
    /// The token cancelled when a later generation replaces this type's state. Shared by every generation that shares
    /// the state: a type that was not dirty in a pass keeps its token. Deferred work of a reconciler runs under it.
    /// </summary>
    /// <exception cref="KeyNotFoundException">No reconciler of that type is registered.</exception>
    CancellationToken Superseded(string aggregateType);

    /// <summary>
    /// Runs <paramref name="activate"/> only while this type's state is still current, atomically with the swap, so
    /// there is no window between a check and the act. The only way deferred work
    /// becomes observable. The library keeps the guarantee for what it publishes, the application for what it
    /// activates itself through this call.
    /// </summary>
    /// <remarks>
    /// <paramref name="activate"/> runs inside the swap's atomic section: it must be short, must not block, and must not
    /// call back into the applier or another <c>TryActivate</c>. An exception it throws propagates to the caller and the
    /// activation did not happen; the swap itself is not affected.
    /// </remarks>
    /// <returns><see langword="true"/> when the state was current and <paramref name="activate"/> ran; <see langword="false"/> once it is superseded.</returns>
    /// <exception cref="KeyNotFoundException">No reconciler of that type is registered.</exception>
    bool TryActivate(string aggregateType, Action activate);
}

/// <summary>Typed reads of an <see cref="IGeneration"/>.</summary>
public static class GenerationExtensions
{
    /// <summary>The derived state of <paramref name="aggregateType"/>, cast to <typeparamref name="T"/>.</summary>
    public static T Get<T>(this IGeneration generation, string aggregateType)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(generation);
        return (T)generation.Get(aggregateType);
    }
}
