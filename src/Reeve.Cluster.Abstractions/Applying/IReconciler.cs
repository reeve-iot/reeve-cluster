namespace Reeve.Cluster;

/// <summary>
/// The application's function for one aggregate type: it reads the type's state from the pass's snapshot and
/// returns the next derived state. It publishes nothing itself. The contract:
/// <list type="bullet">
/// <item>A <b>domain fault</b> — an invalid configuration, a connection that cannot be opened — is a state the
/// reconciler records and returns, not an exception.</item>
/// <item>A reconciler that throws, or does not return within <c>ReconcileTimeout</c>, <b>stalls</b>: the applier
/// retries it with backoff, without hints. Slow success is write latency, not a stall.</item>
/// <item>Slow parts — starting a component, fetching an artifact — run after it returns, as
/// <see cref="ReconcileResult.Deferred"/>, under the type state's <c>Superseded</c> token.</item>
/// <item>Every command it sends has a bounded <c>CommandTimeout</c>, never 0.</item>
/// <item>It never waits for an acknowledgement: <see cref="IChangeAck.WaitAppliedAsync"/> from its own execution
/// context answers <see cref="NotAwaitedReason.Applier"/>.</item>
/// <item>Cancellation is cooperative: honour the token, and expect an attempt that ignores it to end the process.</item>
/// </list>
/// Reconcilers are registered in the order the application declares; the applier runs the dirty ones in that order.
/// A type whose state is derived from another's declares it in <see cref="DependsOn"/>.
/// </summary>
public interface IReconciler
{
    /// <summary>The aggregate type this reconciler owns; unique across the registered reconcilers.</summary>
    string AggregateType { get; }

    /// <summary>
    /// The aggregate types whose derived state this reconciler reads, each registered before this one. A pass that
    /// reconciles any of them reconciles this type too, in the same pass and without hints, and publishes both in one swap, so
    /// a generation never holds this type's state built from a state of theirs it does not hold. The reconciler reads their
    /// state through <see cref="ReconcileContext.GetDependency(string)"/>. Read once, at start-up. A decorator forwards it: a
    /// wrapper that does not answers none, and the inner reconciler's <c>GetDependency</c> then throws. A dependency that is
    /// null, unknown or not registered earlier — a cycle included — refuses the start with a
    /// <see cref="ClusterConfigurationException"/>.
    /// </summary>
    IReadOnlyList<string> DependsOn => [];

    /// <summary>Reads the snapshot and returns the type's next derived state.</summary>
    /// <param name="context">The pass's snapshot and the hints.</param>
    /// <param name="cancellationToken">Cancelled when the attempt times out or its pass is abandoned.</param>
    Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken);
}

/// <summary>What a reconciler returns: the type's next derived state, and the slow work that follows it.</summary>
/// <param name="State">The next derived state of the type; never <see langword="null"/>.</param>
public sealed record ReconcileResult(object State)
{
    /// <summary>
    /// Slow work to run after the generation that holds <see cref="State"/> is published. It receives that generation
    /// and a token that is its type's <c>Superseded</c> token; it makes its result observable only through
    /// <see cref="IGeneration.TryActivate"/>. Dropped with the attempt when the attempt's result is discarded.
    /// </summary>
    public Func<IGeneration, CancellationToken, Task>? Deferred { get; init; }
}
