namespace Reeve.Cluster;

/// <summary>What a once-run does when its executor's record is no longer valid.</summary>
public enum OnceMode
{
    /// <summary>A run whose record fails <c>claim_valid</c> for its own token is reported <see cref="OnceResult.InDoubt"/> and never repeated.</summary>
    AtMostOnce,

    /// <summary>
    /// A convergent sweeper unit re-takes such a run and repeats it, so the effect must be idempotent and must read what it needs
    /// from the primary: the repeat runs without a generation.
    /// </summary>
    AtLeastOnce,
}

/// <summary>
/// Once-runs on PostgreSQL: the effect named by <c>effect</c> runs once for the cluster.
/// The once-record names its executor, <c>(replica_id, epoch)</c>. Exactly once is not offered for an external effect. The record is kept
/// for ever unless <c>ClusterOptions.OnceRetention</c> is set; then its key can run again that long after the call's <c>ttl</c> passed.
/// </summary>
public interface IClusterOnce
{
    /// <summary>
    /// Runs the effect registered as <paramref name="effect"/> (<c>AddClusterOnceEffect</c>) for <paramref name="key"/>, unless the key's
    /// run is complete, valid under an executor, or in doubt. The key identifies the run; its stored mode and effect are not compared
    /// with this call. Only a serving replica starts a run.
    /// </summary>
    /// <param name="key">The run's key, separate from the plain claim key space.</param>
    /// <param name="effect">The name the effect is registered under on every replica.</param>
    /// <param name="mode">What happens to a run that does not complete.</param>
    /// <param name="ttl">How long the executor's record is valid; the effect should finish within it.</param>
    /// <param name="cancellationToken">Ends the call; it is also the effect's token. A grant or completion it interrupts has an unknown outcome and the call throws.</param>
    /// <exception cref="ArgumentException">No effect is registered under <paramref name="effect"/>.</exception>
    /// <exception cref="System.Data.Common.DbException">A database statement failed.</exception>
    /// <remarks>Each effect invocation gets a new service scope. The ambient transaction is suppressed; the effect commits its own work before returning.</remarks>
    Task<OnceResult> RunAsync(string key, string effect, OnceMode mode, TimeSpan ttl, CancellationToken cancellationToken = default);
}

/// <summary>What <see cref="IClusterOnce.RunAsync"/> answered. Only <see cref="Completed"/> says this call ran the effect and its record held to the end.</summary>
public abstract record OnceResult
{
    private protected OnceResult()
    {
    }

    /// <summary>Whether this call ran the effect to its end under a record that stayed valid: this is <see cref="Completed"/>.</summary>
    public bool IsCompleted => this is Completed;

    /// <summary>This call ran the effect, and its record was valid when the completion was written.</summary>
    public sealed record Completed : OnceResult;

    /// <summary>The key's run is complete: another call ran it. The effect did not run.</summary>
    public sealed record AlreadyCompleted : OnceResult;

    /// <summary>Another executor's run is valid; the effect did not run here.</summary>
    /// <param name="Holder">The replica id the read observed.</param>
    public sealed record Running(string Holder) : OnceResult;

    /// <summary>
    /// A record exists and this call could not show it valid or complete: it failed validity, or the read after the conflict was
    /// cancelled. Under <see cref="OnceMode.AtMostOnce"/> it is not repeated; under <see cref="OnceMode.AtLeastOnce"/> the sweeper repeats
    /// an invalid one. The effect may have run.
    /// </summary>
    public sealed record InDoubt : OnceResult;

    /// <summary>The library cancelled this call's grant before it returned. The effect did not run here and a record may not exist; retrying is safe under <see cref="OnceMode.AtLeastOnce"/>. Under <see cref="OnceMode.AtMostOnce"/> a retry may run the effect if no record committed; treat the run as in doubt.</summary>
    public sealed record Unknown : OnceResult;

    /// <summary>The effect ran, but the library could not confirm whether its completion was recorded.</summary>
    public sealed record CompletionUnknown : OnceResult;

    /// <summary>The replica is not serving, so it starts no run; nothing was written.</summary>
    public sealed record NotServing : OnceResult;
}
