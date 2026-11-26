namespace Reeve.Cluster;

/// <summary>
/// Single-flight on PostgreSQL: one computation per key is started at a time, and
/// its result is stored with a time to live so that callers on any replica read it. It bounds starts, not executions: a computation
/// whose claim ended may still run, its result is refused at the store, and the next caller computes again. A caller that waits
/// for another's computation reads the key every 100 ms, trying to claim it first, so many waiters on one key cost the database a short
/// transaction each per interval.
/// </summary>
public interface IClusterSingleFlight
{
    /// <summary>
    /// Returns the stored result of <paramref name="key"/> when there is one, waits for the computation in flight when another
    /// caller holds the key, and otherwise runs <paramref name="compute"/> and stores its result. Only a serving replica starts a
    /// computation. A claim that ended without a result (its time to live passed, its holder's lease ended) is taken over.
    /// </summary>
    /// <param name="key">The computation's key, separate from the plain claim and once-run key spaces.</param>
    /// <param name="compute">The computation. It runs with the caller's ambient transaction suppressed; its exception propagates and the claim is released.</param>
    /// <param name="claimTtl">How long the computation's claim is valid; its result is refused once the claim ended.</param>
    /// <param name="resultTtl">How long the stored result is read by other callers.</param>
    /// <param name="cancellationToken">Ends the call, and is the computation's token; a statement it interrupts has an unknown outcome and the call throws.</param>
    /// <exception cref="InvalidOperationException">This process has an open claim-fenced transaction on the key's claim.</exception>
    /// <exception cref="System.Data.Common.DbException">A database statement failed.</exception>
    Task<SingleFlightResult> RunAsync(
        string key, Func<CancellationToken, Task<byte[]>> compute, TimeSpan claimTtl, TimeSpan resultTtl, CancellationToken cancellationToken = default);
}

/// <summary>
/// What <see cref="IClusterSingleFlight.RunAsync"/> answered. <see cref="Computed"/> and <see cref="Waited"/> say the cluster holds the
/// result; <see cref="ComputedRefused"/> and <see cref="ComputedUnknown"/> carry the value this call computed without that. A caller
/// that tolerates a late computation uses the value of any of the four.
/// </summary>
public abstract record SingleFlightResult
{
    private protected SingleFlightResult()
    {
    }

    /// <summary>The value of the answers that carry one; <see langword="false"/> for <see cref="NotServing"/> and <see cref="Unknown"/>.</summary>
    /// <param name="value">The value, or <see langword="null"/>.</param>
    public bool TryGetValue(out byte[]? value)
    {
        value = this switch
        {
            Computed c => c.Value,
            ComputedRefused r => r.Value,
            ComputedUnknown u => u.Value,
            Waited w => w.Value,
            _ => null,
        };
        return value is not null;
    }

    /// <summary>This call computed the value and stored it under a claim that was valid when the result was written.</summary>
    /// <param name="Value">The computed value.</param>
    public sealed record Computed(byte[] Value) : SingleFlightResult;

    /// <summary>
    /// This call computed the value and the store refused it: the claim ended, the replica no longer served, or the database was on
    /// another timeline. Nothing was stored. A claim that ended computes again for the next caller; a claim kept because
    /// this replica stopped serving lets the next caller compute after <c>claimTtl</c> or the end of the holder's lease.
    /// </summary>
    /// <param name="Value">The value this call computed.</param>
    public sealed record ComputedRefused(byte[] Value) : SingleFlightResult;

    /// <summary>
    /// This call computed the value and could not tell whether the result was stored: the library's bound cancelled the write, or it
    /// failed. The result may be visible.
    /// </summary>
    /// <param name="Value">The value this call computed.</param>
    public sealed record ComputedUnknown(byte[] Value) : SingleFlightResult;

    /// <summary>Another caller computed the value; this call read it from the store and computed nothing.</summary>
    /// <param name="Value">The stored value.</param>
    public sealed record Waited(byte[] Value) : SingleFlightResult;

    /// <summary>The replica is not serving, so it starts no computation; nothing was written.</summary>
    public sealed record NotServing : SingleFlightResult;

    /// <summary>
    /// The library cancelled a grant, takeover or read before it returned: no computation was started here, and a claim may exist.
    /// </summary>
    public sealed record Unknown : SingleFlightResult;
}
