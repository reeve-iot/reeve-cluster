namespace Reeve.Cluster;

/// <summary>Counters on PostgreSQL: an atomic increment, each a transaction of its own. An increment grants nothing, so it is not refused on a replica that does not serve.</summary>
public interface IClusterCounter
{
    /// <summary>Adds one to the counter of <paramref name="key"/>, which starts at zero.</summary>
    /// <param name="key">The counter's key.</param>
    /// <param name="cancellationToken">Ends the call; a statement it interrupts has an unknown outcome and the call throws.</param>
    /// <exception cref="System.Data.Common.DbException">A database statement failed.</exception>
    Task<CounterResult> IncrementAsync(string key, CancellationToken cancellationToken = default);
}

/// <summary>What <see cref="IClusterCounter.IncrementAsync"/> answered.</summary>
public abstract record CounterResult
{
    private protected CounterResult()
    {
    }

    /// <summary>This call incremented the counter.</summary>
    /// <param name="Value">The counter's value after this call.</param>
    public sealed record Incremented(long Value) : CounterResult;

    /// <summary>The library cancelled the increment before it returned. It may or may not be counted; an increment is not idempotent, so a retry may count twice.</summary>
    public sealed record Unknown : CounterResult;
}
