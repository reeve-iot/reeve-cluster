namespace Reeve.Cluster;

/// <summary>
/// Budgets on PostgreSQL: a count of distinct ids per period, on the database's
/// clock. Periods are consecutive fixed windows counted from the Unix epoch. A retried consume of the same id in the same period is
/// not counted twice. Only a serving replica consumes.
/// </summary>
public interface IClusterBudget
{
    /// <summary>
    /// Counts <paramref name="id"/> against the budget of <paramref name="key"/> for the current period, unless the id was counted in it
    /// already or <paramref name="limit"/> ids were. Each call is a transaction of its own on the library's connection.
    /// </summary>
    /// <param name="key">The budget's key.</param>
    /// <param name="id">The id of the thing consumed; a retry of the same consume carries the same id.</param>
    /// <param name="limit">How many distinct ids the period admits; at least one.</param>
    /// <param name="period">How long a period lasts; its windows start at multiples of it since the Unix epoch; at least one millisecond.</param>
    /// <param name="cancellationToken">Ends the call; a statement it interrupts has an unknown outcome and the call throws.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> is below one or <paramref name="period"/> is below one millisecond.</exception>
    /// <exception cref="System.Data.Common.DbException">A database statement failed.</exception>
    Task<BudgetResult> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken = default);
}

/// <summary>What <see cref="IClusterBudget.ConsumeAsync"/> answered. <see cref="IsAllowed"/> is true for the two answers that admit the id.</summary>
public abstract record BudgetResult
{
    private protected BudgetResult()
    {
    }

    /// <summary>Whether the id is within the budget: <see cref="Granted"/> or <see cref="AlreadyCounted"/>.</summary>
    public bool IsAllowed => this is Granted or AlreadyCounted;

    /// <summary>This call counted the id.</summary>
    /// <param name="Used">The ids counted in the period, this one included.</param>
    public sealed record Granted(int Used) : BudgetResult;

    /// <summary>The id was counted earlier in this period, by this caller's earlier attempt or another; nothing was added.</summary>
    public sealed record AlreadyCounted : BudgetResult;

    /// <summary>The period holds <c>limit</c> ids already, and this id is not among them; it was not counted.</summary>
    public sealed record Exhausted : BudgetResult;

    /// <summary>The replica is not serving, so it consumes nothing; nothing was written.</summary>
    public sealed record NotServing : BudgetResult;

    /// <summary>The library cancelled the consume before it returned. The id may or may not be counted; retrying the same id is safe.</summary>
    public sealed record Unknown : BudgetResult;
}
