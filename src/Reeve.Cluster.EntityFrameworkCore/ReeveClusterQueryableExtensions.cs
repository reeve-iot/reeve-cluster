namespace Microsoft.EntityFrameworkCore;

/// <summary>Tags EF Core queries with Reeve Cluster annotations.</summary>
public static class ReeveClusterQueryableExtensions
{
    /// <summary>
    /// The tag that marks a bulk query whose affected aggregates are appended manually. Its value is part of the contract: code that
    /// cannot reference this package may pass the same string to <c>TagWith</c>.
    /// </summary>
    public const string AppendedToChangeLogTag = "reeve-cluster: appended to change log";

    /// <summary>
    /// Marks a bulk query whose affected aggregates the caller appends with <c>IChangeLog.Append</c>. Only a bulk operation that the
    /// capture would have recorded as a tracked save needs it: a delete, or an update of a counted, key or owned column. An update of only
    /// non-counted columns needs neither the append nor the marker.
    /// </summary>
    /// <param name="source">The query to mark.</param>
    /// <returns>The query with the change-log marker tag.</returns>
    public static IQueryable<T> AppendedToChangeLog<T>(this IQueryable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.TagWith(AppendedToChangeLogTag);
    }
}
