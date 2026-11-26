namespace Reeve.Cluster;

/// <summary>
/// An operator's skip for one aggregate type, run directly: its open and its close.
/// The skip endpoint of the ASP.NET Core package runs the same open, and a refusal answers the same way on either
/// path (<c>409 skip_covers_acknowledged</c> with <c>ackedSeq</c>). For a management command that runs without
/// joining the cluster: resolve it from a container built with <c>AddReeveCluster</c> whose host is never started — no hosted
/// service runs, so the process never joins, loads nothing and writes no <c>member</c> row; the calls need only the database.
/// The open neither reads nor waits for derived state, so it also answers on a replica that is fenced or behind. Each call is one transaction of its own, bounded by <c>FencedTransactionTimeout</c> until its commit;
/// the commit's wait for a synchronous standby has no bound but the caller's token, which no timeout of the library's
/// cancels (a cancelled wait commits on the primary all the same). A call cancelled before it returns throws
/// <see cref="OperationCanceledException"/>, and its outcome is unknown: the open may have committed its range, a close its
/// repair entry. The repair entry is appended like any write, so a close made in a client request's scope on a fenced
/// replica is refused as that request's other commits are.
/// <list type="bullet">
/// <item>A close of an id no skip has throws <see cref="KeyNotFoundException"/>.</item>
/// <item>A close of a closed skip appends nothing and returns its repair entry's seq again, so a retry is safe.</item>
/// </list>
/// </summary>
public interface IChangeSkip
{
    /// <summary>
    /// Opens a skip: one transaction of its own that locks <c>change_ack</c>, refuses a <see cref="SkipRequest.FromSeq"/>
    /// at or below <c>acked_seq</c> whatever the type, and otherwise inserts the range.
    /// </summary>
    Task<SkipOpenResult> OpenAsync(SkipRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the skip: writes <c>closed_by</c> and <c>closed_at</c>, appends the type's repair entry and fills
    /// <c>until_seq</c> with its seq.
    /// </summary>
    /// <returns>The repair entry's seq: the range's <c>until_seq</c>, which the range does not cover.</returns>
    /// <exception cref="KeyNotFoundException">No skip has the id.</exception>
    Task<long> CloseAsync(long skipId, string closedBy, CancellationToken cancellationToken = default);
}

/// <summary>A request to open a skip.</summary>
/// <param name="AggregateType">The type that appliers will treat as applied.</param>
/// <param name="FromSeq">The first seq the range covers; the open never raises it.</param>
/// <param name="Reason">Why the operator skips; recorded on the row.</param>
/// <param name="OpenedBy">Who opens it; recorded on the row.</param>
public sealed record SkipRequest(string AggregateType, long FromSeq, string Reason, string OpenedBy);

/// <summary>The result of an open: the new skip, or the refusal.</summary>
public abstract record SkipOpenResult
{
    private SkipOpenResult()
    {
    }

    /// <summary>The range was inserted.</summary>
    /// <param name="SkipId">The <c>change_skip.id</c> of the new row.</param>
    public sealed record Opened(long SkipId) : SkipOpenResult;

    /// <summary>
    /// Refused because <see cref="SkipRequest.FromSeq"/> is at or below <c>acked_seq</c>: a skip must not cover a write
    /// acknowledged without naming it. Nothing was written.
    /// </summary>
    /// <param name="AckedSeq">The <c>acked_seq</c> the open read; a range must start above it.</param>
    public sealed record Refused(long AckedSeq) : SkipOpenResult;
}
