namespace Reeve.Cluster.Owned;

internal enum OwnedWriter
{
    Creator,

    Placer,

    Owner,

    EligibleMember,

    CreatorCleanUp,

    ResourceEnd,

    Sweep,
}

internal enum OwnedStatement
{
    None,

    Insert,

    Take,

    RefuseAlreadyOwned,

    RequestRelease,

    Release,

    Clear,

    Sweep,
}

internal sealed record OwnedTransition(string RowId, OwnedCondition From, OwnedWriter? Writer, OwnedStatement Statement, OwnedCondition To);

internal static class OwnedStateMachine
{
    private static readonly IReadOnlyList<OwnedTransition> Cells = Build();

    private static readonly Dictionary<(OwnedWriter, OwnedCondition), OwnedTransition> Index =
        Cells.Where(c => c.Writer is not null).ToDictionary(c => (c.Writer!.Value, c.From));

    public static IReadOnlyList<OwnedTransition> Table => Cells;

    public static OwnedTransition? Decide(OwnedWriter writer, OwnedCondition from) =>
        Index.GetValueOrDefault((writer, from));

    private static List<OwnedTransition> Build()
    {
        var cells = new List<OwnedTransition>();

        void Cell(string row, OwnedCondition from, OwnedWriter? writer, OwnedStatement statement, OwnedCondition to) =>
            cells.Add(new OwnedTransition(row, from, writer, statement, to));

        Cell("O01", OwnedCondition.Absent, OwnedWriter.Creator, OwnedStatement.Insert, OwnedCondition.Owned);

        Cell("O02", OwnedCondition.Free, OwnedWriter.Creator, OwnedStatement.Take, OwnedCondition.Owned);
        Cell("O02", OwnedCondition.Orphaned, OwnedWriter.Creator, OwnedStatement.Take, OwnedCondition.Owned);
        Cell("O02", OwnedCondition.Owned, OwnedWriter.Creator, OwnedStatement.RefuseAlreadyOwned, OwnedCondition.Owned);
        Cell("O02", OwnedCondition.ReleaseRequested, OwnedWriter.Creator, OwnedStatement.RefuseAlreadyOwned, OwnedCondition.ReleaseRequested);

        Cell("O03", OwnedCondition.Absent, OwnedWriter.Placer, OwnedStatement.Insert, OwnedCondition.Owned);
        Cell("O03", OwnedCondition.Free, OwnedWriter.Placer, OwnedStatement.Take, OwnedCondition.Owned);

        Cell("O04", OwnedCondition.Owned, OwnedWriter.Placer, OwnedStatement.RequestRelease, OwnedCondition.ReleaseRequested);

        Cell("O05", OwnedCondition.Owned, OwnedWriter.Owner, OwnedStatement.Release, OwnedCondition.Free);
        Cell("O05", OwnedCondition.ReleaseRequested, OwnedWriter.Owner, OwnedStatement.Release, OwnedCondition.Free);

        Cell("O06", OwnedCondition.ReleaseRequested, null, OwnedStatement.None, OwnedCondition.ReleaseRequested);

        Cell("O07", OwnedCondition.Owned, null, OwnedStatement.None, OwnedCondition.Orphaned);
        Cell("O07", OwnedCondition.ReleaseRequested, null, OwnedStatement.None, OwnedCondition.Orphaned);

        Cell("O08", OwnedCondition.Orphaned, OwnedWriter.Placer, OwnedStatement.Take, OwnedCondition.Owned);
        Cell("O08", OwnedCondition.Orphaned, OwnedWriter.EligibleMember, OwnedStatement.Take, OwnedCondition.Owned);

        Cell("O09", OwnedCondition.Orphaned, null, OwnedStatement.None, OwnedCondition.Orphaned);

        Cell("O10", OwnedCondition.Orphaned, OwnedWriter.CreatorCleanUp, OwnedStatement.Clear, OwnedCondition.Free);

        Cell("O11", OwnedCondition.Owned, OwnedWriter.ResourceEnd, OwnedStatement.Release, OwnedCondition.Free);

        Cell("O12", OwnedCondition.Free, OwnedWriter.Sweep, OwnedStatement.Sweep, OwnedCondition.Absent);

        return cells;
    }
}
