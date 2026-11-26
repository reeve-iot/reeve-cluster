using Reeve.Cluster.Owned;

namespace Unit.Owned;

public sealed class OwnedStateMachineTests
{
    [Fact]
    public void Every_row_of_the_spec_has_a_cell_in_its_order() =>
        Assert.Equal(
            Enumerable.Range(1, 12).Select(i => $"O{i:D2}"),
            OwnedStateMachine.Table.Select(c => c.RowId).Distinct());

    [Fact]
    public void A_writer_has_at_most_one_cell_per_condition()
    {
        var cells = OwnedStateMachine.Table.Where(c => c.Writer is not null).ToList();
        Assert.Equal(cells.Count, cells.Select(c => (c.Writer, c.From)).Distinct().Count());
    }

    [Theory]
    [InlineData("Creator", "Absent", "Insert", "Owned")]
    [InlineData("Creator", "Free", "Take", "Owned")]
    [InlineData("Creator", "Orphaned", "Take", "Owned")]
    [InlineData("Creator", "Owned", "RefuseAlreadyOwned", "Owned")]
    [InlineData("Creator", "ReleaseRequested", "RefuseAlreadyOwned", "ReleaseRequested")]
    [InlineData("Placer", "Absent", "Insert", "Owned")]
    [InlineData("Placer", "Free", "Take", "Owned")]
    [InlineData("Placer", "Owned", "RequestRelease", "ReleaseRequested")]
    [InlineData("Placer", "Orphaned", "Take", "Owned")]
    [InlineData("Owner", "Owned", "Release", "Free")]
    [InlineData("Owner", "ReleaseRequested", "Release", "Free")]
    [InlineData("EligibleMember", "Orphaned", "Take", "Owned")]
    [InlineData("CreatorCleanUp", "Orphaned", "Clear", "Free")]
    [InlineData("ResourceEnd", "Owned", "Release", "Free")]
    [InlineData("Sweep", "Free", "Sweep", "Absent")]
    public void A_writer_runs_the_statement_of_its_cell(string writer, string from, string statement, string to)
    {
        var cell = OwnedStateMachine.Decide(Enum.Parse<OwnedWriter>(writer), Enum.Parse<OwnedCondition>(from));

        Assert.NotNull(cell);
        Assert.Equal(Enum.Parse<OwnedStatement>(statement), cell.Statement);
        Assert.Equal(Enum.Parse<OwnedCondition>(to), cell.To);
    }

    [Theory]
    [InlineData("Placer", "ReleaseRequested")]
    [InlineData("Owner", "Absent")]
    [InlineData("Owner", "Free")]
    [InlineData("Owner", "Orphaned")]
    [InlineData("EligibleMember", "Absent")]
    [InlineData("EligibleMember", "Free")]
    [InlineData("EligibleMember", "Owned")]
    [InlineData("EligibleMember", "ReleaseRequested")]
    [InlineData("CreatorCleanUp", "Owned")]
    [InlineData("CreatorCleanUp", "Free")]
    [InlineData("ResourceEnd", "ReleaseRequested")]
    [InlineData("ResourceEnd", "Orphaned")]
    [InlineData("Sweep", "Owned")]
    [InlineData("Sweep", "Orphaned")]
    [InlineData("Sweep", "Absent")]
    public void A_writer_has_no_cell_where_the_table_gives_it_none(string writer, string from) =>
        Assert.Null(OwnedStateMachine.Decide(Enum.Parse<OwnedWriter>(writer), Enum.Parse<OwnedCondition>(from)));

    [Fact]
    public void Only_the_owner_and_the_placers_request_start_from_a_held_row()
    {
        var held = OwnedStateMachine.Table
            .Where(c => c.Writer is not null && c.From is OwnedCondition.Owned or OwnedCondition.ReleaseRequested)
            .Where(c => c.Statement is not OwnedStatement.RefuseAlreadyOwned)
            .Select(c => (c.Writer, c.Statement))
            .Distinct()
            .ToList();

        Assert.Equal(
            [(OwnedWriter.Placer, OwnedStatement.RequestRelease), (OwnedWriter.Owner, OwnedStatement.Release), (OwnedWriter.ResourceEnd, OwnedStatement.Release)],
            held);
    }

    [Fact]
    public void A_cell_without_a_writer_writes_nothing() =>
        Assert.All(OwnedStateMachine.Table.Where(c => c.Writer is null), c => Assert.Equal(OwnedStatement.None, c.Statement));
}
