using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;

namespace Unit.Log;

public sealed class CursorValidityTests
{
    private static readonly Guid A = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    private static readonly Guid B = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    [Theory]
    [InlineData(5, "a", "a", 0, true)]
    [InlineData(5, "a", "a", 3, true)]
    [InlineData(5, "a", null, 0, false)]
    [InlineData(5, "a", "b", 0, false)]
    [InlineData(5, null, "a", 0, false)]
    [InlineData(0, null, null, 0, true)]
    [InlineData(0, null, null, 4, false)]
    [InlineData(9, "a", null, 0, false)]
    public void I5_a_cursor_is_valid_iff_it_names_a_row_or_is_0_while_the_horizon_is_0(
        long seq, string? token, string? rowToken, long horizon, bool valid)
    {
        Assert.Equal(valid, CursorValidity.IsValid(new Cursor(seq, Token(token)), Token(rowToken), horizon));
    }

    [Fact]
    public void I5_has_applied_needs_a_valid_cursor_at_or_above_the_seq()
    {
        Assert.True(CursorValidity.HasApplied(new Cursor(5, A), A, 0, 5));
        Assert.False(CursorValidity.HasApplied(new Cursor(5, A), A, 0, 6));
        Assert.False(CursorValidity.HasApplied(new Cursor(5, A), null, 0, 1));
    }

    [Fact]
    public void The_poll_starts_a_pass_when_the_head_differs_from_the_cursor_below_it_too_or_the_cursor_names_no_row()
    {
        var local = new Cursor(5, A);

        Assert.False(ApplierPoll.PassNeeded(local, new Cursor(5, A), A, 0));
        Assert.True(ApplierPoll.PassNeeded(local, new Cursor(7, B), A, 0));
        Assert.True(ApplierPoll.PassNeeded(local, new Cursor(3, B), null, 0));
        Assert.True(ApplierPoll.PassNeeded(local, new Cursor(5, B), B, 0));
        Assert.True(ApplierPoll.PassNeeded(local, new Cursor(9, B), null, 0));
        Assert.False(ApplierPoll.PassNeeded(Cursor.Empty, Cursor.Empty, null, 0));
        Assert.True(ApplierPoll.PassNeeded(Cursor.Empty, Cursor.Empty, null, 2));
    }

    [Theory]
    [InlineData("Host=db;Database=app", 30)]
    [InlineData("Host=db;Database=app;Command Timeout=0", 30)]
    [InlineData("Host=db;Database=app;Command Timeout=7", 7)]
    public async Task The_pass_connections_have_a_bounded_CommandTimeout_never_0_and_are_not_pooled(string connectionString, int expected)
    {
        await using var source = new ApplierDataSource(Options.Create(new ClusterOptions { ConnectionString = connectionString }));

        var built = new NpgsqlConnectionStringBuilder(source.Source.DataSource.ConnectionString);
        Assert.Equal(expected, source.CommandTimeout);
        Assert.Equal(expected, built.CommandTimeout);
        Assert.False(built.Pooling);
    }

    private static Guid? Token(string? name) => name switch
    {
        null => null,
        "a" => A,
        _ => B,
    };
}
