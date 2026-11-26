using Reeve.Cluster.Core;

namespace Unit.Log;

public sealed class SkipReadTests
{
    private const long Seq = 10;

    private const long Bound = 14;

    private static SkipRange Range(long from, long? until, string type = "t") => new(type, from, until);

    [Fact]
    public void I30_A_range_open_below_the_bound_is_named()
    {
        Assert.True(SkipRead.Names(Range(Bound - 1, null), Seq, Bound));
        Assert.True(SkipRead.Names(Range(1, null), Seq, Bound));
    }

    [Fact]
    public void I30_A_range_open_from_exactly_the_bound_is_named_and_one_from_above_it_is_not()
    {
        Assert.True(SkipRead.Names(Range(Bound, null), Seq, Bound));
        Assert.False(SkipRead.Names(Range(Bound + 1, null), Seq, Bound));
    }

    [Fact]
    public void I30_A_range_that_starts_between_the_write_and_the_bound_is_named_the_safe_direction()
    {
        Assert.True(SkipRead.Names(Range(Seq + 1, null), Seq, Bound));
        Assert.True(SkipRead.Names(Range(Seq + 1, Bound + 5), Seq, Bound));
    }

    [Fact]
    public void I30_A_closed_range_whose_repair_entry_is_at_or_below_the_awaited_seq_is_not_named()
    {
        Assert.False(SkipRead.Names(Range(1, Seq), Seq, Bound));
        Assert.False(SkipRead.Names(Range(1, Seq - 1), Seq, Bound));
        Assert.False(SkipRead.Names(Range(1, 2), Seq, Bound));
    }

    [Fact]
    public void I30_A_closed_range_whose_repair_entry_is_above_the_awaited_seq_is_named()
    {
        Assert.True(SkipRead.Names(Range(1, Seq + 1), Seq, Bound));
        Assert.True(SkipRead.Names(Range(1, Bound), Seq, Bound));
        Assert.True(SkipRead.Names(Range(1, Bound + 100), Seq, Bound));
    }

    [Fact]
    public void I30_The_read_looks_at_the_range_only_never_at_the_type()
    {
        SkipRange[] ranges = [Range(1, null, "a"), Range(1, Seq, "b"), Range(Bound + 1, null, "c"), Range(Seq, null, "d")];

        Assert.Equal(["a", "d"], SkipRead.NamedTypes(ranges, Seq, Bound));
    }

    [Fact]
    public void The_named_types_are_each_once_in_ordinal_order()
    {
        SkipRange[] ranges = [Range(1, null, "b"), Range(2, null, "a"), Range(3, null, "b"), Range(4, null, "B"), Range(5, null, "a")];

        Assert.Equal(["B", "a", "b"], SkipRead.NamedTypes(ranges, Seq, Bound));
    }

    [Fact]
    public void Nothing_is_named_when_there_is_no_range()
    {
        Assert.Empty(SkipRead.NamedTypes([], Seq, Bound));
    }
}
