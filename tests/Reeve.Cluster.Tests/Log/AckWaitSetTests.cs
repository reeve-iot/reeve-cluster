using CsCheck;
using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Log;

public sealed class AckWaitSetTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    private const long Seq = 10;

    private static AckCursor At(long seq) => new(seq, true);

    private static AckMember Counted(long applied, long activatedAt = 4) =>
        new(MemberRowState.Active, Now.AddSeconds(5), activatedAt, At(applied));

    [Fact]
    public void I8_An_active_member_with_a_valid_lease_activated_below_the_seq_is_in_the_wait_set()
    {
        Assert.True(AckWaitSet.Includes(Counted(0), Seq, Now));
    }

    [Theory]
    [InlineData(MemberRowState.Joining)]
    [InlineData(MemberRowState.Fenced)]
    [InlineData(MemberRowState.Draining)]
    public void I8_A_member_that_is_not_active_is_not_in_the_wait_set(MemberRowState state)
    {
        Assert.False(AckWaitSet.Includes(Counted(0) with { State = state }, Seq, Now));
    }

    [Fact]
    public void I8_A_lease_is_valid_only_strictly_after_the_database_clock()
    {
        Assert.False(AckWaitSet.Includes(Counted(0) with { LeaseExpiresAt = Now }, Seq, Now));
        Assert.False(AckWaitSet.Includes(Counted(0) with { LeaseExpiresAt = Now.AddTicks(-1) }, Seq, Now));
        Assert.False(AckWaitSet.Includes(Counted(0) with { LeaseExpiresAt = Now.AddDays(-1) }, Seq, Now));
        Assert.True(AckWaitSet.Includes(Counted(0) with { LeaseExpiresAt = Now.AddTicks(1) }, Seq, Now));
    }

    [Fact]
    public void I22_A_member_activated_at_or_above_the_seq_is_excluded_and_one_below_is_waited_for()
    {
        Assert.True(AckWaitSet.Includes(Counted(0, activatedAt: Seq - 1), Seq, Now));
        Assert.False(AckWaitSet.Includes(Counted(0, activatedAt: Seq), Seq, Now));
        Assert.False(AckWaitSet.Includes(Counted(0, activatedAt: Seq + 1), Seq, Now));
    }

    [Fact]
    public void The_wait_set_does_not_depend_on_the_members_cursor()
    {
        Assert.True(AckWaitSet.Includes(Counted(0) with { Cursor = new AckCursor(0, false) }, Seq, Now));
        Assert.True(AckWaitSet.Includes(Counted(Seq * 5), Seq, Now));
    }

    [Fact]
    public void I5_A_cursor_has_applied_a_seq_only_when_it_is_valid_and_at_or_above_it()
    {
        Assert.True(new AckCursor(Seq, true).HasApplied(Seq));
        Assert.True(new AckCursor(Seq + 1, true).HasApplied(Seq));
        Assert.False(new AckCursor(Seq - 1, true).HasApplied(Seq));
        Assert.False(new AckCursor(Seq, false).HasApplied(Seq));
        Assert.False(new AckCursor(Seq + 100, false).HasApplied(Seq));
        Assert.True(new AckCursor(0, true).HasApplied(0));
    }

    [Fact]
    public void I8_With_no_member_counted_the_write_is_ready_at_the_local_cursor_and_the_bound_is_it()
    {
        Assert.Equal(Seq, AckWaitSet.ReadyBound(Seq, At(Seq), [], Now));
        Assert.Equal(Seq + 7, AckWaitSet.ReadyBound(Seq, At(Seq + 7), [], Now));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(long.MinValue)]
    public void A_seq_below_1_is_refused_because_no_write_has_one(long seq)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => AckWaitSet.ReadyBound(seq, At(5), [], Now));
        Assert.NotNull(AckWaitSet.ReadyBound(1, At(5), [], Now));
    }

    [Fact]
    public void I8_Condition_1_the_local_cursor_must_have_applied_the_seq_by_I5()
    {
        Assert.Null(AckWaitSet.ReadyBound(Seq, At(Seq - 1), [], Now));
        Assert.Null(AckWaitSet.ReadyBound(Seq, new AckCursor(Seq, false), [], Now));
        Assert.Null(AckWaitSet.ReadyBound(Seq, new AckCursor(Seq + 5, false), [Counted(Seq)], Now));
    }

    [Fact]
    public void I8_Condition_2_every_counted_member_must_have_applied_the_seq_by_I5()
    {
        Assert.Null(AckWaitSet.ReadyBound(Seq, At(20), [Counted(Seq), Counted(Seq - 1)], Now));
        Assert.Null(AckWaitSet.ReadyBound(Seq, At(20), [Counted(Seq - 1), Counted(Seq)], Now));
        Assert.Null(AckWaitSet.ReadyBound(Seq, At(20), [Counted(Seq + 5) with { Cursor = new AckCursor(Seq + 5, false) }], Now));
        Assert.Null(AckWaitSet.ReadyBound(Seq, At(20), [Counted(0)], Now));
    }

    [Fact]
    public void I8_A_member_outside_the_wait_set_is_not_waited_for_and_does_not_lower_the_bound()
    {
        AckMember[] outside =
        [
            Counted(3) with { State = MemberRowState.Joining },
            Counted(3) with { State = MemberRowState.Fenced },
            Counted(3) with { State = MemberRowState.Draining },
            Counted(3) with { LeaseExpiresAt = Now },
            Counted(3, activatedAt: Seq),
            Counted(3, activatedAt: Seq + 30),
        ];

        foreach (var member in outside)
        {
            Assert.Equal(12, AckWaitSet.ReadyBound(Seq, At(12), [member, Counted(15)], Now));
        }
    }

    [Fact]
    public void The_bound_is_the_lowest_cursor_among_the_local_one_and_the_counted_members()
    {
        Assert.Equal(12, AckWaitSet.ReadyBound(Seq, At(20), [Counted(15), Counted(12), Counted(30)], Now));
        Assert.Equal(11, AckWaitSet.ReadyBound(Seq, At(11), [Counted(15), Counted(12)], Now));
        Assert.Equal(Seq, AckWaitSet.ReadyBound(Seq, At(Seq), [Counted(Seq), Counted(Seq)], Now));
        Assert.Equal(Seq, AckWaitSet.ReadyBound(Seq, At(30), [Counted(Seq)], Now));
    }

    [Fact]
    public void A_member_is_judged_at_the_clock_passed_in()
    {
        var member = Counted(0) with { LeaseExpiresAt = Now.AddSeconds(5) };

        Assert.Null(AckWaitSet.ReadyBound(Seq, At(20), [member], Now));
        Assert.Equal(20, AckWaitSet.ReadyBound(Seq, At(20), [member], Now.AddSeconds(5)));
    }

    [Fact]
    public void I8_The_bound_is_at_or_above_the_seq_and_at_or_below_every_cursor_counted()
    {
        var gen = Gen.Select(
            Gen.Long[1, 50],
            Gen.Long[0, 60],
            Gen.Select(Gen.Int[0, 3], Gen.Long[0, 60], Gen.Long[0, 60], Gen.Bool, Gen.Int[-2, 2])
                .List[0, 5]);

        gen.Sample(t =>
        {
            var (seq, local, rows) = t;
            var members = rows.Select(r => new AckMember(
                (MemberRowState)r.Item1,
                Now.AddSeconds(r.Item5),
                r.Item3,
                new AckCursor(r.Item2, r.Item4))).ToList();

            var bound = AckWaitSet.ReadyBound(seq, At(local), members, Now);

            var counted = members.Where(m => AckWaitSet.Includes(m, seq, Now)).ToList();
            var ready = local >= seq && counted.All(m => m.Cursor.HasApplied(seq));
            Assert.Equal(ready, bound is not null);
            if (bound is { } x)
            {
                Assert.True(x >= seq);
                Assert.True(x <= local);
                Assert.All(counted, m => Assert.True(x <= m.Cursor.Seq));
                Assert.Contains(x, counted.Select(m => m.Cursor.Seq).Append(local));
            }
        });
    }
}
