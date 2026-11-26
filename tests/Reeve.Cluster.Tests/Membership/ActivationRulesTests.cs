using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class ActivationRulesTests
{
    [Theory]
    [InlineData(true, 0L, 0L, true)]
    [InlineData(true, 3L, 3L, true)]
    [InlineData(true, 2L, 3L, true)]
    [InlineData(true, 4L, 3L, false)]
    [InlineData(true, 1L, 0L, false)]
    [InlineData(false, 0L, 5L, false)]
    public void Caught_up_when_the_entries_after_a_valid_cursor_number_no_more_than_the_last_pass_applied(
        bool cursorValid, long entriesAfter, long threshold, bool expected)
    {
        Assert.Equal(expected, ActivationRules.IsCaughtUp(cursorValid, entriesAfter, threshold));
    }

    [Theory]
    [InlineData("1.0.0", null, "Admitted")]
    [InlineData("1.10.0", "1.9.0", "Admitted")]
    [InlineData("1.9.0", "1.10.0", "BelowVersionFloor")]
    [InlineData("1.0.0-rc.1", "1.0.0", "BelowVersionFloor")]
    [InlineData("1.0.0", "1.0.0-rc.1", "Admitted")]
    [InlineData("1.0.0", "1.0.0", "Admitted")]
    [InlineData("1.0.0+build.9", "1.0.0", "Admitted")]
    [InlineData("not-a-version", "1.0.0", "VersionNotSemVer")]
    [InlineData("not-a-version", null, "Admitted")]
    [InlineData("1.0.0", "garbage", "FloorNotSemVer")]
    public void Admission_compares_the_version_with_the_floor_read_by_SemVer_precedence(string version, string? floor, string expected)
    {
        Assert.Equal(Enum.Parse<Admission>(expected), ActivationRules.Admit(version, floor));
    }

    [Theory]
    [InlineData(1, 2, "RefusedByProtocolFloor")]
    [InlineData(2, 2, "Abandoned")]
    [InlineData(3, 2, "Abandoned")]
    [InlineData(1, 0, "Abandoned")]
    public void Zero_rows_is_the_protocol_floors_refusal_only_when_the_floor_read_under_the_lock_is_above_the_protocol(
        int protocol, int floorRead, string expected)
    {
        Assert.Equal(Enum.Parse<ActivationOutcome>(expected), ActivationRules.OnZeroRows(protocol, floorRead));
    }

    [Theory]
    [InlineData("Committed", "ActivationCommitted")]
    [InlineData("Abandoned", "ActivationAbandoned")]
    [InlineData("RefusedByProtocolFloor", "ActivationRefusedByFloor")]
    [InlineData("RefusedByVersionFloor", "ActivationRefusedByFloor")]
    public void Each_outcome_raises_its_cell_of_the_table(string outcome, string trigger)
    {
        Assert.Equal(Enum.Parse<MemberTrigger>(trigger), ActivationRules.TriggerFor(Enum.Parse<ActivationOutcome>(outcome)));
    }

    [Theory]
    [InlineData("NotCaughtUp")]
    [InlineData("Unanswered")]
    [InlineData("LeftCatchingUp")]
    public void An_outcome_that_changed_nothing_raises_no_trigger(string outcome)
    {
        Assert.Null(ActivationRules.TriggerFor(Enum.Parse<ActivationOutcome>(outcome)));
    }

    [Fact]
    public void Each_floor_refusal_has_its_own_named_reason_and_nothing_else_does()
    {
        Assert.Equal(ActivationRules.ProtocolFloorReason, ActivationRules.ReasonCode(ActivationOutcome.RefusedByProtocolFloor));
        Assert.Equal(ActivationRules.VersionFloorReason, ActivationRules.ReasonCode(ActivationOutcome.RefusedByVersionFloor));
        Assert.NotEqual(ActivationRules.ProtocolFloorReason, ActivationRules.VersionFloorReason);
        foreach (var other in Enum.GetValues<ActivationOutcome>().Where(o => o is not (ActivationOutcome.RefusedByProtocolFloor or ActivationOutcome.RefusedByVersionFloor)))
        {
            Assert.Null(ActivationRules.ReasonCode(other));
        }
    }

    [Theory]
    [InlineData(10L, 10L, true)]
    [InlineData(11L, 10L, true)]
    [InlineData(9L, 10L, false)]
    [InlineData(0L, 0L, true)]
    public void Serving_waits_until_the_applied_cursor_reaches_the_head_read_at_activation(long applied, long head, bool expected)
    {
        Assert.Equal(expected, ActivationRules.HasReachedHead(new Cursor(applied, applied == 0 ? null : Guid.NewGuid()), head));
    }
}
