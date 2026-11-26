using Reeve.Cluster;

namespace Unit.Log;

public sealed class AckOutcomeTests
{
    [Fact]
    public void Acknowledged_compares_the_skipped_types_by_value()
    {
        Assert.Equal(new AckOutcome.Acknowledged(["a", "b"]), new AckOutcome.Acknowledged(["a", "b"]));
        Assert.Equal(new AckOutcome.Acknowledged(["a"]).GetHashCode(), new AckOutcome.Acknowledged(["a"]).GetHashCode());
        Assert.NotEqual(new AckOutcome.Acknowledged(["a"]), new AckOutcome.Acknowledged(["b"]));
        Assert.NotEqual(new AckOutcome.Acknowledged(["a"]), new AckOutcome.Acknowledged(["a", "b"]));
        Assert.Equal(AckOutcome.Acknowledged.None, new AckOutcome.Acknowledged([]));
    }

    [Fact]
    public void The_reasons_are_the_specs()
    {
        Assert.Equal(["Fenced", "Draining", "Timeout"], Enum.GetNames<UnconfirmedReason>());
        Assert.Equal(["Applier", "AtCommit", "Joining", "Draining", "Fenced"], Enum.GetNames<NotAwaitedReason>());
    }

    [Fact]
    public void The_three_answers_are_told_apart_by_pattern()
    {
        AckOutcome[] outcomes =
        [
            AckOutcome.Acknowledged.None,
            new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout),
            new AckOutcome.NotAwaited(NotAwaitedReason.Joining),
        ];

        Assert.Equal(
            ["acknowledged", "unconfirmed:Timeout", "not-awaited:Joining"],
            outcomes.Select(o => o switch
            {
                AckOutcome.Acknowledged => "acknowledged",
                AckOutcome.Unconfirmed u => $"unconfirmed:{u.Reason}",
                AckOutcome.NotAwaited n => $"not-awaited:{n.Reason}",
                _ => "other",
            }));
    }

    [Fact]
    public void A_skip_refusal_carries_the_acked_seq()
    {
        SkipOpenResult refused = new SkipOpenResult.Refused(41);

        Assert.Equal(41, Assert.IsType<SkipOpenResult.Refused>(refused).AckedSeq);
        Assert.Equal(7, Assert.IsType<SkipOpenResult.Opened>(new SkipOpenResult.Opened(7)).SkipId);
    }
}
