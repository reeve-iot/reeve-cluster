using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;

namespace Unit.Http;

public sealed class FencedRequestRuleTests
{
    private static TrackerView Client(long[]? seqs = null, bool refused = false, bool optedOut = false, bool open = false) =>
        new(Exempt: false, seqs ?? [], refused, optedOut, open);

    [Fact]
    public void An_exempt_scope_has_nothing_to_add()
    {
        var plan = FencedRequestRule.Plan(new TrackerView(true, [4], CommitRefused: false, OptedOut: false, OpenTransaction: false));

        Assert.Equal(Verdict.Pass, plan.Settled);
    }

    [Fact]
    public void A_refused_commit_with_no_entry_appended_is_the_fenced_answer_even_when_the_request_opted_out()
    {
        Assert.Equal(new Verdict(VerdictKind.WriteRefused), FencedRequestRule.Plan(Client(refused: true)).Settled);
        Assert.Equal(new Verdict(VerdictKind.WriteRefused), FencedRequestRule.Plan(Client(refused: true, optedOut: true)).Settled);
    }

    [Fact]
    public void A_refused_commit_after_an_append_is_unconfirmed_with_the_highest_seq_and_never_waits()
    {
        var plan = FencedRequestRule.Plan(Client([3, 9, 5], refused: true));

        Assert.Equal(new Verdict(VerdictKind.Unconfirmed, 9), plan.Settled);
        Assert.Equal(new Verdict(VerdictKind.Unconfirmed, 3), FencedRequestRule.Plan(Client([3], refused: true, optedOut: true)).Settled);
    }

    [Fact]
    public void A_request_that_appended_nothing_has_nothing_to_add_whatever_it_opted_out_of()
    {
        Assert.Equal(Verdict.Pass, FencedRequestRule.Plan(Client()).Settled);
        Assert.Equal(Verdict.Pass, FencedRequestRule.Plan(Client(optedOut: true)).Settled);
    }

    [Fact]
    public void An_opted_out_request_is_let_through_at_once_with_its_highest_seq()
    {
        Assert.Equal(new Verdict(VerdictKind.AtCommit, 7), FencedRequestRule.Plan(Client([2, 7], optedOut: true)).Settled);
        Assert.Equal(new Verdict(VerdictKind.AtCommit, 7), FencedRequestRule.Plan(Client([7], optedOut: true, open: true)).Settled);
    }

    [Fact]
    public void An_open_transaction_is_unconfirmed_without_a_wait_and_carries_a_seq_only_when_the_request_appended()
    {
        Assert.Equal(new Verdict(VerdictKind.Unconfirmed, 6), FencedRequestRule.Plan(Client([1, 6], open: true)).Settled);
        Assert.Equal(new Verdict(VerdictKind.Unconfirmed), FencedRequestRule.Plan(Client(open: true)).Settled);
    }

    [Fact]
    public void Otherwise_the_highest_seq_is_awaited_once()
    {
        var plan = FencedRequestRule.Plan(Client([4, 12, 8]));

        Assert.Null(plan.Settled);
        Assert.Equal(12, plan.AwaitSeq);
    }

    [Fact]
    public void An_acknowledgement_passes_and_names_the_skipped_types_when_there_are_any()
    {
        Assert.Equal(Verdict.Pass, FencedRequestRule.Settle(5, AckOutcome.Acknowledged.None));
        var skipped = FencedRequestRule.Settle(5, new AckOutcome.Acknowledged(["a", "b"]));

        Assert.Equal(VerdictKind.Acknowledged, skipped.Kind);
        Assert.Equal(["a", "b"], skipped.Skipped);
    }

    [Theory]
    [InlineData(UnconfirmedReason.Fenced)]
    [InlineData(UnconfirmedReason.Draining)]
    [InlineData(UnconfirmedReason.Timeout)]
    public void Every_unconfirmed_answer_is_unconfirmed_with_the_awaited_seq(UnconfirmedReason reason) =>
        Assert.Equal(new Verdict(VerdictKind.Unconfirmed, 5), FencedRequestRule.Settle(5, new AckOutcome.Unconfirmed(reason)));

    [Fact]
    public void A_not_awaited_answer_is_at_commit_for_an_opt_out_and_otherwise_unconfirmed()
    {
        Assert.Equal(new Verdict(VerdictKind.AtCommit, 5), FencedRequestRule.Settle(5, new AckOutcome.NotAwaited(NotAwaitedReason.AtCommit)));
        foreach (var reason in new[] { NotAwaitedReason.Joining, NotAwaitedReason.Fenced, NotAwaitedReason.Draining, NotAwaitedReason.Applier })
        {
            Assert.Equal(new Verdict(VerdictKind.Unconfirmed, 5), FencedRequestRule.Settle(5, new AckOutcome.NotAwaited(reason)));
        }
    }
}
