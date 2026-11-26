using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Log;

public sealed class AckAnswerPolicyTests
{
    private static readonly ClusterScopeKind?[] Callers =
    [
        ClusterScopeKind.Applier,
        ClusterScopeKind.ClientRequest,
        ClusterScopeKind.ExemptRequest,
        ClusterScopeKind.UnitOfWork,
        ClusterScopeKind.Background,
        null,
    ];

    private static AckOutcome? Applier => new AckOutcome.NotAwaited(NotAwaitedReason.Applier);

    private static AckOutcome? AtCommitNot => new AckOutcome.NotAwaited(NotAwaitedReason.AtCommit);

    private static AckOutcome? Joining => new AckOutcome.NotAwaited(NotAwaitedReason.Joining);

    private static AckOutcome? DrainingNot => new AckOutcome.NotAwaited(NotAwaitedReason.Draining);

    private static AckOutcome? FencedNot => new AckOutcome.NotAwaited(NotAwaitedReason.Fenced);

    private static AckOutcome? FencedUnconfirmed => new AckOutcome.Unconfirmed(UnconfirmedReason.Fenced);

    private static Dictionary<ReplicaPhase, AckOutcome?[]> Expected() => new()
    {
        [ReplicaPhase.Starting] = [Applier, Joining, Joining, Joining, Joining, Joining],
        [ReplicaPhase.Loading] = [Applier, Joining, Joining, Joining, Joining, Joining],
        [ReplicaPhase.CatchingUp] = [Applier, Joining, Joining, Joining, Joining, Joining],
        [ReplicaPhase.Activated] = [Applier, Joining, Joining, Joining, Joining, Joining],
        [ReplicaPhase.Serving] = [Applier, null, null, null, null, null],
        [ReplicaPhase.Draining] = [Applier, null, null, DrainingNot, DrainingNot, DrainingNot],
        [ReplicaPhase.Fencing] = [Applier, FencedUnconfirmed, FencedNot, FencedNot, FencedNot, FencedNot],
        [ReplicaPhase.Fenced] = [Applier, FencedUnconfirmed, FencedNot, FencedNot, FencedNot, FencedNot],
        [ReplicaPhase.Exited] = [Applier, FencedUnconfirmed, FencedNot, FencedNot, FencedNot, FencedNot],
    };

    private static string Describe(AckOutcome? outcome) => outcome?.ToString() ?? "waits";

    private static void AssertAnswer(AckOutcome? expected, AckAnswer actual, string because) =>
        Assert.True(Equals(expected, actual.Outcome), $"{because}: expected {Describe(expected)}, got {Describe(actual.Outcome)}");

    [Fact]
    public void The_table_has_a_row_for_every_phase()
    {
        Assert.Equal(Enum.GetValues<ReplicaPhase>().Order(), Expected().Keys.Order());
    }

    [Fact]
    public void I17_Every_phase_and_caller_is_answered_as_the_spec_says()
    {
        foreach (var (phase, row) in Expected())
        {
            for (var i = 0; i < Callers.Length; i++)
            {
                var answer = AckAnswerPolicy.Decide(phase, new AckCaller(Callers[i]));
                AssertAnswer(row[i], answer, $"{phase} + {Callers[i]?.ToString() ?? "no scope"}");
                Assert.Equal(row[i] is null, answer.Waits);
            }
        }
    }

    [Fact]
    public void A_value_that_is_not_a_phase_is_refused()
    {
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => AckAnswerPolicy.Decide((ReplicaPhase)99, new AckCaller(null)));

        Assert.Equal("phase", error.ParamName);
        Assert.Contains("Not a replica phase.", error.Message);
    }

    [Fact]
    public void A_wait_is_not_an_answer()
    {
        Assert.True(AckAnswer.Wait.Waits);
        Assert.Null(AckAnswer.Wait.Outcome);
    }

    [Fact]
    public void The_applier_is_not_awaited_in_every_phase_even_when_it_opted_out_or_was_refused()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>())
        {
            foreach (var atCommit in new[] { false, true })
            {
                foreach (var refused in new[] { false, true })
                {
                    var caller = new AckCaller(ClusterScopeKind.Applier, atCommit, TrackerHoldsSeq: true, refused);
                    AssertAnswer(Applier, AckAnswerPolicy.Decide(phase, caller), $"{phase} applier atCommit={atCommit} refused={refused}");
                }
            }
        }
    }

    [Fact]
    public void An_opted_out_scope_for_a_seq_its_tracker_holds_is_not_awaited_at_once_in_every_phase()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>())
        {
            foreach (var kind in Callers.Where(k => k != ClusterScopeKind.Applier))
            {
                var caller = new AckCaller(kind, AcknowledgedAtCommit: true, TrackerHoldsSeq: true, CommitRefused: false);
                AssertAnswer(AtCommitNot, AckAnswerPolicy.Decide(phase, caller), $"{phase} + {kind?.ToString() ?? "no scope"} at commit");
            }
        }
    }

    [Fact]
    public void The_opt_out_answers_only_while_all_three_of_its_conditions_hold_and_otherwise_the_phase_decides()
    {
        (bool OptedOut, bool Holds, bool Refused)[] variants = [(false, true, false), (true, false, false), (true, true, true)];

        foreach (var (phase, row) in Expected())
        {
            for (var i = 1; i < Callers.Length; i++)
            {
                foreach (var (optedOut, holds, refused) in variants)
                {
                    var caller = new AckCaller(Callers[i], optedOut, holds, refused);
                    AssertAnswer(
                        row[i],
                        AckAnswerPolicy.Decide(phase, caller),
                        $"{phase} + {Callers[i]?.ToString() ?? "no scope"} optedOut={optedOut} holds={holds} refused={refused}");
                }
            }
        }
    }

    [Fact]
    public void I17_An_exempt_request_that_opted_out_records_no_seq_so_its_explicit_wait_answers_by_phase()
    {
        var caller = new AckCaller(ClusterScopeKind.ExemptRequest, AcknowledgedAtCommit: true, TrackerHoldsSeq: false);

        AssertAnswer(null, AckAnswerPolicy.Decide(ReplicaPhase.Serving, caller), "Serving");
        AssertAnswer(FencedNot, AckAnswerPolicy.Decide(ReplicaPhase.Fenced, caller), "Fenced");
        AssertAnswer(Joining, AckAnswerPolicy.Decide(ReplicaPhase.CatchingUp, caller), "CatchingUp");
    }

    [Fact]
    public void A_request_in_a_drain_waits_whether_client_or_exempt_and_a_unit_does_not()
    {
        Assert.True(AckAnswerPolicy.Decide(ReplicaPhase.Draining, new AckCaller(ClusterScopeKind.ClientRequest)).Waits);
        Assert.True(AckAnswerPolicy.Decide(ReplicaPhase.Draining, new AckCaller(ClusterScopeKind.ExemptRequest)).Waits);
        Assert.Equal(DrainingNot, AckAnswerPolicy.Decide(ReplicaPhase.Draining, new AckCaller(ClusterScopeKind.UnitOfWork)).Outcome);
    }

    [Fact]
    public void Only_a_client_request_is_told_unconfirmed_when_fenced()
    {
        foreach (var phase in new[] { ReplicaPhase.Fencing, ReplicaPhase.Fenced })
        {
            foreach (var kind in Callers.Where(k => k is not null and not ClusterScopeKind.Applier))
            {
                var outcome = AckAnswerPolicy.Decide(phase, new AckCaller(kind)).Outcome;
                Assert.Equal(kind == ClusterScopeKind.ClientRequest, outcome is AckOutcome.Unconfirmed);
            }
        }
    }

    [Fact]
    public void A_client_request_that_ran_through_a_fences_step_2_is_told_unconfirmed_in_every_phase()
    {
        foreach (var phase in Enum.GetValues<ReplicaPhase>())
        {
            var caller = new AckCaller(ClusterScopeKind.ClientRequest, AcknowledgedAtCommit: true, TrackerHoldsSeq: true, RanThroughStep2: true);

            AssertAnswer(FencedUnconfirmed, AckAnswerPolicy.Decide(phase, caller), phase.ToString());
        }
    }

    [Fact]
    public void Only_a_client_request_is_answered_by_the_step_2_row()
    {
        foreach (var kind in Callers.Where(k => k is not ClusterScopeKind.ClientRequest))
        {
            Assert.True(AckAnswerPolicy.Decide(ReplicaPhase.Serving, new AckCaller(kind, RanThroughStep2: true)).Outcome is not AckOutcome.Unconfirmed, kind?.ToString());
        }
    }
}
