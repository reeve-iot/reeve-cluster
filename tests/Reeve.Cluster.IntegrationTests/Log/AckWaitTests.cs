using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Log;

public sealed class AckWaitTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Answer = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan Pending = TimeSpan.FromMilliseconds(600);

    private readonly PostgresFixture postgres;

    public AckWaitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I17_I22_a_writer_waits_for_a_peer_activated_before_its_commit_and_answers_only_after_its_record()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2", activatedAtSeq: seq - 1);
        var appended = new List<Wakeup>();
        using var _ = rig.Channel.Subscribe(WakeupKind.Appended, appended.Add);

        var wait = rig.Ack.WaitAppliedAsync(seq);

        await AssertPendingAsync(wait);
        Assert.Equal(0L, await rig.AckedSeqAsync());
        await rig.ApplyAsync("r2", seq);

        Assert.Equal(AckOutcome.Acknowledged.None, await wait.WaitAsync(Answer));
        Assert.Equal(seq, await rig.AckedSeqAsync());
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], appended);
        Assert.Equal(["r2"], WaitSet(Assert.Single(rig.EventsOf(LogActions.AckBegin))));
        var done = Assert.Single(rig.EventsOf(LogActions.AckDone));
        Assert.Equal(["r2"], WaitSet(done));
        Assert.Equal(seq, done.State![ObservationKeys.AwaitedSeq]);
    }

    [Fact]
    public async Task I8_a_member_activated_at_or_after_the_seq_and_rows_with_no_valid_active_lease_do_not_hold_the_wait()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("at-seq", activatedAtSeq: seq);
        await rig.MemberAsync("expired", leaseFor: TimeSpan.FromSeconds(-1));
        await rig.MemberAsync("fenced", state: "fenced");
        await rig.MemberAsync("draining", state: "draining");
        await rig.MemberAsync("joining", state: "joining", activatedAtSeq: null);

        Assert.Equal(AckOutcome.Acknowledged.None, await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.Empty(WaitSet(Assert.Single(rig.EventsOf(LogActions.AckDone))));
    }

    [Fact]
    public async Task I30_the_records_bound_is_the_lowest_cursor_the_wait_counted_and_the_wait_wakes_by_its_poll_without_a_notice()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var first = await rig.WriteAsync();
        var second = await rig.WriteAsync();
        var third = await rig.WriteAsync();
        await rig.LocalAtAsync(third);
        await rig.MemberAsync("r2");
        await rig.MemberAsync("r3");
        await rig.ApplyAsync("r3", third, notice: false);

        var wait = rig.Ack.WaitAppliedAsync(first);
        await AssertPendingAsync(wait);
        await rig.ApplyAsync("r2", second, notice: false);

        Assert.IsType<AckOutcome.Acknowledged>(await wait.WaitAsync(Answer));
        Assert.Equal(second, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task An_applied_notice_wakes_the_wait_before_its_poll()
    {
        await using var rig = await AckRig.CreateAsync(postgres, pollInterval: TimeSpan.FromSeconds(15), time: new FakeTimeProvider());
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        var wait = rig.Ack.WaitAppliedAsync(seq);
        await AssertPendingAsync(wait);

        await rig.ApplyAsync("r2", seq);

        Assert.IsType<AckOutcome.Acknowledged>(await wait.WaitAsync(Answer));
    }

    [Fact]
    public async Task I5_I31_a_cursor_that_names_no_row_counts_as_behind_and_the_wait_ends_in_Unconfirmed_Timeout_at_AckTimeout()
    {
        var ackTimeout = TimeSpan.FromSeconds(60);
        var time = new FakeTimeProvider();
        await using var rig = await AckRig.CreateAsync(postgres, ackTimeout, time: time);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        await rig.ApplyAsync("r2", seq, token: Guid.NewGuid());

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await Poll.UntilAsync(() => rig.EventsOf(LogActions.AckBegin).Count > 0, "the wait's first read");

        await AssertNotBeforeAsync(wait, time, ackTimeout);
        var outcome = await wait.WaitAsync(Answer);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), outcome);
        Assert.Equal(0L, await rig.AckedSeqAsync());
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
        Assert.Empty(rig.EventsOf(LogActions.AckDone));
        Assert.Equal(seq, Assert.Single(rig.EventsOf(LogActions.AckTimeout)).State![ObservationKeys.AwaitedSeq]);
    }

    [Fact]
    public async Task I31_a_record_held_past_AckTimeout_ends_the_wait_in_Unconfirmed_Timeout_at_AckTimeout_and_counts_for_nothing()
    {
        var ackTimeout = TimeSpan.FromSeconds(60);
        var time = new FakeTimeProvider();
        await using var rig = await AckRig.CreateAsync(postgres, ackTimeout, time: time);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RecordBeforeCommit);

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await hold.WaitReachedAsync(timeout: Answer);

        await AssertNotBeforeAsync(wait, time, ackTimeout);
        var outcome = await wait.WaitAsync(Answer);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), outcome);
        Assert.Empty(rig.EventsOf(LogActions.AckDone));
        await rig.Skip.WaitUntilNoSessionIsIdleInATransactionAsync();
        Assert.Equal(0L, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_rolled_back_Append_gets_the_conservative_wait_and_is_acknowledged_only_once_a_later_write_is_applied_everywhere()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var before = await rig.WriteAsync();
        await rig.LocalAtAsync(before);
        await rig.MemberAsync("r2");
        await rig.ApplyAsync("r2", before);
        long seq;
        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var transaction = await connection.BeginTransactionAsync();
            seq = await rig.Log.Log.AppendAsync(transaction, [new ChangeEntry("widget")]);
            await transaction.RollbackAsync();
        }

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await AssertPendingAsync(wait);

        var next = await rig.WriteAsync();
        Assert.True(next > seq);
        await rig.LocalAtAsync(next);
        await AssertPendingAsync(wait);
        await rig.ApplyAsync("r2", next);

        Assert.IsType<AckOutcome.Acknowledged>(await wait.WaitAsync(Answer));
        Assert.Equal(next, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_failed_record_is_retried_at_the_next_wake_up_and_the_answer_waits_for_the_retry()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        using var fail = rig.Skip.Faults.Throw(LogFaultPoints.RecordBetweenUpsertAndSkipRead, () => new InvalidOperationException("the injected failure of the first record"));
        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RecordBeforeCommit);

        var wait = rig.Ack.WaitAppliedAsync(seq);

        await hold.WaitReachedAsync(timeout: Answer);
        await AssertPendingAsync(wait);
        Assert.Equal(2, rig.EventsOf(LogActions.AckRecord).Count);
        hold.Release();

        Assert.Equal(AckOutcome.Acknowledged.None, await wait.WaitAsync(Answer));
        Assert.Equal(seq, await rig.AckedSeqAsync());
        Assert.Single(rig.EventsOf(LogActions.AckDone));
    }

    [Fact]
    public async Task A_fence_while_the_call_waits_answers_Unconfirmed_Fenced_at_once()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        var wait = rig.Ack.WaitAppliedAsync(seq);
        await AssertPendingAsync(wait);

        rig.Log.Gate.MoveTo(ReplicaPhase.Fencing);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Fenced), await wait.WaitAsync(Answer));
        await rig.ApplyAsync("r2", seq);
        await Task.Delay(Pending);
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
    }

    [Fact]
    public async Task Drain_step_1_lets_the_wait_go_on_and_drain_step_4_answers_Unconfirmed_Draining_at_once()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        var wait = rig.Ack.WaitAppliedAsync(seq);
        await AssertPendingAsync(wait);

        rig.Log.Gate.MoveTo(ReplicaPhase.Draining);
        await AssertPendingAsync(wait);
        rig.Log.Gate.RaiseDrainStep4();

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Draining), await wait.WaitAsync(Answer));
    }

    [Fact]
    public async Task A_request_that_starts_its_wait_after_drain_step_4_answers_Unconfirmed_Draining_at_once()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        rig.Log.Gate.MoveTo(ReplicaPhase.Draining);
        rig.Log.Gate.RaiseDrainStep4();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);

        var outcome = await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Draining), outcome);
    }

    [Fact]
    public async Task A_scope_that_acknowledged_at_commit_lists_its_seqs_and_its_wait_answers_NotAwaited_AtCommit_at_once()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        await rig.MemberAsync("r2");
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        long seq;
        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var transaction = await connection.BeginTransactionAsync();
            seq = await rig.Log.Log.AppendAsync(transaction, [new ChangeEntry("widget")]);
            await transaction.CommitAsync();
        }

        Assert.Equal([seq], rig.Ack.Seqs);
        rig.Ack.AcknowledgeAtCommit();

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.AtCommit), await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        var scopeId = ((ClusterScope)request.Scope).Id;
        Assert.Equal(scopeId, Assert.Single(rig.EventsOf(LogActions.AckAtCommit)).State![ObservationKeys.Request]);
        var begin = Assert.Single(rig.EventsOf(LogActions.AckBegin));
        Assert.Equal(scopeId, begin.State![ObservationKeys.Request]);
        Assert.Empty(WaitSet(begin));
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
    }

    [Fact]
    public async Task Outside_every_scope_Seqs_is_empty_and_AcknowledgeAtCommit_marks_nothing()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);

        rig.Ack.AcknowledgeAtCommit();

        Assert.Empty(rig.Ack.Seqs);
        Assert.IsType<AckOutcome.Acknowledged>(await rig.Ack.WaitAppliedAsync(seq).WaitAsync(Answer));
        Assert.Empty(rig.EventsOf(LogActions.AckAtCommit));
    }

    [Fact]
    public async Task A_wait_its_caller_cancels_throws_and_records_nothing()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await rig.MemberAsync("r2");
        using var cancel = new CancellationTokenSource();
        var wait = rig.Ack.WaitAppliedAsync(seq, cancel.Token);
        await AssertPendingAsync(wait);

        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Answer));
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
    }

    internal static async Task AssertNotBeforeAsync(Task wait, FakeTimeProvider time, TimeSpan ackTimeout)
    {
        time.Advance(ackTimeout - TimeSpan.FromMilliseconds(1));
        await AssertPendingAsync(wait);
        time.Advance(TimeSpan.FromMilliseconds(1));
    }

    private static async Task AssertPendingAsync(Task wait)
    {
        await Task.WhenAny(wait, Task.Delay(Pending));
        Assert.False(wait.IsCompleted, $"The wait answered: {(wait.IsCompletedSuccessfully ? ((Task<AckOutcome>)wait).Result : wait.Exception?.GetBaseException())}.");
    }

    private static IReadOnlyList<string> WaitSet(ModelEvent e) => [.. (IEnumerable<string>)e.State![ObservationKeys.WaitSet]!];
}
