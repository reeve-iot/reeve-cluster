using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Log;

public sealed class ClusterScopeTests
{
    private readonly ClusterScopeSource source = new(new FenceSteps());

    [Fact]
    public void Outside_every_scope_there_is_none()
    {
        Assert.Null(source.Current);
        Assert.Null(source.CurrentWriter);
    }

    [Fact]
    public void A_begun_scope_is_ambient_until_its_handle_is_disposed()
    {
        var handle = source.BeginBackground();

        Assert.Same(handle.Scope, source.Current);
        Assert.Same(handle.Scope, source.CurrentWriter);
        Assert.Equal(ClusterScopeKind.Background, source.Current!.Kind);

        handle.Dispose();
        Assert.Null(source.Current);

        handle.Dispose();
        Assert.Null(source.Current);
    }

    [Fact]
    public async Task A_scope_flows_into_the_tasks_begun_inside_it()
    {
        using var handle = source.BeginUnitOfWork();

        var seen = await Task.Run(() => source.Current);

        Assert.Same(handle.Scope, seen);
    }

    [Fact]
    public async Task A_scope_begun_inside_an_async_method_does_not_leak_to_its_caller()
    {
        async Task BeginAndLeaveOpen()
        {
            await Task.Yield();
            source.BeginBackground();
        }

        await BeginAndLeaveOpen();

        Assert.Null(source.Current);
    }

    [Fact]
    public void Two_sources_keep_their_ambient_scopes_apart()
    {
        var other = new ClusterScopeSource(new FenceSteps());
        using var handle = source.BeginBackground();

        Assert.Null(other.Current);
    }

    [Theory]
    [InlineData(ClusterScopeKind.ClientRequest, false)]
    [InlineData(ClusterScopeKind.ClientRequest, true)]
    [InlineData(ClusterScopeKind.ExemptRequest, false)]
    [InlineData(ClusterScopeKind.ExemptRequest, true)]
    [InlineData(ClusterScopeKind.Applier, false)]
    [InlineData(ClusterScopeKind.Applier, true)]
    public void An_application_begin_inside_a_request_or_the_applier_joins_that_scope(ClusterScopeKind outerKind, bool background)
    {
        using var outer = source.Begin(outerKind);

        var inner = background ? source.BeginBackground() : source.BeginUnitOfWork();

        Assert.Same(outer.Scope, inner.Scope);
        Assert.Same(outer.Scope, source.Current);

        inner.Dispose();
        Assert.Same(outer.Scope, source.Current);
    }

    [Fact]
    public void A_write_in_a_unit_of_work_begun_inside_a_client_request_is_the_requests()
    {
        using var request = source.Begin(ClusterScopeKind.ClientRequest);

        using (source.BeginUnitOfWork())
        {
            source.CurrentWriter!.RecordSeq(7);
        }

        Assert.Equal([7L], request.Scope.Seqs);
        Assert.Equal(ClusterScopeKind.ClientRequest, source.Current!.Kind);
    }

    [Fact]
    public void S2_A_scope_a_reconciler_begins_keeps_the_applier_row_of_the_answer_table()
    {
        using var applier = source.Begin(ClusterScopeKind.Applier);

        using var reconcilerScope = source.BeginBackground();
        var answer = AckAnswerPolicy.Decide(ReplicaPhase.Serving, new AckCaller(source.Current?.Kind, TrackerHoldsSeq: true));

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Applier), answer.Outcome);
    }

    [Theory]
    [InlineData(ClusterScopeKind.UnitOfWork, false)]
    [InlineData(ClusterScopeKind.UnitOfWork, true)]
    [InlineData(ClusterScopeKind.Background, false)]
    [InlineData(ClusterScopeKind.Background, true)]
    public void An_application_begin_inside_a_unit_of_work_or_background_scope_begins_a_new_one(ClusterScopeKind outerKind, bool background)
    {
        using var outer = source.Begin(outerKind);

        var inner = background ? source.BeginBackground() : source.BeginUnitOfWork();

        Assert.NotSame(outer.Scope, inner.Scope);
        Assert.Equal(background ? ClusterScopeKind.Background : ClusterScopeKind.UnitOfWork, inner.Scope.Kind);
        Assert.Same(inner.Scope, source.Current);

        inner.Dispose();
        Assert.Same(outer.Scope, source.Current);
    }

    [Fact]
    public void The_librarys_begin_inside_a_request_begins_a_scope_of_its_own()
    {
        using var connection = source.Begin(ClusterScopeKind.ClientRequest);

        using var invocation = source.Begin(ClusterScopeKind.ClientRequest);
        source.CurrentWriter!.RecordSeq(3);

        Assert.NotSame(connection.Scope, invocation.Scope);
        Assert.Equal([3L], invocation.Scope.Seqs);
        Assert.Empty(connection.Scope.Seqs);
    }

    [Fact]
    public void A_handle_disposed_out_of_order_does_not_bring_an_ended_scope_back()
    {
        var outer = source.BeginBackground();
        var inner = source.BeginBackground();

        outer.Dispose();
        Assert.Same(inner.Scope, source.Current);

        inner.Dispose();
        Assert.Null(source.Current);
        Assert.Null(source.CurrentWriter);
    }

    [Fact]
    public async Task A_flow_that_outlives_its_scope_is_outside_it_once_the_handle_is_disposed()
    {
        var current = await AfterTheScopeEndedAsync(ClusterScopeKind.ClientRequest, _ => source.Current);

        Assert.Null(current);
    }

    [Fact]
    public async Task A_flow_that_outlives_an_inner_scope_is_back_in_the_live_outer_one()
    {
        using var outer = source.Begin(ClusterScopeKind.Background);

        var current = await AfterTheScopeEndedAsync(ClusterScopeKind.UnitOfWork, _ => source.CurrentWriter);

        Assert.Same(outer.Scope, current);
    }

    [Theory]
    [InlineData(ClusterScopeKind.ClientRequest, false)]
    [InlineData(ClusterScopeKind.ClientRequest, true)]
    [InlineData(ClusterScopeKind.ExemptRequest, false)]
    [InlineData(ClusterScopeKind.Applier, true)]
    public async Task An_application_begin_in_a_flow_that_outlived_a_request_or_the_applier_begins_a_scope_of_its_own(
        ClusterScopeKind endedKind, bool background)
    {
        var (joined, kind) = await AfterTheScopeEndedAsync(endedKind, ended =>
        {
            using var work = background ? source.BeginBackground() : source.BeginUnitOfWork();
            return (ReferenceEquals(work.Scope, ended), work.Scope.Kind);
        });

        Assert.False(joined);
        Assert.Equal(background ? ClusterScopeKind.Background : ClusterScopeKind.UnitOfWork, kind);
    }

    [Fact]
    public async Task Seqs_of_a_loop_that_outlived_its_request_do_not_go_to_the_ended_request()
    {
        IClusterScope? ended = null;
        await AfterTheScopeEndedAsync(ClusterScopeKind.ClientRequest, request =>
        {
            ended = request;
            for (var i = 1; i <= 3; i++)
            {
                using var work = source.BeginUnitOfWork();
                source.CurrentWriter!.RecordSeq(i);
            }

            return 0;
        });

        Assert.Empty(ended!.Seqs);
    }

    [Fact]
    public async Task A_wait_in_background_work_that_outlived_its_request_answers_as_background()
    {
        var answer = await AfterTheScopeEndedAsync(ClusterScopeKind.ClientRequest, _ =>
        {
            using var work = source.BeginBackground();
            return AckAnswerPolicy.Decide(ReplicaPhase.Draining, new AckCaller(source.Current?.Kind, TrackerHoldsSeq: true));
        });

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Draining), answer.Outcome);
    }

    [Fact]
    public void A_joined_handle_names_the_live_scope_and_ending_the_scope_ends_it_for_the_joined_flow_too()
    {
        var request = source.Begin(ClusterScopeKind.ClientRequest);
        var joined = source.BeginUnitOfWork();

        request.Dispose();

        Assert.Same(request.Scope, joined.Scope);
        Assert.True(((ClusterScope)joined.Scope).Ended);
        Assert.Null(source.Current);
    }

    [Fact]
    public void Ending_a_scope_drops_its_pin()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        scope.Pin(new HeadOnly(3));

        scope.End();

        Assert.Null(scope.PinnedGeneration);
    }

    private async Task<T> AfterTheScopeEndedAsync<T>(ClusterScopeKind kind, Func<IClusterScope, T> work)
    {
        var scopeEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<T> task;
        using (var scope = source.Begin(kind))
        {
            var begun = scope.Scope;
            task = Task.Run(async () =>
            {
                await scopeEnded.Task;
                return work(begun);
            });
        }

        scopeEnded.SetResult();
        return await task.WaitAsync(Bounds.Safety);
    }

    [Theory]
    [InlineData(ClusterScopeKind.ClientRequest)]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public void The_tracker_records_every_seq_in_the_order_appended(ClusterScopeKind kind)
    {
        var scope = new ClusterScope(kind);

        scope.RecordSeq(5);
        scope.RecordSeq(6);
        scope.RecordSeq(9);

        Assert.Equal([5L, 6L, 9L], scope.Seqs);
    }

    [Fact]
    public void An_exempt_requests_seqs_are_not_recorded()
    {
        var scope = new ClusterScope(ClusterScopeKind.ExemptRequest);

        scope.RecordSeq(5);

        Assert.Empty(scope.Seqs);
    }

    [Fact]
    public void A_seq_is_positive()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);

        Assert.Throws<ArgumentOutOfRangeException>(() => scope.RecordSeq(0));
        Assert.Empty(scope.Seqs);
    }

    [Fact]
    public async Task Seqs_recorded_from_many_tasks_are_all_kept()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);

        await Task.WhenAll(Enumerable.Range(0, 16).Select(t => Task.Run(() =>
        {
            for (var i = 1; i <= 500; i++)
            {
                scope.RecordSeq((t * 1000) + i);
            }
        })));

        Assert.Equal(16 * 500, scope.Seqs.Distinct().Count());
    }

    [Fact]
    public void The_tracker_records_a_refused_commit_and_the_opt_out()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        Assert.False(scope.CommitRefused);
        Assert.False(scope.AcknowledgedAtCommit);

        scope.RecordRefusedCommit();
        scope.OptOut();

        Assert.True(scope.CommitRefused);
        Assert.True(scope.AcknowledgedAtCommit);
    }

    [Fact]
    public void A_client_request_is_classified_as_exempt_once_and_only_before_it_recorded_anything()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);

        scope.ClassifyAsExempt();

        Assert.Equal(ClusterScopeKind.ExemptRequest, scope.Kind);
        Assert.Throws<InvalidOperationException>(scope.ClassifyAsExempt);
    }

    [Theory]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public void Only_a_client_request_is_classified_as_exempt(ClusterScopeKind kind)
    {
        var scope = new ClusterScope(kind);

        Assert.Throws<InvalidOperationException>(scope.ClassifyAsExempt);
        Assert.Equal(kind, scope.Kind);
    }

    [Fact]
    public void A_client_request_that_recorded_a_seq_is_not_classified_as_exempt()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        scope.RecordSeq(4);

        Assert.Throws<InvalidOperationException>(scope.ClassifyAsExempt);
        Assert.Equal(ClusterScopeKind.ClientRequest, scope.Kind);
    }

    [Fact]
    public void A_client_request_whose_commit_was_refused_is_not_classified_as_exempt()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        scope.RecordRefusedCommit();

        Assert.Throws<InvalidOperationException>(scope.ClassifyAsExempt);
        Assert.Equal(ClusterScopeKind.ClientRequest, scope.Kind);
    }

    [Fact]
    public void A_scope_lists_an_open_transaction_once_until_it_ends()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        using var first = new FakeTransaction();
        using var second = new FakeTransaction();

        scope.TransactionOpened(first);
        scope.TransactionOpened(first);
        scope.TransactionOpened(second);
        Assert.Equal(2, scope.OpenTransactions.Count);

        scope.TransactionEnded(first);
        Assert.Same(second, Assert.Single(scope.OpenTransactions));
    }

    [Fact]
    public void A_scope_pins_one_generation()
    {
        var scope = new ClusterScope(ClusterScopeKind.ClientRequest);
        var generation = new HeadOnly(7);

        scope.Pin(generation);

        Assert.Same(generation, scope.PinnedGeneration);
        Assert.Throws<InvalidOperationException>(() => scope.Pin(generation));
    }

    [Fact]
    public void PinOrGet_answers_the_pin_and_pins_only_when_there_is_none()
    {
        var scope = new ClusterScope(ClusterScopeKind.UnitOfWork);
        var first = new HeadOnly(7);

        Assert.Same(first, scope.PinOrGet(first));
        Assert.Same(first, scope.PinOrGet(new HeadOnly(8)));
        Assert.Same(first, scope.PinnedGeneration);
    }

    [Fact]
    public void PinOrGet_from_two_threads_at_once_answers_both_the_one_generation_pinned()
    {
        const int Rounds = 20_000;
        var scopes = Enumerable.Range(0, Rounds).Select(_ => new ClusterScope(ClusterScopeKind.UnitOfWork)).ToArray();
        var theirs = new IGeneration[Rounds];
        var mine = new IGeneration[Rounds];
        using var start = new Barrier(2);
        var worker = new Thread(() =>
        {
            for (var round = 0; round < Rounds; round++)
            {
                start.SignalAndWait();
                theirs[round] = scopes[round].PinOrGet(new HeadOnly(2));
            }
        });
        worker.Start();
        for (var round = 0; round < Rounds; round++)
        {
            start.SignalAndWait();
            mine[round] = scopes[round].PinOrGet(new HeadOnly(1));
        }

        Assert.True(worker.Join(Bounds.Safety));
        for (var round = 0; round < Rounds; round++)
        {
            Assert.Same(mine[round], theirs[round]);
            Assert.Same(scopes[round].PinnedGeneration, mine[round]);
        }
    }

    [Fact]
    public void PinOrGet_on_an_ended_scope_answers_the_generation_and_stores_no_pin()
    {
        var scope = new ClusterScope(ClusterScopeKind.Background);
        scope.End();
        var generation = new HeadOnly(1);

        Assert.Same(generation, scope.PinOrGet(generation));
        Assert.Null(scope.PinnedGeneration);
    }

    private sealed class HeadOnly(long head) : IGeneration
    {
        public long Head => head;

        public Guid? HeadToken => null;

        public object Get(string aggregateType) => throw new NotSupportedException();

        public CancellationToken Superseded(string aggregateType) => CancellationToken.None;

        public bool TryActivate(string aggregateType, Action activate) => throw new NotSupportedException();
    }
}
