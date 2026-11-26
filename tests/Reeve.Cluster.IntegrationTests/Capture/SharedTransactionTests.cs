using System.Data.Common;
using System.Diagnostics.Metrics;
using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Capture;

public sealed class SharedTransactionTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public SharedTransactionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<CaptureRig> RigAsync() => CaptureRig.CreateAsync(postgres);

    private static Setting NewSetting(int id, string name = "n", string value = "v") => new() { Id = id, Name = name + id, Value = value };

    [Fact]
    public async Task A_capturing_save_in_a_context_that_joined_an_EF_transaction_of_a_context_without_the_capture_throws_writes_nothing_and_emits_WSaveRefuse()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = new CaptureDb(new DbContextOptionsBuilder<CaptureDb>().UseNpgsql(connection).Options);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        await Assert.ThrowsAsync<InvalidOperationException>(() => second.SaveChangesAsync());
        await transaction.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        var refused = Assert.Single(rig.Log.Events.All, e => e.Action == LogActions.WSaveRefuse);
        Assert.Equal(((ClusterScope)request.Scope).Id, refused.State![ObservationKeys.Request]);
        Assert.Empty(request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
    }

    [Fact]
    public async Task A_capturing_save_under_an_ambient_TransactionScope_throws_and_writes_nothing_even_when_the_scope_completes()
    {
        await using var rig = await RigAsync();
        using (var ambient = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var db = rig.NewContext();
            db.Settings.Add(NewSetting(1));

            await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
            ambient.Complete();
        }

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Single(rig.Log.Events.All, e => e.Action == LogActions.WSaveRefuse);
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_capturing_save_in_a_context_enlisted_in_a_System_Transactions_transaction_throws_and_writes_nothing()
    {
        await using var rig = await RigAsync();
        using var committable = new CommittableTransaction();
        await using (var db = rig.NewContext())
        {
            await db.Database.OpenConnectionAsync();
            db.Database.EnlistTransaction(committable);
            Assert.Null(Transaction.Current);
            db.Settings.Add(NewSetting(1));

            await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
        }

        committable.Commit();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_save_that_captures_nothing_passes_under_an_ambient_transaction()
    {
        await using var rig = await RigAsync();
        using (var ambient = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var db = rig.NewContext();
            db.Audits.Add(new Audit { Id = 1, Text = "x" });
            await db.SaveChangesAsync();
            ambient.Complete();
        }

        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.DoesNotContain(rig.Log.Events.All, e => e.Action == LogActions.WSaveRefuse);
    }

    [Fact]
    public async Task A_joining_context_leaves_the_capturing_contexts_entries_alone_and_the_commit_through_the_capturing_context_appends_them()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();

        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Audits.Add(new Audit { Id = 1, Text = "x" });
        await second.SaveChangesAsync();
        await transaction.CommitAsync();

        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Single(request.Scope.Seqs);
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_commit_through_a_joining_context_is_refused_while_the_capturing_context_holds_entries_and_the_capturing_context_can_still_commit()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();
        await using var joined = await second.Database.UseTransactionAsync(transaction.GetDbTransaction());

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.False(request.Scope.CommitRefused);
        Assert.Equal(transaction.GetDbTransaction(), Assert.Single(request.Scope.OpenTransactions));

        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task A_commit_through_a_joining_context_passes_when_no_context_holds_entries_and_ends_the_listing()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        await using var joined = await second.Database.UseTransactionAsync(raw);
        second.Audits.Add(new Audit { Id = 1, Text = "x" });
        await second.SaveChangesAsync();
        await rig.Log.Log.AppendAsync(raw, [new ChangeEntry("widget", "A")]);
        Assert.Equal(raw, Assert.Single(request.Scope.OpenTransactions));

        await joined!.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Equal([("widget", "A")], await rig.EntriesAsync());
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task The_refusal_of_a_foreign_commit_holds_after_the_capturing_context_is_disposed()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();

        await first.Database.UseTransactionAsync(null);
        await first.DisposeAsync();
        Assert.NotNull(raw.Connection);
        Assert.Equal(1, rig.Interceptor.PairCount);

        await using var joined = await second.Database.UseTransactionAsync(raw);
        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());
        await raw.RollbackAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_context_that_rejoins_the_transaction_it_captured_in_is_refused_and_its_own_commit_still_appends()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await db.Database.UseTransactionAsync(null);

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Database.UseTransactionAsync(raw));
        Assert.Equal(1, rig.Interceptor.PairCount);

        await transaction.CommitAsync();

        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_joining_context_that_detaches_and_joins_again_keeps_the_transaction_listed_open()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();

        await second.Database.UseTransactionAsync(raw);
        await second.Database.UseTransactionAsync(null);
        await second.Database.UseTransactionAsync(raw);

        Assert.Equal(raw, Assert.Single(request.Scope.OpenTransactions));
        Assert.Empty(rig.Logger.Errors);

        await transaction.CommitAsync();
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Single(request.Scope.Seqs);
    }

    [Fact]
    public async Task A_capturing_save_after_a_commit_that_appended_but_did_not_happen_is_appended_at_the_next_commit()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var db = WithLaterInterceptor(rig, connection, new FailFirstCommit());
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        await Assert.ThrowsAsync<LaterInterceptorFailure>(() => transaction.CommitAsync());
        Assert.Single(request.Scope.Seqs);
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        Assert.Equal(2, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1"), ("setting", "n2")], await rig.EntriesAsync());
        Assert.Equal(2, request.Scope.Seqs.Count);
        Assert.Equal([new Wakeup(WakeupKind.Appended, request.Scope.Seqs[^1])], rig.Wakeups);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task After_a_commit_that_appended_but_did_not_happen_a_joiners_commit_is_refused_while_a_later_capture_is_held()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var first = WithLaterInterceptor(rig, connection, new FailFirstCommit());
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();
        await Assert.ThrowsAsync<LaterInterceptorFailure>(() => transaction.CommitAsync());
        first.Settings.Add(NewSetting(2));
        await first.SaveChangesAsync();
        await using var joined = await second.Database.UseTransactionAsync(raw);

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());
        Assert.Equal(0, await rig.CountAsync("public.setting"));

        await transaction.CommitAsync();

        Assert.Equal(2, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1"), ("setting", "n2")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task After_a_commit_that_appended_but_did_not_happen_a_joiners_commit_wakes_the_appliers_for_the_appended_seq()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = WithLaterInterceptor(rig, connection, new FailFirstCommit());
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();
        await Assert.ThrowsAsync<LaterInterceptorFailure>(() => transaction.CommitAsync());
        var seq = Assert.Single(request.Scope.Seqs);
        await using var joined = await second.Database.UseTransactionAsync(raw);

        await joined!.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], rig.Wakeups);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task A_discard_at_TransactionStarted_that_drops_entries_logs_an_error_and_counts_one_discard()
    {
        await using var rig = await RigAsync();
        using var discards = new DiscardListener();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        first.Settings.Add(NewSetting(2));
        await first.SaveChangesAsync();
        await raw.CommitAsync();

        await using var next = await second.Database.BeginTransactionAsync();

        Assert.Same(raw, next.GetDbTransaction());
        Assert.Contains("Discarded 2 captured change-log entries of 1 transaction", Assert.Single(rig.Logger.Errors), StringComparison.Ordinal);
        Assert.Equal([1L], discards.Measurements);
    }

    [Fact]
    public async Task After_a_shared_transaction_ends_through_EF_the_next_start_on_the_reused_object_discards_no_entries()
    {
        await using var rig = await RigAsync();
        using var discards = new DiscardListener();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);

        var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();
        await second.Database.UseTransactionAsync(raw);
        await transaction.DisposeAsync();
        await second.Database.UseTransactionAsync(null);

        await using (var again = await first.Database.BeginTransactionAsync())
        {
            Assert.Same(raw, again.GetDbTransaction());
            first.Settings.Add(NewSetting(2));
            await first.SaveChangesAsync();
            await second.Database.UseTransactionAsync(raw);
            await again.CommitAsync();
        }

        await second.Database.UseTransactionAsync(null);
        await using var last = await second.Database.BeginTransactionAsync();

        Assert.Same(raw, last.GetDbTransaction());
        Assert.Empty(rig.Logger.Errors);
        Assert.Empty(discards.Measurements);
        Assert.Equal([("setting", "n2")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_commit_whose_event_names_no_context_is_refused_while_a_pair_on_its_transaction_holds_entries()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();

        await rig.Interceptor.TransactionCommittingAsync(raw, NoContextEvent.Of(raw), default);

        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await rig.Interceptor.TransactionCommittingAsync(raw, NoContextEvent.Of(raw), default));
        Assert.Throws<InvalidOperationException>(() => rig.Interceptor.TransactionCommitting(raw, NoContextEvent.Of(raw), default));
        Assert.Equal(1, rig.Interceptor.PairCount);
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task Start_use_end_and_disposal_events_that_name_no_context_act_on_no_pair()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);
        var transaction = await db.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        await rig.Interceptor.TransactionStartedAsync(connection, NoContextEvent.Ended(raw), raw);
        await rig.Interceptor.TransactionUsedAsync(connection, NoContextEvent.Of(raw), raw);
        await rig.Interceptor.TransactionRolledBackAsync(raw, NoContextEvent.Ended(raw));
        await rig.Interceptor.TransactionCommittedAsync(raw, NoContextEvent.Ended(raw));

        Assert.Equal(1, rig.Interceptor.PairCount);
        Assert.Empty(rig.Logger.Errors);

        await transaction.CommitAsync();
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransactionStarted_and_TransactionUsed_reach_their_fault_points(bool sync)
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = sync ? first.Database.BeginTransaction() : await first.Database.BeginTransactionAsync();
        if (sync)
        {
            second.Database.UseTransaction(transaction.GetDbTransaction());
        }
        else
        {
            await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        }

        Assert.Equal(1, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.TransactionStarted));
        Assert.Equal(1, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.TransactionUsed));
    }

    [Fact]
    public async Task A_fault_at_TransactionUsed_fails_the_join_and_leaves_the_capturing_contexts_entries()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();
        rig.Ack.Skip.Faults.Throw(CaptureFaultPoints.TransactionUsed, () => new TimeoutException("held"));

        await Assert.ThrowsAsync<TimeoutException>(() => second.Database.UseTransactionAsync(transaction.GetDbTransaction()));
        await transaction.CommitAsync();

        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
    }

    private static CaptureDb WithLaterInterceptor(CaptureRig rig, DbConnection connection, IInterceptor later) =>
        new(new DbContextOptionsBuilder<CaptureDb>().UseNpgsql(connection).AddInterceptors(rig.Interceptor, later).Options);

    private sealed class LaterInterceptorFailure() : Exception("A later interceptor failed the commit.");

    private sealed class FailFirstCommit : IDbTransactionInterceptor
    {
        private int armed = 1;

        public InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Trip();
            return result;
        }

        public ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Trip();
            return ValueTask.FromResult(result);
        }

        private void Trip()
        {
            if (Interlocked.Exchange(ref armed, 0) == 1)
            {
                throw new LaterInterceptorFailure();
            }
        }
    }

    private sealed class DiscardListener : IDisposable
    {
        private static readonly AsyncLocal<DiscardListener?> Owner = new();

        private readonly MeterListener listener = new();
        private readonly List<long> measurements = [];

        public DiscardListener()
        {
            Owner.Value = this;
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == ClusterDiagnostics.Name && instrument.Name == ClusterCaptureInterceptor.DiscardsName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (!ReferenceEquals(Owner.Value, this))
                {
                    return;
                }

                lock (measurements)
                {
                    measurements.Add(value);
                }
            });
            listener.Start();
        }

        public IReadOnlyList<long> Measurements
        {
            get
            {
                lock (measurements)
                {
                    return [.. measurements];
                }
            }
        }

        public void Dispose() => listener.Dispose();
    }
}
