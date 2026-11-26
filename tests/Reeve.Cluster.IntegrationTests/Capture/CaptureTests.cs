using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Capture;

public sealed class CaptureTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public CaptureTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<CaptureRig> RigAsync(IClusterMapping? mapping = null) => CaptureRig.CreateAsync(postgres, mapping);

    private static Setting NewSetting(int id, string name = "n", string value = "v") => new() { Id = id, Name = name + id, Value = value };

    [Fact]
    public async Task A_save_of_a_mapped_entity_owns_a_transaction_appends_its_entry_and_wakes_the_appliers_only_after_the_commit()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1));

        using var hold = rig.Ack.Skip.Faults.HoldNext(LogFaultPoints.AfterCommitBeforeWakeup);
        var save = db.SaveChangesAsync();
        await hold.WaitReachedAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Empty(rig.Wakeups);
        var seq = Assert.Single(request.Scope.Seqs);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, rig.Interceptor.PairCount);

        hold.Release();
        await save.WaitAsync(Bound);
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], rig.Wakeups);
        Assert.Equal([LogActions.WAppend, LogActions.WCommit], rig.Log.Events.All.Select(e => e.Action));
        Assert.Equal(seq, rig.Log.Events.All[1].State![ObservationKeys.Seq]);
        Assert.Equal(1, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.CommittingBeforeAppend));
        Assert.Equal(1, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.CommittingAfterAppend));
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task The_mapping_is_column_scoped_and_an_added_or_deleted_entity_always_produces_an_entry()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        db.Settings.Add(new Setting { Id = 1, Name = "a", Value = "v", Note = "n" });
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        Assert.Equal([("setting", "a")], await rig.EntriesAsync());

        var setting = await db.Settings.SingleAsync();
        setting.Note = "changed";
        (await db.Audits.SingleAsync()).Text = "y";
        await db.SaveChangesAsync();
        Assert.Equal(1, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Equal("changed", await rig.Log.ScalarAsync<string>("SELECT note FROM public.setting WHERE id = 1"));

        setting.Value = "w";
        await db.SaveChangesAsync();
        Assert.Equal([("setting", "a"), ("setting", "a")], await rig.EntriesAsync());

        db.Settings.Remove(setting);
        await db.SaveChangesAsync();
        Assert.Equal(3, (await rig.EntriesAsync()).Count);
    }

    [Fact]
    public async Task A_type_mapped_with_NoKey_produces_an_entry_with_no_key()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        db.Widgets.Add(new Widget { Id = 1, Label = "l" });
        await db.SaveChangesAsync();

        Assert.Equal([("widget", (string?)null)], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_change_of_the_key_gives_the_old_key_an_entry_as_well_as_the_new_one()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1, "old"));
        await db.SaveChangesAsync();

        (await db.Settings.SingleAsync()).Name = "new";
        await db.SaveChangesAsync();

        Assert.Equal([("setting", "old1"), ("setting", "new"), ("setting", "old1")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_transaction_that_spans_saves_appends_once_at_the_commit_as_the_last_statement_and_is_listed_open_until_then()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();

        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        (await db.Settings.FirstAsync(s => s.Id == 1)).Value = "again";
        await db.SaveChangesAsync();

        Assert.Empty(request.Scope.Seqs);
        Assert.Equal(transaction.GetDbTransaction(), Assert.Single(request.Scope.OpenTransactions));
        Assert.True(await rig.Ack.Skip.ChangeLogLockFreeAsync());

        await transaction.CommitAsync();

        Assert.Equal([("setting", "n1"), ("setting", "n2")], await rig.EntriesAsync());
        Assert.Equal(2, request.Scope.Seqs.Count);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal([new Wakeup(WakeupKind.Appended, request.Scope.Seqs[^1])], rig.Wakeups);
        Assert.Equal(1, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.CommittingBeforeAppend));
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_rollback_drops_the_entries_and_nothing_is_appended_or_announced()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        await transaction.RollbackAsync();

        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Empty(request.Scope.Seqs);
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(rig.Wakeups);
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_rollback_to_a_savepoint_keeps_every_entry_captured_and_the_commit_appends_them()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        await transaction.CreateSavepointAsync("sp");
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        await transaction.RollbackToSavepointAsync("sp");
        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1"), ("setting", "n2")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_failed_later_save_keeps_the_earlier_saves_entries_and_the_commit_appends_them()
    {
        await using var rig = await RigAsync();
        await using (var seed = rig.NewContext())
        {
            seed.Widgets.Add(new Widget { Id = 1, Label = "taken" });
            await seed.SaveChangesAsync();
        }

        var seeded = (await rig.EntriesAsync()).Count;
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        db.Widgets.Add(new Widget { Id = 2, Label = "taken" });
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());
        db.ChangeTracker.Entries<Widget>().Single(e => e.Entity.Id == 2).State = EntityState.Detached;
        await transaction.CommitAsync();

        var entries = (await rig.EntriesAsync()).Skip(seeded).ToList();
        Assert.Equal([("setting", "n1"), ("widget", (string?)null)], entries);
        Assert.Equal(1, await rig.CountAsync("public.setting"));
    }

    [Fact]
    public async Task A_failed_implicit_save_drops_its_entries_and_the_context_saves_again()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using (var seed = rig.NewContext())
        {
            seed.Widgets.Add(new Widget { Id = 1, Label = "taken" });
            await seed.SaveChangesAsync();
        }

        var seeded = await rig.CountAsync("reeve_cluster.change_log");
        var seededSeqs = request.Scope.Seqs.Count;
        await using var db = rig.NewContext();
        var duplicate = new Widget { Id = 2, Label = "taken" };
        db.Widgets.Add(duplicate);
        db.Settings.Add(NewSetting(1));
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());

        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(seeded, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Equal(seededSeqs, request.Scope.Seqs.Count);

        duplicate.Label = "free";
        await db.SaveChangesAsync();
        Assert.Equal(seeded + 2, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_failed_implicit_save_drops_its_entries_through_SaveChangesFailed_without_the_disposal_signal()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using (var seed = rig.NewContext())
        {
            seed.Widgets.Add(new Widget { Id = 1, Label = "taken" });
            await seed.SaveChangesAsync();
        }

        rig.Interceptor.Dispose();
        await using var db = rig.NewContext();
        db.Widgets.Add(new Widget { Id = 2, Label = "taken" });
        await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());

        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task The_synchronous_API_captures_appends_and_wakes_the_appliers_as_the_asynchronous_one_does()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1));
        db.SaveChanges();

        using (var transaction = db.Database.BeginTransaction())
        {
            db.Settings.Add(NewSetting(2));
            db.SaveChanges();
            Assert.Equal(transaction.GetDbTransaction(), Assert.Single(request.Scope.OpenTransactions));
            transaction.Commit();
        }

        Assert.Equal([("setting", "n1"), ("setting", "n2")], await rig.EntriesAsync());
        Assert.Equal(2, request.Scope.Seqs.Count);
        Assert.Equal(2, rig.Wakeups.Count);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_rollback_through_the_synchronous_API_drops_the_entries_by_its_own_event()
    {
        await using var rig = await RigAsync();
        rig.Interceptor.Dispose();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        using (var transaction = db.Database.BeginTransaction())
        {
            db.Settings.Add(NewSetting(1));
            db.SaveChanges();
            transaction.Rollback();
        }

        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Empty(request.Scope.Seqs);
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task An_explicit_transaction_disposed_without_a_commit_drops_its_entries_and_the_next_transaction_on_the_reused_object_logs_no_error()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);

        System.Data.Common.DbTransaction first;
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            first = transaction.GetDbTransaction();
            db.Settings.Add(NewSetting(1));
            await db.SaveChangesAsync();
            Assert.Equal(1, rig.Interceptor.PairCount);
        }

        Assert.Equal(0, rig.Interceptor.PairCount);
        db.ChangeTracker.Clear();

        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            Assert.Same(first, transaction.GetDbTransaction());
            db.Settings.Add(NewSetting(2));
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        Assert.Empty(rig.Logger.Errors);
        Assert.Equal([("setting", "n2")], await rig.EntriesAsync());
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_transaction_that_ends_around_EF_leaves_a_pair_and_the_next_start_on_the_reused_object_discards_it_and_logs_an_error()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        first.Settings.Add(NewSetting(1));
        await first.SaveChangesAsync();

        await raw.CommitAsync();
        Assert.Equal(1, rig.Interceptor.PairCount);

        await using var next = await second.Database.BeginTransactionAsync();

        Assert.Same(raw, next.GetDbTransaction());
        Assert.Single(rig.Logger.Errors);
        Assert.Equal(1, rig.Interceptor.PairCount);
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_commit_after_the_requests_scope_moved_on_records_into_the_scope_the_pair_captured()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        IClusterScope other;
        using (var unit = rig.Log.Scopes.Begin(ClusterScopeKind.UnitOfWork))
        {
            other = unit.Scope;
            Assert.NotSame(request.Scope, rig.Log.Scopes.Current);
            await transaction.CommitAsync();
        }

        Assert.Single(request.Scope.Seqs);
        Assert.Empty(other.Seqs);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task A_commit_after_the_requests_scope_ended_records_nowhere_and_is_not_refused()
    {
        await using var rig = await RigAsync();
        await rig.Log.RunFenceStep2Async();
        var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        request.Dispose();

        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Empty(request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
    }

    [Fact]
    public async Task Append_of_A_followed_by_captured_B_in_one_transaction_acknowledges_B_with_A_and_an_open_of_B_is_refused()
    {
        await using var rig = await RigAsync();
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        var a = await rig.Log.Log.AppendAsync(raw, [new ChangeEntry("widget", "A")]);
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        var entries = await rig.EntriesAsync();
        Assert.Equal([("widget", "A"), ("setting", "n1")], entries);
        var b = await rig.Log.ScalarAsync<long>("SELECT max(seq) FROM reeve_cluster.change_log");
        Assert.True(b > a);

        await rig.Ack.LocalAtAsync(b);
        await rig.Ack.MemberAsync("r2", activatedAtSeq: a - 1);
        var wait = rig.Ack.Ack.WaitAppliedAsync(a);
        await rig.Ack.ApplyAsync("r2", b);
        Assert.Equal(AckOutcome.Acknowledged.None, await wait.WaitAsync(Bound));

        Assert.True(await rig.Ack.AckedSeqAsync() >= b);
        var open = await rig.Ack.Skip.OpenAsync("setting", b);
        Assert.Equal(new SkipOpenResult.Refused(await rig.Ack.AckedSeqAsync()), open);
    }

    [Fact]
    public async Task A_capturing_save_in_a_transaction_its_context_did_not_begin_through_EF_throws_and_writes_nothing()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var raw = await connection.BeginTransactionAsync();
        await using var db = rig.NewContext(connection);
        await db.Database.UseTransactionAsync(raw);
        db.Settings.Add(NewSetting(1));

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
        await raw.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_save_that_captures_nothing_passes_in_a_transaction_the_context_joined()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var raw = await connection.BeginTransactionAsync();
        await using var db = rig.NewContext(connection);
        await db.Database.UseTransactionAsync(raw);
        db.Audits.Add(new Audit { Id = 1, Text = "x" });

        await db.SaveChangesAsync();
        await raw.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_refused_commit_after_fence_step_2_commits_nothing_and_the_entries_are_dropped_with_the_transaction()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());
        await transaction.DisposeAsync();

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Empty(rig.Wakeups);
    }
}
