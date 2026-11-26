using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Capture;

public sealed class HandOverTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public HandOverTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<CaptureRig> RigAsync() => CaptureRig.CreateAsync(postgres);

    private static Setting NewSetting(int id) => new() { Id = id, Name = "n" + id, Value = "v" };

    [Fact]
    public async Task A_joining_contexts_capture_is_appended_at_the_beginning_contexts_commit_with_the_seqs_in_its_scope()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(raw);
        second.Settings.Add(NewSetting(1));

        await second.SaveChangesAsync();

        Assert.Equal(raw, Assert.Single(request.Scope.OpenTransactions));
        Assert.Empty(request.Scope.Seqs);

        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        var seq = Assert.Single(request.Scope.Seqs);
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], rig.Wakeups);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.DoesNotContain(rig.Log.Events.All, e => e.Action == LogActions.WSaveRefuse);
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_handed_over_capture_is_dropped_at_the_beginning_contexts_rollback()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));
        await second.SaveChangesAsync();

        await transaction.RollbackAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(request.Scope.Seqs);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Empty(rig.Wakeups);
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_handed_over_capture_is_dropped_at_the_beginning_contexts_disposal_and_the_next_start_logs_no_discard()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        var transaction = await first.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(raw);
        second.Settings.Add(NewSetting(1));
        await second.SaveChangesAsync();

        await transaction.DisposeAsync();
        await using var next = await first.Database.BeginTransactionAsync();

        Assert.Same(raw, next.GetDbTransaction());
        Assert.Empty(rig.Logger.Errors);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(request.Scope.Seqs);
        Assert.Empty(rig.Wakeups);
    }

    [Fact]
    public async Task A_commit_through_the_joining_context_is_refused_while_it_holds_the_handed_over_entries_and_the_beginning_commit_appends_them()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await using var joined = await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));
        await second.SaveChangesAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.False(request.Scope.CommitRefused);

        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Single(request.Scope.Seqs);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task A_hand_over_under_NpgsqlRetryingExecutionStrategy_is_appended_once_after_the_retry_rejoins()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        var transient = new FailFirstSettingInsert();
        await using var first = Retrying(rig, connection);
        var attempts = 0;

        await first.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            attempts++;
            await using var transaction = await first.Database.BeginTransactionAsync();
            await using var second = Retrying(rig, connection, transient);
            await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
            second.Settings.Add(NewSetting(1));
            await second.SaveChangesAsync();
            await transaction.CommitAsync();
        });

        Assert.Equal(2, attempts);
        Assert.Equal(2, transient.Seen);
        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        var seq = Assert.Single(request.Scope.Seqs);
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], rig.Wakeups);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_failed_append_whose_rollback_fails_on_an_open_connection_keeps_the_entries_for_the_retried_commit()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        var raw = transaction.GetDbTransaction();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT generate_series(1, 3)";
            command.Transaction = (NpgsqlTransaction)raw;
            await using var reader = await command.ExecuteReaderAsync();
            await reader.ReadAsync();

            await Assert.ThrowsAnyAsync<NpgsqlOperationInProgressException>(() => transaction.CommitAsync());
            Assert.Equal(1, rig.Interceptor.PairCount);
            Assert.Equal(raw, Assert.Single(request.Scope.OpenTransactions));
        }

        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_connection_lost_under_the_append_throws_the_connection_error_and_ends_every_pair()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await TerminateAsync(rig, connection);

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => transaction.CommitAsync());

        var lost = Assert.IsAssignableFrom<NpgsqlException>(failure);
        Assert.True(lost.IsTransient, $"{lost.GetType().Name}, SqlState {(lost as PostgresException)?.SqlState}, inner {lost.InnerException?.GetType().Name}");
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_connection_lost_before_fence_step_2s_rollback_throws_the_refusal_and_ends_every_pair()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var db = rig.NewContext(connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await TerminateAsync(rig, connection);
        await rig.Log.RunFenceStep2Async();

        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());

        Assert.Contains("fence step 2", refusal.Message, StringComparison.Ordinal);
        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_refusal_at_the_append_ends_every_pair_and_a_later_capture_in_a_raw_transaction_on_the_reused_object_is_refused()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(began);
        second.Settings.Add(NewSetting(1));
        await second.SaveChangesAsync();

        using (var hold = rig.Ack.Skip.Faults.HoldNext(CaptureFaultPoints.CommittingBeforeAppend))
        {
            var commit = transaction.CommitAsync();
            await hold.WaitReachedAsync();
            await rig.Log.RunFenceStep2Async();
            hold.Release();

            await Assert.ThrowsAsync<InvalidOperationException>(() => commit);
        }

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Contains(rig.Log.Events.All, e => e.Action == LogActions.WAppend && Equals(e.State![ObservationKeys.Refused], true));

        await using var raw = await connection.BeginTransactionAsync();
        Assert.Same(began, raw);
        await third.Database.UseTransactionAsync(raw);
        third.Settings.Add(NewSetting(2));

        await AssertRefusedAsync(rig, third);
        await raw.RollbackAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(rig.Logger.Errors);
    }

    [Fact]
    public async Task A_rollback_through_the_joining_context_ends_every_pair_and_a_later_capture_in_a_raw_transaction_on_the_reused_object_is_refused()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        var joined = await second.Database.UseTransactionAsync(began);
        second.Settings.Add(NewSetting(1));
        await second.SaveChangesAsync();

        await joined!.RollbackAsync();

        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);

        await using var raw = await connection.BeginTransactionAsync();
        Assert.Same(began, raw);
        await third.Database.UseTransactionAsync(raw);
        third.Settings.Add(NewSetting(2));

        await AssertRefusedAsync(rig, third);
        await raw.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_commit_through_a_joiner_that_joined_outside_the_request_is_refused_by_the_scope_the_transaction_began_in()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        IDbContextTransaction? joined;
        using (rig.Log.Scopes.Begin(ClusterScopeKind.Background))
        {
            joined = await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        }

        second.Audits.Add(new Audit { Id = 1, Text = "x" });
        await second.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_refused_commit_through_a_joiner_of_another_refused_request_is_recorded_in_both_requests()
    {
        await using var rig = await RigAsync();
        using var beginning = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        using var joining = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        var joined = await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.True(beginning.Scope.CommitRefused);
        Assert.True(joining.Scope.CommitRefused);
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_hand_over_with_no_scope_at_the_begin_and_at_the_save_is_appended()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        await second.SaveChangesAsync();
        await transaction.CommitAsync();

        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_joiners_capture_in_another_scope_of_the_same_kind_is_refused()
    {
        await using var rig = await RigAsync();
        using var firstRequest = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        using (var secondRequest = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest))
        {
            await AssertRefusedAsync(rig, second);
            Assert.Empty(secondRequest.Scope.OpenTransactions);
        }

        await transaction.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(firstRequest.Scope.Seqs);
    }

    [Fact]
    public async Task A_joiners_capture_with_no_ambient_scope_is_refused_for_a_transaction_a_scope_began()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        Task refused;
        using (ExecutionContext.SuppressFlow())
        {
            refused = Task.Run(async () =>
            {
                Assert.Null(rig.Log.Scopes.CurrentWriter);
                await AssertRefusedAsync(rig, second);
            });
        }

        await refused;
        await transaction.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(request.Scope.Seqs);
    }

    [Fact]
    public async Task A_capture_in_a_joined_raw_transaction_is_refused()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var other = rig.NewContext(connection);
        await using var raw = await connection.BeginTransactionAsync();
        await using var db = rig.NewContext(connection);
        await db.Database.UseTransactionAsync(raw);
        db.Settings.Add(NewSetting(1));

        await AssertRefusedAsync(rig, db);
        await raw.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_joiners_capture_after_the_beginning_contexts_transaction_ended_through_EF_is_refused_in_a_raw_transaction_on_the_reused_object()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(began);
        await transaction.CommitAsync();

        await using var raw = await connection.BeginTransactionAsync();
        Assert.Same(began, raw);
        await third.Database.UseTransactionAsync(raw);
        third.Settings.Add(NewSetting(1));

        await AssertRefusedAsync(rig, third);
        await raw.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_capture_after_a_commit_through_a_joining_context_is_refused_in_a_raw_transaction_on_the_reused_object()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        var joined = await second.Database.UseTransactionAsync(began);
        await joined!.CommitAsync();
        Assert.Equal(0, rig.Interceptor.PairCount);

        await using var raw = await connection.BeginTransactionAsync();
        Assert.Same(began, raw);
        await third.Database.UseTransactionAsync(raw);
        third.Settings.Add(NewSetting(1));

        await AssertRefusedAsync(rig, third);
        await raw.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_capture_after_fence_step_2s_rollback_is_refused_in_a_raw_transaction_on_the_reused_object()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(began);
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());
        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);

        await using var raw = await connection.BeginTransactionAsync();
        Assert.Same(began, raw);
        await third.Database.UseTransactionAsync(raw);
        third.Settings.Add(NewSetting(1));

        await AssertRefusedAsync(rig, third);
        await raw.RollbackAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_joiners_capture_after_a_later_TransactionStarted_on_the_object_is_refused()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var third = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        var began = transaction.GetDbTransaction();
        await second.Database.UseTransactionAsync(began);
        await began.CommitAsync();

        await using var later = await third.Database.BeginTransactionAsync();
        Assert.Same(began, later.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        await AssertRefusedAsync(rig, second);
        await later.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_joiners_capture_in_another_scope_than_the_beginning_pairs_is_refused()
    {
        await using var rig = await RigAsync();
        using var background = rig.Log.Scopes.Begin(ClusterScopeKind.Background);
        await using var connection = await rig.OpenAsync();
        await using var first = rig.NewContext(connection);
        await using var second = rig.NewContext(connection);
        await using var transaction = await first.Database.BeginTransactionAsync();
        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        second.Settings.Add(NewSetting(1));

        using (var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest))
        {
            await AssertRefusedAsync(rig, second);
            Assert.Empty(request.Scope.Seqs);
            Assert.Empty(request.Scope.OpenTransactions);
        }

        await transaction.CommitAsync();

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
        Assert.Empty(background.Scope.OpenTransactions);
    }

    internal static async Task TerminateAsync(CaptureRig rig, NpgsqlConnection connection)
    {
        await using var admin = await rig.OpenAsync();
        await using var terminate = new NpgsqlCommand("SELECT pg_terminate_backend(@pid, 10000)", admin);
        terminate.Parameters.AddWithValue("pid", connection.ProcessID);
        Assert.True((bool)(await terminate.ExecuteScalarAsync())!);
    }

    private static async Task AssertRefusedAsync(CaptureRig rig, CaptureDb db)
    {
        var refused = rig.Log.Events.All.Count(e => e.Action == LogActions.WSaveRefuse);
        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
        Assert.Contains("UseTransaction", refusal.Message, StringComparison.Ordinal);
        Assert.Equal(refused + 1, rig.Log.Events.All.Count(e => e.Action == LogActions.WSaveRefuse));
    }

    internal static CaptureDb Retrying(CaptureRig rig, DbConnection connection, IInterceptor? later = null)
    {
        var builder = new DbContextOptionsBuilder<CaptureDb>()
            .UseNpgsql(connection, o => o.ExecutionStrategy(d => new NpgsqlRetryingExecutionStrategy(d, 1, TimeSpan.FromMilliseconds(10), ["40001"])))
            .AddInterceptors(rig.Interceptor);
        if (later is not null)
        {
            builder.AddInterceptors(later);
        }

        return new CaptureDb(builder.Options);
    }

    private sealed class FailFirstSettingInsert : DbCommandInterceptor
    {
        private int seen;

        public int Seen => Volatile.Read(ref seen);

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            Check(command);
            return ValueTask.FromResult(result);
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            Check(command);
            return ValueTask.FromResult(result);
        }

        private void Check(DbCommand command)
        {
            if (command.CommandText.Contains("INSERT INTO", StringComparison.OrdinalIgnoreCase)
                && command.CommandText.Contains("setting", StringComparison.OrdinalIgnoreCase)
                && Interlocked.Increment(ref seen) == 1)
            {
                throw new PostgresException("could not serialize access", "ERROR", "ERROR", PostgresErrorCodes.SerializationFailure);
            }
        }
    }
}
