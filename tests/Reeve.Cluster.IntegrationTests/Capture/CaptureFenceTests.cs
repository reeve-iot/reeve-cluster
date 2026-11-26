using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Capture;

public sealed class CaptureFenceTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public CaptureFenceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<CaptureRig> RigAsync() => CaptureRig.CreateAsync(postgres);

    private static Setting NewSetting(int id) => new() { Id = id, Name = "n" + id, Value = "v" };

    [Fact]
    public async Task A_client_requests_explicit_transaction_that_captured_nothing_is_refused_and_the_tracker_records_it()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());
        await transaction.DisposeAsync();

        Assert.True(request.Scope.CommitRefused);
        Assert.Empty(request.Scope.Seqs);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
        Assert.Equal(0, rig.Interceptor.PairCount);

        Assert.DoesNotContain(rig.Log.Events.All, e => e.Action == LogActions.WAppend);
    }

    [Fact]
    public async Task A_refused_commit_rolls_the_transaction_back_so_a_commit_around_EF_after_it_commits_nothing()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.GetDbTransaction().CommitAsync());
        Assert.Equal(0, await rig.CountAsync("public.audit"));
    }

    [Fact]
    public async Task A_commit_is_judged_by_the_scope_its_transaction_began_in_not_the_one_ambient_at_the_commit()
    {
        await using var rig = await RigAsync();
        await rig.Log.RunFenceStep2Async();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using (var refused = await db.Database.BeginTransactionAsync())
        {
            db.Audits.Add(new Audit { Id = 1, Text = "x" });
            await db.SaveChangesAsync();
            using (rig.Log.Scopes.Begin(ClusterScopeKind.Applier))
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => refused.CommitAsync());
            }
        }

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
        db.ChangeTracker.Clear();

        IDbContextTransaction committed;
        using (rig.Log.Scopes.Begin(ClusterScopeKind.Applier))
        {
            committed = await db.Database.BeginTransactionAsync();
        }

        await using (committed)
        {
            db.Audits.Add(new Audit { Id = 2, Text = "y" });
            await db.SaveChangesAsync();
            await committed.CommitAsync();
        }

        Assert.Equal(1, await rig.CountAsync("public.audit"));
    }

    [Fact]
    public async Task A_client_requests_implicit_save_that_captures_nothing_is_refused_and_writes_nothing()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await rig.Log.RunFenceStep2Async();
        await using var db = rig.NewContext();
        db.Audits.Add(new Audit { Id = 1, Text = "x" });

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
        Assert.Equal(0, rig.Interceptor.PairCount);
    }

    [Fact]
    public async Task A_client_requests_captured_commit_is_refused_before_its_append_and_emits_the_refused_WAppend()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.CommittingBeforeAppend));
        var append = Assert.Single(rig.Log.Events.All, e => e.Action == LogActions.WAppend);
        Assert.Equal(true, append.State![ObservationKeys.Refused]);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
    }

    [Fact]
    public async Task A_commit_through_a_joining_context_of_a_client_request_is_refused()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var raw = await connection.BeginTransactionAsync();
        await using var db = rig.NewContext(connection);
        await using var joined = await db.Database.UseTransactionAsync(raw);
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
    }

    [Fact]
    public async Task A_joiners_commit_while_the_capturing_context_holds_entries_is_refused_by_the_fence_recorded_and_rolled_back()
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
        await using var joined = await second.Database.UseTransactionAsync(raw);
        await rig.Log.RunFenceStep2Async();

        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => joined!.CommitAsync());

        Assert.Contains("fence step 2", refusal.Message, StringComparison.Ordinal);
        Assert.True(request.Scope.CommitRefused);
        await Assert.ThrowsAsync<InvalidOperationException>(() => raw.CommitAsync());
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_commit_whose_event_names_no_context_is_refused_in_a_client_request()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var raw = await connection.BeginTransactionAsync();
        await rig.Log.RunFenceStep2Async();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await rig.Interceptor.TransactionCommittingAsync(raw, NoContextEvent.Of(raw), default));

        Assert.True(request.Scope.CommitRefused);
    }

    [Theory]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public async Task A_transaction_of_an_exempt_request_or_outside_a_client_request_commits_and_its_entries_propagate(ClusterScopeKind kind)
    {
        await using var rig = await RigAsync();
        using var scope = rig.Log.Scopes.Begin(kind);
        await rig.Log.RunFenceStep2Async();
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1));
        db.Audits.Add(new Audit { Id = 1, Text = "x" });

        await db.SaveChangesAsync();

        Assert.False(scope.Scope.CommitRefused);
        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        Assert.Single(rig.Wakeups);

        Assert.Equal(kind == ClusterScopeKind.ExemptRequest ? 0 : 1, scope.Scope.Seqs.Count);
    }

    [Fact]
    public async Task A_transaction_outside_every_scope_commits()
    {
        await using var rig = await RigAsync();
        await rig.Log.RunFenceStep2Async();
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1));

        await db.SaveChangesAsync();

        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_client_request_begun_after_the_rejoin_commits()
    {
        await using var rig = await RigAsync();
        await rig.Log.RunFenceStep2Async();
        rig.Log.Gate.MoveTo(ReplicaPhase.Fenced);
        rig.Log.Gate.MoveTo(ReplicaPhase.Starting);

        using var after = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using (var db = rig.NewContext())
        {
            db.Audits.Add(new Audit { Id = 1, Text = "x" });
            await db.SaveChangesAsync();
        }

        Assert.False(after.Scope.CommitRefused);
        Assert.Equal(1, await rig.CountAsync("public.audit"));
    }

    [Fact]
    public async Task A_request_running_when_step_2_ran_is_refused_after_the_rejoin()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var db = rig.NewContext();
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        await rig.Log.RunFenceStep2Async();
        rig.Log.Gate.MoveTo(ReplicaPhase.Fenced);
        rig.Log.Gate.MoveTo(ReplicaPhase.Starting);
        Assert.False(rig.Log.Refusal.Refusing);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await rig.CountAsync("public.audit"));
    }
}
