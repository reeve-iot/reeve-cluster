using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Capture;

public sealed class AutoTransactionTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public AutoTransactionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public static TheoryData<ClusterScopeKind?> NotAClientRequest => new()
    {
        null,
        ClusterScopeKind.Background,
        ClusterScopeKind.UnitOfWork,
        ClusterScopeKind.ExemptRequest,
        ClusterScopeKind.Applier,
    };

    private static Setting NewSetting(int id) => new() { Id = id, Name = "n" + id, Value = "v" };

    [Theory]
    [MemberData(nameof(NotAClientRequest))]
    public async Task A_save_that_captures_nothing_on_a_context_that_has_not_captured_runs_without_a_transaction_outside_a_client_request(ClusterScopeKind? kind)
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var scope = kind is { } k ? rig.Log.Scopes.Begin(k) : null;
        var starts = new TransactionStarts();
        await using var db = Counted(rig, starts);
        db.Audits.Add(new Audit { Id = 1, Text = "x" });

        await db.SaveChangesAsync();

        Assert.Equal(AutoTransactionBehavior.WhenNeeded, db.Database.AutoTransactionBehavior);
        Assert.Equal(0, starts.Count);
        Assert.Equal(1, await rig.CountAsync("public.audit"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_context_keeps_Always_after_a_save_that_set_it()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        var starts = new TransactionStarts();
        await using var db = Counted(rig, starts);
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        Assert.Equal(AutoTransactionBehavior.Always, db.Database.AutoTransactionBehavior);
        Assert.Equal(1, starts.Count);

        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();

        Assert.Equal(AutoTransactionBehavior.Always, db.Database.AutoTransactionBehavior);
        Assert.Equal(2, starts.Count);
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task A_client_requests_save_that_captures_nothing_runs_in_a_transaction_and_is_refused_at_fence_step_2()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        var starts = new TransactionStarts();
        await using var db = Counted(rig, starts);
        db.Audits.Add(new Audit { Id = 1, Text = "x" });
        await db.SaveChangesAsync();
        Assert.Equal(AutoTransactionBehavior.Always, db.Database.AutoTransactionBehavior);
        Assert.Equal(1, starts.Count);

        await rig.Log.RunFenceStep2Async();
        db.Audits.Add(new Audit { Id = 2, Text = "y" });

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(1, await rig.CountAsync("public.audit"));
    }

    private static CaptureDb Counted(CaptureRig rig, TransactionStarts starts) =>
        new(new DbContextOptionsBuilder<CaptureDb>().UseNpgsql(rig.Log.ConnectionString).AddInterceptors(rig.Interceptor, starts).Options);

    private sealed class TransactionStarts : DbTransactionInterceptor
    {
        private int count;

        public int Count => Volatile.Read(ref count);

        public override InterceptionResult<DbTransaction> TransactionStarting(DbConnection connection, TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result)
        {
            Interlocked.Increment(ref count);
            return result;
        }

        public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(DbConnection connection, TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref count);
            return ValueTask.FromResult(result);
        }
    }
}
