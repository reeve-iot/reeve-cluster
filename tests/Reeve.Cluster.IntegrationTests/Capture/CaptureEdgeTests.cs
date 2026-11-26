using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Harness.Containers;

namespace Int.Capture;

public sealed class CascadeParent
{
    public int Id { get; set; }

    public string Name { get; set; } = "";

    public List<CascadeChild> Children { get; set; } = [];
}

public sealed class CascadeChild
{
    public int Id { get; set; }

    public int ParentId { get; set; }

    public string Value { get; set; } = "";
}

public sealed class CascadeDb(DbContextOptions<CascadeDb> options) : DbContext(options)
{
    public DbSet<CascadeParent> Parents => Set<CascadeParent>();

    public DbSet<CascadeChild> Children => Set<CascadeChild>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CascadeParent>(e =>
        {
            e.ToTable("cascade_parent");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Name).HasColumnName("name");
            e.HasMany(x => x.Children).WithOne().HasForeignKey(c => c.ParentId).OnDelete(DeleteBehavior.Cascade);
        });
        modelBuilder.Entity<CascadeChild>(e =>
        {
            e.ToTable("cascade_child");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.ParentId).HasColumnName("parent_id");
            e.Property(x => x.Value).HasColumnName("value");
        });
    }
}

public sealed class CaptureEdgeTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public CaptureEdgeTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private static Setting NewSetting(int id, string name = "n", string value = "v") => new() { Id = id, Name = name + id, Value = value };

    private static CaptureDb Context(CaptureRig rig, DbConnection? connection, Action<NpgsqlDbContextOptionsBuilder>? npgsql, params IInterceptor[] extra)
    {
        var builder = new DbContextOptionsBuilder<CaptureDb>();
        if (connection is null)
        {
            builder.UseNpgsql(rig.Log.ConnectionString, npgsql);
        }
        else
        {
            builder.UseNpgsql(connection, npgsql);
        }

        var all = new IInterceptor[extra.Length + 1];
        all[0] = rig.Interceptor;
        extra.CopyTo(all, 1);
        return new CaptureDb(builder.AddInterceptors(all).Options);
    }

    [Fact]
    public async Task A_cancelled_implicit_save_then_an_explicit_transaction_on_the_reused_object_appends_its_entries()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        using var cts = new CancellationTokenSource();
        var events = new SaveEvents();
        var cancel = new OnSettingInsert(() =>
        {
            cts.Cancel();
            return new OperationCanceledException(cts.Token);
        });
        await using var db = Context(rig, connection, null, cancel, events);

        db.Settings.Add(NewSetting(1));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));
        var pairsAfterCancel = rig.Interceptor.PairCount;
        db.ChangeTracker.Clear();

        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        var listed = request.Scope.OpenTransactions.Count;
        var pairs = rig.Interceptor.PairCount;
        await transaction.CommitAsync();

        var entries = await rig.EntriesAsync();
        Assert.Equal(1, listed);
        Assert.Equal([("setting", "n2")], entries);
    }

    [Fact]
    public async Task A_save_cancelled_before_its_transaction_started_leaves_nothing_that_a_later_explicit_transaction_drops()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        using var cts = new CancellationTokenSource();
        var events = new SaveEvents();
        await using var db = Context(rig, null, null, new CancelFirstOpen(cts), events);

        db.Settings.Add(NewSetting(1));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));
        db.ChangeTracker.Clear();

        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        var listed = request.Scope.OpenTransactions.Count;
        var pairs = rig.Interceptor.PairCount;
        await transaction.CommitAsync();

        var entries = await rig.EntriesAsync();
        Assert.Equal(1, listed);
        Assert.Contains(("setting", (string?)"n2"), entries);
    }

    [Fact]
    public async Task A_save_refused_by_a_later_SavingChanges_interceptor_leaves_nothing_that_a_later_explicit_transaction_drops()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        var events = new SaveEvents();
        await using var db = Context(rig, null, null, new ThrowOnFirstSaving(), events);

        db.Settings.Add(NewSetting(1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
        db.ChangeTracker.Clear();

        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        var listed = request.Scope.OpenTransactions.Count;
        var pairs = rig.Interceptor.PairCount;
        await transaction.CommitAsync();

        var entries = await rig.EntriesAsync();
        Assert.Equal(1, listed);
        Assert.Contains(("setting", (string?)"n2"), entries);
    }

    [Fact]
    public async Task An_implicit_save_retried_by_an_execution_strategy_commits_its_entry_with_the_row()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        var transient = new OnSettingInsert(() => new TransientException());
        var events = new SaveEvents();
        await using var db = Context(rig, null, o => o.ExecutionStrategy(d => new RetryOnceStrategy(d)), transient, events);

        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        var entries = await rig.EntriesAsync();
        var settings = await rig.CountAsync("public.setting");
        Assert.Equal(1, settings);
        Assert.Equal([("setting", "n1")], entries);
    }

    [Fact]
    public async Task A_concurrency_failure_of_an_implicit_save_leaves_nothing_that_a_later_explicit_transaction_drops()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        await using (var seed = rig.NewContext())
        {
            seed.Settings.Add(NewSetting(1));
            await seed.SaveChangesAsync();
        }

        var seeded = (await rig.EntriesAsync()).Count;
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        var events = new SaveEvents();
        await using var db = Context(rig, connection, null, events);
        var setting = await db.Settings.SingleAsync();
        await using (var other = await rig.OpenAsync())
        {
            await using var delete = new NpgsqlCommand("DELETE FROM public.setting WHERE id = 1", other);
            await delete.ExecuteNonQueryAsync();
        }

        setting.Value = "changed";
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync());
        db.ChangeTracker.Clear();

        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(2));
        await db.SaveChangesAsync();
        var listed = request.Scope.OpenTransactions.Count;
        await transaction.CommitAsync();

        var entries = (await rig.EntriesAsync()).Skip(seeded).ToList();
        Assert.Equal(1, listed);
        Assert.Contains(("setting", (string?)"n2"), entries);
        Assert.All(entries, e => Assert.Contains(e.Key, new[] { "n1", "n2" }));
    }

    [Fact]
    public async Task A_key_change_gives_the_old_and_the_new_key_an_entry_when_the_key_column_is_not_counted()
    {
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<Setting>("setting").Key(s => s.Name).Columns(s => s.Value);
            b.Entity<Widget>("widget").NoKey().AllColumns();
        });
        await using var rig = await CaptureRig.CreateAsync(postgres, mapping);
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1, "old"));
        await db.SaveChangesAsync();

        (await db.Settings.SingleAsync()).Name = "new";
        await db.SaveChangesAsync();

        var entries = await rig.EntriesAsync();
        Assert.Equal([("setting", "old1"), ("setting", "new"), ("setting", "old1")], entries);
    }

    [Fact]
    public async Task A_counted_column_that_is_not_a_property_of_the_entity_throws_at_the_first_modified_save()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Setting>("setting").Key(s => s.Name).Columns("Nope"));
        await using var rig = await CaptureRig.CreateAsync(postgres, mapping);
        await using var db = rig.NewContext();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();

        (await db.Settings.SingleAsync()).Value = "changed";

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());
        Assert.Contains("Nope", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_save_under_CascadeTiming_OnSaveChanges_throws_and_writes_nothing()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        await using var db = rig.NewContext();
        db.ChangeTracker.CascadeDeleteTiming = CascadeTiming.OnSaveChanges;
        db.Settings.Add(NewSetting(1));

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveChangesAsync());

        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Equal(0, await rig.CountAsync("reeve_cluster.change_log"));
    }

    [Fact]
    public async Task A_dependent_deleted_by_an_immediate_cascade_gets_its_entry()
    {
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<CascadeParent>("cascade-parent").Key(p => p.Id.ToString(CultureInfo.InvariantCulture)).AllColumns();
            b.Entity<CascadeChild>("cascade-child").Key(c => c.Id.ToString(CultureInfo.InvariantCulture)).AllColumns();
        });
        await using var rig = await CaptureRig.CreateAsync(postgres, mapping);
        await using (var connection = await rig.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                """
                CREATE TABLE public.cascade_parent (id int PRIMARY KEY, name text NOT NULL);
                CREATE TABLE public.cascade_child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES public.cascade_parent (id) ON DELETE CASCADE, value text NOT NULL);
                """, connection);
            await command.ExecuteNonQueryAsync();
        }

        CascadeDb NewDb() => new(new DbContextOptionsBuilder<CascadeDb>().UseNpgsql(rig.Log.ConnectionString).AddInterceptors(rig.Interceptor).Options);

        await using (var seed = NewDb())
        {
            seed.Parents.Add(new CascadeParent { Id = 1, Name = "p", Children = [new CascadeChild { Id = 1, Value = "c" }] });
            await seed.SaveChangesAsync();
        }

        var before = (await rig.EntriesAsync()).Count;
        await using var db = NewDb();
        db.Parents.Remove(await db.Parents.Include(p => p.Children).SingleAsync());
        await db.SaveChangesAsync();

        var after = (await rig.EntriesAsync()).Skip(before).ToList();
        Assert.Equal(0, await rig.CountAsync("public.cascade_child"));
        Assert.Contains(("cascade-child", (string?)"1"), after);
        Assert.Contains(("cascade-parent", (string?)"1"), after);
    }

    private sealed class SaveEvents : SaveChangesInterceptor
    {
        private readonly List<string> seen = [];

        public string Seen
        {
            get
            {
                lock (seen)
                {
                    return string.Join(",", seen);
                }
            }
        }

        public override void SaveChangesFailed(DbContextErrorEventData eventData) => Add("Failed:" + eventData.Exception.GetType().Name);

        public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken cancellationToken = default)
        {
            Add("Failed:" + eventData.Exception.GetType().Name);
            return Task.CompletedTask;
        }

        public override void SaveChangesCanceled(DbContextEventData eventData) => Add("Canceled");

        public override Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken cancellationToken = default)
        {
            Add("Canceled");
            return Task.CompletedTask;
        }

        public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
        {
            Add("Saved");
            return result;
        }

        public override ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            Add("Saved");
            return ValueTask.FromResult(result);
        }

        private void Add(string what)
        {
            lock (seen)
            {
                seen.Add(what);
            }
        }
    }

    private sealed class OnSettingInsert(Func<Exception?> onFirst) : DbCommandInterceptor
    {
        private int seen;

        public int Seen => Volatile.Read(ref seen);

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            Check(command);
            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            Check(command);
            return ValueTask.FromResult(result);
        }

        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
        {
            Check(command);
            return result;
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            Check(command);
            return ValueTask.FromResult(result);
        }

        private void Check(DbCommand command)
        {
            var sql = command.CommandText;
            if (sql.Contains("INSERT INTO", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("setting", StringComparison.OrdinalIgnoreCase)
                && Interlocked.Increment(ref seen) == 1
                && onFirst() is { } fault)
            {
                throw fault;
            }
        }
    }

    private sealed class CancelFirstOpen(CancellationTokenSource cts) : DbConnectionInterceptor
    {
        private int fired;

        public override ValueTask<InterceptionResult> ConnectionOpeningAsync(DbConnection connection, ConnectionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref fired, 1) == 0)
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            }

            return ValueTask.FromResult(result);
        }
    }

    private sealed class ThrowOnFirstSaving : SaveChangesInterceptor
    {
        private int fired;

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default) =>
            Interlocked.Exchange(ref fired, 1) == 0
                ? throw new InvalidOperationException("a validation interceptor refused the save")
                : ValueTask.FromResult(result);
    }

    private sealed class TransientException() : Exception("a transient failure");

    private sealed class RetryOnceStrategy(ExecutionStrategyDependencies dependencies) : ExecutionStrategy(dependencies, 1, TimeSpan.FromMilliseconds(10))
    {
        protected override bool ShouldRetryOn(Exception exception) => exception is TransientException;
    }
}
