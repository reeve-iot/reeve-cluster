using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;
using Unit.Log;

namespace Unit.EntityFrameworkCore;

public sealed class BulkOperationGuardTests
{
    private static readonly ClusterCaptureInterceptor CaptureInterceptor = CreateCaptureInterceptor();

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Unmarked_mapped_bulk_operations_are_refused(bool update, bool async)
    {
        await using var db = NewContext();
        var error = async
            ? await Assert.ThrowsAsync<InvalidOperationException>(() => update ? db.Mapped.ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "x")) : db.Mapped.ExecuteDeleteAsync())
            : Assert.Throws<InvalidOperationException>(() => { if (update) db.Mapped.ExecuteUpdate(s => s.SetProperty(e => e.Value, "x")); else db.Mapped.ExecuteDelete(); });
        Assert.Contains(nameof(Mapped), error.Message);
        Assert.Contains("AppendedToChangeLog", error.Message);
    }

    [Fact]
    public async Task An_unmarked_bulk_operation_throws_again_each_time()
    {
        await using var db = NewContext();
        for (var i = 0; i < 2; i++)
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => db.Mapped.ExecuteDeleteAsync());
            Assert.Contains(nameof(Mapped), error.Message);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Marker_before_or_after_filter_passes(bool before)
    {
        await using var db = NewContext();
        var query = before ? db.Mapped.AppendedToChangeLog().Where(e => e.Id == 1) : db.Mapped.Where(e => e.Id == 1).AppendedToChangeLog();
        await Assert.ThrowsAsync<SentinelException>(() => query.ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Different_tag_does_not_pass() => await Assert.ThrowsAsync<InvalidOperationException>(() => NewContext().Mapped.TagWith("other").ExecuteDeleteAsync());

    [Fact]
    public async Task Marker_inside_predicate_does_not_pass()
    {
        await using var db = NewContext();
        var query = db.Mapped.Where(e => db.Mapped.AppendedToChangeLog().Any(m => m.Id == e.Id));
        await Assert.ThrowsAsync<InvalidOperationException>(() => query.ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Unmapped_type_passes() => await Assert.ThrowsAsync<SentinelException>(() => NewContext().Unmapped.ExecuteDeleteAsync());

    [Fact]
    public async Task Derived_mapped_type_and_unmapped_tph_base_with_mapped_derived_are_refused()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Deriveds.ExecuteDeleteAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.TphBases.ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Ordinary_query_on_mapped_type_passes() => await Assert.ThrowsAsync<SentinelException>(() => NewContext().Mapped.ToListAsync());

    [Fact]
    public async Task Fresh_mappings_across_25_contexts_do_not_create_new_internal_service_providers()
    {
        for (var i = 0; i < 25; i++)
        {
            await using var db = NewContext();
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => db.Mapped.ExecuteDeleteAsync());
            Assert.Contains(nameof(Mapped), error.Message);
        }
    }

    [Fact]
    public async Task Join_projection_update_of_a_mapped_entity_is_refused_and_marker_allows_it()
    {
        await using var db = NewContext();
        var query = db.Unmapped.Join(db.Mapped, a => a.Id, s => s.Id, (a, s) => new { a, s });
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => query.ExecuteUpdateAsync(x => x.SetProperty(p => p.s.Value, "v")));
        Assert.Contains(typeof(Mapped).FullName!, error.Message);

        await Assert.ThrowsAsync<SentinelException>(() => query.AppendedToChangeLog().ExecuteUpdateAsync(x => x.SetProperty(p => p.s.Value, "v")));
    }

    [Fact]
    public async Task Ef_property_selector_on_a_mapped_join_projection_is_refused()
    {
        await using var db = NewContext();
        var query = db.Unmapped.Join(db.Mapped, a => a.Id, s => s.Id, (a, s) => new { a, s });
        await Assert.ThrowsAsync<InvalidOperationException>(() => query.ExecuteUpdateAsync(x =>
            x.SetProperty(p => EF.Property<string>(p.s, nameof(Mapped.Value)), "v")));
    }

    [Fact]
    public async Task Top_level_tag_on_an_ordinary_query_is_not_treated_as_a_bulk_operation()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<SentinelException>(() => db.Mapped.TagWith("other").ToListAsync());
    }

    [Fact]
    public async Task Unrecognised_update_setters_on_a_mapped_root_fail_closed_unless_the_source_is_marked()
    {
        await using var db = NewRecordingContext();

        await CaptureUpdateAsync(db.Mapped);
        var unmarked = RebuildWithEmptySetters(ExpressionRecorder.Instance.Expression!);
        Assert.Throws<InvalidOperationException>(() => BulkOperationGuard.Instance.QueryCompilationStarting(unmarked, ExpressionRecorder.Instance.EventData!));

        await CaptureUpdateAsync(db.Mapped.AppendedToChangeLog());
        var marked = RebuildWithEmptySetters(ExpressionRecorder.Instance.Expression!);
        Assert.Same(marked, BulkOperationGuard.Instance.QueryCompilationStarting(marked, ExpressionRecorder.Instance.EventData!));
    }

    [Fact]
    public async Task Unrecognised_update_setters_on_an_unmapped_root_pass_without_throwing()
    {
        await using var db = NewRecordingContext();

        await CaptureUpdateAsync(db.Unmapped);
        var unmapped = RebuildWithEmptySetters(ExpressionRecorder.Instance.Expression!);
        Assert.Same(unmapped, BulkOperationGuard.Instance.QueryCompilationStarting(unmapped, ExpressionRecorder.Instance.EventData!));
    }

    [Fact]
    public async Task Update_of_only_non_counted_columns_passes()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<SentinelException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Other, "x")));
        await Assert.ThrowsAsync<SentinelException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => EF.Property<string>(e, nameof(Counted.Other)), "x")));
    }

    [Fact]
    public async Task Update_that_sets_a_counted_column_is_refused_alone_or_beside_a_non_counted_one()
    {
        await using var db = NewContext();
        var alone = await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "x")));
        Assert.Contains(nameof(Counted), alone.Message);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Other, "x").SetProperty(e => e.Value, "y")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => EF.Property<string>(e, nameof(Counted.Value)), "x")));
        await Assert.ThrowsAsync<SentinelException>(() => db.Counteds.AppendedToChangeLog().ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "x")));
    }

    [Fact]
    public async Task Update_that_sets_a_primary_key_or_foreign_key_is_refused_though_neither_is_counted()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Id, 5)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.ParentId, 5)));
    }

    [Fact]
    public async Task Update_of_a_column_of_an_all_columns_type_is_refused_whichever_it_is()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Mapped.ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "x")));
    }

    [Fact]
    public async Task Owned_column_update_is_refused_for_the_mapped_root_though_it_is_not_counted()
    {
        await using var db = NewContext();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Detail.Note, "x")));
        Assert.Contains(nameof(Counted), error.Message);
    }

    [Fact]
    public async Task Update_through_a_navigation_to_a_type_owned_twice_is_refused_for_the_mapped_root()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Detail.Note, "x")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteUpdateAsync(s => s.SetProperty(e => e.Extra.Note, "x")));
    }

    [Fact]
    public async Task Join_projection_writing_a_counted_column_is_refused_and_a_non_counted_one_passes()
    {
        await using var db = NewContext();
        var query = db.Unmapped.Join(db.Counteds, a => a.Id, c => c.Id, (a, c) => new { a, c });
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => query.ExecuteUpdateAsync(x => x.SetProperty(p => p.c.Value, "v")));
        Assert.Contains(typeof(Counted).FullName!, error.Message);
        await Assert.ThrowsAsync<SentinelException>(() => query.ExecuteUpdateAsync(x => x.SetProperty(p => p.c.Other, "v")));
    }

    [Fact]
    public async Task Delete_of_a_mapped_type_with_counted_columns_is_refused()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Counteds.ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Mapped_value_read_does_not_guard_an_unmapped_update_target()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<SentinelException>(() => db.Unmapped.ExecuteUpdateAsync(x =>
            x.SetProperty(p => p.Id, p => db.Mapped.Select(m => m.Id).FirstOrDefault())));
    }

    [Fact]
    public async Task Ef_property_selector_on_a_mapped_type_is_refused()
    {
        await using var db = NewContext();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Mapped.ExecuteUpdateAsync(x => x.SetProperty(p => EF.Property<string>(p, nameof(Mapped.Value)), "v")));
    }

    [Fact]
    public async Task Compiled_async_delete_is_guarded_and_marked_compiled_async_delete_passes()
    {
        await using var db = NewContext();
        var unmarked = EF.CompileAsyncQuery((GuardDb context) => context.Mapped.ExecuteDeleteAsync());
        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => unmarked(db));
        Assert.Contains("AppendedToChangeLog", Assert.IsType<InvalidOperationException>(refusal.InnerException).Message, StringComparison.Ordinal);

        var marked = EF.CompileAsyncQuery((GuardDb context) => context.Mapped.AppendedToChangeLog().ExecuteDeleteAsync());
        var passed = await Assert.ThrowsAsync<InvalidOperationException>(() => marked(db));
        Assert.IsType<SentinelException>(passed.InnerException);
    }

    [Fact]
    public void Marker_rejects_null_source() => Assert.Throws<ArgumentNullException>(() => ReeveClusterQueryableExtensions.AppendedToChangeLog<Unmapped>(null!));

    private static GuardDb NewContext()
    {
        var mapping = BuildMapping();
        var builder = new DbContextOptionsBuilder<GuardDb>().UseNpgsql("Host=127.0.0.1;Port=1");
        builder.AddClusterCapture(CaptureInterceptor, mapping).AddInterceptors(SentinelInterceptor.Instance);
        return new GuardDb(builder.Options);
    }

    private static IClusterMapping BuildMapping() => ClusterMapping.Build(b =>
    {
        b.Entity<Mapped>("mapped").NoKey().AllColumns();
        b.Entity<Root>("root").NoKey().AllColumns();
        b.Entity<TphDerived>("tph").NoKey().AllColumns();
        b.Entity<Counted>("counted").Key(e => e.Id.ToString()).Columns(e => e.Value);
    });

    private static GuardDb NewRecordingContext()
    {
        var builder = new DbContextOptionsBuilder<GuardDb>().UseNpgsql("Host=127.0.0.1;Port=1");
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(new ClusterCaptureOptionsExtension(BuildMapping()));
        builder.AddInterceptors(ExpressionRecorder.Instance);
        return new GuardDb(builder.Options);
    }

    private static async Task CaptureUpdateAsync<T>(IQueryable<T> query)
        where T : class
    {
        ExpressionRecorder.Instance.Clear();
        await Assert.ThrowsAsync<SentinelException>(() => query.ExecuteUpdateAsync(s => s.SetProperty(e => EF.Property<int>(e, "Id"), 1)));
        Assert.IsAssignableFrom<MethodCallExpression>(ExpressionRecorder.Instance.Expression);
        Assert.NotNull(ExpressionRecorder.Instance.EventData);
    }

    private static MethodCallExpression RebuildWithEmptySetters(Expression expression)
    {
        var bulk = Assert.IsAssignableFrom<MethodCallExpression>(expression);
        var setters = Assert.IsAssignableFrom<NewArrayExpression>(bulk.Arguments[1]);
        var empty = Expression.NewArrayInit(setters.Type.GetElementType()!);
        return bulk.Update(bulk.Object, [bulk.Arguments[0], empty]);
    }

    private static ClusterCaptureInterceptor CreateCaptureInterceptor()
    {
        var gate = new EventGate();
        var steps = new FenceSteps();
        var scopes = new ClusterScopeSource(steps);
        var refusal = new CommitRefusal(gate, steps);
        var transactions = new EfTransactions();
        var changeLog = new ChangeLog(scopes, refusal, transactions, NoFaultPoints.Instance, NoModelEvents.Instance);
        var channel = new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance);
        return new ClusterCaptureInterceptor(
            ClusterMapping.Build(_ => { }), scopes, changeLog, refusal, transactions,
            new PostCommitWakeup(channel, NoFaultPoints.Instance, NoModelEvents.Instance),
            NoFaultPoints.Instance, NoModelEvents.Instance, NullLogger<ClusterCaptureInterceptor>.Instance);
    }

    private sealed class GuardDb(DbContextOptions<GuardDb> options) : DbContext(options)
    {
        public DbSet<Mapped> Mapped => Set<Mapped>();
        public DbSet<Unmapped> Unmapped => Set<Unmapped>();
        public DbSet<Counted> Counteds => Set<Counted>();
        public DbSet<Root> Roots => Set<Root>();
        public DbSet<Derived> Deriveds => Set<Derived>();
        public DbSet<TphBase> TphBases => Set<TphBase>();
        protected override void OnModelCreating(ModelBuilder model)
        {
            model.Entity<Root>();
            model.Entity<Derived>();
            model.Entity<TphBase>();
            model.Entity<TphDerived>();
            model.Entity<Counted>(e =>
            {
                e.OwnsOne(c => c.Detail);
                e.OwnsOne(c => c.Extra);
                e.HasOne<Unmapped>().WithMany().HasForeignKey(c => c.ParentId);
            });
        }
    }

    private sealed class Mapped { public int Id { get; set; } public string Value { get; set; } = ""; }
    private sealed class Unmapped { public int Id { get; set; } }
    private sealed class Counted
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
        public string Other { get; set; } = "";
        public int ParentId { get; set; }
        public CountedDetail Detail { get; set; } = new();
        public CountedDetail Extra { get; set; } = new();
    }
    private sealed class CountedDetail { public string Note { get; set; } = ""; }
    private class Root { public int Id { get; set; } }
    private sealed class Derived : Root;
    private class TphBase { public int Id { get; set; } }
    private sealed class TphDerived : TphBase;

    private sealed class SentinelException : Exception;
    private sealed class SentinelInterceptor : IQueryExpressionInterceptor
    {
        public static SentinelInterceptor Instance { get; } = new();

        public Expression QueryCompilationStarting(Expression queryExpression, QueryExpressionEventData eventData) => throw new SentinelException();
    }

    private sealed class ExpressionRecorder : IQueryExpressionInterceptor
    {
        public static ExpressionRecorder Instance { get; } = new();

        public Expression? Expression { get; private set; }

        public QueryExpressionEventData? EventData { get; private set; }

        public void Clear()
        {
            Expression = null;
            EventData = null;
        }

        public Expression QueryCompilationStarting(Expression queryExpression, QueryExpressionEventData eventData)
        {
            Expression = queryExpression;
            EventData = eventData;
            throw new SentinelException();
        }
    }
}
