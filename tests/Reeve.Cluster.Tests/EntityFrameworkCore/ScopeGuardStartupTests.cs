using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster.Core;
using Reeve.Cluster.Wakeup;
using Unit.Log;
using Reeve.Cluster.Log;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;

namespace Unit.EntityFrameworkCore;

public sealed class ScopeGuardStartupTests
{
    private sealed class MappedReconciler : IReconciler
    {
        public string AggregateType => "mapped";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("mapped"));
    }

    private sealed class MappedRow { public int Id { get; set; } }
    private sealed class OtherRow { public int Id { get; set; } }
    private sealed class MissingDependency;
    private sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options)
    {
        public DbSet<MappedRow> Rows => Set<MappedRow>();
    }
    private sealed class UnmappedContext(DbContextOptions<UnmappedContext> options) : DbContext(options)
    {
        public DbSet<OtherRow> Rows => Set<OtherRow>();
    }
    private sealed class BrokenContext : DbContext
    {
        public BrokenContext(DbContextOptions<BrokenContext> options, MissingDependency missing) : base(options) => _ = missing;
    }

    [Fact]
    public async Task Mapped_context_without_capture_is_refused_at_start()
    {
        var failure = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Start(services => services.AddDbContext<TestContext>(o => o.UseNpgsql("Host=unused"))));
        Assert.Equal(ClusterConfigurationReason.ContextWithoutCapture, failure.Reason);
        Assert.Contains(nameof(TestContext), failure.Message);
        Assert.Contains(nameof(MappedRow), failure.Message);
    }

    [Fact]
    public async Task Captured_context_starts()
    {
        await Start(services => services.AddDbContext<TestContext>((sp, o) => o.UseNpgsql("Host=unused").UseReeveClusterCapture(sp)));
    }

    [Fact]
    public async Task Capture_added_in_OnConfiguring_starts() => await Start(services => services.AddDbContext<ConfiguringContext>(o => o.UseNpgsql("Host=unused")));

    [Fact]
    public async Task Context_with_only_unmapped_entities_starts()
    {
        await Start(services => services.AddDbContext<UnmappedContext>(o => o.UseNpgsql("Host=unused")));
    }

    [Fact]
    public async Task Factory_context_without_capture_is_refused_and_with_capture_starts()
    {
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Start(services => services.AddDbContextFactory<TestContext>(o => o.UseNpgsql("Host=unused"))));
        Assert.Equal(ClusterConfigurationReason.ContextWithoutCapture, refusal.Reason);
        Assert.Contains(nameof(MappedRow), refusal.Message);
        await Start(services => services.AddDbContextFactory<TestContext>((sp, o) => o.UseNpgsql("Host=unused").UseReeveClusterCapture(sp)));
    }

    [Fact]
    public async Task Pooled_context_without_capture_is_refused_and_with_capture_starts()
    {
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Start(services => services.AddDbContextPool<TestContext>(o => o.UseNpgsql("Host=unused"))));
        Assert.Equal(ClusterConfigurationReason.ContextWithoutCapture, refusal.Reason);
        Assert.Contains(nameof(MappedRow), refusal.Message);
        await Start(services => services.AddDbContextPool<TestContext>((sp, o) => o.UseNpgsql("Host=unused").UseReeveClusterCapture(sp)));
    }

    [Fact]
    public async Task Cannot_build_context_without_capture_is_refused_with_inner_exception()
    {
        var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => Start(services => services.AddDbContext<BrokenContext>(o => o.UseNpgsql("Host=unused"))));
        Assert.Equal(ClusterConfigurationReason.ContextWithoutCapture, refusal.Reason);
        Assert.Contains(nameof(BrokenContext), refusal.Message);
        Assert.NotNull(refusal.InnerException);
    }

    [Fact]
    public async Task Unmapped_context_from_factory_starts()
    {
        await Start(services => services.AddDbContextFactory<UnmappedContext>(o => o.UseNpgsql("Host=unused")));
    }

    [Fact]
    public async Task Unmapped_context_from_pooled_factory_starts()
    {
        await Start(services => services.AddPooledDbContextFactory<UnmappedContext>(o => o.UseNpgsql("Host=unused")));
    }

    private static async Task Start(Action<IServiceCollection> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddReeveClusterEntityFrameworkCore(b => b.Entity<MappedRow>("mapped").NoKey().AllColumns());
        builder.Services.AddSingleton<ClusterCaptureInterceptor>(sp => CreateInterceptor(sp.GetRequiredService<IClusterMapping>()));
        builder.Services.AddSingleton<IReconciler, MappedReconciler>();
        configure(builder.Services);
        using var host = builder.Build();
        await host.StartAsync();
        await host.StopAsync();
    }

    private static ClusterCaptureInterceptor CreateInterceptor(IClusterMapping mapping)
    {
        var steps = new FenceSteps();
        var scopes = new ClusterScopeSource(steps);
        var transactions = new EfTransactions();
        var refusal = new CommitRefusal(new EventGate(), steps);
        var events = NoModelEvents.Instance;
        var faults = NoFaultPoints.Instance;
        var log = new ChangeLog(scopes, refusal, transactions, faults, events);
        return new ClusterCaptureInterceptor(mapping, scopes, log, refusal, transactions,
            new PostCommitWakeup(new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), faults, events),
            faults, events, NullLogger<ClusterCaptureInterceptor>.Instance);
    }

    private sealed class ConfiguringContext(DbContextOptions<ConfiguringContext> options, IServiceProvider services) : DbContext(options)
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder.UseReeveClusterCapture(services);
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<MappedRow>();
    }
}
