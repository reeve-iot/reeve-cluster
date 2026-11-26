using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster.Core;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;
using Unit.Log;

namespace Unit.EntityFrameworkCore;

public sealed class RequestScopeGuardTests
{
    private sealed class MappedRow { public int Id { get; set; } }
    private sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options)
    {
        public DbSet<MappedRow> Rows => Set<MappedRow>();
    }

    [Fact]
    public async Task Captured_write_in_request_without_scope_is_refused_before_connection()
    {
        using var interceptor = CreateInterceptor(ClusterMapping.Build(b => b.Entity<MappedRow>("mapped").NoKey().AllColumns()));
        var options = new DbContextOptionsBuilder<TestContext>().UseNpgsql("Host=unused").AddInterceptors(interceptor).Options;
        await using var context = new TestContext(options);
        using (HttpRequestFlow.Enter())
        {
            context.Rows.Add(new MappedRow());
            var failure = await Assert.ThrowsAsync<ClusterConfigurationException>(() => context.SaveChangesAsync());
            Assert.Equal(ClusterConfigurationReason.WriteOutsideClusterScope, failure.Reason);
            Assert.Contains(nameof(MappedRow), failure.Message);
        }
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
}
