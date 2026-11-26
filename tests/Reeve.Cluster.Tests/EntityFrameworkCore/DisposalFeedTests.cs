using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;
using Unit.Log;

namespace Unit.EntityFrameworkCore;

public sealed class DisposalFeedTests : IDisposable
{
    private const string ListenerName = "Microsoft.EntityFrameworkCore";

    private readonly EventGate gate = new();
    private readonly FenceSteps steps = new();
    private readonly ClusterScopeSource scopes;
    private readonly CommitRefusal refusal;
    private readonly EfTransactions efTransactions = new();
    private readonly ClusterCaptureInterceptor interceptor;
    private readonly DiagnosticListener listener = new(ListenerName);
    private readonly DbContext context = new(new DbContextOptions<DbContext>());

    public DisposalFeedTests()
    {
        scopes = new ClusterScopeSource(steps);
        refusal = new CommitRefusal(gate, steps);
        var changeLog = new ChangeLog(scopes, refusal, efTransactions, NoFaultPoints.Instance, NoModelEvents.Instance);
        interceptor = new ClusterCaptureInterceptor(
            ClusterMapping.Build(_ => { }), scopes, changeLog, refusal, efTransactions,
            new PostCommitWakeup(new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance), NoFaultPoints.Instance, NoModelEvents.Instance),
            NoFaultPoints.Instance, NoModelEvents.Instance, NullLogger<ClusterCaptureInterceptor>.Instance);
    }

    public void Dispose()
    {
        listener.Dispose();
        interceptor.Dispose();
        context.Dispose();
        refusal.Dispose();
    }

    [Theory]
    [MemberData(nameof(NonDisposalEvents))]
    public void An_event_other_than_the_disposal_leaves_the_pair_in_place(string eventName)
    {
        using var transaction = new FakeTransaction();
        var id = Begin(transaction);

        listener.Write(eventName, Data(transaction, id));

        Assert.Equal(1, interceptor.PairCount);
    }

    [Fact]
    public void The_disposal_event_drops_the_pair_of_its_transaction()
    {
        using var transaction = new FakeTransaction();
        var id = Begin(transaction);

        listener.Write(RelationalEventId.TransactionDisposed.Name!, Data(transaction, id));

        Assert.Equal(0, interceptor.PairCount);
    }

    [Fact]
    public void The_disposal_event_of_another_transaction_id_leaves_the_pair_in_place()
    {
        using var transaction = new FakeTransaction();
        Begin(transaction);

        listener.Write(RelationalEventId.TransactionDisposed.Name!, Data(transaction, Guid.NewGuid()));

        Assert.Equal(1, interceptor.PairCount);
    }

    public static TheoryData<string> NonDisposalEvents() => new()
    {
        RelationalEventId.TransactionCommitting.Name!,
        RelationalEventId.TransactionCommitted.Name!,
        RelationalEventId.CreatingTransactionSavepoint.Name!,
        RelationalEventId.CreatedTransactionSavepoint.Name!,
        RelationalEventId.TransactionStarted.Name!,
        RelationalEventId.TransactionUsed.Name!,
    };

    private Guid Begin(DbTransaction transaction)
    {
        var id = Guid.NewGuid();
        var started = new TransactionEndEventData(null!, null!, transaction, context, id, Guid.NewGuid(), false, DateTimeOffset.UtcNow, TimeSpan.Zero);
        interceptor.TransactionStarted(null!, started, transaction);
        Assert.Equal(1, interceptor.PairCount);
        return id;
    }

    private TransactionEventData Data(DbTransaction transaction, Guid id) =>
        new(null!, null!, transaction, context, id, Guid.NewGuid(), false, DateTimeOffset.UtcNow);

    private sealed class FakeTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}
