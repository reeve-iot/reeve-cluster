using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class WriteStimulusTests(StackFixture stack)
{
    private static ReplicaSpec Spec() => new() { ConfigureServices = s => s.TryAddSingleton<IPhaseGate, QuietGate>() };

    [Fact]
    public async Task A_write_that_commits_records_WBegin_WAppend_and_WCommit_under_one_xid()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack.Stack);
        await run.MigrateAsync();
        var replica = await run.PrepareAsync("r1", Spec());

        await using var write = await HarnessOperations.StartWrite(replica);
        var seq = await write.AppendAsync([new ChangeEntry("widget")]);
        await write.CommitAsync();

        Assert.Equal([seq], write.Scope.Seqs);
        var events = run.Events.Snapshot().Where(e => e.Replica == "r1").ToList();
        Assert.Equal([LogActions.WBegin, LogActions.WAppend, LogActions.WCommit], events.Select(e => e.Action));
        Assert.All(events, e => Assert.Equal(write.Xid, e.State![ObservationKeys.Xid]));
        Assert.Equal(seq, events[^1].State![ObservationKeys.Seq]);
        Assert.Contains(LogFaultPoints.AfterCommitBeforeWakeup.Name, run.Arrivals.Snapshot().Select(a => a.Point));
    }

    [Fact]
    public async Task A_write_rolled_back_after_its_append_records_WRollback_and_keeps_its_seq_in_the_tracker()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack.Stack);
        await run.MigrateAsync();
        var replica = await run.PrepareAsync("r1", Spec());

        await using var write = await HarnessOperations.StartWrite(replica);
        var seq = await write.AppendAsync([new ChangeEntry("widget")]);
        await HarnessOperations.RollbackWrite(write);

        Assert.Equal([seq], write.Scope.Seqs);
        var events = run.Events.Snapshot().Where(e => e.Replica == "r1").ToList();
        Assert.Equal([LogActions.WBegin, LogActions.WAppend, LogActions.WRollback], events.Select(e => e.Action));
        Assert.All(events, e => Assert.Equal(write.Xid, e.State![ObservationKeys.Xid]));
    }

    [Fact]
    public async Task A_capturing_save_in_a_transaction_handed_to_EF_records_WSaveRefuse_and_writes_nothing()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack.Stack);
        await run.MigrateAsync();
        var replica = await run.PrepareAsync("r1", new ReplicaSpec
        {
            ConfigureServices = s =>
            {
                s.TryAddSingleton<IPhaseGate, QuietGate>();
                HarnessCapture.Register(s);
            },
        });

        await using var write = await HarnessOperations.StartWrite(replica);
        await HarnessOperations.SaveInForeignTransaction(write);
        await write.CommitAsync();

        Assert.Empty(write.Scope.Seqs);
        Assert.Equal([LogActions.WBegin, LogActions.WSaveRefuse], run.Events.Snapshot().Where(e => e.Replica == "r1").Select(e => e.Action));
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand("SELECT count(*) FROM reeve_cluster.change_log", connection);
        Assert.Equal(0L, (long)(await command.ExecuteScalarAsync())!);
    }

    private sealed class QuietGate : IPhaseGate
    {
        public ReplicaPhase Phase => ReplicaPhase.Starting;

        public long? FenceStartTimestamp => null;

        public bool ExitAfterFence => false;

        public IReadinessReasons Readiness => throw new NotSupportedException();

        public IDisposable Subscribe(Action<PhaseEvent> handler) => new Nothing();

        public ServingCheckResult CheckServing() => throw new NotSupportedException();

        public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

        public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

        public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

        public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

        private sealed class Nothing : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
