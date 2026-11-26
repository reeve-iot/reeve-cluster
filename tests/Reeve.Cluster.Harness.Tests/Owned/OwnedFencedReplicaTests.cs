using System.Collections.Concurrent;
using Harness.Placer;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Owned;

[Collection(StackCollection.Name)]
public sealed class OwnedFencedReplicaTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = "widget";
    private const string Ledger = "ledger";

    private static readonly EligibilityFilter Hosting = EligibilityFilter.Any.Require(PlacerRig.WidgetHost);

    [Fact]
    public async Task A_units_write_on_a_fenced_replica_commits_and_answers_NotAwaited_Fenced()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        await rig.ExecuteAsync("CREATE TABLE ledger_item (id text PRIMARY KEY, value text NOT NULL)");
        var spec = rig.Spec(services => services.AddClusterUnit<StopWriter>(Kind, o => o.Eligibility = Hosting), reconcilers: [typeof(LedgerReconciler)]);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        var start = await rig.Log.StartAsync(s => s.Replica == "r1" && s.Kind == Kind, "the unit on r1");
        var gate = r1.Services.GetRequiredService<LocalPhaseGate>();
        var epoch = gate.Epoch!.Value;
        var write = r1.Services.GetRequiredService<StopWriter>().WriteAtStop();

        using var held = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep5);
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await held.WaitReachedAsync(timeout: OwnedRig.Limit);
        var (seq, outcome, phase) = await write.WaitAsync(OwnedRig.Limit);

        Assert.Equal(ReplicaPhase.Fencing, phase);
        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Fenced), outcome);
        Assert.Equal(1, await rig.ScalarAsync($"SELECT count(*) FROM reeve_cluster.change_log WHERE seq = {seq}"));

        Assert.IsType<AckOutcome.Acknowledged>(await r2.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq).WaitAsync(OwnedRig.Limit));

        held.Release();
        await OwnedRig.UntilAsync(() => Task.FromResult(gate.Phase == ReplicaPhase.Serving && gate.Epoch > epoch), "r1 to rejoin");
        Assert.True(r1.Services.GetRequiredService<IClusterGenerations>().Current!.Head >= seq, "the rejoin did not load the entry the fenced replica committed");
    }

    [Fact]
    public async Task I21_after_an_acknowledged_write_no_action_of_a_unit_reads_a_generation_without_it()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        await rig.ExecuteAsync("CREATE TABLE ledger_item (id text PRIMARY KEY, value text NOT NULL)");
        var spec = rig.Spec(services => services.AddClusterUnit<ConfigObserver>(Kind, o => o.Eligibility = Hosting), reconcilers: [typeof(LedgerReconciler)]);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        await rig.Log.StartAsync(s => s.Replica == "r1" && s.Kind == Kind, "the unit on r1");
        var unit = r1.Services.GetRequiredService<ConfigObserver>();

        for (var i = 1; i <= 2; i++)
        {
            Assert.IsType<AckOutcome.Acknowledged>(await WriteAsync(rig, r2, $"w{i}"));
            unit.Acknowledged(i);
        }

        using var held = r1.Faults.HoldNext(LogFaultPoints.PassBegin);
        var third = Task.Run(() => WriteAsync(rig, r2, "w3"));
        await held.WaitReachedAsync(timeout: OwnedRig.Limit);
        await OwnedRig.UntilAsync(async () => await rig.ScalarAsync("SELECT count(*) FROM ledger_item WHERE id = 'w3'") == 1, "w3 to commit");
        var seen = unit.Observations.Count;
        await OwnedRig.UntilAsync(() => Task.FromResult(unit.Observations.Count >= seen + 2), "two reads of the unit while r1's applier is held");
        Assert.False(third.IsCompleted, "the write was acknowledged while r1 had not applied it");
        Assert.Contains(unit.Observations.Skip(seen), o => o.Acked == 2 && !o.State.ContainsKey("w3"));

        held.Release();
        Assert.IsType<AckOutcome.Acknowledged>(await third.WaitAsync(OwnedRig.Limit));
        unit.Acknowledged(3);
        var after = unit.Observations.Count;
        await OwnedRig.UntilAsync(() => Task.FromResult(unit.Observations.Skip(after).Count(o => o.Acked == 3) >= 3), "reads of the unit after the acknowledgement");

        Assert.All(unit.Observations, o => Assert.True(o.Acked == 0 || o.State.ContainsKey($"w{o.Acked}"), $"a read that began after w{o.Acked} was acknowledged lacks it"));
    }

    [Fact]
    public async Task I21_a_unit_on_a_fenced_replica_pins_no_generation_and_reads_the_acknowledged_write_after_the_rejoin()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        await rig.ExecuteAsync("CREATE TABLE ledger_item (id text PRIMARY KEY, value text NOT NULL)");
        var spec = rig.Spec(services => services.AddClusterUnit<ConfigObserver>(Kind, o => o.Eligibility = Hosting), reconcilers: [typeof(LedgerReconciler)]);
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        var start = await rig.Log.StartAsync(s => s.Replica == "r1" && s.Kind == Kind, "the unit on r1");
        var gate = r1.Services.GetRequiredService<LocalPhaseGate>();
        var epoch = gate.Epoch!.Value;

        using var held = r1.Faults.Hold(MembershipFaultPoints.FenceBeforeStep5);
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await held.WaitReachedAsync(timeout: OwnedRig.Limit);

        Assert.IsType<AckOutcome.Acknowledged>(await WriteAsync(rig, r2, "w1"));

        using (r1.Services.GetRequiredService<IClusterScopeAccessor>().BeginUnitOfWork())
        {
            Assert.Throws<OwnershipLostException>(() => start.Context.Configuration);
            Assert.Null(r1.Services.GetRequiredService<IClusterGenerations>().TryPin());
        }

        held.Release();
        await OwnedRig.UntilAsync(() => Task.FromResult(gate.Phase == ReplicaPhase.Serving && gate.Epoch > epoch), "r1 to rejoin");
        var unit = r1.Services.GetRequiredService<ConfigObserver>();
        await OwnedRig.UntilAsync(
            () => Task.FromResult(unit.Observations.Any(o => o.State.ContainsKey("w1"))),
            "the unit that runs after the rejoin to read a generation with the acknowledged write");
    }

    private static async Task<AckOutcome> WriteAsync(OwnedRig rig, Replica replica, string id)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        long seq;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await using (var insert = new NpgsqlCommand("INSERT INTO ledger_item (id, value) VALUES (@id, 'v')", connection, transaction))
            {
                insert.Parameters.AddWithValue("id", id);
                await insert.ExecuteNonQueryAsync();
            }

            seq = await replica.Services.GetRequiredService<IChangeLog>().AppendAsync(transaction, [new ChangeEntry(Ledger, id)]);
            await transaction.CommitAsync();
        }

        return await replica.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq);
    }

    internal sealed class LedgerReconciler : IReconciler
    {
        public string AggregateType => Ledger;

        public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
        {
            var state = new Dictionary<string, string>(StringComparer.Ordinal);
            await using var command = new NpgsqlCommand("SELECT id, value FROM ledger_item", (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                state[reader.GetString(0)] = reader.GetString(1);
            }

            return new ReconcileResult(state);
        }
    }

    internal sealed class ConfigObserver(OwnedLog log, IOptions<ClusterOptions> options, IClusterScopeAccessor scopes) : IOwnedUnit
    {
        private readonly ConcurrentQueue<Observation> observations = new();
        private int acked;

        public IReadOnlyCollection<Observation> Observations => [.. observations];

        public void Acknowledged(int number) => Volatile.Write(ref acked, number);

        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            log.Started(new OwnedStart(options.Value.ReplicaId!, context.Kind, context.Key, context.Token, context, null));
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var before = Volatile.Read(ref acked);
                    using (scopes.BeginUnitOfWork())
                    {
                        observations.Enqueue(new Observation(before, context.Configuration.Get<Dictionary<string, string>>(Ledger)));
                    }

                    await Task.Delay(10, stop);
                }
            }
            catch (OwnershipLostException)
            {
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
        }

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }

    internal sealed record Observation(int Acked, Dictionary<string, string> State);

    internal sealed class StopWriter(OwnedLog log, IOptions<ClusterOptions> options, IServiceProvider services) : IOwnedUnit
    {
        private TaskCompletionSource<(long Seq, AckOutcome Outcome, ReplicaPhase Phase)>? write;

        public Task<(long Seq, AckOutcome Outcome, ReplicaPhase Phase)> WriteAtStop()
        {
            write = new TaskCompletionSource<(long, AckOutcome, ReplicaPhase)>(TaskCreationOptions.RunContinuationsAsynchronously);
            return write.Task;
        }

        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            log.Started(new OwnedStart(options.Value.ReplicaId!, context.Kind, context.Key, context.Token, context, null));

            var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (stop.Register(() => stopped.TrySetResult()))
            {
                await stopped.Task;
            }

            if (Interlocked.Exchange(ref write, null) is { } armed)
            {
                try
                {
                    armed.SetResult(await WriteAsync());
                }
                catch (Exception exception)
                {
                    armed.SetException(exception);
                }
            }
        }

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;

        private async Task<(long, AckOutcome, ReplicaPhase)> WriteAsync()
        {
            using var scope = services.GetRequiredService<IClusterScopeAccessor>().BeginUnitOfWork();
            var phase = services.GetRequiredService<IPhaseGate>().Phase;
            long seq;
            await using (var connection = new NpgsqlConnection(options.Value.ConnectionString))
            {
                await connection.OpenAsync();
                await using var transaction = await connection.BeginTransactionAsync();
                seq = await services.GetRequiredService<IChangeLog>().AppendAsync(transaction, [new ChangeEntry(Ledger, "stop")]);
                await transaction.CommitAsync();
            }

            return (seq, await services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq), phase);
        }
    }
}
