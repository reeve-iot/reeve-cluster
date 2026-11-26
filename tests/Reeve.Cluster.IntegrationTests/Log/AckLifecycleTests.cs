using System.Collections.Concurrent;
using Int.Membership;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class AckLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<IHost> hosts = [];
    private MemberDatabase db = null!;

    public AckLifecycleTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        foreach (var host in hosts)
        {
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
            host.Dispose();
        }

        await db.DisposeAsync();
    }

    [Fact]
    public async Task A_write_during_start_up_answers_NotAwaited_Joining_the_replica_still_activates_and_a_write_in_Serving_is_acknowledged()
    {
        var events = new RecordedEvents();
        var faults = new HarnessFaultPoints("r1", TimeProvider.System, new ArrivalLog(new RunSequence()));
        using var load = faults.HoldNext(LogFaultPoints.InsideFirstLoad);
        var host = Build(events, faults, []);
        var ack = host.Services.GetRequiredService<IChangeAck>();
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Joining), await ack.WaitAppliedAsync(await WriteAsync()).WaitAsync(Wait));

        var start = host.StartAsync();
        await load.WaitReachedAsync(timeout: Wait);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Joining), await ack.WaitAppliedAsync(await WriteAsync()).WaitAsync(Wait));
        load.Release();
        await start.WaitAsync(Wait);
        await AwaitServingAsync(gate);

        var seq = await WriteAsync();
        Assert.IsType<AckOutcome.Acknowledged>(await ack.WaitAppliedAsync(seq).WaitAsync(Wait));
        var done = Assert.Single(events.All, e => e.Action == LogActions.AckDone);
        Assert.Equal<IEnumerable<string>>(["r1"], (IEnumerable<string>)done.State![ObservationKeys.WaitSet]!);
        Assert.True((long)done.State[ObservationKeys.Bound]! >= seq);
    }

    [Fact]
    public async Task A_reconcilers_wait_answers_NotAwaited_Applier_at_once_and_does_not_stall_its_pass()
    {
        var host = Build(new RecordedEvents(), null, [typeof(WaitingReconciler)]);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await host.StartAsync();
        await AwaitServingAsync(gate);

        var seq = await WriteAsync(WaitingReconciler.Type);

        Assert.IsType<AckOutcome.Acknowledged>(await host.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq).WaitAsync(Wait));
        var answers = host.Services.GetServices<IReconciler>().OfType<WaitingReconciler>().Single().Answers;
        Assert.NotEmpty(answers);
        Assert.All(answers, a => Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Applier), a));
    }

    private IHost Build(RecordedEvents events, HarnessFaultPoints? faults, Type[] reconcilers)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton<IModelEventSink>(events);
        if (faults is not null)
        {
            builder.Services.AddSingleton<IFaultPoints>(faults);
        }

        builder.Services.AddReeveCluster(new ClusterPorts(reconcilers, typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
        });
        var host = builder.Build();
        hosts.Add(host);
        return host;
    }

    private async Task<long> WriteAsync(string type = "widget")
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('{type}', 'k') RETURNING seq", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task AwaitServingAsync(LocalPhaseGate gate)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (gate.Phase == ReplicaPhase.Serving)
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private sealed class WaitingReconciler(IServiceProvider services) : IReconciler
    {
        public const string Type = "waiting";

        public ConcurrentQueue<AckOutcome> Answers { get; } = new();

        public string AggregateType => Type;

        public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
        {
            if (context.Head > 0)
            {
                Answers.Enqueue(await services.GetRequiredService<IChangeAck>().WaitAppliedAsync(context.Head, cancellationToken));
            }

            return new ReconcileResult(context.Head);
        }
    }
}
