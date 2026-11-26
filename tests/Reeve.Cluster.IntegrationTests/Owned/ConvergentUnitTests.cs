using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Int.Owned;

public sealed class ConvergentUnitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string Kind = "purge";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly List<IHost> hosts = [];
    private readonly Int.Log.PointProbe probe = new();
    private OwnedDatabase db = null!;

    public ConvergentUnitTests(PostgresFixture postgres) => this.postgres = postgres;

    public async Task InitializeAsync() => db = await OwnedDatabase.CreateAsync(postgres);

    public async Task DisposeAsync()
    {
        probe.Release();
        foreach (var host in hosts)
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(bound.Token);
            host.Dispose();
        }
    }

    private async Task<(IHost Host, LocalPhaseGate Gate)> StartAsync(string replica)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        if (replica == "r1")
        {
            builder.Services.AddSingleton<IFaultPoints>(probe);
            builder.Services.AddSingleton(s => new WritingUnit(s.GetRequiredService<IChangeLog>(), s.GetRequiredService<IChangeAck>(), s.GetRequiredService<LocalPhaseGate>(), db.ConnectionString));
            builder.Services.AddClusterUnit<WritingUnit>(Kind, o => o.Convergent = true);
        }

        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = replica;
            o.ConnectionString = db.ConnectionString;
        });
        var host = builder.Build();
        hosts.Add(host);
        await host.StartAsync().WaitAsync(Wait);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        await AwaitAsync(gate, g => g.Phase == ReplicaPhase.Serving);
        return (host, gate);
    }

    private static async Task AwaitAsync(LocalPhaseGate gate, Func<LocalPhaseGate, bool> condition)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (condition(gate))
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    [Fact]
    public async Task A_fence_stops_a_convergent_unit_at_step_5_and_the_placer_places_it_again_after_the_rejoin()
    {
        probe.Hold(MembershipFaultPoints.FenceBeforeStep5);
        var (host, gate) = await StartAsync("r1");
        var unit = host.Services.GetRequiredService<WritingUnit>();
        var first = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var epoch = gate.Epoch!.Value;

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));

        Assert.True(first.IsCancellationRequested);
        await probe.Arrived.WaitAsync(Wait);
        Assert.False(unit.LostStarted.IsCompleted);

        probe.Release();
        Assert.Equal((ReplicaPhase.Fencing, (long?)epoch), await unit.LostStarted.WaitAsync(Wait));
        await AwaitAsync(gate, g => g.Phase == ReplicaPhase.Serving && g.Epoch > epoch);

        var again = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.False(again.IsCancellationRequested);
        var row = (await host.Services.GetRequiredService<OwnedStore>().ReadAsync(Kind, Kind, CancellationToken.None)).Row!.Value;
        Assert.Equal(("r1", gate.Epoch!.Value), (row.OwnerReplica, row.OwnerEpoch));
    }

    [Fact]
    public async Task Its_configuration_write_on_a_fenced_replica_before_step_5_answers_NotAwaited_Fenced_and_is_applied_everywhere()
    {
        probe.Hold(MembershipFaultPoints.FenceBeforeStep5);
        var (host1, gate1) = await StartAsync("r1");
        var (host2, _) = await StartAsync("r2");
        var unit = host1.Services.GetRequiredService<WritingUnit>();
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var epoch = gate1.Epoch!.Value;
        var write = unit.WriteAtStop();

        gate1.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await probe.Arrived.WaitAsync(Wait);
        var (seq, outcome) = await write.WaitAsync(Wait);

        Assert.False(unit.LostStarted.IsCompleted);
        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Fenced), outcome);

        Assert.IsType<AckOutcome.Acknowledged>(await host2.Services.GetRequiredService<IChangeAck>().WaitAppliedAsync(seq).WaitAsync(Wait));

        probe.Release();
        await AwaitAsync(gate1, g => g.Phase == ReplicaPhase.Serving && g.Epoch > epoch);
        Assert.True(host1.Services.GetRequiredService<IGenerationSource>().Current!.Head >= seq);
    }

    private sealed class WritingUnit(IChangeLog log, IChangeAck ack, LocalPhaseGate gate, string connectionString) : IOwnedUnit
    {
        private readonly TaskCompletionSource<(ReplicaPhase Phase, long? Epoch)> lostStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<(long Seq, AckOutcome Outcome)>? write;

        public Channel<CancellationToken> Acquired { get; } = Channel.CreateUnbounded<CancellationToken>();

        public Task<(ReplicaPhase Phase, long? Epoch)> LostStarted => lostStarted.Task;

        public Task<(long Seq, AckOutcome Outcome)> WriteAtStop()
        {
            write = new TaskCompletionSource<(long, AckOutcome)>(TaskCreationOptions.RunContinuationsAsynchronously);
            return write.Task;
        }

        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            Acquired.Writer.TryWrite(stop);

            var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (stop.Register(stopped.SetResult))
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

        public Task OnLostAsync(OwnedContext context)
        {
            lostStarted.TrySetResult((gate.Phase, gate.Epoch));
            return Task.CompletedTask;
        }

        private async Task<(long, AckOutcome)> WriteAsync()
        {
            long seq;
            await using (var data = NpgsqlDataSource.Create(connectionString))
            await using (var connection = await data.OpenConnectionAsync())
            {
                await using var transaction = await connection.BeginTransactionAsync();
                seq = await log.AppendAsync(transaction, [new ChangeEntry("purge-setting", "k")]);
                await transaction.CommitAsync();
            }

            return (seq, await ack.WaitAppliedAsync(seq));
        }
    }
}
