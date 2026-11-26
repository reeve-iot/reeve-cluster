using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ApplierStartupTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public ApplierStartupTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task The_first_pass_runs_before_any_hosted_service_starts()
    {
        var log = await LogRig.CreateAsync(postgres);
        await using (var connection = await log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('counted', 'k') RETURNING seq", connection);
            await command.ExecuteScalarAsync();
        }

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddReeveCluster(new ClusterPorts([typeof(CountingReconciler)], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = log.ConnectionString;
        });
        builder.Services.AddSingleton<Observer>();
        builder.Services.AddHostedService(sp => sp.GetRequiredService<Observer>());
        using var host = builder.Build();

        await host.StartAsync();
        try
        {
            var seen = host.Services.GetRequiredService<Observer>().Seen!;
            Assert.NotNull(seen.Generation);
            Assert.Equal(1, seen.Generation!.Head);
            Assert.Equal(1, seen.Generation.Get<CountingState>("counted").Calls);
            Assert.Contains(seen.Phase, new[] { ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving });
            Assert.Equal(1, host.Services.GetRequiredService<Applier>().LastPublished!.Entries);
        }
        finally
        {
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
        }
    }

    private sealed record CountingState(int Calls);

    private sealed class CountingReconciler : IReconciler
    {
        private int calls;

        public string AggregateType => "counted";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(new CountingState(Interlocked.Increment(ref calls))));
    }

    private sealed record Seen(IGeneration? Generation, ReplicaPhase Phase);

    private sealed class Observer(IGenerationSource generations, LocalPhaseGate gate) : IHostedService
    {
        public Seen? Seen { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Seen = new Seen(generations.Current, gate.Phase);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
