using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ApplierHostStopTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public ApplierHostStopTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task M14_a_graceful_stop_during_the_first_load_writes_draining_on_the_RunAsync_path()
    {
        var log = await LogRig.CreateAsync(postgres);
        var probe = new PointProbe();
        probe.Hold(LogFaultPoints.InsideFirstLoad);
        var host = Build(log, probe);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        var run = host.RunAsync();
        var ended = "is still running";
        try
        {
            await probe.Arrived.WaitAsync(Wait);
            Assert.Equal(ReplicaPhase.Loading, gate.Phase);
            Assert.Equal("joining", (await RowAsync(log)).State);

            lifetime.StopApplication();
            probe.Release();
            try
            {
                await run.WaitAsync(Wait);
                ended = "ran to completion";
            }
            catch (TimeoutException)
            {
                ended = "was still running after the bound";
            }
            catch (Exception exception)
            {
                ended = $"threw {exception.GetType().Name}";
            }
        }
        finally
        {
            probe.Release();
            if (!run.IsCompleted)
            {
                using var bound = new CancellationTokenSource(Wait);
                try
                {
                    await host.StopAsync(bound.Token);
                    await run.WaitAsync(Wait);
                }
                catch (Exception)
                {
                }
            }
        }

        var row = await RowAsync(log);
        Assert.True(
            row.State == "draining",
            $"M14: after a graceful stop in Loading the row is '{row.State}' (its lease still valid: {row.LeaseValid}); RunAsync {ended}; "
            + $"the gate is in {gate.Phase}.");
    }

    [Fact]
    public async Task M14_the_same_stop_through_an_explicit_StopAsync_writes_draining()
    {
        var log = await LogRig.CreateAsync(postgres);
        var probe = new PointProbe();
        probe.Hold(LogFaultPoints.InsideFirstLoad);
        using var host = Build(log, probe);

        var start = host.StartAsync();
        try
        {
            await probe.Arrived.WaitAsync(Wait);
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
        }
        finally
        {
            probe.Release();
            try
            {
                await start.WaitAsync(Wait);
            }
            catch (Exception)
            {
            }
        }

        Assert.Equal("draining", (await RowAsync(log)).State);
    }

    [Fact]
    public async Task A_hosted_service_registered_before_the_library_starts_after_the_first_pass()
    {
        var log = await LogRig.CreateAsync(postgres);
        await using (var connection = await log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('one', 'k')", connection);
            await command.ExecuteNonQueryAsync();
        }

        using var host = Build(log, before: services =>
        {
            services.AddSingleton<Observer>();
            services.AddHostedService(sp => sp.GetRequiredService<Observer>());
        });
        using (var bound = new CancellationTokenSource(Wait))
        {
            await host.StartAsync(bound.Token);
        }

        try
        {
            var seen = host.Services.GetRequiredService<Observer>().Seen!;
            Assert.NotNull(seen.Generation);
            Assert.Equal(1, seen.Generation!.Head);
            Assert.Contains(seen.Phase, new[] { ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving });
        }
        finally
        {
            using var bound = new CancellationTokenSource(Wait);
            await host.StopAsync(bound.Token);
        }
    }

    private static IHost Build(LogRig log, IFaultPoints? points = null, Action<IServiceCollection>? before = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        before?.Invoke(builder.Services);
        if (points is not null)
        {
            builder.Services.AddSingleton(points);
        }

        builder.Services.AddReeveCluster(new ClusterPorts([typeof(OneReconciler)], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = log.ConnectionString;
        });

        builder.Services.AddSingleton<IHostLifetime, QuietLifetime>();
        return builder.Build();
    }

    private static async Task<(string State, bool LeaseValid)> RowAsync(LogRig log)
    {
        await using var connection = await log.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT state::text, lease_expires_at > clock_timestamp() FROM reeve_cluster.member WHERE replica_id = 'r1'", connection);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? (reader.GetString(0), reader.GetBoolean(1)) : ("absent", false);
    }

    private sealed record OneState(int Calls);

    private sealed class OneReconciler : IReconciler
    {
        public string AggregateType => "one";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(new OneState(1)));
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

    private sealed class QuietLifetime : IHostLifetime
    {
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
