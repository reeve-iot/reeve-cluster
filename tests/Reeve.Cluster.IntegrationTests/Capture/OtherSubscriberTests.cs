using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;

namespace Int.Capture;

public sealed class OtherSubscriberTests(PostgresFixture postgres) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    private sealed class SettingReconciler : IReconciler
    {
        public string AggregateType => "setting";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(new object()));
    }

    [Fact]
    public async Task A_plain_save_appends_an_entry_beside_another_subscriber()
    {
        await using var app = await StartAsync();
        var before = await CountEntriesAsync(app.ConnectionString);

        using (app.Scopes.BeginBackground())
        {
            await using var scope = app.Host.Services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<CaptureDb>();
            db.Settings.Add(new Setting { Id = 1, Name = "plain", Value = "v" });
            await db.SaveChangesAsync();
        }

        Assert.Equal(before + 1, await CountEntriesAsync(app.ConnectionString));
    }

    [Fact]
    public async Task A_save_in_an_explicit_transaction_appends_an_entry_beside_another_subscriber()
    {
        await using var app = await StartAsync();
        var before = await CountEntriesAsync(app.ConnectionString);

        using (app.Scopes.BeginBackground())
        {
            await using var scope = app.Host.Services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<CaptureDb>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Settings.Add(new Setting { Id = 2, Name = "explicit", Value = "v" });
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        Assert.Equal(before + 1, await CountEntriesAsync(app.ConnectionString));
    }

    private async Task<StartedApp> StartAsync()
    {
        var connectionString = await postgres.CreateDatabaseAsync();
        var otherSubscriber = new EnableEverything();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddReeveCluster(
            new ClusterPorts([typeof(SettingReconciler)], typeof(NoFenceSink)),
            o =>
            {
                TestProfile.ApplyStallTolerant(o);
                o.ReplicaId = "r1";
                o.ConnectionString = connectionString;
            });
        builder.Services.AddReeveClusterEntityFrameworkCore(mapping => mapping.Entity<Setting>("setting").Key(s => s.Name).Columns(s => s.Name, s => s.Value));
        builder.Services.AddDbContext<CaptureDb>((services, options) => options.UseNpgsql(connectionString).UseReeveClusterCapture(services));
        var host = builder.Build();
        var app = new StartedApp(host, connectionString, otherSubscriber);

        try
        {
            await host.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var create = new NpgsqlCommand("CREATE TABLE public.setting (id int PRIMARY KEY, name text NOT NULL, value text NOT NULL, note text NULL)", connection);
                await create.ExecuteNonQueryAsync();
            }

            await host.StartAsync().WaitAsync(Bound);
            return app;
        }
        catch
        {
            await app.DisposeAsync();
            throw;
        }
    }

    private sealed class StartedApp(IHost host, string connectionString, IDisposable otherSubscriber) : IAsyncDisposable
    {
        public IHost Host => host;

        public string ConnectionString => connectionString;

        public IClusterScopeAccessor Scopes => host.Services.GetRequiredService<IClusterScopeAccessor>();

        public async ValueTask DisposeAsync()
        {
            try
            {
                await host.StopAsync();
            }
            finally
            {
                host.Dispose();
                otherSubscriber.Dispose();
            }
        }
    }

    private static async Task<long> CountEntriesAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM reeve_cluster.change_log", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private sealed class EnableEverything : IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>, IDisposable
    {
        private readonly List<IDisposable> subscriptions = [];
        private readonly IDisposable all;

        public EnableEverything() => all = DiagnosticListener.AllListeners.Subscribe(this);

        public void OnNext(DiagnosticListener listener)
        {
            if (listener.Name == "Microsoft.EntityFrameworkCore")
            {
                lock (subscriptions)
                {
                    subscriptions.Add(listener.Subscribe(this, (_, _, _) => true));
                }
            }
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose()
        {
            all.Dispose();
            lock (subscriptions)
            {
                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }
            }
        }
    }
}
