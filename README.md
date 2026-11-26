# reeve-cluster

Reeve.Cluster lets a .NET service run as several replicas over one PostgreSQL database and still behave like a single
process. You write a feature as if one process ran it. The library handles what changes when several do:

- Configuration: a change is applied by every live replica before the write is acknowledged.
- Single-owner work: a unit of work or a keyed resource has at most one owner whose fenced effects are accepted, and a
  declared failover time when the owner is lost.
- Requests to an owner: a request reaches the owner of a key and gets a typed outcome.
- At-most-once helpers: claims, once-only effects, budgets and counters across replicas.
- Each replica's capabilities, version and status.

PostgreSQL is the only authority. Replicas hold leases in it and take themselves out of service when they cannot renew
one. Redis is optional: it lowers latency, carries `Ask` between processes and backs faster variants of some primitives.

reeve-cluster is a library, not a service. It is built for the Reeve platform, and any ASP.NET Core service can use it.

## Limits

- It is designed for a handful of replicas. Every configuration write waits for every leased replica, and writes are
  serialised, so it is not a store for data that changes many times a second.
- The guarantees survive a database failover only with PostgreSQL 18 and synchronous replication. One database holds
  one cluster.
- An effect outside PostgreSQL and Redis is protected only by the unit calling `EnsureOwner()` before it. That narrows
  the window but does not close it.

The full list of what is not guaranteed is in the [architecture overview](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/architecture.md).

## Packages

| Package | Contents |
|---|---|
| `Reeve.Cluster.Abstractions` | Interfaces, options and result types. No dependencies. |
| `Reeve.Cluster` | Membership, the change log and applier, owned work, the PostgreSQL primitives, `Ask` and durable commands, the version gate, the status registry and the migrations. |
| `Reeve.Cluster.EntityFrameworkCore` | The capture interceptor that turns saved configuration entities into change-log entries, and fenced transactions on a `DbContext`. |
| `Reeve.Cluster.AspNetCore` | The readiness check, fence middleware, acknowledgement filter, read index and the skip and status endpoints. |
| `Reeve.Cluster.Redis` | Wake-ups, the `Ask` relay, `FenceRedis` and the Redis primitives. |

The packages depend on each other at an exact version, so install them at the same version.

## Requirements

- .NET 11.
- PostgreSQL 18 with `synchronous_commit = on` or stronger on the database that holds the `reeve_cluster` schema. The
  connection string must name the primary, and `transaction_timeout` and `statement_timeout` must be 0 for the
  library's role.
- A replica id that is unique for each live process (on Kubernetes, usually the pod name).
- Redis 6.2.5 or later, optional. Use `maxmemory-policy noeviction` if you use a Redis fence or a Redis primitive.

The [deployment guide](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/deployment.md) has the full contract, the checks run at start and the
database roles.

## Installation

```bash
dotnet add package Reeve.Cluster
dotnet add package Reeve.Cluster.AspNetCore
```

## Quick start

This application runs one scheduler on whichever replica owns the key `report-scheduler`. If that replica crashes,
drains or fences, another replica takes the key over. The unit writes through a fenced transaction, which the database
refuses once another replica owns the key. The example needs an empty PostgreSQL 18 database and no Redis.

```csharp
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Npgsql;
using Reeve.Cluster;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration["Cluster:ConnectionString"]
    ?? throw new InvalidOperationException("Cluster:ConnectionString is required.");

// No configuration types (no reconcilers) and no connection that stays open past a request (NoFenceSink).
builder.Services.AddReeveCluster(
    new ClusterPorts([], typeof(NoFenceSink)),
    options => builder.Configuration.GetSection(ClusterOptions.SectionName).Bind(options));
builder.Services.AddReeveClusterAspNetCore();
builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);

// The application's own data source for the unit's fenced writes. Its role needs transaction_timeout = 0 and
// statement_timeout = 0.
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

// A named singleton unit: the placer gives its one key to one serving replica.
builder.Services.AddClusterUnit<ReportScheduler>("report-scheduler");

var app = builder.Build();

await app.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();
await using (var setup = NpgsqlDataSource.Create(connectionString))
{
    await setup.CreateCommand("CREATE TABLE IF NOT EXISTS report_run (run_at timestamptz NOT NULL)").ExecuteNonQueryAsync();
}

app.UseReeveCluster();
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") })
    .AllowWhenFenced().SkipReadIndex();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false })
    .AllowWhenFenced().SkipReadIndex();
app.Run();

public sealed class ReportScheduler(NpgsqlDataSource database, ILogger<ReportScheduler> logger) : IOwnedUnit
{
    private Task body = Task.CompletedTask;

    // Runs while this replica owns the key. The library does not wait for this task when it stops the unit.
    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        body = RunAsync(context, stop);
        return body;
    }

    // The library waits for this (up to StopBudget) before the key may start elsewhere,
    // so wait here for the body to end.
    public async Task OnLostAsync(OwnedContext context)
    {
        try
        {
            await body;
        }
        catch (Exception exception) when (exception is OperationCanceledException or OwnershipLostException)
        {
        }
    }

    private async Task RunAsync(OwnedContext context, CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));
        while (await timer.WaitForNextTickAsync(stop))
        {
            try
            {
                await RunOnceAsync(context, stop);
            }
            catch (OwnershipLostException)
            {
                return;   // this replica no longer owns the key; OnLostAsync runs
            }
            catch (CommitOutcomeUnknownException exception)
            {
                logger.LogWarning(exception, "The commit outcome is unknown; the next tick reads the state again.");
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // Handled here, a failed run waits for the next tick. If it escaped, the library would
                // restart the whole body after a backoff.
                logger.LogError(exception, "The report run failed; the next tick tries again.");
            }
        }
    }

    private async Task RunOnceAsync(OwnedContext context, CancellationToken stop)
    {
        // A write that must stop when the owner changes goes through a fenced transaction. An effect outside
        // the database calls context.EnsureOwner() first.
        await using var connection = await database.OpenConnectionAsync(stop);
        await using var transaction = await context.BeginFencedTransactionAsync(connection, cancellationToken: stop);
        await using var insert = new NpgsqlCommand("INSERT INTO report_run (run_at) VALUES (now())", connection, transaction.Transaction);
        await insert.ExecuteNonQueryAsync(stop);
        await transaction.CommitAsync(stop);
    }
}
```

The timings without a default go in `appsettings.json`. An application with no reconcilers gets `ReconcileTimeout`
10 s; one with reconcilers sets it too.

```json
{
  "Cluster": {
    "ActionTimeBudget": "00:00:05",
    "ProxyReadTimeout": "00:01:00"
  }
}
```

Run two processes with different replica ids and ports:

```bash
Cluster__ConnectionString="Host=127.0.0.1;Port=5432;Username=app;Password=pw;Database=app" \
Cluster__ReplicaId=r1 ASPNETCORE_URLS=http://127.0.0.1:5001 dotnet run
```

A process answers no HTTP request until it has joined the cluster and loaded its state. `/health/ready` reports when
it is serving. Start a second process with `Cluster__ReplicaId=r2` and another port, then stop the first to watch the
unit move.

The next step is a configuration table replicated to every replica: a reconciler, the EF Core capture and acknowledged
writes. See the [configuration log](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/guide/configuration-log.md). The
[sample application](https://github.com/reeve-iot/reeve-cluster/blob/main/samples/Reeve.Cluster.Sample) shows three aggregate types, explicit transactions, a hub, the
status and skip endpoints and authorization.

## Documentation

- [Documentation index](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/README.md)
- [Guide](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/guide/getting-started.md): getting started, membership, the configuration log, owned work, primitives and the ASP.NET Core, EF Core and Redis packages
- [Architecture](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/architecture.md): the model, the building blocks and what is not
  guaranteed
- Operations: [deployment](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/deployment.md), [configuration](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/configuration.md),
  [monitoring](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/monitoring.md), [upgrading](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/upgrading.md) and
  [troubleshooting](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/operations/troubleshooting.md)
- [FAQ](https://github.com/reeve-iot/reeve-cluster/blob/main/docs/faq.md)
- [CHANGELOG](https://github.com/reeve-iot/reeve-cluster/blob/main/CHANGELOG.md) and [CONTRIBUTING](https://github.com/reeve-iot/reeve-cluster/blob/main/CONTRIBUTING.md)

## Licence

MIT. See [LICENSE](https://github.com/reeve-iot/reeve-cluster/blob/main/LICENSE).
