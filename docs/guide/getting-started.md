# Getting started

This page takes an ASP.NET Core service from nothing to a cluster of replicas running one owned work item.

## Packages

| Package | Install it when |
|---|---|
| `Reeve.Cluster` | Always. Membership, the configuration log, owned work, primitives, `Ask`, commands and the migrations |
| `Reeve.Cluster.AspNetCore` | Your service is an ASP.NET Core application: readiness, the fence middleware and the write acknowledgement |
| `Reeve.Cluster.EntityFrameworkCore` | You use EF Core and want saved entities to be captured into the configuration log |
| `Reeve.Cluster.Redis` | You want faster wake-ups, the `Ask` relay across processes or the Redis primitives |

```bash
dotnet add package Reeve.Cluster
dotnet add package Reeve.Cluster.AspNetCore
```
Install them at the same version: they depend on each other at an exact version.

## Requirements

- .NET 11.
- PostgreSQL 18, with `synchronous_commit = on` or stronger and a synchronous standby if the guarantees must survive
  a failover. The connection string names the primary, and `transaction_timeout` and `statement_timeout` are 0 for the
  library's role.
- A `ReplicaId` that is unique for each live process. On Kubernetes, use the pod name.
- Redis 6.2.5 or later, optional, with `maxmemory-policy noeviction` if you use a Redis fence or primitive.

The [deployment guide](../operations/deployment.md) has the full contract, the database roles and the probes.

## Register the library

`AddReeveCluster` takes the reconcilers that turn configuration tables into in-memory state (none yet) and the fence
sink that aborts connections that stay open past a request.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddReeveCluster(
    new ClusterPorts([], typeof(NoFenceSink)),
    options => builder.Configuration.GetSection(ClusterOptions.SectionName).Bind(options));
builder.Services.AddReeveClusterAspNetCore();
builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);
```

Use `AspNetCoreFenceSink` instead of `NoFenceSink` if the service maps a SignalR hub. A hub with `NoFenceSink`
refuses the start.

`ConnectionString` and `ReplicaId` are required, and so are the HTTP timings, which depend on your balancer:

```json
{
  "Cluster": {
    "ConnectionString": "Host=db;Username=app;Password=...;Database=app", "ReplicaId": "r1",
    "ReconcileTimeout": "00:00:10", "ActionTimeBudget": "00:00:05", "ProxyReadTimeout": "00:01:00"
  }
}
```

On a real deployment set `ReplicaId` from the environment (`Cluster__ReplicaId`). Every setting is in
[configuration](../operations/configuration.md). The start is refused, with one message per problem, when a required
setting is missing or the timings contradict each other.

## Migrate at start

The library creates and upgrades its own schema. Run the migration runner before the host starts serving:

```csharp
var app = builder.Build();

await app.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();
```

Replicas that start together migrate one at a time. You can also run the migrations from a separate job: see
[deployment](../operations/deployment.md#running-the-migrations).

## Wire the pipeline and readiness

`UseReeveCluster()` goes first. It adds the scope middleware, routing and the fence middleware. Authentication and
authorization come after it.

```csharp
app.UseReeveCluster();
app.UseAuthentication();
app.UseAuthorization();

app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") })
    .AllowWhenFenced().SkipReadIndex();
```

`AllowWhenFenced()` makes an endpoint answer when the replica is not serving, and `SkipReadIndex()` stops it waiting
for the log. Probes need both, including a liveness probe, which should run no check.

A process answers no HTTP request until it has joined the cluster and loaded its state. `/health/ready` is healthy
only while serving. Otherwise it names the phase and each reason, such as `loading.failed`. Use it as
the readiness probe, and give the startup probe a window longer than `LeaseTtl` plus the first load. See
[membership](membership.md) and [monitoring](../operations/monitoring.md).

## A first owned work item

An owned unit runs on exactly one replica at a time. This one writes a row every minute, and moves to another replica
if its owner stops:

```csharp
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddClusterUnit<ReportScheduler>("report-scheduler");

public sealed class ReportScheduler(NpgsqlDataSource database) : IOwnedUnit
{
    private Task body = Task.CompletedTask;

    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => body = RunAsync(context, stop);

    public async Task OnLostAsync(OwnedContext context)
    {
        try { await body; }
        catch (Exception e) when (e is OperationCanceledException or OwnershipLostException) { }
    }

    private async Task RunAsync(OwnedContext context, CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));
        while (await timer.WaitForNextTickAsync(stop))
        {
            await using var connection = await database.OpenConnectionAsync(stop);
            await using var tx = await context.BeginFencedTransactionAsync(connection, cancellationToken: stop);
            await using var insert = new NpgsqlCommand("INSERT INTO report_run (run_at) VALUES (now())", connection, tx.Transaction);
            await insert.ExecuteNonQueryAsync(stop);   // the report_run table is the application's own
            await tx.CommitAsync(stop);
        }
    }
}
```

The database refuses the fenced transaction once another replica owns the key. The
[owned work guide](owned-work.md) covers the lifecycle and the errors a real unit handles.

## Try it
Run two processes on one empty PostgreSQL 18 database, with different replica ids and ports:

```bash
Cluster__ConnectionString="Host=127.0.0.1;Username=app;Password=pw;Database=app" \
Cluster__ReplicaId=r1 ASPNETCORE_URLS=http://127.0.0.1:5001 dotnet run
```

Start a second with `Cluster__ReplicaId=r2` and another port, then stop the first and watch the unit move. See the
[sample application](../../samples/Reeve.Cluster.Sample) for the whole stack.

Next: [owned work](owned-work.md), the [configuration log](configuration-log.md) and
[membership](membership.md).
