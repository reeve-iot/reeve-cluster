# Reeve.Cluster.Sample

A small ASP.NET Core application that shows how to use the library. The test harness also runs it as several
processes behind a balancer. It is not packed or released.

## Running it

You need a PostgreSQL 18 primary. Give each replica its own `Cluster:ReplicaId`:

```bash
Cluster__ConnectionString="Host=127.0.0.1;Port=5432;Username=postgres;Password=pw;Database=postgres" \
Cluster__ReplicaId=r1 Cluster__CheckSynchronousReplication=false \
ASPNETCORE_URLS=http://127.0.0.1:5127 dotnet run --project samples/Reeve.Cluster.Sample
```

`CheckSynchronousReplication=false` silences the replication check on a local server. Leave it on in a real
deployment. The timings are in `appsettings.json`. `Sample:SigningKey` and `Sample:OperatorPassword` are sample-only
values, and every replica must use the same key.

## What it shows

| Where | Pattern |
|---|---|
| `Program.cs` | `AddReeveCluster` with named ports (`AspNetCoreFenceSink` as the sink), the EF mapping, `UseReeveClusterCapture`, and the migration runner and the sample's own schema run before the host starts |
| `Reconcilers.cs` | Three aggregate types. `flag` uses hints, the others re-read everything. Every command is bounded. |
| `PUT /flags/{name}` | An explicit transaction, begun, saved and committed through one context and ended with `await using` |
| `PUT /quotas/{tenant}` | A save with no explicit transaction |
| `GET /config`, `POST /config`, hub `GetConfig` | Quotas and settings (two aggregate types) read as one pinned generation and changed in one transaction. The linearizability check records these. |
| `POST /flags/import` | One transaction per item, so a request commits more than once. `?pauseMs=` adds a delay between the commits. `?hold=<flag>` (here and on `PUT /flags/{name}`) holds the request before its first commit until that flag exists, which the harness uses to force a refused commit. |
| `POST /auth/sign-in`, `/health/*`, `/phase` | Exempt from the fence with `AllowWhenFenced().SkipReadIndex()`. Sign-in is stateless, so any replica accepts a token another replica issued. `/phase` returns the process's own phase for the harness. |
| `GET /status` | `MapReeveClusterStatus(...).RequireAuthorization()`: every member's row, sections and fences. It needs authorization because the body names operators, skip reasons and unit keys. |
| `/ops/skips` | `MapReeveClusterSkips(...).RequireAuthorization()`. The package adds no authorization of its own. |
| `Hubs.cs`, `/hubs/flags` | A hub that reads a pinned generation. Clients connect with `WithAutomaticReconnect()`. |

## What to avoid

In an application that uses the capture, do not use `TransactionScope` or `UseTransaction(null)`, do not end a
transaction through a context other than the one that began it, and do not save with capture in a context that joined
a transaction with `UseTransaction`.
