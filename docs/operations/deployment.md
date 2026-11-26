# Deploying a host application

What a deployment of an application built on Reeve.Cluster has to provide, and how to run its replicas. The settings
are in [configuration.md](configuration.md). The balancer in front of the replicas is in
[balancer-and-ingress.md](balancer-and-ingress.md).

## Requirements

| Component | Requirement |
|---|---|
| Runtime | .NET 11. The packages ship `net11.0` assets only, built against EF Core 11 and Npgsql 11. |
| Database | PostgreSQL 18, the only version the tests run on. One primary with a synchronous standby for the database that holds the `reeve_cluster` schema. |
| Redis | Optional. 6.2.5 or later with `maxmemory-policy noeviction` if a unit kind declares `RedisFence` or you register Redis claims, single-flight or slots. Redis budgets and counters need `noeviction` too. |
| Orchestrator | Kubernetes, or anything that restarts a process that exits. The library ends the process in some cases and relies on the restart to rejoin. |
| Replicas | A few. Every configuration write waits for every leased active replica, so the design is for single-digit membership. |

## PostgreSQL

### Synchronous replication

The library assumes that a commit acknowledged by synchronous replication survives a failover. The deployment has to
make that true: enable synchronous replication for the database that holds `reeve_cluster`, for example with
CloudNativePG `minSyncReplicas >= 1` and `synchronous_commit` set to `on`, `remote_write` or `remote_apply`.

- A commit whose wait for the standby ended early (cancelled by a token or `CommandTimeout`, ended by
  `statement_timeout`, its backend terminated, the primary restarted in place) is committed on the primary. It is lost
  if a standby that never received it is promoted. The library never answers based on a commit it cancelled itself,
  but it cannot stop the application or an operator from cancelling one.
- A switchover or failover starts a new WAL timeline. Every replica that notices it fences and rejoins with a full
  load. Detection takes `RenewInterval + RenewTimeout` after the database answers on the new primary: 6 s with the
  defaults. A restart of the primary in place keeps the timeline.
- A planned switchover is therefore a short fleet-wide outage: about 6 s plus a full load, with writes answered `503`
  and every owned unit restarting after the rejoin.
- Keep the timeline history files in a WAL archive (CloudNativePG's object-store backup does). A promoted standby that
  never saw a sibling's history file could pick its timeline ID again.
- Synchronous commit latency must stay well below `RenewTimeout` (3 s). Sustained above it, every renewal is unknown
  and the fleet fences. A slow activation adds a second fence and `RejoinBackoff` (60 s). Watch
  `reeve_cluster_lease_renewal_duration_seconds` and `reeve_cluster_commit_duration_seconds`.

The library also checks the replication settings at start-up and periodically and warns when commits do not wait for a
standby. It never fails readiness because of it ([monitoring.md](monitoring.md#synchronous-replication-check)).

### Connection requirements

Every connection the library opens, and every connection the application hands it for a fenced transaction, runs a
probe when it opens. A failed probe fails the start with a `ClusterConfigurationException`:

| Probe finding | Reason |
|---|---|
| The connection reached a standby (`pg_is_in_recovery()` is true) | `NotPrimary` |
| The server has no `transaction_timeout` setting (below PostgreSQL 18) | `PostgreSqlTooOld` |
| `transaction_timeout` is not 0 | `TransactionTimeoutNotZero` |
| `statement_timeout` is not 0 | `StatementTimeoutNotZero` |

The probe reads the effective session values, so a setting on the role, the database or the session all count.

- To name the primary: with one host, set no `Target Session Attributes` (Npgsql accepts only `any` there, and the
  probe confirms the host is the primary). With several hosts, set `Target Session Attributes=primary` (or
  `read-write`). `any`, `standby` and the `prefer-*` values can reach a standby and are refused at start-up.
- Leave both timeouts at 0 for the role, database and session on the library's connection string and on the
  connections you run fenced transactions on. The library bounds those transactions itself with
  `SET LOCAL transaction_timeout`. A non-zero session value is armed at `BEGIN` and `SET LOCAL` does not re-arm it.
  A `statement_timeout` can also cancel the `COMMIT`'s wait for the standby while PostgreSQL keeps the commit.
- **Bound your appending transactions.** A transaction that appends to the change log (through the EF Core capture or
  `IChangeLog.AppendAsync`) holds the fleet-wide change-log lock until it ends, and the library does not bound it. Set
  `transaction_timeout` on the database role the application uses for these transactions. It must be a different role
  from the library's connection string and the fenced-transaction connections, because those are not checked.

### Roles and grants

| Role | Needs |
|---|---|
| Migrator (runs `IClusterMigrationRunner.MigrateAsync`) | `CREATE` on the database and on the `reeve_cluster` schema (EF Core creates the history table `reeve_cluster."__ClusterMigrationsHistory"` if it is missing), and ownership of the tables a pending migration alters or drops. |
| Runtime (the library's connection string) | DML on the tables of `reeve_cluster`, `USAGE` on its sequences, `EXECUTE` on its functions. No DDL. |

No role needs `pg_read_all_stats`. The synchronous-replication check works with any role as long as
`pg_stat_replication` is readable by `PUBLIC` (the PostgreSQL default).

If the runtime role has no DDL privileges, run the migrations as a separate job with the migrator role, and do not call
`MigrateAsync` in the replicas.

### Running the migrations

The library does not migrate by itself. Call `IClusterMigrationRunner.MigrateAsync` before the host starts, or run a
migration-only process. Replicas that start together take turns on the runner's advisory lock, and the later ones find
nothing to do. Nothing is written on a server below PostgreSQL 18.

```csharp
var app = builder.Build();
await app.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();
app.Run();
```

For a Kubernetes Job that only migrates, `AddReeveClusterMigrations(o => o.ConnectionString = ...)` registers the
runner alone, with nothing that needs a lease or Redis.

A run that also writes captured configuration, such as seeding through an EF Core context with the capture, registers
`AddReeveCluster` without `AddReeveClusterRedis`, uses a `ReplicaId` no member uses, and builds the service provider
without starting the host (a started host joins as a member). The members apply the seeded entries like any other.

The runner opens its own connections: one for the lock and one for the migrations. Schema ownership and the upgrade
procedure are in [upgrading.md](upgrading.md#the-migration-runner-and-schema-ownership).

### One cluster per database

The schema name `reeve_cluster` and the advisory-lock keys are fixed, so two applications on one database form one
cluster. They share one change log and one acknowledgement, and one application's version floor refuses the other's
replicas. Give each application its own database, and on Redis give each cluster its own `KeyPrefix`.

## Redis

`Reeve.Cluster.Redis` (`AddReeveClusterRedis`) carries the wake-ups, the owner nudges and the `Ask` relay, and hosts
`FenceRedis` and the Redis primitives. Redis is never the authority. Without it, or with it cut, replicas converge by
polling with more latency, and an `Ask` to another process ends `NotTaken`.

- **Version.** `RedisFence` and Redis claims, single-flight and slots need 6.2.5 or later, because `EXEC` must abort
  once a watched key's TTL has passed. The start refuses a connected primary below it (`RedisTooOld`). Budgets,
  counters, wake-ups and the relay set no version floor.
- **Eviction policy.** `RedisFence`, claims, single-flight, budgets, counters and slots need
  `maxmemory-policy noeviction` on every primary. Claim, flight and budget keys carry a TTL that is part of the grant,
  and a `volatile-*` policy may evict it (an evicted claim key ends the claim early). A `FenceRedis` high-water key
  and a slot's sorted set have no TTL, and an `allkeys-*` policy may evict them (an evicted slot set restarts the count
  from zero). Some managed Redis services default to a `volatile-*` policy. See
  [monitoring.md](monitoring.md#redis-start-up-check) for the check.
- **The connection.** Register your own `IConnectionMultiplexer`, before or after `AddReeveCluster`. The package opens
  no connection of its own except the short-lived one of the start-up check, which needs `INFO` (an ACL user without
  `INFO` is refused).
- **Key prefix.** Every key and channel is under `ClusterRedisOptions.KeyPrefix` (`reeve:` by default). Replicas that
  are to hear each other share it. Two clusters on one Redis need different prefixes, or they collide on fence keys.

For a Redis ACL that restricts keys or channels (`<p>` is the key prefix):

| Name | What |
|---|---|
| `<p>wakeup` | the wake-up pub/sub channel (no key) |
| `<p>ask:q:<process id>:<replica id>`, `<p>ask:r:<process id>` | the `Ask` relay's request and reply channels, two per process |
| `<p>ask:c:<request id>` | short-lived `Ask` keys |
| `{<p>owned:...` | `FenceRedis` keys of a tenure. The brace is part of the key (`{reeve:owned:*` by default), and the application's own keys under a tenure's hash tag match too |
| `{<p>claim:...`, `{<p>flight:...`, `{<p>budget:...`, `{<p>counter:...`, `{<p>slot:...` | the Redis primitives; each key's effect keys share its hash tag |

## Installing

The packages and their dependencies come from nuget.org. No extra package source is needed.

```bash
dotnet add package Reeve.Cluster
dotnet add package Reeve.Cluster.AspNetCore
dotnet add package Reeve.Cluster.EntityFrameworkCore   # configuration written through EF Core
dotnet add package Reeve.Cluster.Redis                 # optional
```

Use the same version for every `Reeve.Cluster.*` package: each pins its siblings to that exact version
([upgrading.md](upgrading.md#exact-sibling-pins-and-dependency-floors)). What each package brings, and the order of the
registrations, is in [../reference/registration.md](../reference/registration.md).

### Trimming and NativeAOT

`Reeve.Cluster.Abstractions`, `Reeve.Cluster`, `Reeve.Cluster.Redis` and `Reeve.Cluster.AspNetCore` are marked
`IsAotCompatible`. A trimmed or NativeAOT application gets a warning where a call still needs reflection:

- `IClusterMigrationRunner.MigrateAsync` runs EF Core migrations, which EF Core does not support under NativeAOT. Run
  them from a JIT-compiled process, for example a migration job built from the same code without `PublishAot`.
- The `ClusterPorts` constructor takes the reconciler types as a list, so the trimmer cannot see that the container
  creates them. Keep their public constructors, for example with `[DynamicDependency]`.
- A status section value must be one of the types `StatusSection.Values` lists: `string`, `bool`, `int`, `long`,
  `double`, `decimal`, `Guid`, `DateTime`, `DateTimeOffset`, `TimeSpan` or `JsonElement`; a `Dictionary<string, object?>`,
  `List<object?>`, `object?[]` or `List<Dictionary<string, object?>>` of those; or a `List<string>`, `string[]` or
  `Dictionary<string, string>`. A section with another type is left out of the report with a logged warning.

`Reeve.Cluster.AspNetCore` adds its JSON metadata to the application's `JsonOptions`, so its endpoints need nothing
from your own `JsonSerializerContext`. `Reeve.Cluster.EntityFrameworkCore` is not AOT-compatible: EF Core itself needs
unreferenced and dynamic code, and the package's start check constructs your `DbContext` types by reflection.

## Workload and ReplicaId

`ReplicaId` must be unique per live process. A second process with the same id is refused at join (`joining.refused`,
retried every `RenewInterval`) while the first holds the lease.

The library never deletes a member row, so an id that is not used again leaves its row behind, with an expired lease
or a `draining` state. Such rows show in the status endpoint but do not hold back retention or the acknowledgement,
which look only at members with a valid lease.

| Workload | `ReplicaId` | Consequence |
|---|---|---|
| StatefulSet | the pod name (`app-0`, `app-1`, ...) | Stable ids, rows reused. A pod's replacement starts after the old pod ended, so a rollout has no surge. |
| Deployment, several replicas | the pod name, from the downward API | Unique per pod, and a surge rollout works. Every new pod name adds a member row that stays. |
| Deployment, one replica | a fixed id | Use `strategy: Recreate` or `maxSurge: 0`. With a surge the new pod's join is refused while the old pod holds the lease, so a rollout that waits for the new pod to be ready never finishes. |

```yaml
env:
  - name: Cluster__ReplicaId
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: Cluster__ConnectionString
    valueFrom:
      secretKeyRef: { name: app-db, key: connection-string }
```

## Probes and the start

The host's start waits for the join and the first full load. The join waits up to `LeaseTtl` when the id is still
held. Only then do the hosted services, Kestrel among them, start. So no HTTP endpoint answers before the first load
is done, and a `loading.failed` or `joining.refused` on a first start shows only in the log and in metrics an exporter
pushes (OTLP). A Prometheus scrape served by the application cannot see it.

- Give the replica a startup probe with a window of at least `LeaseTtl` plus the expected first load.
- If you set the host's `StartupTimeout`, make it at least as long. A shorter one ends the start, which then stops
  gracefully and does not serve.
- Map the probe endpoints with `AllowWhenFenced().SkipReadIndex()`, or the fence middleware answers them.

```csharp
builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);
// ...
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") })
    .AllowWhenFenced().SkipReadIndex();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false })
    .AllowWhenFenced().SkipReadIndex();
```

```yaml
ports:
  - name: http
    containerPort: 8080
startupProbe:
  httpGet: { path: /health/live, port: http }
  periodSeconds: 5
  failureThreshold: 24          # 120 s: LeaseTtl plus the expected first load
readinessProbe:
  httpGet: { path: /health/ready, port: http }
  periodSeconds: 5
livenessProbe:
  httpGet: { path: /health/live, port: http }
  periodSeconds: 10
```

`/health/live` runs no check. `/health/ready` runs the readiness check.

| Replica | `/health/live` | `/health/ready` |
|---|---|---|
| Joining or loading on its first start | no answer: Kestrel has not started | no answer |
| Activating after the first load (`CatchingUp`, `Activated`), or refused by a floor (`activation.version-floor`, `activation.protocol-floor`, `activation.upgrade-path`) | `200` | `503`, naming the phase and the reasons |
| `Serving`, no reason raised | `200` | `200` |
| Fenced, rejoining, or loading again after a rejoin (`loading.failed`, `joining.refused`) | `200` | `503` |
| Draining | `200` | `503` |
| Exited (a `higher_epoch` rejoin decision, a fatal fault, the end of a drain) | the process has ended and the restart policy restarts it | |

The liveness probe never restarts a replica the library has taken out of service. A fenced replica rejoins by itself,
and a replica refused by a floor retries its activation every `RenewInterval`. A floor refusal shows up as a pod that
never becomes ready and a rollout that stalls ([upgrading.md](upgrading.md#the-version-gate-and-its-floors)).

A startup probe on `/health/ready` would restart a replica that does not reach `Serving` within its window, a
floor-refused one included. If you choose that, size the window for the join, the first load and the activation.

## Stopping: preStop and termination grace

A graceful stop (SIGTERM) drains the replica:

1. The replica leaves `Serving`. The placer assigns it nothing new, and new requests and hub invocations are answered
   `503 replica_not_serving`. SignalR closes hub connections itself when the host signals the stop.
2. It releases its owned units, each stop bounded by `StopBudget`. A unit of an `AnyEligible` keyed kind is not
   released: the `draining` write orphans it and another eligible member adopts it.
3. Its applier and lease renewal keep running until the row write below.
4. It waits for the requests that were running at step 1, for at most `ProxyReadTimeout`. A request still running then
   has overrun the HTTP bound and is cut with the process. The replica then aborts the connections that stay open
   (WebSockets, long polls, hub connections), whose clients reconnect to a replica that serves. Pending
   acknowledgement waits answer as unconfirmed, the row is written `draining` (writers stop waiting for it at once),
   and the process exits.

The drain does not release held slots: a slot stays valid until its holder releases it or its renewal lapses. A fence
during a drain takes over, and the process exits at the end of the fence instead of rejoining.

Step 1 answers `503` from the moment SIGTERM arrives, while the endpoint removal is still propagating to the
balancer. A `preStop` sleep of a few seconds keeps requests routed in that window from getting a `503`. Count it in
the termination grace period, which must exceed `preStop + StopBudget + ProxyReadTimeout + 2 × RenewTimeout` (the two
row writes of the drain). The `sleep` action needs Kubernetes 1.30 or later. An older cluster runs a `sleep` command in
an `exec` hook.

```yaml
terminationGracePeriodSeconds: 80   # 5 s preStop + 5 s StopBudget + 60 s ProxyReadTimeout + 2 × 3 s RenewTimeout, with margin
containers:
  - name: app
    lifecycle:
      preStop:
        sleep: { seconds: 5 }
```

A stop that overruns the grace period never writes `draining`. Its lease then expires, and writers wait as after a
crash, up to `LeaseTtl + AckPollInterval`.

## Connections per replica

A replica opens several Npgsql pools on `ConnectionString`. A pool opens a connection only when a caller needs one, so
these sizes are upper bounds.

| Pool | Used for | Size |
|---|---|---|
| main | the applier's poll of the log head, the activation, status writes, retention, the version floors, `Ask`'s database lookups | the connection string's `Maximum Pool Size` |
| renewal | the lease renewal, on its own timer, with TCP keepalive | at most 8 |
| member write | the join, the `draining` write, the fence's history record | `Maximum Pool Size` |
| acknowledgement | the acknowledgement's record, a skip's open and close | `Maximum Pool Size` |
| owned | ownership statements, claims, once-runs, single-flight, budgets, counters, slots, durable commands; only with the `OwnedWork` area | `Maximum Pool Size` |
| read index | the read index of requests, the acknowledgement's wait-set reads, the status read | `min(16, Maximum Pool Size)` |
| applier passes | each pass's snapshot, and each `OpenSnapshotSessionAsync` | unpooled: one connection per pass or session, closed when it ends |

The migration runner's two connections and your own pools (your `DbContext`, the data source of your fenced
transactions) come on top. To size `max_connections` for N replicas, add up the pool sizes for one replica (Npgsql's
default `Maximum Pool Size` is 100), add the applier and your own pools, and multiply by N. Lowering `Maximum Pool
Size` lowers the four pools that take it, and the read-index pool when it is below 16.

### The read index and its connections

Every endpoint that is not marked `[SkipReadIndex]` (or `.SkipReadIndex()`) pays a round trip to the primary before it
runs, on the read-index pool. A request that gets no connection within `ReadIndexTimeout` is answered
`503 replica_behind`, and so are the waiting requests when an applier pass takes longer than that. Mark
`SkipReadIndex` on endpoints that read no configuration, and do not let them read derived state.

The status endpoint's read uses the same pool, so a burst of status requests queues there and cannot take
connections from the activation, a rejoin, a floor raise or the applier's poll.

## Throughput of configuration writes

Every append serialises on one fleet-wide advisory lock, held through the synchronous-replication commit. One
appending transaction commits at a time, so write throughput is bounded by the commit latency. A write's response then
waits for every leased active replica to apply it. Batch changes into one transaction where you can.

## Restore from backup

A restore is offline:

1. Stop every replica.
2. Restore the database. This rewinds `change_ack` and `change_skip` with the rest of it.
3. If a unit kind declares `RedisFence`, clear the tenure-fence keys with
   `ClusterRedisRestore.ClearOwnedKeysAsync(redis, keyPrefix)`. A fence key holds the highest tenure token Redis has
   seen, and the restored database draws tokens again from the backup's point.
4. Start the replicas. Each joins with a full load.

`ClearOwnedKeysAsync` deletes the keys matching `{<prefix>owned:*` with `SCAN` and `UNLINK` on every primary,
including your own keys under a tenure's hash tag. It fails closed: when a listed server is not connected, or none is
a primary, it deletes nothing and throws. Run it again once every server answers. Wake-ups store no key.

A replica that keeps running across a restore is fenced at its next renewal and gets no guarantee for that window. A
process treats an epoch at or below one it already held as ended. If its rejoin draws such an epoch, it logs a
critical message and exits, and the orchestrator's restart joins afresh, refused for up to `LeaseTtl` while the old
lease runs. The same happens after a failover that lost a join. After a restore, expect every replica that was running
to exit once, and make sure the restart policy restarts it.
