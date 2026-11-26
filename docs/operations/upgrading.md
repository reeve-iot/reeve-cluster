# Upgrading

How to move a host application to a new release of the library: rolling updates, the version gate and its floors, the
migration runner, package pins and the upgrade procedure. The [CHANGELOG](../../CHANGELOG.md) lists what each release
contains.

## Rolling update or full stop

The library keeps one release of overlap for its tables and messages. A migration only adds. A change that has to
remove or replace something takes two releases. A build one release older keeps running against a database a newer
build migrated. So an upgrade by one release can be a rolling update:

1. Run the migrator of the new release once ([deployment.md](deployment.md#running-the-migrations)), or let the new
   replicas run it at their start. The old replicas keep running on the migrated database.
2. Roll the replicas. Each old replica drains on its stop
   ([deployment.md](deployment.md#stopping-prestop-and-termination-grace)), and each new one joins with a full load.

An upgrade that skips a release is not covered by the overlap. Step through each release, or do a full stop: stop every
replica, run the migrator, start the new replicas. A release whose CHANGELOG entry says so is a full stop too. A fleet
mixing more than one release is not supported.

Give every replica the same timing settings before and after the upgrade
([configuration.md](configuration.md#sizing-the-timings)).

## The application's version

The version gate compares the entry assembly's informational version. Set a SemVer 2.0 version at build. Without
`<Version>` every build reports the SDK default 1.0.0, and with no attribute at all the library uses 0.0.0. An
informational version that is not SemVer 2.0 refuses the start (`VersionNotSemVer`).

## The version gate and its floors

A feature that must not run until every member can take part gates on a durable answer, not on a deployment's say-so:

```csharp
if (await gate.RequireVersionAsync("1.2.0", ct))   // IClusterVersionGate: true once every leased member runs at least 1.2.0
{
    UseTheNewFormat();
}
```

`RequireVersionAsync(v)` and `RequireProtocolAsync(p)` answer true when the floor is already at or above the argument.
That read takes no lock, so a feature may ask on every call. Otherwise one transaction under the change-log lock checks
every member with a valid lease (`joining` and `active`). If all are at or above the argument, it raises the floor and
answers true. If not, it answers false and writes nothing. A raise is durable: afterwards an older binary cannot
activate.

`AllMembersAtLeastAsync(v)` and `AllMembersSpeakAsync(p)` observe the leased members without a lock and authorise
nothing. Use them for status and for decisions that can be undone.

Arguments are validated: a version that is not SemVer 2.0 is an `ArgumentException`. A raise above the calling binary's
own version (`ArgumentException`) or above the library's protocol (`ArgumentOutOfRangeException`) is refused, because
it would refuse the caller too.

What the operator sees:

- A refused member stays `joining`, names the floor in its readiness (`activation.version-floor` or
  `activation.protocol-floor`), counts each refused round in `reeve_cluster_activation_refusals_total` and retries every
  `RenewInterval`. A lowered floor admits it without a restart. With the probes from
  [deployment.md](deployment.md#probes-and-the-start) it is not restarted: it stays not ready and the rollout stalls,
  which signals a rollback below a raised floor or a skipped release.
- A release that declares that every other member must speak at least protocol P stays `joining`
  (`activation.upgrade-path`) while an `active` member speaks less. The library's own releases declare protocol 1,
  which every member speaks.
- While the floor is not raised, and while a leased member is below it, every call takes the change-log lock for a
  round. A raise's statements before its commit are bounded by `RenewTimeout + 1 s` on the server. Past that Npgsql
  throws a `PostgresException` with SQLSTATE `25P04`, nothing changed, and the caller can retry. The commit's wait for
  the standby ends only with the caller's token, so `ActionTimeBudget` must cover a raise inside a handler. A raise can
  also wait behind a stalled retention run.

### Rolling back below a raised floor

The library never lowers a floor itself. An operator who has undone what a raise authorised lowers it with
`IClusterFloorCommand`, which records who moved it and when (the `floors` status section). Resolve it, like the skip
command, from a container built with `AddReeveCluster` whose host is never started, with a `ReplicaId` no member uses:

```csharp
var floors = provider.GetRequiredService<IClusterFloorCommand>();
await floors.LowerVersionFloorAsync("1.1.0", loweredBy: "alice");   // or LowerProtocolFloorAsync(1, "alice"); a null version clears the floor
```

It answers `FloorLowering.Lowered`, `NotAbove` (the floor was already at or below) or `Changed` (it moved between the
command's read and its write, so call again). A refused member is admitted by its next activation round.

A feature that asks `Require...` on every call raises the floor again at its next call. Lower the floor while no member
that asks holds a lease, or after those members have stopped. Otherwise the `floors` section shows `raise:<replica>`
again and the older members stay refused.

## The migration runner and schema ownership

- The schema `reeve_cluster` is created by EF Core migrations that ship in `Reeve.Cluster`, run from an internal
  `DbContext` that no host registers. The runner builds it from `ClusterMigrationOptions.ConnectionString`, so your
  `AddDbContext` cannot change it. The library reads and writes the schema with raw Npgsql, and EF Core is used for
  migrations only.
- The history is the library's own table, `reeve_cluster."__ClusterMigrationsHistory"`. Your `__EFMigrationsHistory` is
  never touched.
- A migration after the baseline only adds, and a merged migration is never edited.
- `IClusterMigrationRunner.MigrateAsync` returns `ClusterMigrationResult(AppliedMigrations, LastMigration)`. A run with
  nothing to apply changes nothing. A history row of a newer build is left alone, so an older replica that restarts
  after a newer one migrated keeps running.
- The runner takes a transaction-scoped advisory lock (key `REEVMIGR` in ASCII, distinct from the change-log lock) on a
  connection of its own, and runs the migrations on a second connection, each in its own transaction. Runners that
  start together migrate one at a time, and the later ones find the history complete. Both connections use TCP
  keepalive, so a dead migrator releases its locks promptly.
- Commits run without a `CommandTimeout`, since the standby wait must not be cancelled. Every other statement keeps the
  connection string's `Command Timeout`. A cancelled run fails before the migration in progress commits. After a
  cancel during a commit the outcome is unknown, and the next run reads the history.
- Nothing is written on a server below PostgreSQL 18 (`PostgreSqlTooOld`), on a standby, or on a connection with a
  non-zero `transaction_timeout` or `statement_timeout`.

Known limits of the runner:

- The lock connection is idle in a transaction while the migrations run. A failover, a network drop or an
  `idle_in_transaction_session_timeout` shorter than the run releases the lock early, so a second runner may start.
  EF Core's `ACCESS EXCLUSIVE` lock on the history table still serialises the migrations. A double application fails
  on a primary key or a `CREATE`, and the run restarts.
- The migrator takes `ACCESS EXCLUSIVE` on the history table at every start, which conflicts with a running `pg_dump`.
  Each start waits its statement timeout and fails until the dump ends. A restart recovers.
- A connection reset during EF Core's existence check reads as a missing database. The run fails (`42P04` or an
  authentication error) and restarts with nothing written.

## Exact sibling pins and dependency floors

**Deploy the packages as one matched set.** The adapters (`Redis`, `AspNetCore`, `EntityFrameworkCore`) reach core
internals, so each `Reeve.Cluster.*` package depends on its siblings at an exact version (`[X.Y.Z]`), not
`>= X.Y.Z`. NuGet refuses a restore that mixes versions, such as `Reeve.Cluster.Redis` 1.0.0 next to `Reeve.Cluster`
1.1.0. Upgrade every `Reeve.Cluster.*` reference together.

**Dependency floors.** The packages depend on the current majors of their dependencies at their `.0` release, so an
application is not forced to upgrade to take the library: Npgsql, `Npgsql.EntityFrameworkCore.PostgreSQL` and EF Core
11.0.0, and StackExchange.Redis 3.4.35. The Microsoft.Extensions.* abstractions come from the .NET 11 shared
framework.

A floor is a minimum, and a host may run newer patches and minors. A project that pins an older version gets a NuGet
downgrade error until it raises it, and a host that pins other EF Core versions restores the highest. A floor rises only
for a feature the library needs, and the CHANGELOG says which. The Npgsql EF Core provider ships with `Reeve.Cluster`
because the migrations need it. The EF Core design package is build-time only and never in a package's dependencies.

**.NET majors.** The packages ship `net11.0` assets only, compiled with runtime async and built against EF Core 11 and
Npgsql 11. A restore that keeps an older major fails with a NuGet downgrade or version conflict error. The library moves
to the next EF Core or Npgsql major only in a release that says so.

## Upgrade procedure

For every release:

1. Read the release's entry in the [CHANGELOG](../../CHANGELOG.md). A release that needs more than a rolling update says
   so there, with what the host has to do.
2. Move every `Reeve.Cluster.*` reference to the new version together, and take the dependency floors the entry names.
3. Upgrade one release at a time, as a rolling update ([above](#rolling-update-or-full-stop)). Before a release that the
   overlap does not cover, stop every replica, run the migrator and start the new replicas.
4. Raise a version floor only once the fleet has settled on the release that must hold it
   ([above](#the-version-gate-and-its-floors)).

Because a migration only adds, the replicas of the previous release keep running against a migrated database.
Application code written against the previous release compiles against the next minor or patch. A removed or renamed
public member is a major change.
