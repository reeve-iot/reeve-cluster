# Changelog

All notable changes to the `Reeve.Cluster` packages (`Reeve.Cluster.Abstractions`, `Reeve.Cluster`,
`Reeve.Cluster.Redis`, `Reeve.Cluster.EntityFrameworkCore`, `Reeve.Cluster.AspNetCore`) are documented here. The
packages follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): a removed or renamed public member is a
major change.

## 1.0.1 - 2026-11-26

### Fixed

- The `Reeve.Cluster` package ships without a `runtimeconfig.json`.
- The XML documentation of `ReeveClusterHttpOptions.WriteError` and `ClusterHttpError` lists the status endpoint's
  `503 status_unavailable` and the skip endpoint's `403 skip_endpoint_unprotected`,
  `IClusterMigrationRunner.MigrateAsync` names the `statement_timeout` refusal, and `ClusterDiagnostics.Name` names
  only the meter.
- Documentation: the EF Core reconciler example reads through the pass's connection and transaction, Redis Cluster
  effect keys use the `RedisPrimitiveKeys` hash tags, and the readiness reasons, log levels, start checks, validation
  order and primitive outcomes match the library.

## 1.0.0 - 2026-11-25

First public release. It targets `net11.0` and PostgreSQL 18 and builds against EF Core 11 and Npgsql 11. What the
library does not guarantee is in [the architecture overview](docs/architecture.md); deployment and upgrading are in
[the operations guide](docs/operations/deployment.md).

### Membership and fencing

- Leased membership over PostgreSQL. A replica joins, activates, renews its lease and drains. It takes itself out of
  service (fences) when it cannot renew, when its row is gone or when the database moves to a new timeline, and
  rejoins on its own.
- Self-fencing with a declared bound: a local deadline, a stop budget and bounded fence steps, so no replica serves
  state the others have moved past.
- A version gate (`IClusterVersionGate`) with durable version and protocol floors, an operator command that lowers a
  floor (`IClusterFloorCommand`) and an upgrade-path guard for activation.
- Per-replica capabilities and a status registry with typed reads and application sections.

### Configuration

- A configuration log: a change written once is applied by every live replica before the write is acknowledged.
  Reconcilers apply the log per type, a read index makes reads wait for writes in flight, and a write the replicas have
  not all acknowledged within `AckTimeout` answers `202 write_unconfirmed`.
- Skips: a poison entry makes its type unavailable until an operator opens a skip, which a repair entry closes. Both
  go through the skip command and the skip endpoints.
- Capture of saved configuration entities by an EF Core interceptor, including owned and JSON-mapped changes,
  many-to-many links and database cascades. Unsafe setups are refused: a mapped write outside the cluster scope of a
  request, an unmarked bulk operation, a context with mapped types and no capture, and a mapped hub without a fence
  sink.

### Owned work

- Named singleton units and keyed resources with at most one owner whose fenced effects are accepted. A placer
  assigns them with eligibility filters and capabilities, and they move on a crash, a drain or a fence.
- Fenced transactions on a raw Npgsql connection and on a `DbContext`: the database refuses a write once another
  tenure holds the key.
- `Ask`: a request reaches the owner of a key with a typed outcome. Durable commands have a state machine, sweeps and
  an in-doubt policy.
- Convergent units and the library's own units (sweeps and retention).

### Primitives

- Claims, once-runs, single-flight, budgets, counters and held slots on PostgreSQL, each failing closed, with a
  retention sweep.
- Redis variants of claims, single-flight, budgets, counters and slots, and `FenceRedis`, an increase-only fence in
  Redis. Redis is never the authority: a lost message costs latency, not correctness. The start checks the eviction
  policy (`noeviction`) and the server version (6.2.5 or later).

### ASP.NET Core

- The readiness check, the fence middleware with its three `503` answers, request admission and drain accounting, the
  acknowledgement filter, and the read index for requests and SignalR hub invocations.
- Endpoint markers (`AllowWhenFenced`, `SkipReadIndex`) and the status and skip endpoints. A skip endpoint without
  authorization answers `403 skip_endpoint_unprotected`.

### Operations

- The `reeve_cluster` schema is created by additive EF Core migrations that `IClusterMigrationRunner` runs under an
  advisory lock. Unknown schema states fail closed.
- The start refuses a server below PostgreSQL 18, a standby, a non-zero `transaction_timeout` or `statement_timeout`,
  and a version that is not SemVer 2.0. The synchronous-replication check only warns and reads only what every role
  may read.
- Metrics, activities and logs, with a monitoring guide and a troubleshooting guide.
- `AddReeveCluster` registers all areas or only the named ones (`ClusterAreas`).
- All packages except `Reeve.Cluster.EntityFrameworkCore` are `IsAotCompatible`. The packages carry XML
  documentation, symbols and SourceLink, and depend on each other at an exact version.

### Verification

- Unit and architecture tests, plus integration and multi-replica harness tests against PostgreSQL, Redis and
  Toxiproxy in containers, with fault injection, failovers and offline restores.
- Replays of recorded failure schedules, a linearizability check of configuration reads and mixed-version runs beside
  the previous release.
