# Architecture

This page explains how Reeve.Cluster works, so you know what to rely on and what to plan for. It describes behaviour,
not internals. For setup, see the [getting started guide](guide/getting-started.md).

## The idea

A service with several replicas has to answer questions a single process never asks. Has every replica seen this
configuration change? Which replica runs the scheduler? What if two of them think they do? Reeve.Cluster answers them
with one rule: PostgreSQL decides, and a replica that cannot prove it is current stops serving.

Replicas never vote and never talk to each other to agree on anything. Every decision about order, ownership,
liveness or "only once" is a row or a transaction in PostgreSQL. A network partition between replicas changes
nothing, because they coordinate only through the database.

## PostgreSQL is the source of truth

The library keeps its state in the `reeve_cluster` schema of your database. The migrations ship in `Reeve.Cluster`
and run when you call `IClusterMigrationRunner.MigrateAsync()`. The schema holds:

- one row per replica (its lease, version, capabilities and status);
- the configuration log, an ordered list of what changed;
- one row per owned unit or keyed resource, with its current owner;
- claims, once-records, budgets and held slots;
- durable commands and the operator's skip ranges.

All reads and writes that matter go to the primary. The library needs PostgreSQL 18, and a synchronous standby if you
want the guarantees to survive a database failover. See [deployment](operations/deployment.md).

## Member leases and fencing

Each running process is a member of the cluster, identified by its `ReplicaId`. A member holds a lease in the
`member` table and renews it every `RenewInterval` (3 s by default). The lease lasts `LeaseTtl` (30 s by default) and is
measured on the database clock. All liveness in the library is this one lease: owners, the set of replicas a write
waits for, routing and status derive from it.

A replica must be able to prove its lease is still good before it acts. It checks its own monotonic clock before
every action that depends on being in service: admitting a request, starting a unit, granting a claim, writing through
a fenced transaction. If it cannot renew, because PostgreSQL is unreachable, the lease is lost, or a reconciler is
stuck, it takes itself out of service before the database lets the lease expire. This is called fencing.

A fenced replica:

1. stops serving: readiness fails and requests that need the cluster are refused with `503`;
2. refuses commits of client requests, so a half-finished write cannot slip through;
3. releases its lease if the database answers;
4. aborts long-lived connections (WebSockets, SignalR) through your fence sink;
5. stops its owned units;
6. rejoins with a full load of configuration, or exits so the orchestrator can restart it.

Fencing is the library's normal answer to trouble. A pod restart, a network blip or a database failover ends in
fence and rejoin without an operator. A graceful stop is a drain: the replica stops taking new requests, lets running
ones finish, hands its units over and writes `draining` so that writers stop waiting for it.

Every replica must have a unique `ReplicaId`, usually the pod name. See [membership](guide/membership.md) for what
the application sees.

## The configuration log

Configuration is state that every replica holds in memory in a derived form: feature flags, schedules, the settings
of a connection to another system. It lives in ordinary tables of your database.

When you change a mapped entity, the library appends an entry to the change log in the same transaction. Each replica
reads the log, re-reads the changed state from one database snapshot and builds a new immutable generation. Your
code reads from a generation, never from a half-applied state.

A write is acknowledged to the caller only when every serving replica has applied it. A request that arrives later,
on any replica, waits until its replica has caught up with everything committed before it, then reads one
generation. So a client that saw the write succeed never sees the old state afterwards. See the
[configuration log guide](guide/configuration-log.md).

## Owned work and placement

Some work must run on one replica at a time: a scheduler, a retention purge, a connection to one external device.
The library calls the code an owned unit and the answer to "who runs it" an owned row in PostgreSQL.

- A named unit has one key. A placer, itself an owned unit, assigns it to the least loaded eligible replica.
- A keyed resource has many keys, each created by the application and owned by one replica.
- Each change of owner draws a new, higher token. A fenced transaction checks that token in the database, so a write
  from a previous owner is refused even if that owner is paused or partitioned.

When an owner crashes, another replica takes over after a declared failover bound. When an owner is asked to move,
it stops first and the next owner starts after. See the [owned work guide](guide/owned-work.md).

## The version gate

Replicas of different releases run side by side during a rolling upgrade. Each member advertises its application
version and the library's protocol number. The cluster row carries a protocol floor and a version floor. A replica
below a floor cannot activate: it stays `joining` and never serves.

A feature that must not run before every replica has the new code asks `IClusterVersionGate.RequireVersionAsync`. If
every live member qualifies, the floor is raised and the answer is yes for good, so an old binary can no longer join.
The library keeps one release of overlap in its own tables and messages, so upgrading one release at a time can be a
rolling update. See [upgrading](operations/upgrading.md).

## Where Redis fits

Redis is optional and is never the authority. With `Reeve.Cluster.Redis` it:

- carries wake-ups between replicas, so appliers and owners react in milliseconds instead of at their next poll;
- relays `Ask` requests between processes;
- hosts faster variants of claims, single-flight, budgets, counters and held slots;
- holds `FenceRedis`, the token fence for state a unit keeps in Redis.

Losing Redis costs latency: polling takes over and `Ask` answers `NotTaken` or `TakenUnanswered`. Nothing fences.
A primitive that runs on Redis follows the failure policy you declared and never falls back to PostgreSQL unasked.

## What happens when something fails

| Failure | What happens |
|---|---|
| A replica crashes | Its lease expires within `LeaseTtl`. Writes wait for it until then, then complete. Its units move to other replicas within their failover bound |
| A replica cannot reach PostgreSQL | It cannot renew and fences itself. It rejoins when the database is back |
| PostgreSQL restarts briefly | Writes fail meanwhile and nothing fences. The tolerance is at least 15 s, up to 22 s with the defaults |
| PostgreSQL restarts for longer, or fails over | Every replica fences and rejoins with a full load within seconds of the database returning. Acknowledged commits survive on a synchronous standby |
| Redis is down | Wake-ups and the `Ask` relay stop. Polling carries on. Redis primitives follow their declared policy |
| A reconciler throws on one replica | That replica fences after its stall limit and rejoins. Others are unaffected |
| A reconciler throws on every replica | Every replica fences. An operator opens a skip for the type to get the fleet past the entry |
| A replica stalls or is paused | Its next serving check finds its deadline passed, and it fences before it acts. Its fenced writes are refused |

The [troubleshooting guide](operations/troubleshooting.md) maps symptoms to these cases.

## Guarantees

Plain terms, assuming the deployment meets the [requirements](operations/deployment.md):

| Guarantee | What it means |
|---|---|
| Configuration | Once a write is acknowledged, every replica that serves traffic has applied it. No request served afterwards, on any replica, sees the previous state, and no owned unit acts on it through a fenced path. The exception is a type an acknowledgement names as skipped by an operator. |
| Whole, ordered reads | A request or hub invocation reads one generation: the state of every configuration type at one point of the log. It is never older than a generation another request already read, and never a mix of two. |
| One owner | A unit or keyed resource has at most one owner whose fenced effects are accepted at any instant. The database refuses a stale token, and a replica whose lease cannot be proven stops acting. |
| Requests reach the owner | `Ask` for a key reaches its owner and returns one of five typed outcomes, so the caller never has to find the holder itself. |
| At most once | A claim, a once-only effect or a budget is decided once for the whole cluster, with a failure policy you chose explicitly. |
| Fail closed | When the library cannot prove a guarantee, it refuses: a write is not acknowledged, a replica leaves service, a claim is not granted. It does not degrade to a state that is only probably consistent. |
| Acknowledged writes survive | A write acknowledged to a client, and everything visible before it, survives a failover of a synchronously replicated database. |

## What it does not guarantee

| Not guaranteed | What it means |
|---|---|
| Effects outside PostgreSQL and Redis | A packet sent to a device, an e-mail, a call to another service. A unit calls `EnsureOwner()` immediately before each such effect, which narrows the window but cannot close it: a process paused between the check and the send still sends. External systems need their own idempotency. |
| High-rate data | Telemetry, observed device state and realtime fan-out do not belong in the configuration log. Every configuration write waits for every serving replica and writes are serialised, so it is not a store for data that changes many times a second. The design targets a handful of replicas. |
| Writes that bypass the library | Raw SQL that does not append to the log, state a component keeps in its own store, sessions in Redis, an external identity provider, and Entity Framework bulk operations that are not marked as appended. |
| Exempt and opted-out writes | An endpoint marked `[AllowWhenFenced]` is served in every phase and its writes are not awaited. A write that calls `AcknowledgeAtCommit()` still propagates, but a unit on another replica may briefly act on the previous state. |
| Long-lived connections | After connect, a hub connection, WebSocket or server-sent-events stream keeps the generation it pinned. Reads outside a request or invocation have the acknowledgement guarantee but not one whole, ordered read. |
| Work in flight | An acknowledgement says every replica applied a change. It does not drain operations that started before it. |
| Redis between a lease's end and the successor | A delayed write to Redis can land in that window. State that must stop at the lease belongs in PostgreSQL. A Redis failover that loses acknowledged writes can lose fence state, which is the one case where losing Redis costs correctness. |
| Availability | A write waits for the slowest serving replica, up to `AckTimeout`, after which an HTTP write answers `202 write_unconfirmed`. A database outage longer than the tolerance fences every replica. A poison configuration entry can take every replica out of service until an operator skips it. |
| Commits seen before they were replicated | Between a database failover and the moment a replica notices the new timeline (within `RenewInterval + RenewTimeout`), it may serve or act on a commit the failover then removes. An acknowledged write is never affected. |
| Restores and manual changes | A restore from backup is offline, and the guarantees across it are not given. Deleting a member row by hand has the same effect until that replica next renews. |
| A broken deployment | Nothing is guaranteed without synchronous replication, bounded clock skew, one process per replica id and a balancer that resends only what is safe. See [deployment](operations/deployment.md) and [balancer and ingress](operations/balancer-and-ingress.md). |

See also: [configuration](operations/configuration.md), [monitoring](operations/monitoring.md) and the
[FAQ](faq.md).
