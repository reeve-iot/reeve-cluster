# Monitoring

What a replica reports and where: metrics, readiness, the status registry, the background checks, and the log lines
worth an alert.

## Metrics

Every instrument is on one meter, `Reeve.Cluster` (`ClusterDiagnostics.Name`):

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(m => m.AddMeter(ClusterDiagnostics.Name));
```

Instrument names are OpenTelemetry names (`reeve_cluster.lease.renewal.duration`). The tables use the names the
OpenTelemetry Prometheus exporter writes: dots become underscores, a duration in seconds gets `_seconds` and is a
histogram (`_bucket`, `_sum`, `_count`), a counter gets `_total`, and brace units are dropped.

### Stability

An instrument's name, kind, unit, tag keys and tag values are part of the public contract, like a public type's
members. A rename, a removal, a change of kind or unit, or a removed tag key or value happens only in a major release.
New instruments, tag keys and tag values may arrive in a minor or patch release. The [CHANGELOG](../../CHANGELOG.md)
lists every change. Descriptions and bucket boundaries are not part of the contract.

### Tags

No tag carries a replica id, a process id, an epoch, a token or a key: the scrape target (the pod) tells replicas
apart. Tag values come from a set the library names, except `kind` and `aggregate_type`. Their values are the unit
kinds and aggregate types your application registers, so they are bounded by your code, not by your data. `kind` also
includes the library's own kinds: `placer`, `reeve.free-row-sweep`, `reeve.once-sweeper`,
`reeve.primitive-retention`, `reeve.command-sweep`.

### Instruments

| Prometheus name | Type | Tags | What it shows |
|---|---|---|---|
| `reeve_cluster_replica_phase` | gauge | `phase`: `starting`, `loading`, `catching_up`, `activated`, `serving`, `fencing`, `fenced`, `draining`, `exited` | 1 for the replica's current phase, 0 for the other eight |
| `reeve_cluster_fences_total` | counter | `trigger`: `fence_start`, `applier_stall`, `stuck_attempt`, `fatal_fault`, `zero_row_renewal`, `timeline`, `unanswered_activation` | Fences, once per epoch, by what started them. A database switchover or failover fences a serving replica once: with `timeline` when its renewal reaches the new primary before its fence start, otherwise with `fence_start` or `zero_row_renewal`. A trigger that ends the process without a fence is not counted |
| `reeve_cluster_fence_step_overruns_total` | counter | `step`: `4`, `5`, `6` | Fence steps 4 and 5 the coordinator stopped awaiting at `StopBudget` (they are started, not awaited past it), and step-6 disposes of the pass connection not returned by then |
| `reeve_cluster_lease_renewal_duration_seconds` | histogram | `outcome`: `renewed`, `other_timeline`, `not_mine`, `unknown` | Lease renewal round trips, the commit's wait for the standby included. `unknown` is a renewal that got no answer within `RenewTimeout` or got an error |
| `reeve_cluster_applier_full_load_duration_seconds` | histogram | `outcome`: `loaded`, `failed` | Each attempt of the full load a replica runs on start and on every rejoin; size `ReconcileTimeout` from it |
| `reeve_cluster_change_log_head` | gauge | none | The change-log head as the replica's applier last read it, at its poll or in a pass's snapshot |
| `reeve_cluster_change_log_horizon` | gauge | none | `horizon_seq` as the applier last read it at its poll: 0 until retention first deletes |
| `reeve_cluster_change_log_retained` | gauge | none | Entries the log keeps: the head, or the head minus `horizon_seq` plus 1 once retention has deleted. A seq a rolled-back write consumed is counted, so it is an upper bound |
| `reeve_cluster_generation_head` | gauge | none | The head the replica's current generation stands for |
| `reeve_cluster_generation_pins` | gauge | `generation`: `current`, `superseded` | Scopes holding a pin on the current generation and on one a later publication replaced. Pins on a superseded generation are the long-lived ones (a WebSocket or SSE connection keeps its connect-time pin) |
| `reeve_cluster_applier_attempt_duration_seconds` | histogram | `aggregate_type`; `outcome`: `accepted`, `failed`, `timed_out`; `pass`: `full`, `incremental` | Each reconciler attempt, from its start to its outcome; an attempt the pass stopped awaiting records nothing |
| `reeve_cluster_applier_deferred_running` | gauge | `aggregate_type` | Deferred work started by a publication and not yet returned; 0 for a type with none |
| `reeve_cluster_applier_deferred_oldest_age_seconds` | gauge | `aggregate_type` | Age of the oldest running deferred work of the type; it grows for deferred work that ignores its `Superseded` token |
| `reeve_cluster_applier_invalid_cursor_reloads_total` | counter | none | Passes whose snapshot found the local cursor invalid and reloaded every type in full: the log lost or replaced the entry the cursor names (a failover that lost a suffix, a restore) or retention deleted it. Each is also an `Information` log line with the cursor and the head; a retried pass counts again; a first start counts nothing |
| `reeve_cluster_rejoins_total` | counter | `decision`: `rejoin`, `higher_epoch` | Decisions that end a wait in `Fenced`: the replica re-enters `Starting` (`rejoin`) or finds a row with a higher epoch and exits (`higher_epoch`) |
| `reeve_cluster_rejoin_duration_seconds` | histogram | none | The time out of service a rejoin ended, from the first fence's step 0 since the replica last served to its next entry into `Serving`. With the defaults it includes `RejoinBackoff` after a short epoch, and the full load. A replica that does not serve again records nothing |
| `reeve_cluster_drain_duration_seconds` | histogram | `outcome`: `drained`, `unanswered`, `taken_over` | One graceful stop. `unanswered`: the `draining` write got no answer, so writers wait for the lease to expire as after a crash; `taken_over`: a fence took the drain over |
| `reeve_cluster_drain_awaited_requests` | gauge | none | Requests and hub invocations that were running at drain step 1 and still are: what step 4 waits for, at most `ProxyReadTimeout`. No measurement before the drain starts |
| `reeve_cluster_activation_refusals_total` | counter | `reason`: `activation.protocol-floor`, `activation.version-floor` | Activation attempts a floor refused, one per `RenewInterval` while refused: it counts attempts, not members |
| `reeve_cluster_commit_duration_seconds` | histogram | `transaction`: `library`, `fenced`; `outcome`: `committed`, `unknown`, `failed` | `COMMIT` round trips of the library's write transactions and of fenced transactions: the wait for the standby plus the primary's flush and one round trip. A commit that fence step 2 refuses sends no `COMMIT` and records `failed` |
| `reeve_cluster_owned_takeover_duration_seconds` | histogram | `kind` | From the write that gave a unit or keyed resource to its new owner to the new owner calling `OnAcquiredAsync` |
| `reeve_cluster_owned_release_overdue` | gauge | `kind` | Release requests in force beyond `ReleaseGrace` from their request; reported only by the replica that runs the placer |
| `reeve_cluster_owned_running` | gauge | `kind` | Tenures of the kind this replica runs, from memory; 0 for a registered kind it runs none of |
| `reeve_cluster_skip_open` | gauge | `aggregate_type` | Open skips, as each replica's skip monitor last read them |
| `reeve_cluster_capture_discards_total` | counter | none | Transactions that ended around EF Core, whose captured entries were discarded |
| `reeve_cluster_admission_refusals_total` | counter | `reason`: `replica_not_serving`, `replica_fenced`, `replica_behind` | Requests, hub connections and hub invocations refused with a `503` before anything ran |
| `reeve_cluster_write_refused_total` | counter | none | Requests answered `503 write_refused`, and hub invocations that threw `write_refused` |
| `reeve_cluster_write_unconfirmed_total` | counter | none | Requests answered `202 write_unconfirmed`, and hub invocations that threw `write_unconfirmed` |
| `reeve_cluster_slot_acquires_total` | counter | `store`: `postgresql`, `redis`; `result`: `acquired`, `full`, `not_serving`, `unknown` | Answered slot acquires; a call that throws records nothing |
| `reeve_cluster_slot_renewals_total` | counter | `store`: `postgresql`, `redis`; `result`: `renewed`, `ended`, `not_serving`, `unknown` | Answered slot renewals, an `Ended` answered without a statement included |
| `reeve_cluster_slot_held` | gauge | `store`: `postgresql`, `redis` | The slots this process holds: its registry's entries whose local deadline has not passed. PostgreSQL and Redis slots have registries of their own |
| `reeve_cluster_retention_deleted_total` | counter | `record`: `claim`, `once`, `flight`, `budget`, `budget_id`, `counter`, `slot` | Rows `reeve.primitive-retention` deleted from that record's table |
| `reeve_cluster_redis_policy_uses_total` | counter | `primitive`: `claim`, `single_flight`, `budget`, `slot`; `policy`: `grant`, `refuse` | Times a Redis primitive applied its `OnRedisUnavailable` policy because Redis was unavailable at a decision that grants, or a claim's grant was sent three times in one call and each read found the claim gone |

### Reading them

- An EF Core fenced commit in `reeve_cluster_commit_duration_seconds` includes the capture's change-log append, which
  waits for the lock other appending transactions hold. Under concurrent configuration writes it measures more than
  the standby wait.
- Every replica reports the skip gauge, so N replicas report N per skip.
  `sum by (kind) (reeve_cluster_owned_running)` is the number of owners of a kind: 1 for a placed unit, 0 while it has
  no owner, and the key count for a keyed kind. Replicas are scraped at different moments, so a handover can sum to 0
  or 2 for one scrape.
- The change-log values are the applier's last reads. They stand still while the applier does not
  poll (`starting`, `fenced`, after the exit), and the phase gauge says which. Nothing is reported before the
  applier's first read, and a replaced pod's series ends with the pod. A `reeve_cluster_change_log_retained` that is
  flat and then grows, together with a member whose `appliedSeq` lags, means a replica stuck loading is holding
  retention.
- Takeover time starts at the write that gave the row to its new owner. It excludes the time from the previous
  owner's crash, fence or drain to that write, which no replica can measure. The kind's failover bound covers it
  ([configuration.md](configuration.md#timing-budgets)), and the previous owner's `reeve_cluster_fences_total` shows
  when it fenced.
- The drain ends with the process, and so does a rejoin that decides `higher_epoch`. A
  pull exporter (Prometheus scrape) sees `reeve_cluster_drain_duration_seconds` and
  `reeve_cluster_rejoins_total{decision="higher_epoch"}` only if a scrape falls between the record and the exit. An
  exporter that flushes at shutdown (OTLP) sends them. `reeve_cluster_drain_awaited_requests` is visible while step 4
  of the drain waits, which can take tens of seconds.
- On a first start, no endpoint answers before the first load, so a scrape served by the application sees nothing of a
  replica stuck joining or loading on its first start. Use the log or a push exporter.
- Not measured: the join and the activation have no duration metric (the floors' refusals are counted). No pin age
  is exported: a `superseded` pin count above 0 for a long time is the signal. Commits made outside a fenced
  transaction, and the activation's, retention's and migration runner's commits, are not in
  `reeve_cluster_commit_duration_seconds`. Npgsql's own meter covers those.

### Bucket advice

The histograms carry explicit bucket boundaries as instrument advice, sized for seconds. An SDK that ignores the advice
uses its own defaults, sized for milliseconds, unless a View sets them.

| Histogram | Boundaries (s) |
|---|---|
| lease renewal | 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5 |
| commit | 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 |
| full load | 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600 |
| takeover | 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60, 120 |
| reconciler attempt | 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300 |
| rejoin | 1, 2.5, 5, 10, 30, 60, 90, 120, 180, 300, 600 |
| drain | 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 90, 120, 180 |

### Queries

`pod` stands for whatever label the scrape gives the target.

```promql
# Phase per replica, for a state timeline
max by (pod, phase) (reeve_cluster_replica_phase) == 1
# Renewal p99 per replica
histogram_quantile(0.99, sum by (pod, le) (rate(reeve_cluster_lease_renewal_duration_seconds_bucket[5m])))
# Commit p99 of fenced transactions
histogram_quantile(0.99, sum by (le) (rate(reeve_cluster_commit_duration_seconds_bucket{transaction="fenced"}[5m])))
# Applier lag in seqs: entries the log holds that the current generation does not
reeve_cluster_change_log_head - reeve_cluster_generation_head
# Entries the log keeps
reeve_cluster_change_log_retained
# Reconciler attempt p99 by aggregate type
histogram_quantile(0.99, sum by (aggregate_type, le) (rate(reeve_cluster_applier_attempt_duration_seconds_bucket[5m])))
# Pins held on generations a later publication replaced
reeve_cluster_generation_pins{generation="superseded"}
# Rejoins that did not reach Serving again (compare the histogram's count with the decisions)
sum(increase(reeve_cluster_rejoins_total{decision="rejoin"}[1h])) - sum(increase(reeve_cluster_rejoin_duration_seconds_count[1h]))
# Fences in the last hour, by trigger
sum by (trigger) (increase(reeve_cluster_fences_total[1h]))
```

### What to alert on

| Signal | Why |
|---|---|
| `reeve_cluster_fences_total` increasing with a trigger other than `timeline` | A replica lost its lease, stalled or hit a fault. `timeline` follows a planned switchover |
| Renewal p99 approaching `RenewTimeout` (3 s), or `outcome="unknown"` increasing | Sustained above `RenewTimeout`, the fleet fences |
| `reeve_cluster_skip_open > 0` | A skip is open: the fleet treats a type as applied without applying it |
| `reeve_cluster_activation_refusals_total` increasing | A replica is refused by a version or protocol floor: a rollback or a skipped release |
| `reeve_cluster_applier_full_load_duration_seconds{outcome="failed"}` | A full load exceeds `ReconcileTimeout` and the replica cannot serve |
| `reeve_cluster_owned_release_overdue > 0` | A unit's owner does not stop within `ReleaseGrace` |
| `reeve_cluster_fence_step_overruns_total` increasing | Fence steps do not finish within `StopBudget` |
| `reeve_cluster_write_unconfirmed_total` increasing | Writes that may or may not have committed reach clients |
| Applier lag growing, or `reeve_cluster_change_log_retained` growing without bound | A replica is behind and holds retention |
| `reeve_cluster_redis_policy_uses_total` increasing | Redis was unavailable at a decision that grants |

## Readiness

`AddHealthChecks().AddReeveClusterReadiness(name = "reeve-cluster", tags)` is healthy while the replica passes its
serving check and no named reason is raised. Otherwise it is unhealthy and names the phase and each reason, which the
log and the metrics report too. It makes the same call every serving action makes, so a replica paused across its
fence start reports unready at its first probe, and the probe itself starts the fence.

| Reason | Meaning |
|---|---|
| `joining.refused` | Another process holds the member row's lease (a second process with this `ReplicaId`, or a restored row). Retried every `RenewInterval` |
| `joining.unanswered` | The join got no answer from the database. Retried every `RenewInterval` |
| `loading.failed` | A full load failed, often because it took longer than `ReconcileTimeout`. Retried with backoff |
| `activation.failing` | An activation round failed before its commit was sent (a missing row, a privilege, a bound). Retried every `RenewInterval` |
| `activation.version-floor`, `activation.protocol-floor` | A floor refuses this replica's version or protocol ([upgrading.md](upgrading.md#the-version-gate-and-its-floors)) |
| `activation.upgrade-path` | The release declares that every other member must speak a protocol an active member does not |

On a first start no endpoint answers until the first load is done, so `joining.*` and `loading.failed` appear in
readiness only on a rejoin.

An open skip is added to the answer's data and description as a critical detail but never changes the status, because
a skip must not take replicas out of readiness. The synchronous-replication check is never a readiness reason. Map the
check as an endpoint marked `AllowWhenFenced().SkipReadIndex()`
([deployment.md](deployment.md#probes-and-the-start)).

## The status registry

Every `StatusReportInterval`, each replica writes status sections, read from its own memory, to the database for its
current epoch. It reports in every phase that has an epoch (a fenced replica says so). The write uses the main pool and
is bounded by that interval. The record is best effort, and no decision reads it.

| Section | Contents |
|---|---|
| `lease` | the lease and the WAL timeline |
| `phase` | the local phase |
| `applier` | cursor, generation, the stall and the first unapplied seq |
| `owned` | the tenures held, and `ReleaseGrace` |
| `releases` | release requests |
| `skips` | open skips |
| `floors` | the version and protocol floors, who moved one last and when |
| `replication` | `enabled`, `outcome` (`NoStandby`, `Synchronous`, `NotSynchronous`, `Unreadable`), `warning`, `walsenders`, `synchronousStandbyNames`, `synchronousCommit`, `checkedAt` |
| `ask` (with the Redis package) | `subscriptions`: `holding`, `lost` or `not-subscribed` |

- A failed write is logged as a warning once per streak, and the next tick retries.
- A source that throws, is cancelled by anything but the host's stop, or does not answer within the interval drops its
  section for that tick (a warning once until it collects again). The old row stays, with a growing age.
- A section lives while the member row of its epoch has a valid lease and for `LeaseTtl` after it ended, so a fenced
  replica's last report can still be read. Writes outside that window are refused, reads do not return those rows, and
  the sweep deletes them. Age is measured on the database clock.
- NUL characters in strings and keys are replaced with U+FFFD, because `jsonb` refuses `\u0000`.

### Reading it

`app.MapReeveClusterStatus(pattern = "/reeve/status")` maps one `GET` that answers every member's row with its
sections and fence history. It is exempt from the fence and the read index. It adds no authorization, and its body
names operators, skip reasons, unit keys and exact versions, so add your own and keep the sign-in route exempt, so an
operator can still reach it on a fenced fleet.

```csharp
app.MapReeveClusterStatus("/status").RequireAuthorization();
```

It answers `200` with the following, in camel case whatever your JSON options say:

```text
{ readAt, members: [ { replicaId, epoch, state, leaseExpiresAt, leaseValid, appliedSeq,
  servingSince, version, protocol, sections: [ { name, ageSeconds, values } ],
  fences: [ { epoch, trigger, fencedAt, spanSeconds } ] } ] }
```

It answers `503 status_unavailable` when the read failed or took longer than `ActionTimeBudget`. Like every endpoint
it is refused until the replica has loaded its first generation.

The envelope is stable. Its enumerated strings (`state`, a fence's `trigger`, the `ask` `subscriptions`) may gain
values in a minor release, so treat an unknown one as unknown. The keys inside a section's `values` are diagnostic and
may change in a minor release. A time inside `values` is as of the member's write (subtract `ageSeconds`).

The same read is available in code as `IClusterStatus.ReadAsync`, which also works from a container whose host is never
started. It uses the read-index pool ([deployment.md](deployment.md#the-read-index-and-its-connections)).

Stopped and crashed replicas stay listed, because the library never deletes a member row. A `joining` or `active` row
with a valid lease is running, and so is a `fenced` row (the process lives and is readmitted, and `leaseValid` is
false). A `draining` row, or a `joining` or `active` row whose lease is no longer valid, is not.

**Application sections.** Report your own sections with `AddClusterStatusSection<T>()`, where `T` implements
`IStatusSectionSource`. A source is a singleton and must return quickly from its own memory. Sources are collected one
after the other, each bounded by `StatusReportInterval`, and the library does not cancel a collect it stopped waiting
for. Section names must be unique, non-empty and none of the library's (`applier`, `ask`, `floors`, `lease`, `owned`,
`phase`, `releases`, `replication`, `skips`; `ask` is reserved whether or not the Redis package is added), or the host
does not start. Prefix your names, since a later minor release may add a library section.

## Skip monitor

The skip monitor reads open skips at start-up and every `StatusReportInterval`, on the main pool, and keeps the last
result in memory for the `skips` section and the readiness check. A newly seen open skip is a critical log line and
adds one to `reeve_cluster_skip_open`. One no longer open logs at information and subtracts it. A failed read keeps
the last result and logs a warning. No decision of the library reads the monitor. A process stuck in its first load
reports no skip, but the serving replicas do. How to open and close a skip is in
[troubleshooting.md](troubleshooting.md#a-reconciler-cannot-get-past-an-entry).

## Synchronous-replication check

The check reads the configuration at start-up and every `ReplicationCheckInterval`, with any role (it needs no
`pg_read_all_stats`): the number of physical walsenders, and its session's `synchronous_standby_names` and
`synchronous_commit`.

| Finding | Outcome |
|---|---|
| No physical walsender (a single instance) | passes (`NoStandby`) |
| Walsenders, a non-empty `synchronous_standby_names` and `synchronous_commit` of `on`, `remote_write` or `remote_apply` | passes (`Synchronous`) |
| Walsenders, and commits do not wait for one (`synchronous_standby_names` empty, or `synchronous_commit` `local` or `off`) | warns after three consecutive reads (`NotSynchronous`) |
| The database revoked `pg_stat_replication` from `PUBLIC` | warns at once, reported as "cannot read", not an outage (`Unreadable`) |

- The warning is logged when it starts and when it ends, and shown in the `replication` section. It is never a
  readiness reason: without the guarantee a failover may lose a recent acknowledged commit, but the library does not
  turn that into an outage.
- It waits for several consecutive reads because a replica that joins runs `pg_basebackup`, which counts as a
  physical walsender until the operator adds the new standby to the settings.
- Without `pg_read_all_stats` a role sees another role's walsender without its state, so the check counts physical
  walsenders by a null `datid` (a logical subscriber has one set).
- It reads settings, not the standbys' state. A name in `synchronous_standby_names` that matches no standby passes it,
  although commits then block, which the library sees as a database outage.
- A failed read keeps the last result: an unreachable database is an outage the lease and the gate already handle.
- `CheckSynchronousReplication=false` turns it off. The opt-out is logged at start-up and shown in the section.

## Ask relay reachability

With the Redis package, the `ask` section reports whether `Ask` can reach this process, which is whether the relay's
subscriptions succeeded and still hold. Only the request channel counts. The reply channel only wakes this process's
own calls, which also poll.

StackExchange.Redis reports any channel as connected while the server is reachable, subscribed or not, so the relay
keeps its own ledger. A channel is held from a successful `SUBSCRIBE` until it is unsubscribed or the subscription
connection fails, and held again once the connection returns. A relay whose subscriptions do not hold takes nothing
and logs one warning while it retries. A Redis that is down at start does not fail the start.

One gap: a resubscription that Redis refuses after a reconnect (an ACL changed at run time) still reads as `holding`.

## Redis start-up check

The check runs when a unit kind declares `RedisFence` or the application registers a Redis claim, single-flight,
budget, counter or slot. With none of these nothing is checked, and an unavailable Redis is logged and skipped.

- Where a kind declares `RedisFence` or you register a Redis claim, single-flight or slot, every
  connected primary must run 6.2.5 or later (`RedisTooOld`). Budgets and counters set no floor.
- Every connected primary must have `maxmemory-policy noeviction` (`RedisEvictionPolicy`). The
  policy is read with `INFO memory`, which needs `AllowAdmin`, so the check uses a short-lived connection of its own,
  cloned from your configuration string. Your application's connection needs no `AllowAdmin`.
- The start is refused on a policy other than `noeviction`, on one it cannot read (`INFO` refused by an ACL
  or the provider), when every server the clone lists is a replica, and when the check's connection cannot be made.
- If Redis is down at start, for `RedisFence` and slots the start is also refused when no primary is connected or a
  listed server is not reached. For claims, single-flight, budgets and counters, an unreached server only produces a
  warning. The policy is read once, at the start, so a process that started with Redis down never checks it and can
  run with an evicting policy unnoticed.
- The clone does not carry TLS callbacks (`CertificateValidation`, client certificates), a
  `DefaultOptionsProvider` set in code, and a user or password set in code that contains a `,`. If the check's
  connection needs one of these, it cannot connect or authenticate and the start is refused (the refusal leaves out a
  parse error's message, which can carry the password). `CheckEvictionPolicy = false` is the way past it.
- Under Sentinel, a replica that Sentinel still lists after it went away (until `SENTINEL RESET`) fails every such
  start.
- `ClusterRedisOptions.CheckEvictionPolicy = false` turns the policy check off, with a start-up warning that names the
  primitives it covers. The version is still checked.

## Log lines worth alerting on

Every line comes from a logger in the `Reeve.Cluster` namespaces. The texts are the message templates.

| Level | Message (start) | Meaning |
|---|---|---|
| Critical | `A skip is open: the replicas treat {AggregateType} as applied from seq {FromSeq} on ...` | An operator opened a skip. Acknowledgements name the type until it closes |
| Critical | `Replica {ReplicaId} was given epoch {Epoch}, at or below epoch {Highest} that this process held ...` | A restore under a running replica, or a failover that lost the join. The process exits and restarts |
| Critical | `A reconciler attempt is stuck: ...; the process ends.` | An attempt did not return within `ReconcileTimeout + ApplyStallLimit` |
| Critical | `A module reported a fatal local fault in phase {Phase}: {Reason}` | A fatal fault. The replica fences, or exits when it holds no row |
| Critical | `{Trigger} in {Phase} ({Detail}): the process exits.` / `Replica {ReplicaId} exits from {Phase}.` | The process ends and the orchestrator restarts it |
| Warning | `Fence from {Phase}: {Trigger} ({Detail}).` | A fence started |
| Warning | `The applier has stalled for {StalledFor} in {Phase}, past ReconcileTimeout + ApplyStallLimit ...` | A stall: a fence in `Activated`, `Serving` or `Draining`, retries in `Loading` or `CatchingUp` |
| Warning | `The reconciler of {AggregateType} failed in pass {Pass}; the pass is retried.` / `... did not return within ReconcileTimeout ...` | A reconciler fails. The line names the types a skip would have to cover |
| Error | `The full load of epoch {Epoch} failed after {Elapsed}, with ReconcileTimeout {ReconcileTimeout} ...` | A full load fails. The replica cannot serve until one publishes |
| Error | `The activation of epoch {Epoch} failed before its commit was sent ...` | `activation.failing` |
| Error | `Discarded {Entries} captured change-log entries of {Transactions} transaction(s) on a reused DbTransaction ...` | A transaction ended around EF Core. If it committed, its changes have no change-log entries |
| Error | `A transaction begun through EF is still open with a write in it ...` | An action returned with an open transaction. The request answers unconfirmed |
| Warning | `The database has {Walsenders} physical walsender(s), and synchronous_standby_names = ... does not make a commit wait for a standby ...` | Synchronous replication is not configured |
| Warning | `The synchronous-replication check cannot read pg_stat_replication ...` | `pg_stat_replication` revoked from `PUBLIC` |
| Warning | `Cluster:CheckSynchronousReplication is off ...` | The check is turned off (logged at start) |
| Warning | `The open skips could not be read; the last result stands.` | The skip monitor's read failed |
