# Troubleshooting

What to check and do when a fleet misbehaves, by symptom. The [FAQ](../faq.md) lists common problems with their exact
error messages.

## Where to look first

| Question | Where |
|---|---|
| What phase is each replica in? | `reeve_cluster_replica_phase`, or the `phase` section of the status endpoint |
| Why is a replica not ready? | its `/health/ready` answer: the phase and every raised reason ([monitoring.md](monitoring.md#readiness)) |
| Why did a replica fence? | `reeve_cluster_fences_total` by `trigger`, the `Fence from {Phase}: {Trigger} ({Detail}).` log line, and the member's `fences` in the status endpoint |
| Is the applier behind or stalled? | the `applier` section (cursor, stall, first unapplied seq), `reeve_cluster_change_log_head - reeve_cluster_generation_head` |
| Is a skip open? | the `skips` section, `reeve_cluster_skip_open`, the critical log line |
| Who owns a unit? | the `owned` section of each member, `sum by (kind) (reeve_cluster_owned_running)` |
| Is the database set up as assumed? | the `replication` section, and the start-up probe errors ([deployment.md](deployment.md#connection-requirements)) |
| A replica that never started serving | its log only: no endpoint answers before the first load ([deployment.md](deployment.md#probes-and-the-start)) |

The status endpoint is exempt from the fence and the read index, so it answers on a fenced fleet. When no replica is
ready, reach a pod with `kubectl port-forward`, which does not go through the ingress.

## The host does not start

A refused start throws a `ClusterConfigurationException` (with a `Reason`) or an `OptionsValidationException` (with one
message per broken rule). Most refusals come before the replica serves anything. The hub check is the exception: it
runs once the web host has built its endpoints, after the join.

| Reason or message | Cause | Fix |
|---|---|---|
| Options validation: `... is required` | `ReplicaId`, `ConnectionString` or `ReconcileTimeout` missing; `ProxyReadTimeout` or `ActionTimeBudget` missing with the ASP.NET Core package | Set it ([configuration.md](configuration.md#clusteroptions)) |
| Options validation: `... must be positive` / `must not be negative` / `must be at most ...` | A duration out of range | [configuration.md](configuration.md#validation) lists the limits |
| Options validation: a relation (`RenewInterval ... must be at most`, `AckTimeout ... must be more than`, `ProxyReadTimeout ... must exceed`, ...) | Timings that break a budget | The message names every term and its value; move the setting it names, or the one you control (often `ActionTimeBudget` or the proxy's timeout) |
| Options validation: `CommandPendingTtl ... must be at least the failover bound of kind '...'` | A command kind's failover bound (with its `StartTime` and `RedisFence`) exceeds `CommandPendingTtl` | Set `CommandPendingTtl` to the value the message names, or more |
| Options validation: `ConnectionString must name the primary: ...` | `Target Session Attributes` that may reach a standby | [FAQ](../faq.md#start-up) |
| `... is registered, but AddReeveCluster was called without ClusterAreas.X` | A unit kind, keyed resource, `Ask` handler, durable command handler, once effect or Redis primitive whose area is off | Add the area named, or remove the registration ([../reference/registration.md](../reference/registration.md)) |
| `HeaderPrefix "..." must consist of ASCII letters, digits and hyphens and end in a hyphen.` | An invalid `ReeveClusterHttpOptions.HeaderPrefix` | Fix the prefix |
| `NotPrimary`, `TransactionTimeoutNotZero`, `StatementTimeoutNotZero`, `PostgreSqlTooOld` | The connection probe ([deployment.md](deployment.md#connection-requirements)) | [FAQ](../faq.md#start-up) |
| `RedisEvictionPolicy`, `RedisTooOld` | The Redis start-up check ([monitoring.md](monitoring.md#redis-start-up-check)) | [FAQ](../faq.md#start-up) |
| `RedisPolicyRequired` | A Redis claim, single-flight, budget or slot registered without a valid `OnRedisUnavailable` value (such as `default`) | Pass `OnRedisUnavailable.Grant` or `Refuse` |
| `ReconcilerDependencyUnknown` | A reconciler's `DependsOn` is null, or names a type no registered reconciler owns | Return an empty list, or register the reconciler of that type |
| `ReconcilerDependencyNotEarlier` | A reconciler depends on itself, a later type, or a cycle | Order the reconcilers in `ClusterPorts` so dependencies come first |
| `AggregateTypeWithoutReconciler` | A mapped aggregate type has no registered reconciler, so replicas would serve stale data | Register its reconciler, or unmap it |
| `ContextWithoutCapture` | A context with mapped entities is registered without `UseReeveClusterCapture`, or could not be checked | Add `UseReeveClusterCapture(services)` to its options |
| `StatusSectionNameDuplicate`, `StatusSectionNameReserved`, `StatusSectionNameInvalid` | Two sources share a section name, one uses a library section's name, or a name is empty | Rename the application's section ([monitoring.md](monitoring.md#the-status-registry)) |
| `VersionNotSemVer` | The entry assembly's informational version is not SemVer 2.0 | Set a SemVer `<Version>` at build ([upgrading.md](upgrading.md#the-applications-version)) |
| `HubFilterWithoutFenceSink` | A hub is mapped while the fence sink is `NoFenceSink` | [FAQ](../faq.md#start-up). The refusal comes after the join, so the member row and any units it acquired stay until the lease expires |
| `42501` from `MigrateAsync` | The migrator role cannot create in the schema | Grant the migrator role ([deployment.md](deployment.md#roles-and-grants), [upgrading.md](upgrading.md#the-migration-runner-and-schema-ownership)) |
| `MigrateAsync` waits, then fails on a statement timeout | A running `pg_dump` holds a lock on the history table | Let the dump finish; the restart recovers |

## A replica never becomes ready

Read its readiness answer, or its log when no endpoint answers yet.

| Readiness reason or log | Cause | Fix |
|---|---|---|
| No endpoint answers; pods restarted | The startup probe is shorter than `LeaseTtl` plus the first load | [FAQ](../faq.md#pods-are-restarted-before-they-ever-become-ready) |
| `joining.refused` | Another live process holds the member row: a duplicate `ReplicaId`, a surge rollout with a fixed id, or a restored row whose lease still runs | Give each process its own id, or roll without a surge; a restored row's lease runs out within `LeaseTtl` |
| `joining.unanswered` | The join got no answer from the database | Check database reachability; the join retries every `RenewInterval` |
| `loading.failed`, Error `The full load of epoch ... failed after ...` | A full load longer than `ReconcileTimeout`, or a reconciler that throws | [FAQ](../faq.md#the-configuration-log); size `ReconcileTimeout` from `reeve_cluster_applier_full_load_duration_seconds` |
| `activation.failing`, Error `The activation of epoch ... failed before its commit was sent ...` | A missing row, a privilege, or the activation's bound (`RenewTimeout + 1 s`) | Run the migrator; check the runtime role's grants; check the change-log lock is not held by a long application transaction |
| `activation.version-floor`, `activation.protocol-floor` | The floor is above this build | Roll forward, or lower the floor ([upgrading.md](upgrading.md#rolling-back-below-a-raised-floor)) |
| `activation.upgrade-path` | An active member speaks a protocol below this release's declared minimum | Upgrade through the intermediate release |

## Replicas fence

Look at the trigger first.

| Trigger | Cause | What to do |
|---|---|---|
| `timeline` (all replicas at once) | A database switchover or failover: a new WAL timeline | Expected; the fleet rejoins with a full load. Expect about 6 s plus a full load of `503`s |
| `fence_start`, `zero_row_renewal` (all replicas) | A database outage longer than the fleet rides out (15 to 22 s with the defaults), or a failover detected through the renewal | Check the database; raise `LeaseTtl` if outages of that length are routine ([configuration.md](configuration.md#sizing-the-timings)) |
| `fence_start` (one replica) | Its renewals did not get through: a process pause, CPU starvation, a network problem, or synchronous commit latency near `RenewTimeout` | Check `reeve_cluster_lease_renewal_duration_seconds` p99 and `outcome="unknown"`; check the standby's commit latency |
| `zero_row_renewal` (one replica) | The member row is no longer this process's: a restore, a manual delete, or another process took the id | Check for a duplicate `ReplicaId` |
| `applier_stall` | A reconciler failed or timed out for longer than `ReconcileTimeout + ApplyStallLimit` | The warnings `The reconciler of {AggregateType} failed in pass ...` name the type; see [below](#a-reconciler-cannot-get-past-an-entry) |
| `stuck_attempt` (the process ends) | A reconciler attempt did not return at all | Find the blocking call in the reconciler; every command it runs must be bounded |
| `fatal_fault` | A module reported a fatal fault; the critical log line names it | Read the line |
| `unanswered_activation` | The activation's commit got no answer | Check the database and the standby |

A replica that fenced with an epoch shorter than `LeaseTtl` waits `RejoinBackoff` (60 s) before it rejoins; a restart of
the process joins at once. `reeve_cluster_rejoins_total` against `reeve_cluster_rejoin_duration_seconds_count` shows
rejoins that did not reach `Serving` again.

`reeve_cluster_fence_step_overruns_total` increasing means fence steps did not finish within `StopBudget`: a fence sink
whose connection abort does not finish (step 4), owned units whose `OnLostAsync` does not return or the fence-history
write (step 5), or an applier pass connection that does not close (step 6). The fence goes on; make the sink and the
units stop within `StopBudget`.

## A reconciler cannot get past an entry

A poison entry is one a reconciler cannot apply. Every replica stalls on it and fences. The way past it is an
operator's skip, through the skip endpoint or `IChangeSkip`:

```http
POST /reeve/skips
Content-Type: application/json

{ "aggregateType": "flag", "fromSeq": 1, "reason": "poison entry, fix in the next release" }
```

The open answers `201` with `{ "skipId" }`, or `409 skip_covers_acknowledged` with `ackedSeq` when `fromSeq` is at or
below the acknowledged seq; resend with `ackedSeq + 1`. Close it with `POST /reeve/skips/{id}/close` and a JSON body
(`{}` at least) once the fix runs on every replica; the close answers the repair entry's seq. On an authenticated request
the operator is the authenticated user and `openedBy` / `closedBy` in the body are ignored; an unauthenticated request
must name its operator in the body. The order of skips for a dependent type, and the cases a skip cannot help, are in
the [FAQ](../faq.md#the-configuration-log).

While a skip is open, the readiness answer carries it as a critical detail without changing the status, every write's
acknowledgement that reaches the skip's range names the skipped type (`Reeve-Skipped`), and each replica reports it in
`reeve_cluster_skip_open`.

## Writes are slow or unconfirmed

| Symptom | Cause | Fix |
|---|---|---|
| Configuration writes take as long as the synchronous commit, one at a time | Appends serialise on one fleet-wide lock held through the commit | Expected ([deployment.md](deployment.md#throughput-of-configuration-writes)); batch changes into one transaction |
| Writes wait up to `LeaseTtl` and then answer | A replica crashed, or a stop overran the grace period and never wrote `draining` | Size the termination grace period ([deployment.md](deployment.md#stopping-prestop-and-termination-grace)) |
| `reeve_cluster_write_unconfirmed_total` increasing | Acknowledgements time out at `AckTimeout`: a member stalled, or fenced while waiting | Look for stalls and fences above; clients read before resending |
| `reeve_cluster_commit_duration_seconds{transaction="fenced"}` high | The standby's commit latency, or for EF Core the wait for the change-log lock behind other appends | Check the standby; keep appending transactions short |
| Appends and activations stall fleet-wide | An application transaction that appends holds the change-log lock (a paused host, a long transaction) | Bound appending transactions with `transaction_timeout` on their role ([deployment.md](deployment.md#connection-requirements)) |

## Requests are refused

| Answer | Cause | Fix |
|---|---|---|
| `503 replica_not_serving` | The replica is starting, catching up or draining | Expected around starts and stops; a `preStop` sleep removes most of them on a stop |
| `503 replica_fenced` | The replica fenced | See [Replicas fence](#replicas-fence) |
| `503 replica_behind` | The read index was not reached within `ReadIndexTimeout`, or the read-index pool had no free connection | [FAQ](../faq.md#running-replicas); check the applier's lag and the pool ([deployment.md](deployment.md#the-read-index-and-its-connections)) |
| `503 write_refused` | A commit was refused because the replica fenced while the handler ran | Retry idempotent work only ([balancer-and-ingress.md](balancer-and-ingress.md#what-a-client-retries)) |
| Requests resent, executed twice, or cut at a fixed time | The balancer's retry or timeout configuration | [balancer-and-ingress.md](balancer-and-ingress.md) |

## Owned work

| Symptom | Cause | Fix |
|---|---|---|
| A unit runs nowhere after its owner crashed | Within the failover bound (41 s plus start time with the defaults) this is expected | Wait for the bound ([configuration.md](configuration.md#timing-budgets)); check that the kind is registered on the replica that holds the placer: the placer places only the kinds its own replica registers |
| `reeve_cluster_owned_release_overdue > 0` | An owner does not stop a unit within `ReleaseGrace` after a release request | Make `OnLostAsync` return promptly |
| A unit's rows are never swept | The area that owns the sweep is off on every replica | Turn the area on |

## Redis and Ask

| Symptom | Cause | Fix |
|---|---|---|
| The `ask` section shows `lost` or `not-subscribed`; `Ask`s to this replica end `NotTaken` | The relay's subscriptions do not hold (Redis down or unreachable); the relay retries and logs a warning | Restore Redis connectivity. Note that a resubscription refused by an ACL change still reads `holding` |
| Higher latency for applies and handovers, no errors | Redis is down: wake-ups and nudges are lost and the polls converge instead | Expected; Redis is never the authority |
| `reeve_cluster_redis_policy_uses_total` increasing | Redis was unavailable at a decision that grants, and the registered `OnRedisUnavailable` policy applied | Restore Redis; with `Grant`, the grants made meanwhile had no Redis exclusion |
| A replica started while Redis was down runs with an evicting policy | The eviction policy is read once, at start; claim, single-flight, budget and counter registrations only warn when Redis is down | Restart the replicas once Redis is back, or check the policy by hand |
| New owners of a key are refused after a restore | The Redis fence keys were not cleared | [deployment.md](deployment.md#restore-from-backup) |

## Status and monitoring

| Symptom | Cause | Fix |
|---|---|---|
| A section is missing for a tick, or its age keeps growing | Its source threw, timed out at `StatusReportInterval`, or the status write failed (a warning once per streak) | Make the source read its own memory; check the database |
| `503 status_unavailable` | The status read failed or took longer than `ActionTimeBudget` | Check the database and the read-index pool |
| The synchronous-replication warning while a new standby joins | `pg_basebackup` counts as a physical walsender until the standby is in `synchronous_standby_names` | Expected; it ends when the standby is configured |
| Old member rows stay listed | The library never deletes a member row | [FAQ](../faq.md#running-replicas) |
| `reeve_cluster_change_log_retained` grows without bound | A leased member's applied seq does not advance (a replica stuck loading), which holds retention | Find the member whose `appliedSeq` lags in the status read |
| `reeve_cluster_generation_pins{generation="superseded"}` stays above 0 | A long-lived connection or an undisposed scope keeps an old generation in memory | Expected for WebSockets and SSE; otherwise dispose the scope |
