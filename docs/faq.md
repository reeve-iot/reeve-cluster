# FAQ

Known problems, grouped by where they show up, followed by common questions. Settings are described in
[operations/configuration.md](operations/configuration.md). For a step-by-step guide to an incident, see
[operations/troubleshooting.md](operations/troubleshooting.md).

## Start-up

### "The connection reached a standby (pg_is_in_recovery() is true)"

The library reads and writes only on the primary, and the probe that runs on every new physical connection found
a standby. Point the connection string at the primary. With several hosts, set `Target Session Attributes=primary` (or
`read-write`).

### "Target Session Attributes=... may reach a standby" or "is only supported with several hosts"

With several hosts, the connection string needs `Target Session Attributes=primary` or `read-write`. Anything else, or
no attribute, would let a standby answer. With one host, set no attribute at all: the library refuses every value and
checks for the primary on every connection itself.

### "transaction_timeout is ... on a connection opened by the library" or "statement_timeout is ..."

`transaction_timeout` or `statement_timeout` is non-zero for the role, the database or the session. A non-zero
`transaction_timeout` is armed at `BEGIN` and the library's `SET LOCAL` cannot re-arm it. A non-zero `statement_timeout`
can cancel `COMMIT`'s wait for the synchronous standby while the commit stays on the primary. On the library's own data
source this fails the start. On a connection the application passes to `BeginFencedTransactionAsync` or the EF helper, it
refuses that transaction.

Set both to 0 for the library's role and for the role of every connection that runs fenced, claim, slot and EF fenced
transactions (`ALTER ROLE ... SET transaction_timeout = 0`, the same for `statement_timeout`, and check the database
level too). Bound the application's other transactions with a different role.

### "The server has no transaction_timeout setting; the library needs PostgreSQL 18 or later"

`transaction_timeout` was introduced in PostgreSQL 18. Run PostgreSQL 18 or later, the only version the library is
tested on.

### "Redis ... has maxmemory-policy ..., it must be noeviction" or "policy could not be read"

A `FenceRedis` kind or a Redis claim, single-flight, budget, counter or slot is registered, and a connected primary has
another policy (or `INFO memory` could not be read). A `volatile-*` policy can evict keys that have a TTL, so a claim
ends early and a second caller is granted. An `allkeys-*` policy can evict keys that have none (a high-water key, a
slot set).

Set `maxmemory-policy noeviction` on every Redis primary. `ClusterRedisOptions.CheckEvictionPolicy = false` turns the
check off, but evictions then silently end claims early or lose slots. If Redis is down at start, claims,
single-flight, budgets and counters only warn (and the policy is never checked for that process), while `FenceRedis`
and Redis slots refuse to start. See [Redis](guide/redis.md).

### "Redis ... runs ...; RedisFence needs 6.2.5 or later"

`FenceRedis` and Redis claims, single-flight and slots rely on `EXEC` aborting when a watched key's TTL has passed,
which needs Redis 6.2.5. Upgrade every primary the application connects to.

### "A SignalR hub is mapped with ClusterHubFilter, but IFenceSink is NoFenceSink"

Hub connections stay open past a request, so a fenced replica must be able to abort them, and the start-up check found
no fence sink. Register an `IFenceSink` that aborts the application's hub connections. The check does not discover raw
WebSocket and SSE endpoints, so their sinks are the application's responsibility. See
[Aspnetcore](guide/aspnetcore.md).

### Start-up is refused by the options validator

A required setting is missing (`ReconcileTimeout`, and `ProxyReadTimeout` and `ActionTimeBudget` when the ASP.NET Core
package is used), or a timing relation does not hold. For example, `ProxyReadTimeout` must exceed
`ReadIndexTimeout + ActionTimeBudget + AckTimeout`. The message names the setting and the relation. The relations are
listed in [operations/configuration.md](operations/configuration.md).

### Pods are restarted before they ever become ready

No endpoint answers until the replica has joined and finished its first full load. The join waits up to `LeaseTtl` if
the replica id is still held, then the full load runs, and only then does Kestrel start. A startup probe or host
`StartupTimeout` shorter than that ends the start. Give the startup probe at least `LeaseTtl` plus the expected first
load, and set any `StartupTimeout` at least as long. See [operations/deployment.md](operations/deployment.md).

### A single-replica Deployment never finishes its rollout

With a fixed `ReplicaId` and a surge, the new pod's join is refused (`joining.refused`) while the old pod holds the
lease, so the rollout waits for a pod that cannot join. Use `strategy: Recreate` or `maxSurge: 0`, or take the replica
id from the pod name.

## Running replicas

### Requests are answered `503 replica_behind`

The replica had not applied up to the log head within `ReadIndexTimeout`, or the read index got no connection from its
pool in time. This is routine just after a connect or while the applier lags. The request did not run and is safe to
retry with any method (`Retry-After: 1`). A persisting stretch logs one warning at its start.

If it persists, look at the applier lag (`reeve_cluster_change_log_head - reeve_cluster_generation_head`) and the
reconciler attempt durations. Mark endpoints that read no configuration with `SkipReadIndex()`. See
[operations/monitoring.md](operations/monitoring.md).

### `503 write_refused` versus `202 write_unconfirmed`

`write_refused` (503) means the replica fenced while the handler ran and fence step 2 refused its commit. No
configuration write took effect, but the handler's other effects did happen. Retry only idempotent operations.

`write_unconfirmed` (202) means the write may be committed. It was appended but not acknowledged by every member within
`AckTimeout`, or the replica fenced while waiting. Read before resending.

`replica_fenced` and `replica_not_serving` mean nothing ran. See [Aspnetcore](guide/aspnetcore.md).

### Configuration writes take seconds, or answer `202` after a replica crashed

A configuration write waits for every leased active replica. A crashed replica stays in the wait set until its lease
expires (up to `LeaseTtl` plus `AckPollInterval`), and a stalled one until it fences after `ApplyStallLimit`. A batch
that dirties many types waits for the sum of its reconciles. Past `AckTimeout`, an HTTP write answers
`202 write_unconfirmed`.

This is expected. A write whose actors do not need the guarantee can opt out per call with
`IChangeAck.AcknowledgeAtCommit()`.

### Every replica fenced at once after a database switchover or failover

A promotion starts a new WAL timeline, and a replica acts only on the timeline it joined on. The whole fleet
fences once and rejoins with a full load within seconds of the database answering. `reeve_cluster_fences_total` counts
it with `trigger="timeline"` (or `fence_start` or `zero_row_renewal` if the renewal did not reach the new primary
first). Nothing needs fixing: this is the fail-closed answer to commits a failover may have removed.

### Every replica fenced during a database outage

An outage longer than the lease tolerance (at least 15 s, up to 22 s with the defaults) leaves every replica unable to
renew before its fence start. Shorter outages make writes fail and fence nothing. Each replica rejoins within seconds
of the database returning (after `RejoinBackoff` only if its epoch's span was short). See
[Membership](guide/membership.md).

### A replica exits with a critical "was given epoch ..., at or below epoch ... that this process held"

The epoch sequence went backwards: the database was restored while the replica was running, or a failover lost the
join. Let the orchestrator restart the process, which then joins afresh. After a restore, expect every running replica
to exit once. A restore is meant to be done offline.

### A pod stays not ready with `activation.version-floor` or `activation.protocol-floor`

A feature raised the version or protocol floor and this binary is below it. The replica stays `joining`, retries every
`RenewInterval` and counts `reeve_cluster_activation_refusals_total`. The liveness probe does not restart it.

Roll forward. Or, after undoing what the raise authorised, lower the floor with `IClusterFloorCommand`. A feature that
asks `Require...` on every call raises the floor again, so lower it while no member that asks holds a lease. See
[operations/upgrading.md](operations/upgrading.md).

### Two applications on one database interfere with each other

The schema name `reeve_cluster` and the advisory-lock keys are fixed, so two applications on one database form one
cluster, with one change log, one acknowledgement and one set of floors. Give each application its own database, and on
Redis its own `KeyPrefix`.

### Appends and activations stall cluster-wide while one application host is paused

An application transaction that appends outside a fenced transaction (through the capture or `IChangeLog.AppendAsync`)
holds the change-log lock until it ends, and the library does not bound it. Set `transaction_timeout` on the database
role those transactions use. It cannot be the role of the library's own data source or of the connections that run
fenced transactions, which need 0.

### Old member rows stay in the status read

The library never deletes a member row. With a Deployment, every new pod name adds a row that stays, with an expired
lease or `draining`. This is harmless, because retention and the acknowledgement look only at members with a valid
lease. A StatefulSet reuses its ids.

### A graceful stop still makes writes wait for the lease

The stop overran `terminationGracePeriodSeconds` and never wrote `draining`, so the lease expires as after a crash.
Make the grace period longer than `preStop + StopBudget + ProxyReadTimeout + 2 × RenewTimeout`. See
[operations/deployment.md](operations/deployment.md).

## The configuration log

### The replica stays in `Loading` and logs "The full load of epoch ... failed after ..."

`ReconcileTimeout` bounds every reconciler attempt, including the full load of a type. A type whose full load takes
longer can never publish and retries for ever. Each failed attempt logs at Error with the elapsed time against
`ReconcileTimeout` and raises the readiness reason `loading.failed`.

Raise `ReconcileTimeout` (and the settings derived from it) to two to three times the slowest replica's full load of
the largest type, or make that reconciler's full load faster. `reeve_cluster_applier_full_load_duration_seconds` shows
the load times.

### Every replica fenced on one entry (a poison entry)

A reconciler fails on an entry on every replica. The acknowledgement waits for all of them, so each fences after
`ApplyStallLimit`.

Open a skip of the failing type through the skip endpoint (`MapReeveClusterSkips`) or `IChangeSkip`, starting at
`acked_seq + 1`. Send `fromSeq` 1, and if the answer is `409 skip_covers_acknowledged`, resend with the `ackedSeq` it
carries plus one. For a failing dependent type, skip it and every type it depends on, dependencies first. Close the
skips only after the fix runs on every replica. The close appends a repair entry. See
[Configuration log](guide/configuration-log.md).

### A skip opened from `acked_seq + 1` does not get the fleet past the poison entry

The poison entry is at or below `acked_seq`. An acknowledgement completed while every member it counted had passed it,
so the fleet already applied it and a skip may not cover it. Ship a release that applies the entry, or roll back to the
version that did.

### A process started after the poison entry stays in `Loading` although a skip is open

A process with no generation to keep across its first load cannot use the skip's kept state, so it cannot load the
skipped type. A process stuck in its first load reports no skip, but the serving replicas do. Ship a release that
applies the entry. Until then, the serving replicas carry the load.

### The skip endpoint answers `403 skip_endpoint_unprotected`

`MapReeveClusterSkips` adds no authorization, and the endpoint carries neither authorization metadata nor a fallback
policy that runs. Chain `RequireAuthorization()` (or an explicit `AllowAnonymous()`, if that is intended) on the builder
it returns. Keep the sign-in route exempt from the fence so an operator can still reach it on a fenced fleet.

### No replica is ready, so the skip endpoint cannot be reached through the ingress

When no replica is ready, the ingress controller answers its own `503` (without an `errorCode`) and routes nothing.
Reach a pod directly with `kubectl port-forward pod/<pod>`. You can also use the management command (`IChangeSkip` from
a container whose host is never started), which reaches the skip at any time.

### A save throws "A save that captures configuration changes must run without a transaction, in one its context began through EF ..."

The capture interceptor sees commits only of transactions a capturing context began through EF
(`Database.BeginTransaction`), or that another capturing context joined with `UseTransaction` in the same scope. A raw,
foreign, ended or ambient (`System.Transactions`) transaction would commit the changes without their change-log
entries. Nothing was written. Capture in the context that began the transaction, or append with `IChangeLog.AppendAsync`.
See [Entity framework core](guide/entity-framework-core.md).

### "Bulk operation on mapped entity type ... is not captured"

An `ExecuteUpdate` or `ExecuteDelete` on a mapped type writes a counted or key column, or deletes, which the change log
does not see. Append the changed aggregates with `IChangeLog.AppendAsync` in the same transaction and mark the query
with `.AppendedToChangeLog()`.

### `reeve_cluster_capture_discards_total` grows

Transactions were ended around EF Core (committed through their `DbTransaction` or a SQL `COMMIT`), so the interceptor
saw no commit and discarded their captured entries. Those changes committed without entries. Commit and roll back
through EF in the context that began the transaction.

## Owned work, commands and Ask

### A keyed resource runs on a member its kind's filter excludes

The owner keeps the key until it drains, fences, stops or crashes, or the resource is ended. Its poll logs one warning
per tenure. A free keyed row is the resource's tombstone, so the library does not release it while the owner lives.
Drain or restart the owner, or end the resource. Under `AnyEligible`, a drain leaves the row for an eligible member.

### `WaitAsync` returns `Unfinished` for a command on every call

The command was taken by a tenure that ended together with its key, and the key never got an owner again. The expire
sweep touches only `pending` rows and retention only finished ones, so the row stays `taken`. Create the key again.
The command then re-runs (`ReRun`) or is recorded `in_doubt` (`Complete`).

### Replicas crash one after another on the same durable command

Under `ReRun`, a handler that kills its process (a stack overflow, `Environment.FailFast`, memory exhaustion) is
re-taken and run by every next owner, and nothing counts attempts. Fix the handler, or use `Complete`, which stops
after one attempt. See [Owned work](guide/owned-work.md).

### A command handler runs while its successor's run starts

The release of a key waits for `OnLostAsync` up to `StopBudget`, not for the handler. After a handover, a rebalance or
a pause across the lease, the next owner's `ReRun` can start while the old run finishes. Only the effects overlap, and
the old completion is refused. Write `ReRun` handlers to be idempotent, including their effects outside the
database.

### Ask reported `NotTaken` but the handler ran

The Ask relay's record lives in Redis. After a Redis failover with replication lag, or a restart from a snapshot, a loss
the caller's connection does not see can report `NotTaken` for a request whose handler ran. A feature whose retry is
not idempotent must not rely on `NotTaken` across a Redis failover.

### A capability change threw, but placement changed anyway

`SetAsync` changes the replica's own view only after the member-row write returned. A cancel or timeout while the
commit waits for the standby can leave the row committed and the call failed. Retry `SetAsync` until it succeeds. A
rejoin writes the replica's own view back to the row.

## Redis

### "The restore clearing needs every Redis server connected and a primary among them; nothing was deleted"

`ClusterRedisRestore.ClearOwnedKeysAsync` found a listed server not connected, or no primary. Run it again when every
server answers. It must run while every replica is stopped.

### New owners of a key are refused after a restore

The `FenceRedis` high-water key was not cleared, and its stored token outranks the tokens drawn after the restore. Stop
every replica and clear the library's Redis keys (every key matching `{<prefix>owned:*`) before starting them.

## Balancers and ingress

The full configurations are in [operations/balancer-and-ingress.md](operations/balancer-and-ingress.md).

### `POST` requests are executed twice

The balancer resends non-idempotent methods. With Cilium's Gateway API, any `retry` stanza on the HTTPRoute resends
every method, `POST` included, on a `503`, a reset or an invalid response, possibly to the same replica. A client can
also turn retries on with `x-envoy-retry-on` and `x-envoy-max-retries` headers. With nginx, `non_idempotent` in
`proxy_next_upstream` does the same.

On Cilium, put no `retry` stanza on routes to the replicas, and add a `RequestHeaderModifier` that removes
`x-envoy-retry-on`, `x-envoy-retry-grpc-on` and `x-envoy-max-retries`. Re-check after a Cilium upgrade. On nginx, never
list `non_idempotent`.

## Building

### Every build reports version 1.0.0 to the version gate

The gate compares the entry assembly's informational version. Without `<Version>`, the SDK stamps 1.0.0 (with no
attribute at all, the library uses 0.0.0). An informational version that is not SemVer 2.0 refuses the start
(`VersionNotSemVer`). Stamp a SemVer 2.0 version at build.

## Questions

### How long until a crashed owner's unit runs elsewhere?

Within the kind's failover bound: the dead owner's lease, the placer's pass, the new owner's poll, the fenced
transactions the change of owner waits for and the start time, plus one `PlacementInterval` in case the dead replica
held the placer. That is about 41 s plus the start time with the defaults. See
[Owned work](guide/owned-work.md).

### Can I run without synchronous replication?

Yes, and the library keeps serving, but a failover may then lose a recent acknowledged commit. The check warns (it is
never a readiness reason) and can be turned off with `Cluster:CheckSynchronousReplication`.

### How do I restore the database from a backup?

Offline. Stop every replica, restore, clear the library's Redis keys with `ClusterRedisRestore.ClearOwnedKeysAsync` if
you use the Redis package, then start the replicas. See [operations/deployment.md](operations/deployment.md).

### Can I roll back to an older release?

By one release, yes: tables and messages keep one release of overlap. Once a floor has been raised, the older binary
stays not ready until the floor is lowered. See [operations/upgrading.md](operations/upgrading.md).
