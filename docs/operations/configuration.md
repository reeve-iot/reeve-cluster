# Configuration reference

Every setting of the library, its default, and the relations the start-up validator enforces. Give every replica the
same timing values ([Sizing](#sizing-the-timings)).

## Binding

`ClusterOptions` binds to the configuration section `Cluster` (`ClusterOptions.SectionName`). Durations are
`TimeSpan` strings (`hh:mm:ss`, or `d.hh:mm:ss`). The deployment usually supplies `ReplicaId` and `ConnectionString`
as environment variables (`Cluster__ReplicaId`, `Cluster__ConnectionString`) and the application's timings come from
`appsettings.json`:

```json
{
  "Cluster": {
    "ReconcileTimeout": "00:00:10",
    "ActionTimeBudget": "00:00:05",
    "ProxyReadTimeout": "00:01:00"
  }
}
```

```csharp
builder.Services.AddReeveCluster(
    ports,
    options => builder.Configuration.GetSection(ClusterOptions.SectionName).Bind(options));
```

A setting whose default derives from others reads the derived value until you set it. While `ReconcileTimeout` is
missing, everything derived from it (`ApplyStallLimit`, `AckTimeout`, the relation bounds) is computed with zero. The
validator then reports the missing setting and judges no relation that reads it.

## Clock assumptions

The database's clock judges the lease, and each replica's monotonic clock judges its local deadline. The library
assumes:

- the database clock does not step forward by more than `SafetyMargin / 2` within one `LeaseTtl` (NTP slews small
  corrections, and a VM pause of the database pauses every lease with it);
- a primary and its standbys agree within `SafetyMargin / 2`, since a failover changes the clock that judges leases;
- replicas' monotonic clocks drift by less than `SafetyMargin / 2` per `LeaseTtl`.

The three errors may add up to `1.5 × SafetyMargin`. A replica's local deadline is `SafetyMargin` before the lease
expiry it computed, and it stops serving (fence start) `StopBudget` before that deadline. `SafetyMargin` and
`StopBudget` together absorb the errors, which is why `StopBudget` must exceed `SafetyMargin / 2`. Realistic NTP keeps
all three far inside these bounds.

## ClusterOptions

| Setting | Type | Default | Effect |
|---|---|---|---|
| `ConnectionString` | `string` | required | The primary of the database that holds the `reeve_cluster` schema ([deployment.md](deployment.md#connection-requirements)). |
| `ReplicaId` | `string` | required | The member row's id, supplied by the deployment; unique per live process ([deployment.md](deployment.md#workload-and-replicaid)). |
| `LeaseTtl` | `TimeSpan` | 30 s | The lease each renewal grants. It is the writers' wait after a hard crash and sets the database outage the fleet rides out. |
| `SafetyMargin` | `TimeSpan` | 3 s | Subtracted from the lease to give the local deadline; covers the clock assumptions with `StopBudget`. |
| `StopBudget` | `TimeSpan` | 5 s | The time a replica allows itself to stop serving and stop owned work; fence start is this long before the local deadline. Fence steps not done within it are no longer awaited. |
| `RenewTimeout` | `TimeSpan` | 3 s | Bound of one renewal attempt. It also bounds the join, the drain's row writes, fence step 3 (the lease release) and the activation transaction, whose server-side bound is `RenewTimeout + 1 s` (4 s, below the default `FencedTransactionTimeout`). |
| `RenewInterval` | `TimeSpan` | 3 s | The renewal period. At most `(LeaseTtl − SafetyMargin − StopBudget − RenewTimeout) / 3`: 6.33 s with the other defaults. Also the retry period of a refused join, a failed activation and a floor-refused activation. |
| `ReconcileTimeout` | `TimeSpan?` | required when `ClusterPorts` names a reconciler; `AddReeveCluster` sets 10 s when it names none and the setting is unset | Bound of one reconciler attempt, a full load included. The number to size ([below](#sizing-the-timings)). |
| `ApplyStallLimit` | `TimeSpan` | `2 × ReconcileTimeout` | How long the applier may stall. Past `ReconcileTimeout + ApplyStallLimit` an `Activated`, `Serving` or `Draining` replica fences; in `Loading` or `CatchingUp` it keeps retrying. A reconciler attempt that has not returned by then ends the process. At least `2 × ReconcileTimeout`. |
| `AckTimeout` | `TimeSpan` | `max(LeaseTtl, ReconcileTimeout + ApplyStallLimit + RenewTimeout) + AckPollInterval + 2 s` | Bound of a write's acknowledgement wait; past it the write answers unconfirmed (`202`). Must exceed the bound without the 2 s, which cover the poll query's latency, the record transaction and the database clock's error on the expiry. |
| `AckPollInterval` | `TimeSpan` | 1 s | The acknowledgement wait's poll of the member table. |
| `ApplyPollInterval` | `TimeSpan` | `AckPollInterval` | The applier's poll of the log head, which starts a pass when a wake-up was lost. It enters no validated relation; a lost wake-up delays a pass by up to it, which adds to the read-index wait. |
| `RetentionInterval` | `TimeSpan` | 60 s | How often a member runs change-log retention, the free-row sweep, the primitives' retention and the command sweep. |
| `ReplicationCheckInterval` | `TimeSpan` | `LeaseTtl` | How often the synchronous-replication check runs after start-up. |
| `StatusReportInterval` | `TimeSpan` | `RenewInterval` | How often a member writes its status sections, and how often the skip monitor reads open skips; each write or read is bounded by it. |
| `ReadIndexTimeout` | `TimeSpan` | 10 s | Bound of the read-index wait before a request runs; past it the request is answered `503 replica_behind`. |
| `ProxyReadTimeout` | `TimeSpan?` | required with the ASP.NET Core package | The shortest time any balancer, proxy or client between a client and a replica waits for a response to start ([HTTP bound](#the-http-bound)). Also the drain's wait for running requests. |
| `ActionTimeBudget` | `TimeSpan?` | required with the ASP.NET Core package | The longest an endpoint's own work takes: the application's declaration. The library enforces it only on the status read (`IClusterStatus.ReadAsync` and the status endpoint), which throws `TimeoutException` past it. |
| `FencedTransactionTimeout` | `TimeSpan` | 5 s | Bound of a fenced transaction; a change of owner waits for open ones. Also the longest a `RedisFence` kind's start waits for its install raise. |
| `PlacementInterval` | `TimeSpan` | 2 s | The placer's pass, and an owner's poll of its own rows. |
| `ReleaseGrace` | `TimeSpan` | `2 × StopBudget + PlacementInterval` (12 s) | How long a release request may stay in force before it counts as overdue. Must equal its default; any other explicit value is refused. |
| `StabilisationWindow` | `TimeSpan` | 30 s | No rebalance within it after a membership change. Zero means no window. |
| `OwnedRowRetention` | `TimeSpan` | 24 h | A freed `owned` row is deleted after it, and so are a plain claim, a flight, a budget window and a slot row after their end. |
| `OnceRetention` | `TimeSpan?` | none | With a value, a once-record is deleted that long after its time to live ended, and its key can run again ("once" then holds for the call's `ttl` plus this). The record of an `AtLeastOnce` run that is not complete is kept whatever its age. With none, every once-run leaves a row for ever: set it when the keys are many. At most 3650 days. |
| `CounterRetention` | `TimeSpan?` | none | A counter idle this long is deleted (PostgreSQL, by the sweep) or expires (Redis, renewed by each increment), and starts again at zero. None deletes no counter. At most 3650 days. See [Counters on Redis](#counterretention-on-redis). |
| `CommandPendingTtl` | `TimeSpan` | 10 min | How old a `pending` durable command must be, from `created_at`, before the sweep may expire it. One value for the fleet. At least the failover bound of every kind with a command handler plus `PlacementInterval`; at most 3650 days. |
| `CommandRetention` | `TimeSpan` | 24 h | A `done`, `in_doubt` or `expired` durable command row is deleted this long after `completed_at`. At most 3650 days. |
| `HintLimit` | `int` | 1000 | Keys per aggregate type above which a pass runs the reconciler without hints (a full re-read of the type). |
| `RejoinBackoff` | `TimeSpan` | 60 s | The wait, counted from the fence, before a fenced replica rejoins when its epoch lasted less than `LeaseTtl`. It applies within one process: a new process joins at once, and the orchestrator's restart backoff spaces repeated restarts. Zero means no backoff. |
| `CheckSynchronousReplication` | `bool` | `true` | The advisory synchronous-replication check ([monitoring.md](monitoring.md#synchronous-replication-check)). Turning it off is logged at start-up and shown in the status registry; the guarantee is then the operator's to keep. |

Read-only members that state the relations, for code that computes a setting:

| Member | Value |
|---|---|
| `AckTimeoutRelationBound` | `max(LeaseTtl, ReconcileTimeout + ApplyStallLimit + RenewTimeout) + AckPollInterval`; `AckTimeout` must be strictly greater |
| `AckTimeoutDefaultSlack` | 2 s, which the `AckTimeout` default adds to the bound |
| `ReleaseGraceRelationValue` | `2 × StopBudget + PlacementInterval` |
| `RenewIntervalLimit` | `(LeaseTtl − SafetyMargin − StopBudget − RenewTimeout) / 3`, truncated to whole ticks: the largest valid `RenewInterval` |

### CounterRetention on Redis

On Redis a counter expires by the key's expiry, which every increment under the option renews. Unsetting the option
does not remove an expiry already set on a live counter, and an increment without the option (a replica configured
without it) does not renew it: the counter still expires, and starts at zero, when its last increment under the option
is older than the retention. To stop that, set the option to 3650 days or `PERSIST` the counter keys. A key with an
expiry can be evicted under a `volatile-*` policy, so use `noeviction` with this option.

## ClusterMigrationOptions

`AddReeveClusterMigrations(o => ...)` (the migration runner alone, for a migration-only run).

| Setting | Type | Default | Effect |
|---|---|---|---|
| `ConnectionString` | `string` | required | The primary of the database that holds `reeve_cluster`, with the same target rules as `ClusterOptions.ConnectionString`. `AddReeveCluster` registers the runner with the cluster's connection string. |

## ReeveClusterHttpOptions

`AddReeveClusterAspNetCore(o => ...)`. The timing settings of the ASP.NET Core package are `ClusterOptions`'s; adding the
package makes `ProxyReadTimeout` and `ActionTimeBudget` required and the HTTP bound validated.

| Setting | Type | Default | Effect |
|---|---|---|---|
| `HeaderPrefix` | `string` | `Reeve-` | The prefix of the response headers the package adds (`Reeve-Seq`, `Reeve-Skipped`, `Reeve-Replication`). ASCII letters, digits and hyphens, ending in a hyphen; anything else refuses the start. |
| `WriteError` | `Func<HttpContext, ClusterHttpError, Task>?` | none | Writes the body of every error answer of the package (the three admission `503`s, `503 write_refused`, `202 write_unconfirmed`, the status endpoint's `503 status_unavailable`, and the skip endpoint's `409`, `404`, `400` and `403 skip_endpoint_unprotected`) in the application's envelope. Status, `Retry-After` and the headers are already set when it runs; it sets the content type and writes the body. Without it the body is `application/problem+json` with an `errorCode` member. Hub invocations answer with the bare code in a `HubException` and do not reach it. |

`ClusterHttpError` carries `StatusCode`, `ErrorCode`, `Detail` (a sentence for a person) and `Extensions` (`ackedSeq` on
`409 skip_covers_acknowledged`, empty otherwise).

## ClusterRedisOptions

`AddReeveClusterRedis(o => ...)`. Nothing here changes what the library guarantees: Redis is never the authority.

| Setting | Type | Default | Effect |
|---|---|---|---|
| `KeyPrefix` | `string` | `reeve:` | The prefix of every key and channel the library uses in Redis. Replicas that are to hear each other share it; two clusters on one Redis differ in it. |
| `CheckEvictionPolicy` | `bool` | `true` | The start-up check that every primary's `maxmemory-policy` is `noeviction` ([monitoring.md](monitoring.md#redis-start-up-check)). Off, the start warns and names the primitives the check covers; the version is still checked, and the deployment answers for the policy. |

The Redis primitive registrations take a required policy for when Redis is unavailable at a decision that grants:

| Registration | Policy argument |
|---|---|
| `AddReeveClusterRedisClaims(OnRedisUnavailable)`, `AddReeveClusterRedisSingleFlight(...)`, `AddReeveClusterRedisBudget(...)`, `AddReeveClusterRedisSlots(...)` | `OnRedisUnavailable.Refuse` answers `Unknown`; `OnRedisUnavailable.Grant` grants without Redis, and each use is counted in `reeve_cluster_redis_policy_uses_total`. A value that is not one of the two (such as `default`) is refused (`RedisPolicyRequired`). Registering a primitive twice throws `InvalidOperationException`, whatever the policy. |
| `AddReeveClusterRedisCounter()` | none |

## Reeve.Cluster.EntityFrameworkCore

The EF Core package has no settings. `AddReeveClusterEntityFrameworkCore(mapping => ...)` takes the configuration
mapping (which entities are configuration, their aggregate type, key and counted columns), and
`UseReeveClusterCapture(services)` adds the capture interceptor to a context; both are application code
([Entity Framework Core](../guide/entity-framework-core.md)).

## UnitKindOptions

Declared per unit kind in `AddClusterUnit<T>(kind, o => ...)` or `AddClusterKeyedResource<T>(kind, o => ...)`. Two of
them enter the validated relations through the kind's failover bound.

| Setting | Default | Effect |
|---|---|---|
| `Policy` | `Placer` | Who takes an orphaned row over. A unit must keep `Placer`; a keyed resource sets `AnyEligible` or `None`. |
| `Convergent` | `false` | An idempotent, self-correcting unit that gets no token. It cannot declare `RedisFence`. |
| `Eligibility` | `EligibilityFilter.Any` | The capabilities a member needs to own a key of the kind. |
| `StartTime` | zero | The unit's start time; added to the kind's failover bound. |
| `RedisFence` | `false` | The kind keeps state in Redis behind `FenceRedis`; adds `FencedTransactionTimeout` to the failover bound and requires Redis 6.2.5 and `noeviction`. |

## Validation

A replica with an invalid combination or a missing required setting refuses to start. There is one message per
problem, naming the relation and the values of its terms. The checks run in this order. A failure of the positivity or
timer-limit checks skips the later groups, and a missing setting skips only the relations that read it:

1. Required settings. `ReplicaId`, `ConnectionString` (which must also name the primary), `ReconcileTimeout`, and,
   with the ASP.NET Core package, `ProxyReadTimeout` and `ActionTimeBudget`.
2. Positive values. Every duration and `HintLimit` must be positive. `StabilisationWindow` and `RejoinBackoff` may
   be zero ("no window", "no backoff"), not negative.
3. Upper limits. Every duration the library waits on or arms a timer with must not exceed `uint.MaxValue − 1` ms
   (49.17:02:47.294, about 49.7 days), the longest wait `Task.Delay` and the timers accept: `LeaseTtl`, `StopBudget`,
   `RenewTimeout`, `RenewInterval`, `ReconcileTimeout`, `ApplyStallLimit`, `AckTimeout`, `AckPollInterval`,
   `ApplyPollInterval`, `RetentionInterval`, `ReplicationCheckInterval`, `StatusReportInterval`, `ReadIndexTimeout`,
   `FencedTransactionTimeout`, `PlacementInterval`, `StabilisationWindow`, `RejoinBackoff`.
   `CommandPendingTtl`, `CommandRetention`, `OnceRetention` and `CounterRetention` are capped at 3650 days, because the
   sweeps subtract them from `clock_timestamp()` and a larger value makes that fail at every pass (SQLSTATE 22008).
   `SafetyMargin`, `ReleaseGrace`, `OwnedRowRetention`, `ProxyReadTimeout` and `ActionTimeBudget` are not capped.
   `ApplyStallLimit` and `AckTimeout` have defaults that are sums, so they are checked only once what they sum is in
   range.
4. Relations:

| Relation | Refused with |
|---|---|
| `RenewInterval ≤ (LeaseTtl − SafetyMargin − StopBudget − RenewTimeout) / 3` | `RenewInterval ... must be at most (LeaseTtl ... - SafetyMargin ... - StopBudget ... - RenewTimeout ...) / 3 = ...` |
| `StopBudget > SafetyMargin / 2` | `StopBudget ... must be more than SafetyMargin ... / 2` |
| `ApplyStallLimit ≥ 2 × ReconcileTimeout` | `ApplyStallLimit ... must be at least 2 x ReconcileTimeout ...` |
| `AckTimeout > max(LeaseTtl, ReconcileTimeout + ApplyStallLimit + RenewTimeout) + AckPollInterval` | `AckTimeout ... must be more than max(...) + AckPollInterval ... = ...` |
| `ReleaseGrace = 2 × StopBudget + PlacementInterval` | `ReleaseGrace ... must equal 2 x StopBudget ... + PlacementInterval ... = ...` |
| `CommandPendingTtl ≥` the largest failover bound of a kind with a durable command handler `+ PlacementInterval` (checked when a command handler is registered) | `CommandPendingTtl ... must be at least the failover bound of kind '...' (...) + PlacementInterval ... = ...; set it to ... or more.` |
| `ReadIndexTimeout + ActionTimeBudget + AckTimeout < ProxyReadTimeout` (with the ASP.NET Core package) | `ProxyReadTimeout ... must exceed ReadIndexTimeout ... + ActionTimeBudget ... + AckTimeout ... = ...` |

`CommandPendingTtl` must outlive a failover and the new owner's first poll, or a pending command may expire while its
new owner is on the way. A handler registered for a kind that no unit registers is checked with the default
`UnitKindOptions`.

Other start-up refusals (an area that is off, a status section name, the connection probe, the Redis check, the
migration runner) are in [troubleshooting.md](troubleshooting.md). The validator compares each replica's own options
only, not other replicas'.

## Timing budgets

Each end-to-end bound is a sum of per-step bounds. With the defaults and `ReconcileTimeout` 10 s:

| Bound | Sum | Default value | Kept by |
|---|---|---|---|
| Fence start before the database's lease expiry | the three clock errors, each below `SafetyMargin / 2`, are absorbed by `SafetyMargin + StopBudget` | | `StopBudget > SafetyMargin / 2` with the clock assumptions |
| Database outage the fleet rides out | the last renewal that can finish before fence start begins at the largest multiple of `RenewInterval` not later than `LeaseTtl − SafetyMargin − StopBudget − RenewTimeout` after the last successful one | at least 15 s, up to 22 s depending on where the outage falls in the renewal schedule | the `RenewInterval` relation |
| Failover of a placed unit after its owner crashes | `LeaseTtl + 3 × PlacementInterval + FencedTransactionTimeout + StartTime`, plus `FencedTransactionTimeout` for a `RedisFence` kind ([below](#failover-of-a-placed-unit)) | 41 s plus start time, 46 s with `RedisFence` | a stated bound: `CommandPendingTtl` is checked against it, and `Ask` and durable command timeouts are capped by it |
| Acknowledgement after a replica crashes | `LeaseTtl + AckPollInterval`; not counted: the poll query's latency, the record transaction, the database clock's error | 31 s | the `AckTimeout` relation; a larger error costs a `202`, never safety |
| Acknowledgement after a replica stalls | `ReconcileTimeout + ApplyStallLimit` (the stalled replica fences) + `RenewTimeout` (its lease release) + `AckPollInterval`; when the database does not answer the release, the lease runs out instead | 34 s | the `AckTimeout` relation |
| A write's response starts before the proxy gives up | `ReadIndexTimeout + ActionTimeBudget + AckTimeout < ProxyReadTimeout` | `AckTimeout` 36 s: behind a 60 s proxy, `ActionTimeBudget` must stay below 14 s | the HTTP bound |
| Termination grace period | `> preStop + StopBudget + ProxyReadTimeout + 2 × RenewTimeout` | 76 s with a 5 s `preStop` and a 60 s `ProxyReadTimeout` | the deployment ([deployment.md](deployment.md#stopping-prestop-and-termination-grace)) |

Draining a replica that holds `AnyEligible` keyed resources can take up to
`StopBudget + ProxyReadTimeout + PlacementInterval + StartTime` before another member runs them, above the crash bound.
These bounds are sums of settings, not measurements.

### Failover of a placed unit

The failover bound adds up these steps:

| Step | Bound |
|---|---|
| The dead owner's lease runs out | `LeaseTtl` |
| The placer's pass | `PlacementInterval` |
| The new owner's poll of its rows (a Redis nudge shortens it) | `PlacementInterval` |
| The adopting member's poll, when the dead replica also held the placer | `PlacementInterval` |
| The change of owner waits for open fenced transactions | `FencedTransactionTimeout` |
| The unit starts | `StartTime` |
| The install raise of a `RedisFence` kind | `FencedTransactionTimeout` |

When the dead replica did not hold the placer, the adopting member's poll drops out and the bound is about 39 s. When
the owner fenced with the database reachable, the sum starts from the fence instead of `LeaseTtl`.

### The HTTP bound

`ProxyReadTimeout` is the smallest read or idle timeout over every hop between a client and a replica. A write waits
for its read index, runs, and waits for its acknowledgement, all before its response starts. The library therefore
refuses to start unless `ReadIndexTimeout + ActionTimeBudget + AckTimeout < ProxyReadTimeout`. If an edge's timeout is
below that sum, raise it. Declaring a larger value than the real one lets a committed write be cut off with no `202`.

An application request timeout (`AddRequestTimeouts`) must exceed the same sum, because the acknowledgement wait runs
on `RequestAborted`. Typical hop timeouts are in
[balancer-and-ingress.md](balancer-and-ingress.md#proxyreadtimeout).

## Sizing the timings

- **`ReconcileTimeout`** bounds every reconciler attempt, a full load included. Set it to at least 2 to 3 times the
  slowest replica's full load of the largest type, plus a pause of the process. A replica whose full load of a type
  exceeds it never leaves `Loading`: each failed attempt is logged at Error with its elapsed time, retried with
  backoff, and readiness reports `loading.failed` until a load publishes. Measure it with
  `reeve_cluster_applier_full_load_duration_seconds`. Raising it raises the defaults of `ApplyStallLimit` and
  `AckTimeout`, and so the HTTP bound.
- **`LeaseTtl`** trades the database outage the fleet rides out against the writers' wait after a hard crash (`SIGKILL`,
  OOM, node loss; a graceful stop costs nothing). Choose it against the database's restart time and the outages of its
  reachability. A switchover or failover fences the fleet whatever `LeaseTtl` is, since it changes the timeline. A
  restart in place keeps the timeline, and the tolerance decides it.
- **Same values everywhere.** Size for the slowest node and the largest full load, and give every replica the same
  numbers. One replica's `AckTimeout` covers the budgets of the others (a crashed member's `LeaseTtl`, a stalled
  member's `ReconcileTimeout` and `ApplyStallLimit`), and one replica's activation bound sets how long it may hold the
  change-log lock that the others' fenced transactions wait for. There is no per-replica option.
- **Slow hardware** (a Raspberry Pi, an Intel N100) runs on the defaults. A stuck-attempt decision that runs late
  because the process was paused waits once for `ApplyPollInterval` before it decides, while the stall clock does not,
  so a pause of more than a few seconds can fence an `Activated`, `Serving` or `Draining` replica.
- **A failing activation** (a missing row, a privilege, a statement's bound) is logged at Error with the exception and
  retried every `RenewInterval`. Readiness reports `activation.failing` meanwhile.

A worked example: `ReconcileTimeout` 10 s and the other defaults give `ApplyStallLimit` 20 s and `AckTimeout`
`max(30, 10 + 20 + 3) + 1 + 2 = 36 s`. Behind a proxy with a 60 s read timeout and the default `ReadIndexTimeout` of
10 s, `ActionTimeBudget` must stay below 14 s, and the grace period must exceed 71 s plus the `preStop` sleep.
