# Redis

`Reeve.Cluster.Redis` is optional. It lowers latency and adds a few features, but never decides what PostgreSQL decides.
Without Redis, or with it down, replicas converge by polling.

## What it adds

- **Wake-ups.** Other replicas learn of a change at once instead of at their next poll.
- **Ask relay.** It delivers `IClusterAsk` requests between processes. Without it Ask reaches only this process.
- **`FenceRedis`.** A unit keeps state in Redis and writes it only while it owns its key.
- **Redis primitives.** Claims, single-flight, budgets, counters and slots on Redis, for hot paths.

## Requirements

- Redis 6.2.5 or later for `FenceRedis`, claims, single-flight and slots. Budgets, counters, wake-ups and the relay set
  no minimum.
- `maxmemory-policy noeviction` on every primary when you use `FenceRedis` or any Redis primitive. An evicted claim key
  ends the claim early, an evicted budget key restarts its window, and an evicted fence or slot key loses its state.
  Some managed Redis services default to a `volatile-*` policy, so check yours.
- One Redis deployment (standalone, Sentinel or Cluster).
- Your own `IConnectionMultiplexer`. The package opens no connection except the short-lived one of the start-up check,
  whose user needs `INFO`.
- A key prefix shared by the replicas that should hear each other. Two clusters on one Redis need different prefixes.
  The keys a Redis ACL must allow are in [deployment](../operations/deployment.md#redis).

## Registration

```csharp
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));

builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = "reeve:");           // wake-ups, Ask relay, FenceRedis
builder.Services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);       // IRedisClaims
builder.Services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse); // IRedisSingleFlight
builder.Services.AddReeveClusterRedisBudget(OnRedisUnavailable.Grant);        // IRedisBudget
builder.Services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);        // IRedisSlots
builder.Services.AddReeveClusterRedisCounter();                               // IRedisCounter, no policy
```

Register only what you use. The primitives need the `Primitives` area of `AddReeveCluster` and the relay the `Ask` area
([registration](../reference/registration.md)). The Redis interfaces sit beside the PostgreSQL ones and return the same
result types.

`ClusterRedisOptions` has two settings. `KeyPrefix` (default `reeve:`) prefixes every key and channel. With
`CheckEvictionPolicy` (default `true`) off, the start only warns about the eviction policy and the deployment answers
for it.

## The start-up check

A check runs before the replica joins when a kind declares `RedisFence` or you register a Redis claim, single-flight,
budget, counter or slot.

The start is refused for a connected primary older than 6.2.5 (`RedisTooOld`), and for one whose `maxmemory-policy` is
not `noeviction` or cannot be read (`ClusterConfigurationException`). If Redis is down at start, a host with
`RedisFence` or slots is refused, and a host with only claims, single-flight, budgets or counters warns and starts.

The policy is read once, at start. A process that started while Redis was down never checks it, and a later change goes
unnoticed ([monitoring](../operations/monitoring.md#redis-start-up-check)).

## When Redis is down

| Feature | Behaviour |
|---|---|
| Wake-ups | Lost. Polls converge with more latency |
| Ask relay | `NotTaken` or `TakenUnanswered` |
| `FenceRedis` | Refuses until a raise succeeds |
| Redis primitives | The `OnRedisUnavailable` policy decides each grant |
| Membership, change log, PostgreSQL primitives, durable commands | Unaffected |

A Redis primitive never falls back to PostgreSQL. A Redis that loses acknowledged writes (a failover with replication
lag, a restore from an old snapshot) can cost correctness, as described below.

## Redis primitives

Use a Redis primitive for its rate, not for its guarantee. Redis decides once while it answers and the key survives, and
a failover that loses a key can grant twice. If correctness depends on it, use the PostgreSQL primitive
([primitives](primitives.md)).

Claims, single-flight, budgets and slots take a required `OnRedisUnavailable` policy, and any other value is refused
(`RedisPolicyRequired`). It decides a grant when Redis does not answer, or after three lost grants that each found no
claim on the read that followed:

| Primitive | `Refuse` answers | `Grant` answers |
|---|---|---|
| Claim | `Unknown` | `Won` with a token no key holds, so its own effects are refused |
| Single-flight | `Unknown`, nothing computed | Computes without a claim and answers `ComputedUnknown`, never `Computed` |
| Budget | `Unknown` | `Granted(0)`: nothing was counted |
| Slot | `Unknown` | `Acquired` with a token no member holds. Renewals answer `Ended`, effects `NotServing` |

`Grant` is a fail-open you choose and admits callers while another may hold the key. Each use is counted in
`reeve_cluster.redis.policy_uses` (`reeve_cluster_redis_policy_uses_total` in Prometheus). Releases, effects, result
writes, renewals and increments are never decided by the policy: an unanswered one is unknown, and an unanswered release
throws `CommitOutcomeUnknownException`.

- **Claims and single-flight.** A claim is valid for its token and its time to live. Redis cannot see the holder's
  lease, so a holder that paused after its serving check can still write until the claim's time to live passes or a
  successor takes it. An effect that must stop at the holder's lease uses a PostgreSQL claim. A Redis claim fences Redis
  effects through `IRedisClaims.ExecuteAsync(key, token, tx => ...)`, which answers `Committed`, `Refused` (the claim
  is gone or the key changed), `NotServing` or `Unknown`.
- **Budgets.** Windows run on Redis's own clock. An outage can leave one id over the limit until the window ends.
- **Counters.** An increment grants nothing, so there is no policy, and an unanswered one is `Unknown`. With
  `CounterRetention`, every increment renews the key's expiry, and unsetting the option does not remove an expiry
  already set.
- **Slots.** The holder renews as with PostgreSQL slots, and `IRedisSlots.ExecuteAsync` runs your Redis commands only
  while the slot is valid. A fenced member's slot ends at its time to live, not at the fence. A key or id containing `{`
  or `}` is refused. A failover that loses writes can lose a grant, so more than `limit` holders may hold.
- Once-runs have no Redis variant. Redis keys expire by their own time to live.

Under Redis Cluster the keys an effect writes must be in the same slot as the primitive's own key. Build them on the
hash tag from `RedisPrimitiveKeys`, with the `KeyPrefix` the package was registered with: `ClaimHashTag(prefix, key)`
for `IRedisClaims.ExecuteAsync` and `SlotHashTag(prefix, key)` for `IRedisSlots.ExecuteAsync`.

```csharp
var tag = RedisPrimitiveKeys.ClaimHashTag("reeve:", "export:42");
var result = await claims.ExecuteAsync("export:42", token, tx => tx.StringSetAsync($"{tag}:result", payload), ct);
```

## FenceRedis

A unit that keeps state in Redis (a stream consumer's position, say) declares `RedisFence` and writes that state through
the context, so a former owner cannot overwrite a newer one:

```csharp
services.AddClusterUnit<StreamConsumer>("stream-consumer", o => o.RedisFence = true);

// in the unit, with the OwnedContext the library passed to OnAcquiredAsync
var ok = await context.FenceRedisAsync(tx => tx.StringSetAsync($"{context.RedisHashTag()}:position", position), ct);
```

Under Redis Cluster every key the transaction writes must carry `context.RedisHashTag()`. The delegate may run more
than once, since it only queues commands.

| Result | Meaning |
|---|---|
| `true` | The transaction committed |
| `false` | A condition of yours failed, or the keys you watch kept changing |
| `RedisFenceRefusedException` | The fence is not raised for this tenure yet, or no longer holds its token. Nothing was written |
| `OwnershipLostException` | The unit is stopping or the replica no longer serves |
| `RedisTimeoutException`, `RedisConnectionException` | Unknown outcome. The transaction may have committed |

The fence guarantees order: once a successor's token is installed, nothing of a previous owner is accepted. It does not
end a tenure at the lease. A write that passed `EnsureOwner()` and was delayed on the wire still lands within `LeaseTtl`
of its owner's last refresh, and before any successor installed. A Redis effect that must not outlive the lease goes
through PostgreSQL: a fenced row that the Redis write then follows idempotently.

If Redis has not answered at the start of a tenure, the unit starts anyway and the fence refuses until a raise
succeeds, so PostgreSQL work is not held. A fence key lost in an outage is installed again at the next refresh. A
failover that loses acknowledged writes can lose a fence key together with its high-water key, and a delayed raise of an
older tenure then installs again. That is where a Redis loss costs correctness rather than latency.

### After a database restore

A restored database draws tenure tokens again from the backup's point, so clear the old fence keys while every replica
is stopped:

```csharp
var deleted = await ClusterRedisRestore.ClearOwnedKeysAsync(multiplexer, keyPrefix: "reeve:");
```

It deletes only the `{<prefix>owned:*` keys on every primary, and deletes nothing and throws when a listed server is
not connected. Run it again once every server answers ([restore procedure](../operations/deployment.md#restore-from-backup)).

## Ask relay

With the package, `IClusterAsk` reaches other processes. The caller writes a request record to Redis and publishes it,
the target writes a receipt and runs the handler, and the caller waits for the answer. The relay starts before the
replica joins and keeps its subscriptions across fences and rejoins.

If the record could not be written, or the request was withdrawn before
a target took it with no Redis connection event meanwhile, the answer is `NotTaken`, which is safe to retry. If a
connection failed or came back during the call, a target took the request and did not answer, or the record is gone
(a restart or expiry), the answer is `TakenUnanswered`, because a receipt may have existed. The caller returns within
its timeout however slow Redis is.

If the subscriptions do not hold (Redis down at start), the relay retries every 2 s, this process takes no request and
callers end `NotTaken`. The host still starts. The `ask` status section reports `holding`, `not-subscribed` or `lost`,
but a resubscription refused after an ACL change at run time still reads `holding`.

A Redis that loses an acknowledged write can bring a request record back as pending after a receipt, so `NotTaken` can
be reported for a request whose handler ran. Do not rely on `NotTaken` across a Redis failover for work that is not
idempotent.

## Wake-ups

Wake-ups only shorten a poll, so the cluster does not depend on their delivery. A bounded queue and a background loop
send them, so a slow or cut Redis never delays a commit or an acknowledgement.

## Troubleshooting

See [Redis and Ask](../operations/troubleshooting.md#redis-and-ask) and the [FAQ](../faq.md).
