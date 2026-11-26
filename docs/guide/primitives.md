# Coordination primitives

The primitives let replicas decide something once for the whole cluster: who runs a job, whether a request is a
duplicate, how many callers may use a resource, and how to reach the replica that owns a key. They are registered by
`AddReeveCluster`, use PostgreSQL, and keep working when Redis is down.

| Primitive | Interface | Use it to |
|---|---|---|
| Claim | `IClusterClaims` | Let the first caller of a key win for a time to live |
| Once | `IClusterOnce` | Run a named effect once for the cluster |
| Single-flight | `IClusterSingleFlight` | Compute a value once and share it |
| Budget | `IClusterBudget` | Count distinct ids per period against a limit |
| Counter | `IClusterCounter` | Increment a number atomically |
| Slot | `IClusterSlots` | Allow at most `limit` concurrent holders of a key |
| Ask | `IClusterAsk` | Send a request to the replica that owns a key |
| Durable command | `IClusterCommands` | Send a request that survives a change of owner |

They need the `Primitives`, `Ask` and `Commands` areas ([registration](../reference/registration.md)). Claims,
single-flight, budgets, counters and slots also have Redis variants for hot paths ([Redis](redis.md)).

## What every primitive shares

- Claims, once-runs, single-flight, budgets and slots grant only on a serving replica. A starting, draining or fenced
  replica answers `NotServing`. Counters, Ask and durable commands have their own outcomes, described in their sections.
- Expected outcomes are result types, not exceptions. `Unknown` means the library cancelled the call at its own time
  bound before the commit was confirmed, so it may or may not have taken effect. Your own cancel throws.
- Each decision is its own transaction, never your ambient one, judged on the database's clock.
- Exactly-once does not exist for effects outside PostgreSQL (an email, an HTTP call). You get at-most-once or
  at-least-once, and each section says which.

## Claims

A claim gives one caller a key for a time to live. Use it for "only one replica does this now", such as a nightly
export.

```csharp
var result = await claims.ClaimAsync("nightly-export", TimeSpan.FromMinutes(5), ct);
if (result.TryGetToken(out var token))
{
    await using var effect = await claims.BeginClaimFencedTransactionAsync(
        "nightly-export", token, connection, cancellationToken: ct);   // ClaimLostException if the claim is over
    await effect.CommitAsync(ct);
    await claims.ReleaseAsync("nightly-export", token, ct);   // optional: lets the next caller win at once
}
```

Results: `Won(token)`, `Lost(holder)`, `NotServing`, and `Unknown` (the row may exist and ends with its time to
live).

A claim is valid only while its holder's membership lease is valid. If the holder fences, its claim is over even
before the time to live, and another caller can take it over.

A claim-fenced transaction keeps a stale holder from writing to PostgreSQL: a takeover waits for an open one (at most
`FencedTransactionTimeout`). The connection must be open, have no transaction, and have `transaction_timeout` and
`statement_timeout` at 0.

A PostgreSQL claim does not stop an effect outside the database. `ClaimAsync` and `ReleaseAsync` throw
`InvalidOperationException` while this process has a claim-fenced transaction open on the key. Keys starting with
`reeve.once:` or `reeve.flight:` are reserved.

## Once

Once runs a registered effect one time for the cluster. A delegate cannot be stored in the database, so you register
the effect by name on every replica and run it by name.

```csharp
services.AddClusterOnceEffect("send-welcome", (sp, key, ct) =>
    sp.GetRequiredService<MailService>().SendWelcomeAsync(key, ct));

var result = await once.RunAsync("welcome:42", "send-welcome", OnceMode.AtLeastOnce, TimeSpan.FromMinutes(5), ct);
```

The effect runs in a new service scope with no transaction open. The key alone identifies the run: a later call's mode
and effect are not compared.

Results: `Completed` (this call ran it), `AlreadyCompleted`, `Running(holder)`, `InDoubt` (a record exists that is
neither running nor complete, so the effect may have run), `CompletionUnknown` (it ran, but its completion was not
confirmed), `Unknown` (it did not start here) and `NotServing`.

`AtMostOnce` never repeats an in-doubt run. `AtLeastOnce` hands it to a sweeper (`reeve.once-sweeper`) that runs the
effect again on a replica that registered it, every `RetentionInterval`. So an `AtLeastOnce` effect must be
idempotent and safe to run beside the original. A failing effect is retried on every sweep, with no limit or back-off.

Once-records are kept for ever by default. `OnceRetention` deletes them after their time to live, which lets the key
run again. An incomplete `AtLeastOnce` record is never deleted by age. There is no Redis variant. Other finished
records are deleted `OwnedRowRetention` (24 h) after their end, and counters after `CounterRetention`, which is off by
default ([configuration](../operations/configuration.md)).

## Single-flight

Single-flight starts one computation per key and shares its result with callers on every replica.

```csharp
var result = await flights.RunAsync("price-table", async ct => Serialize(await LoadPricesAsync(ct)),
    claimTtl: TimeSpan.FromSeconds(30), resultTtl: TimeSpan.FromMinutes(5), ct);
if (result.TryGetValue(out var bytes)) { /* use the value */ }
```

`Computed` and `Waited` carry a stored result. `ComputedRefused` and `ComputedUnknown` carry the value this call
computed, but its claim had ended or storing it was not confirmed. `NotServing` and `Unknown` computed nothing.

Callers that find a running computation poll every 100 ms, so keep `claimTtl` short for keys with many waiters. It
limits starts, not executions: a computation whose claim ended keeps running, its result is refused, and the next
caller computes again. Results have no size limit.

## Budgets and counters

A budget counts distinct ids per period, for quotas such as "100 messages per customer per day". A retry of the same
id is not counted twice.

```csharp
var result = await budget.ConsumeAsync("sms:customer-7", id: messageId, limit: 100, period: TimeSpan.FromDays(1), ct);
if (!result.IsAllowed) { /* Exhausted, NotServing or Unknown */ }
```

`Granted(used)` and `AlreadyCounted` allow the call, `Exhausted` means the limit is reached, and after `Unknown` a
retry is safe. A period starts at a multiple of `period` since the Unix epoch. Use one period per key.

A counter is an atomic increment that needs no serving check:

```csharp
var result = await counter.IncrementAsync("orders-created", ct);   // Incremented(value) or Unknown
```

An increment is not idempotent, so a retry after `Unknown` may count twice. `CounterRetention` deletes idle counters,
which then start again at zero. It is off by default.

## Slots

A slot is one of at most `limit` concurrent holdings of a key, kept alive by the holder's renewals. Use it to cap work
that runs for a while across the cluster, such as a workflow run or a camera stream. A mutex is a slot with `limit` 1.

```csharp
var ttl = TimeSpan.FromSeconds(30);
var result = await slots.AcquireAsync("camera:7", id: runId, limit: 2, ttl, ct);
if (result.TryGetToken(out var token))
{
    // Renew in your own loop, well inside ttl: Renewed, Ended, NotServing or Unknown.
    var renewal = await slots.RenewAsync("camera:7", runId, token, ttl, ct);

    await using var effect = await slots.BeginSlotFencedTransactionAsync(
        "camera:7", runId, token, connection, cancellationToken: ct);   // SlotLostException if not valid
    await effect.CommitAsync(ct);

    await slots.ReleaseAsync("camera:7", runId, token, ct);   // optional: frees the slot at once
}
```

`AcquireAsync` answers `Acquired(token)`, `Full`, `NotServing` or `Unknown`.
- **The holder renews.** The library runs no renewal for you. When a renewal answers `Ended` or `NotServing`, your
  tenure is over: stop the work and acquire again.
- **Ids must differ between holders.** Acquiring again with a valid id returns the same slot. `ttl` must exceed
  `SafetyMargin`. Lowering `limit` ends no slot.
- **Who holds it.** `ReadHoldersAsync(key)` returns the replica id and epoch of each valid slot. It is a snapshot:
  use it to address a holder (for example with `AskMemberAsync`), never to conclude that a key is free.
- **A drain does not release slots.** A slot ends when its holder releases it, stops renewing, or its replica fences.

Work outside a slot-fenced transaction is not stopped: after a fence, more than `limit` holders can briefly run
external work.

## Ask

Ask delivers a request to the replica that owns a key and returns the answer, so the caller never looks up the owner.
Register a handler for a unit or keyed-resource kind on every replica:

```csharp
services.AddClusterAskHandler<CameraAskHandler>("camera");       // requests to the owner of a camera
services.AddClusterMemberAskHandler<ReplicaAskHandler>();        // requests to one named replica

var result = await ask.AskAsync("camera", "cam-7", payload, TimeSpan.FromSeconds(5), ct);
if (result.Outcome == AskOutcome.Answered) { var bytes = result.Response; }
else if (result.SafeToRetry) { /* nothing ran */ }
```

A handler implements `IAskHandler.HandleAsync(AskRequest, ct)`, and `AskRequest.Owner` is the tenure's context.
`AskMemberAsync(replicaId, ...)` targets a named replica, and an overload with `takeTimeout` withdraws a request it
has not taken in time.

| Outcome | Meaning | Retry |
|---|---|---|
| `Answered` | The handler returned, or threw (`Error` holds its message) | no |
| `NotTaken`, `NoOwner` | Nobody took it, or no serving owner appeared within the wait | yes |
| `TakenUnanswered` | A target took it and did not answer in time. The effect may have happened | only if idempotent |
| `OwnerLost` | The owner lost the key before answering | only if idempotent |

The timeout must be finite and bounds the whole call. Without Redis only this process is reached, and a request to
another process ends `NotTaken`. The Redis package adds the relay ([Redis](redis.md)). A handler runs in a scope of
its own, so it does not inherit the caller's request. After a Redis failover that loses data, `NotTaken` can be
reported for a request whose handler ran, so do not rely on it for non-idempotent work.

## Durable commands

A durable command is an Ask that survives a change of owner. It is written to PostgreSQL, picked up by the owner, and
needs no Redis. The kind must be a unit or keyed resource.

```csharp
services.AddClusterCommandHandler<CameraCommandHandler>("camera", InDoubtPolicy.ReRun);

var sent = await commands.SendDurableAsync("camera", "cam-7", payload, idempotencyKey: "restart-1", ct);
var outcome = await commands.WaitAsync("camera", "cam-7", "restart-1", TimeSpan.FromSeconds(30), ct);
```

`SendDurableAsync` answers `Sent`, `Existing` (same key and payload), `Resent` (an expired command, safe) or
`IdempotencyConflict` (same key, other payload; nothing written).

| `WaitAsync` outcome | Meaning | Retry |
|---|---|---|
| `Completed` | The handler returned; `Result` holds its bytes | no |
| `Failed` | The handler threw. It may have had effects | only if idempotent |
| `InDoubt` | The owner changed while the handler may have run (policy `Complete`) | only if idempotent |
| `NoOwner` | No owner took it within `CommandPendingTtl`. It never ran | yes, send again |
| `Unfinished` | The command is still pending or running | wait again |
| `NotFound` | Never sent, or deleted by retention | no |

When the owner changes while a handler may have run, `ReRun` runs it again under the new owner, so it must be
idempotent. `Complete` records the command as in doubt and runs nothing.

- A handler can overlap its successor briefly, but the old completion is refused. Do not throw to say "not yet",
  because a throw is final.
- The error text is stored and returned to the caller, so keep secrets out of it.
- There is no per-command timeout. The idempotency key protects only while the finished command is kept
  (`CommandRetention`).
- `CommandPendingTtl` must be at least every handler kind's failover time plus `PlacementInterval`, or the host does not
  start.

## Capabilities and status

A replica can advertise capabilities such as `gpu`, and a unit kind can require or forbid them
(`EligibilityFilter.Require(...)`, `Forbid(...)`). `IClusterCapabilities.SetAsync(["gpu"], ct)` replaces the
application's capabilities, and names starting with `unit:` are reserved. A capability is not readiness: a degraded
replica still serves.

Each replica also reports status sections to PostgreSQL. `IClusterStatus.ReadAsync(ct)` returns every member with its
`State`, `LeaseValid`, sections and fences, and the status endpoint serves it over HTTP
([ASP.NET Core](aspnetcore.md#status-and-skip-endpoints)). A stopped replica stays listed, so judge "running" from
`State` and `LeaseValid`. The report is best effort and no decision of the library reads it. Add a section of your own:

```csharp
services.AddClusterStatusSection<QueueDepthSource>();

public sealed class QueueDepthSource(IBillingQueue queue) : IStatusSectionSource
{
    public string Name => "queue-depth";

    public ValueTask<StatusSection> CollectAsync(CancellationToken ct) => ValueTask.FromResult(
        new StatusSection { Name = Name, Values = new Dictionary<string, object?> { ["queued"] = queue.Count } });
}
```

A name must be unique and not one of the library's, or the host does not start. A source must return quickly. What to
alert on is in [monitoring](../operations/monitoring.md).
