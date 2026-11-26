# Owned work

Owned work is code that runs on one replica at a time: a scheduler, a retention purge, the connection to one device.
The library decides which replica runs it, moves it when that replica goes away, and makes the database refuse the
writes of a previous owner.

## Units and keyed resources

There are two shapes. Both are classes that implement `IOwnedUnit`, registered once and resolved as a singleton that
serves every key of its kind.

| Shape | Register with | Key | Who places it |
|---|---|---|---|
| Unit | `AddClusterUnit<T>(kind, o => ...)` | the kind's name | the placer picks a replica |
| Keyed resource | `AddClusterKeyedResource<T>(kind, o => ...)` | chosen by your code | the replica that creates it, then adoption |

```csharp
builder.Services.AddClusterUnit<ReportScheduler>("report-scheduler");
builder.Services.AddClusterKeyedResource<CameraUnit>("camera", o => o.Policy = AdoptionPolicy.AnyEligible);
```

Register the same kinds with the same options on every replica: the library does not detect a mismatch. Take an
`IServiceScopeFactory` for anything scoped.

`UnitKindOptions` has these settings:

| Option | Meaning |
|---|---|
| `Policy` | Who takes a row whose owner is gone: `Placer` (default, units only), `AnyEligible` or `None` (keyed only) |
| `Eligibility` | An `EligibilityFilter` over capabilities: `EligibilityFilter.Any.Require("gpu").Forbid("degraded")` |
| `Convergent` | The unit tolerates two copies and gets no token (see below) |
| `StartTime` | How long the unit takes to be useful. It is added to the kind's failover bound |
| `RedisFence` | The unit keeps state in Redis and writes it through `context.FenceRedisAsync`. Needs `AddReeveClusterRedis` |
## The unit contract

```csharp
public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop);
public Task OnLostAsync(OwnedContext context);
```

`OnAcquiredAsync` runs while this replica owns the key and is serving. It may return early: the key stays owned until
it is lost. `stop` is cancelled when the library begins stopping the unit (a release request, a drain, a fence, a row
that is no longer yours).

The library does not wait for the body's task when it stops the unit. `OnLostAsync` starts when the key is lost,
possibly while the body still unwinds, and the library waits for it, at most `StopBudget` (5 s), before the key may
start elsewhere. So keep the body's task and await it there.

If `OnAcquiredAsync` throws, the library logs it and calls it again for the same tenure after one second, doubling to
a minute. A failure does not release the key. Catch the errors you expect.

```csharp
public sealed class ReportScheduler(NpgsqlDataSource database) : IOwnedUnit
{
    private Task body = Task.CompletedTask;

    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => body = RunAsync(context, stop);

    public async Task OnLostAsync(OwnedContext context)
    {
        try { await body; }
        catch (Exception e) when (e is OperationCanceledException or OwnershipLostException) { }
    }

    private async Task RunAsync(OwnedContext context, CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));
        while (await timer.WaitForNextTickAsync(stop))
        {
            try { await RunOnceAsync(context, stop); }   // the fenced write shown below
            catch (OwnershipLostException) { return; }   // another replica owns the key now
            catch (CommitOutcomeUnknownException) { }   // neither success nor rollback: read the state again next tick
        }
    }
}
```

## What a unit does while it owns the key

`OwnedContext` carries `Kind`, `Key` and `Token`, the number of this tenure. A token is never reused and is higher
than every earlier tenure of the key.

**Database writes** go through a fenced transaction. It checks the token first and locks the owner row, so a change
of owner waits for it and a write that passed the check cannot commit after the row changed hands.

```csharp
await using var connection = await database.OpenConnectionAsync(stop);
await using var tx = await context.BeginFencedTransactionAsync(connection, cancellationToken: stop);
await using var insert = new NpgsqlCommand("INSERT INTO report_run (run_at) VALUES (now())", connection, tx.Transaction);
await insert.ExecuteNonQueryAsync(stop);
await tx.CommitAsync(stop);
```

With `Reeve.Cluster.EntityFrameworkCore`, `context.BeginFencedTransactionAsync(db)` does the same on a `DbContext` with
no retrying execution strategy. The connection must reach the primary, and its role needs `transaction_timeout` and
`statement_timeout` at 0. The transaction is bounded by `FencedTransactionTimeout` (5 s).

**Effects outside the database** call `context.EnsureOwner()` immediately before the effect. It throws
`OwnershipLostException` once the unit is being stopped or the replica's fence deadline has passed.

```csharp
context.EnsureOwner();
await device.SendAsync(command, stop);
```
This narrows the window without closing it: a process paused between the check and the send still sends, so the
external system needs its own idempotency.

**Configuration** is read per action. Begin a scope, read `context.Configuration`, act, end the scope:

```csharp
using (scopes.BeginUnitOfWork())     // IClusterScopeAccessor
{
    var flags = context.Configuration.Get<FlagState>("flag");
    // act on flags
}
```

Do not hold the generation across actions ([configuration log](configuration-log.md)).

**Errors.** `OwnershipLostException` means the unit may no longer act: stop and let `OnLostAsync` run.
`CommitOutcomeUnknownException` means a fenced commit was cancelled or lost its connection before it returned and may
not survive a failover. Treat it as neither success nor rollback: read the state again, or repeat an idempotent write.

## Placement
A placer, itself an owned unit, assigns the `Placer`-policy kinds every `PlacementInterval` (2 s):

- An unowned unit goes to the eligible serving replica holding the fewest placed units. Eligible means it registered
  the kind, passes the `Eligibility` filter and is serving.
- When replicas are unbalanced, the placer moves one unit per pass, and never within `StabilisationWindow` (30 s) of a
  membership change, so a rolling restart does not shuffle units on every join.

A replica advertises `unit:<kind>` for each kind it registers, plus names you set with
`IClusterCapabilities.SetAsync`. An owner that stops passing its filter, for example in a degraded mode, stops the
unit and releases it at its next poll, and the placer assigns it elsewhere. With no eligible replica the unit runs
nowhere until one appears.

## Handover
To move a unit off a live owner, the placer writes a release request. At its next poll the owner stops the unit:
`EnsureOwner()` throws, `stop` is cancelled and `OnLostAsync` runs. Once it has returned, or `StopBudget` has passed,
the owner releases the row and the placer assigns it to the next replica. The two never run the unit at the same time
through a fenced path. A hung `OnLostAsync` does not hold the key, but the unit does not start again on that replica
until the call returns.

## Failover
When an owner crashes, its lease runs out and the placer reassigns the unit. The kind is without an owner for at most:

```text
LeaseTtl + 3 x PlacementInterval + FencedTransactionTimeout + StartTime
```

With the defaults and no `StartTime` that is about 41 s. A kind with `RedisFence` adds another
`FencedTransactionTimeout`. If the owner fenced while PostgreSQL was reachable, it released its lease and the bound
counts from the fence. Design the feature for this gap: the scheduler does not fire for up to that long.

## What a unit sees on a fence or a drain
| Event | What the unit sees |
|---|---|
| A fence (lost lease, stalled reconciler, new database timeline) | `stop` is cancelled and `EnsureOwner()` throws at once. Fenced writes are refused locally, then by the database. `OnLostAsync` runs. After the rejoin the unit is placed again, perhaps here |
| A graceful drain, or a release request for one key | `stop` is cancelled, `OnLostAsync` runs, and the row is released so another replica can take it |
| A hung stop | The handover does not wait past `StopBudget`. The unit's fenced effects are already refused |

A fenced transaction already open when a fence began may still commit within `FencedTransactionTimeout`.
## Keyed resources
Use a keyed resource for many things of one shape, such as one `connection:{endpoint}` per endpoint. The creator
calls `IKeyedResources.CreateAsync(kind, key)` whenever the resource should exist, and the unit starts there.

```csharp
var outcome = await resources.CreateAsync("camera", cameraId, ct);   // IKeyedResources
```
`CreateAsync` answers `Created` (this replica owns the key now), `AlreadyOwned`, `NotServing`, `Stopping` (the previous
tenure on this replica is still stopping: call again) or `NotEligible` (this replica fails the kind's filter). Only
`Created` writes a row.

A unit ends its resource with `context.EndAsync()`. After a crash or a drain, `AnyEligible` lets a serving eligible
replica adopt the key. With `None`, the row waits for its creator, who frees it with `CleanUpAsync(kind, key)`, and
`Ask` answers `NoOwner` meanwhile. The filter applies only on creation and adoption: an owner that stops passing it
keeps its keys.

## Convergent units
Set `o.Convergent = true` for work that is idempotent and self-correcting, such as a retention purge, and that reads
every input from the primary as it acts. It is placed and handed over like any other unit, but gets no token:
`Token`, `EnsureOwner()` and fenced transactions throw `InvalidOperationException`. Two copies may run briefly after a
fault, and the next run corrects what they leave.

## Reaching the owner
`IClusterAsk.AskAsync(kind, key, request, timeout)` delivers a request to the current owner of a key and returns
`Answered`, `NotTaken` (safe to retry), `TakenUnanswered`, `OwnerLost` or `NoOwner`. Register the handler with
`AddClusterAskHandler<T>(kind)`. `AskMemberAsync` addresses a named replica, and across processes it needs the Redis
relay. A durable command (`IClusterCommands`) is a request stored in PostgreSQL that the owner runs, with a policy you
declare for an owner change midway.

## Settings that matter
`LeaseTtl`, `PlacementInterval`, `StopBudget`, `FencedTransactionTimeout` and `StabilisationWindow` set the failover
and handover times ([configuration](../operations/configuration.md)). If a unit does not start, see
[troubleshooting](../operations/troubleshooting.md#owned-work). What is not guaranteed is in the
[architecture](../architecture.md#what-it-does-not-guarantee).
