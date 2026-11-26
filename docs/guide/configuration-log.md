# Configuration log

The configuration log makes a change to configuration visible on every replica before the write is acknowledged.
Configuration here is state that each replica holds in memory in a derived form (feature flags, schedules, connection
settings) and that lives in ordinary tables of your database.

## How a change travels

1. A write to a mapped table appends an entry to the change log in the same transaction.
2. Each replica re-reads the changed state from one database snapshot and builds a new immutable generation.
3. The write is acknowledged once every serving replica has applied it.
4. A later request waits until its replica has applied everything committed before it, then reads one generation.

You provide a mapping from entities to aggregate types, a reconciler for each type, and the code that reads it.

## Map entities to aggregate types

An aggregate type is the unit of change, such as `flag` or `quota`. With the EF Core package, you map entities to it
and register the capture interceptor on your context:

```csharp
builder.Services.AddReeveClusterEntityFrameworkCore(mapping =>
{
    mapping.Entity<Flag>("flag").Key(f => f.Name).AllColumns();
    mapping.Entity<Quota>("quota").Key(q => q.Tenant).Columns(q => q.DailyLimit);
});
builder.Services.AddDbContext<AppDb>((services, o) => o.UseNpgsql(connectionString).UseReeveClusterCapture(services));
```

`Key` names the entity inside its type, as a hint that narrows what the reconciler re-reads. Use `NoKey()` when it
always re-reads the whole type. `Columns` limits entries to changes of those columns, and `AllColumns()` counts every
change. Every mapped type needs a reconciler, or the start is refused. Register `UseReeveClusterCapture` after your own
interceptors that change entities in `SavingChanges`.

## Write a reconciler

A reconciler reads a type's state from the pass's snapshot and returns the next derived state. It publishes nothing
itself. Reconcilers are singletons, registered in the order you give `ClusterPorts`. A type that reads another's
state is listed after it.

```csharp
builder.Services.AddReeveCluster(
    new ClusterPorts([typeof(FlagReconciler), typeof(QuotaReconciler)], typeof(AspNetCoreFenceSink)), ...);

public sealed record FlagState(IReadOnlyDictionary<string, bool> Flags);

public sealed class FlagReconciler : IReconciler
{
    public string AggregateType => "flag";

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken ct)
    {
        var flags = new Dictionary<string, bool>();
        await using var command = new NpgsqlCommand("SELECT name, enabled FROM flags",
            (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction) { CommandTimeout = 5 };
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            flags[reader.GetString(0)] = reader.GetBoolean(1);
        }

        return new ReconcileResult(new FlagState(flags));
    }
}
```

Read only through `context.Transaction` or `context.Connection`: what you read elsewhere is not covered by the
guarantees. With EF Core, build a context of your own over that connection and call
`UseTransaction(context.Transaction)`. To read in parallel, use `context.OpenSnapshotSessionAsync()`.

`Hints` names the keys that changed and `Previous` is the type's current state, so a reconciler can patch it instead of
reloading. `Hints` is `null` on a full load, after a retry and above `HintLimit` (1000) keys.

The rules of a reconciler:

- A domain fault, such as an invalid setting or a connection that cannot be opened, is a state you record and
  return. It is not an exception.
- A reconciler that throws, or does not return within `ReconcileTimeout`, stalls. The applier retries it with backoff,
  and past `ReconcileTimeout + ApplyStallLimit` the replica fences itself and rejoins. Slow success is write
  latency, not a stall. Size `ReconcileTimeout` for the slowest full load of the largest type, with margin.
- Give every command a bounded `CommandTimeout`, never 0, and honour the cancellation token: an attempt that never
  returns ends the process.
- Do not wait for an acknowledgement inside a reconciler. `WaitAppliedAsync` there answers `NotAwaited(Applier)`.
- If a type's state is built from another's, override `IReadOnlyList<string> DependsOn => ["other"]` and read it with
  `context.GetDependency<T>("other")`. Both are published in one swap.

Slow work, such as starting a component or fetching an artifact, runs after the reconciler returns, as
`ReconcileResult.Deferred`. It gets the new generation and a token cancelled when a later generation supersedes the
type, and becomes visible only through `generation.TryActivate(type, () => ...)`, which runs while the state is still
current.

## Read configuration

| Where | How |
|---|---|
| An HTTP request or hub invocation | The read index pins one generation for the request: `scopes.Current!.PinnedGeneration!.Get<FlagState>("flag")` with `IClusterScopeAccessor` |
| An owned unit | `context.Configuration`, inside `scopes.BeginUnitOfWork()` for each action |
| A background service or other code | `scopes.BeginBackground()`, then `generations.TryPin()` (`IClusterGenerations`) |

`TryPin()` returns `null` when the replica is not serving: skip the action and try at the next one. Never act on
`IReplicaPhase` alone; pin a generation for each action.

A pinned reader never sees a mix of two generations. A raw WebSocket or server-sent-events request, a hub's
`OnConnectedAsync` and a streaming method's body keep the generation pinned at connect: read again in a new request.

## Write and wait for the acknowledgement

With EF Core, a normal `SaveChanges` or transaction captures the entry itself. In MVC the filter registered by
`AddReeveClusterAspNetCore` waits before the response starts. On a minimal API endpoint or group, add
`AcknowledgeWrites()`:

```csharp
var writes = app.MapGroup("").AcknowledgeWrites();
writes.MapPut("/flags/{name}", async (string name, FlagBody body, AppDb db, CancellationToken ct) =>
{
    db.Flags.Update(new Flag { Name = name, Enabled = body.Enabled });
    await db.SaveChangesAsync(ct);
    return Results.Ok();
});
```

What the client sees:

| Response | Meaning |
|---|---|
| The endpoint's own status | The write was acknowledged: every serving replica has it |
| `202`, `errorCode: write_unconfirmed`, header `Reeve-Seq` | The write is committed and propagating, but not acknowledged within `AckTimeout`, or the replica fenced or drained meanwhile. Read first |
| `503`, `errorCode: write_refused`, `Retry-After: 1` | A commit was refused because the replica is fencing and no entry was appended. No configuration write took effect, but the handler ran and its other effects may have landed |
| `503`, `errorCode: replica_behind` | The request has not run. Any method may retry it |

An endpoint marked `[AllowWhenFenced]` is served in every phase, and its writes are not awaited.
For a write outside HTTP, or one that is not captured, use the log directly:

```csharp
await using var tx = await connection.BeginTransactionAsync(ct);
// business statements
var seq = await changeLog.AppendAsync(tx, [new ChangeEntry("flag", name)], ct);   // IChangeLog, last before commit
await tx.CommitAsync(ct);
var outcome = await ack.WaitAppliedAsync(seq, ct);   // IChangeAck, after the transaction ended
```

The outcome is `Acknowledged(Skipped)`, `Unconfirmed(Reason)` (`Fenced`, `Draining` or `Timeout`) or
`NotAwaited(Reason)` (`Applier`, `AtCommit`, `Joining`, `Draining` or `Fenced`). Do not await inside the transaction: no
replica sees the entry before the commit.

Bulk operations skip the capture: `ExecuteDelete` of a mapped type, or `ExecuteUpdate` of a counted column, throws
unless the query ends in `.AppendedToChangeLog()` and you append the entries yourself.

`IChangeAck.AcknowledgeAtCommit()` opts one scope out of the wait. The write still propagates, but a unit on another
replica may briefly act on the previous state. Use it for a label or a note, never for a write that revokes or
disables something.

## What your application must make idempotent

Retries are normal, so make these safe to repeat:

- **A client may repeat a request.** After `202 write_unconfirmed` or `503 write_refused` a client retries, and a
  balancer may resend an idempotent request. A write that rolled back after its append may also answer `202`. Make the
  handler's other effects safe to run twice.
- **Reconcilers run many times.** Each attempt, retry and full reload runs again over the same snapshot. A reconciler
  has no side effects beyond building state.
- **Deferred work may be dropped and repeated.** An attempt that is superseded or discarded never starts its deferred
  work, and a newer one starts its own.
- **Acknowledgement does not drain work.** Operations that started before a change still run on the old state.

## When a reconciler fails everywhere

A reconciler that throws on every replica takes every replica out of service. That is the price of the guarantee. An
operator gets the fleet moving with a skip, which tells every replica to treat a range of one type as applied. Map
the skip endpoint behind authorization, or use `IChangeSkip` from a management command:

```csharp
app.MapReeveClusterSkips("/ops/skips").RequireAuthorization();
```

The endpoint refuses a skip that would cover an acknowledged write (`409 skip_covers_acknowledged`). Fix the
reconciler, then close the skip ([troubleshooting](../operations/troubleshooting.md#a-reconciler-cannot-get-past-an-entry)).

Propagation and write latency depend on `ReconcileTimeout` (required), `ApplyStallLimit`, `AckTimeout`,
`ReadIndexTimeout` and `HintLimit`: see [configuration](../operations/configuration.md).
