# Entity Framework Core

`Reeve.Cluster.EntityFrameworkCore` connects your own `DbContext` to the cluster. When you save a configuration entity,
the package records the change in the cluster's change log in the same transaction, so every replica can apply it
before the write is acknowledged. It also gives a unit the EF Core form of a fenced transaction.

## What it adds

- **Capture.** A save of a mapped entity appends a change-log entry as the last statement of the transaction that
  commits it. You write no `Append` call.
- **A commit fence.** On a fenced replica, a commit of a request's transaction is refused and rolled back, so a stale
  replica cannot write configuration ([ASP.NET Core](aspnetcore.md#writes-and-acknowledgement)).
- **A bulk guard.** The package refuses `ExecuteUpdate` and `ExecuteDelete` that would change mapped data unseen.
- **Fenced transactions for units**, through `BeginFencedTransactionAsync`.

## Registration

Register the mapping after `AddReeveCluster`, then add the capture to every context that saves mapped entities:

```csharp
builder.Services.AddReeveClusterEntityFrameworkCore(mapping =>
{
    mapping.Entity<Flag>("flag").Key(f => f.Name).AllColumns();
    mapping.Entity<Rule>("rule").NoKey().Columns(r => r.Expression, r => r.Enabled);
});

builder.Services.AddDbContext<AppDb>((services, options) =>
    options.UseNpgsql(connectionString).UseReeveClusterCapture(services));
```

Register `UseReeveClusterCapture` after your own interceptors, because a `TransactionCommitting` interceptor that runs
after it runs while the change-log lock is held, which stalls every other write.

### The mapping

For each configuration entity you name the aggregate type, how its key is computed and which columns count.

- Choose both explicitly: `Key(...)` or `NoKey()`, and `Columns(...)` or `AllColumns()`. A mapping that omits one,
  repeats one or maps a type twice is refused at start.
- A change that touches only uncounted columns makes no entry. Added and deleted entities always do.
- Owned types are attributed to their mapped root. A key function should read only key or counted columns.

### Reconcilers

Every mapped aggregate type needs a reconciler, which builds the in-memory state from the database on every replica.

```csharp
public sealed class FlagReconciler : IReconciler
{
    public string AggregateType => "flag";

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken ct)
    {
        // Read on the pass's connection and inside its transaction. context.Hints names the changed keys.
        var options = new DbContextOptionsBuilder<AppDb>().UseNpgsql(context.Connection).Options;
        await using var db = new AppDb(options);
        await db.Database.UseTransactionAsync(context.Transaction, ct);
        return new ReconcileResult(await db.Flags.AsNoTracking().ToDictionaryAsync(f => f.Name, f => f.Enabled, ct));
    }
}
```

A reconciler reads only through `context.Connection` and `context.Transaction`, or a session from
`context.OpenSnapshotSessionAsync()`. A context from the application's
factory opens a connection of its own, and what it reads is not covered by the guarantees.

Pass the types to `ClusterPorts`, in the order they should run. A request reads the result from the state pinned to it:
`scopeAccessor.Current!.PinnedGeneration!.Get<Dictionary<string, bool>>("flag")`.

### Start-up checks

Before the replica joins the cluster, the host refuses a mapped aggregate type
without a reconciler (`AggregateTypeWithoutReconciler`) and a context that cannot be built or has a mapped entity but
no capture (`ContextWithoutCapture`). `ChangeTracker.CascadeDeleteTiming` and `DeleteOrphansTiming` must be
`Immediate` or `Never`: with `OnSaveChanges`, EF Core deletes dependents after the capture has read the changes.

## Saves and transactions

An ordinary save needs nothing from you:

```csharp
db.Flags.Add(new Flag { Name = "beta", Enabled = true });
await db.SaveChangesAsync(ct);   // saves the row and appends the entry in one transaction
```

For a save that captures, and for every save in a client request, the package sets `AutoTransactionBehavior.Always`, so
there is a transaction to append to. Other saves (ingest, telemetry) keep your setting.

With an explicit transaction, the entry is appended at commit, after your statements:

```csharp
await using var tx = await db.Database.BeginTransactionAsync(ct);
db.Flags.Add(flag);
await db.SaveChangesAsync(ct);
await tx.CommitAsync(ct);   // appends the entries, then commits
```

- Commit through the context that began the transaction. Several contexts can share one transaction with
  `Database.UseTransaction`, and their entries go with the context that began it.
- Savepoints are not tracked. After a rollback to one, the transaction may append entries for aggregates that did not
  change. This costs a reconcile of those keys, never correctness.
- A capturing save throws and writes nothing when no context transaction can take its entries: a raw `DbTransaction`, a
  transaction begun by a context without the capture, or an ambient `System.Transactions` transaction.
- Commit through EF. A commit through the `DbTransaction` directly, or a SQL `COMMIT`, commits without the entries.

In an HTTP request, a mapped save outside the request's live cluster scope throws and writes nothing. This catches
middleware registered before `UseReeveCluster()`.

### Fence and acknowledgement as the application sees them

A request that saves configuration does not return until every serving replica has applied it. Your handler
sees no difference: the commit appends, and the ASP.NET Core filter then waits and chooses the answer.

If the replica fences before the commit, the commit throws and rolls back, so the write does not happen, and the
client gets `503 write_refused` (retry idempotent work only). If the commit succeeded but not every replica confirmed
it in time, the client gets `202 write_unconfirmed` and reads before writing again. A transaction still open when the
handler returns is not awaited, because its entries are appended only at commit.

## Raw SQL and bulk operations

Raw SQL and bulk operations bypass the change tracker. For raw SQL, append the entry yourself:

```csharp
await changeLog.AppendAsync(transaction, [new ChangeEntry("flag", "beta")], ct);   // IChangeLog
```

In a context with the capture, `ExecuteDelete` of a mapped type is refused. So is an `ExecuteUpdate` that sets a counted
column, any column of an all-columns type, a key, or a column the guard cannot read. An update of only uncounted columns
is allowed. If you append the affected aggregates yourself, tag the query with `.AppendedToChangeLog()` to pass.

## Fenced transactions for units

A unit that owns a key writes through a transaction the database refuses once another replica owns it:

```csharp
await using var tx = await context.BeginFencedTransactionAsync(db, cancellationToken: ct);   // OwnedContext
db.Reports.Add(report);
await db.SaveChangesAsync(ct);
await tx.CommitAsync(ct);
```

The begin checks that the unit still owns its key, that the connection reached the primary on PostgreSQL 18 or newer,
and that `transaction_timeout` and `statement_timeout` are 0 (`ClusterConfigurationException` otherwise). The context
must use Npgsql, have no transaction and no retrying execution strategy.

- Every statement in the transaction is covered, and a change of owner waits for it (at most `FencedTransactionTimeout`).
- Commit through `FencedDbContextTransaction.CommitAsync`. It throws `CommitOutcomeUnknownException` when the outcome
  is not known, which a commit through `.Transaction` does not.
- `OwnershipLostException` means the replica lost the key, and the unit should stop. Other errors, such as `40001`
  under `REPEATABLE READ` after a change of owner, are thrown as `PostgresException` for you to retry.

## What it does not guarantee

- Raw SQL that does not append, a tagged bulk operation that does not append, and a bulk operation in an EF
  precompiled query (which the guard never sees) change data without an entry. So does an `ExecuteDelete` of an
  unmapped principal whose database cascade changes mapped dependents.
- An unconfirmed write may have been committed.
