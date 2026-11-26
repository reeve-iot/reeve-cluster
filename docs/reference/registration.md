# Registration

How to register the library, which call each package brings, and what the start checks. The settings are in
[../operations/configuration.md](../operations/configuration.md).

## AddReeveCluster

```csharp
builder.Services.AddReeveCluster(
    new ClusterPorts([typeof(FlagReconciler), typeof(QuotaReconciler)], typeof(AspNetCoreFenceSink)),
    options => builder.Configuration.GetSection(ClusterOptions.SectionName).Bind(options));
```

`AddReeveCluster(ports, configure)` registers the library with every area on. `ClusterPorts` takes two required
arguments:

- the reconciler types, in the order the applier runs them (empty if the application has no configuration). A type a
  reconciler depends on comes before it.
- the fence sink type: `NoFenceSink` when no connection stays open past a request, `AspNetCoreFenceSink` for hub
  connections. A mapped hub with `NoFenceSink` refuses the start.

`configure` sets `ClusterOptions`. `ReplicaId` and `ConnectionString` are required, and so is `ReconcileTimeout` when
`ClusterPorts` names a reconciler. With no reconciler, `AddReeveCluster` sets `ReconcileTimeout` to 10 s if you did
not.

### Areas

An overload registers only the areas an application uses:

```csharp
builder.Services.AddReeveCluster(ports, ClusterAreas.OwnedWork | ClusterAreas.Ask, options => ...);
```

`ClusterAreas` is a `[Flags]` enum: `None`, `OwnedWork`, `Primitives`, `Ask`, `Commands` and `All`; a value with a flag
that is not an area throws `ArgumentOutOfRangeException`. `Primitives`, `Ask` and `Commands` imply `OwnedWork`.
Membership, the change log, the read index, the status registry and the migration runner are always registered.

| Area | What it brings |
|---|---|
| `OwnedWork` | the unit runtime, unit kinds and keyed resources, the placer and the free-row sweep (`reeve.free-row-sweep`), the owned connection pool |
| `Primitives` | claims, once-runs, single-flight, budgets, counters and held slots, the units `reeve.once-sweeper` and `reeve.primitive-retention`; the Redis primitives |
| `Ask` | `IClusterAsk` and its handlers, and the Redis relay where the Redis package is added |
| `Commands` | `IClusterCommands`, durable command handlers, the pickup and the unit `reeve.command-sweep` |

- Areas register in one fixed order, so the hosted services start in the same order on every replica.
- Resolving a service of an area that is off (`IClusterClaims`, `IClusterAsk`, `IClusterCommands` and the like) is a
  dependency-injection error.
- With `Ask` off, `AddReeveClusterRedis` registers no `Ask` relay and subscribes no relay channel, whether the Redis
  package is added before or after `AddReeveCluster`.
- An area turned off on every replica leaves its rows in the database unswept.

### What the start validates

The start is refused when:

| Check | Refusal |
|---|---|
| A unit kind or keyed resource is registered and `OwnedWork` is off | `The unit kind '...' is registered, but AddReeveCluster was called without ClusterAreas.OwnedWork: add it to the areas, or remove the registration.` |
| An `Ask` handler is registered and `Ask` is off | the same message, naming the handler and `ClusterAreas.Ask` |
| A durable command handler is registered and `Commands` is off | the same, naming `ClusterAreas.Commands` |
| A once effect or a Redis primitive is registered and `Primitives` is off | the same, naming `ClusterAreas.Primitives` |
| A required setting is missing or a timing relation is broken | one message per problem ([configuration.md](../operations/configuration.md#validation)) |
| A reconciler's dependencies, a mapped type without a reconciler, a context without the capture, a status section name, the version string | a `ClusterConfigurationException` with a named `Reason` ([troubleshooting.md](../operations/troubleshooting.md#the-host-does-not-start)) |

Register the same kinds, with the same options, and the same `Ask` and command handlers on every replica. The placer
places only the kinds its own replica registers.

## The other registrations

| Call | Package | What it registers |
|---|---|---|
| `AddClusterUnit<T>(kind, o => ...)` | `Reeve.Cluster` | A unit kind (a placed singleton). A `Policy` of `AnyEligible` or `None` throws `ArgumentException`; the kind name `placer` is reserved |
| `AddClusterKeyedResource<T>(kind, o => ...)` | `Reeve.Cluster` | A keyed-resource kind, with `Policy` `AnyEligible` or `None` |
| `AddClusterOnceEffect(name, effect)` | `Reeve.Cluster` | A once effect, by name |
| `AddClusterAskHandler<T>(kind)`, `AddClusterMemberAskHandler<T>()` | `Reeve.Cluster` | `Ask` handlers, for a kind's owner or for a member |
| `AddClusterCommandHandler<T>(kind, policy)` | `Reeve.Cluster` | A durable command handler and its in-doubt policy |
| `AddClusterStatusSection<T>()` | `Reeve.Cluster` | An application status section; `T` is registered once as a singleton |
| `AddReeveClusterMigrations(o => ...)` | `Reeve.Cluster` | The migration runner alone, for a migration-only run |
| `AddReeveClusterEntityFrameworkCore(mapping => ...)` | `Reeve.Cluster.EntityFrameworkCore` | The configuration mapping |
| `UseReeveClusterCapture(services)` on `DbContextOptionsBuilder` | `Reeve.Cluster.EntityFrameworkCore` | The capture interceptor on a context |
| `AddReeveClusterAspNetCore(o => ...)` | `Reeve.Cluster.AspNetCore` | The filters, the exception handler, the HTTP options; makes `ProxyReadTimeout` and `ActionTimeBudget` required |
| `AddHealthChecks().AddReeveClusterReadiness(name, tags)` | `Reeve.Cluster.AspNetCore` | The readiness check |
| `app.UseReeveCluster()` | `Reeve.Cluster.AspNetCore` | The scope middleware, routing and the fence middleware |
| `MapReeveClusterSkips(pattern)`, `MapReeveClusterStatus(pattern)` | `Reeve.Cluster.AspNetCore` | The skip endpoint (`/reeve/skips`) and the status endpoint (`/reeve/status`); neither adds authorization |
| `.AllowWhenFenced()`, `.SkipReadIndex()`, `.AcknowledgeWrites()` | `Reeve.Cluster.AspNetCore` | Endpoint conventions: serve in every phase, skip the read index, await the write's acknowledgement |
| `AddReeveClusterRedis(o => ...)` | `Reeve.Cluster.Redis` | Wake-ups, the `Ask` relay, `FenceRedis` |
| `AddReeveClusterRedisClaims(policy)`, `...SingleFlight(policy)`, `...Budget(policy)`, `...Slots(policy)`, `...Counter()` | `Reeve.Cluster.Redis` | The Redis primitives; they need the `Primitives` area |

The application registers its own `IConnectionMultiplexer` for the Redis package, before or after `AddReeveCluster`.

## Order in an ASP.NET Core application

- `UseReeveCluster()` goes first. It adds the scope middleware, routing and the fence middleware. A mapped write in
  an HTTP request outside its live scope (in middleware registered earlier, for example) is refused with
  `WriteOutsideClusterScope`. Authentication and authorization come after it, and `UseReeveCluster()` does not add
  them, even in a `WebApplication`.
- Middleware after it sees only requests the replica admits. `UseCors` after it leaves the `503` without CORS headers.
  Static files, health checks mapped as middleware and remote-authentication callbacks have no endpoint to mark, so
  map them as endpoints and mark them.
- Register `AddReeveClusterAspNetCore` before any application `IHubFilter`: one registered earlier runs outside each
  invocation's scope and pin.
- Register `UseReeveClusterCapture` after the application's own EF Core interceptors: a `TransactionCommitting`
  interceptor that runs after it runs under the fleet-wide change-log lock.

```csharp
var app = builder.Build();
await app.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();

app.UseReeveCluster();
app.UseAuthentication();
app.UseAuthorization();

app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") })
    .AllowWhenFenced().SkipReadIndex();
app.MapReeveClusterStatus("/status").RequireAuthorization();
app.MapReeveClusterSkips("/ops/skips").RequireAuthorization();
```

The sample application in [../../samples/Reeve.Cluster.Sample](../../samples/Reeve.Cluster.Sample) registers every
package this way.

## Dependencies

| Package | Depends on |
|---|---|
| `Reeve.Cluster.Abstractions` | nothing |
| `Reeve.Cluster` | Npgsql, `Npgsql.EntityFrameworkCore.PostgreSQL` (for the schema migrations), Microsoft.Extensions.* |
| `Reeve.Cluster.EntityFrameworkCore` | EF Core |
| `Reeve.Cluster.AspNetCore` | ASP.NET Core |
| `Reeve.Cluster.Redis` | StackExchange.Redis |

Everything restores from nuget.org ([../operations/deployment.md](../operations/deployment.md#installing)). Each
`Reeve.Cluster.*` package depends on its siblings at the exact same version. The dependency floors are in
[../operations/upgrading.md](../operations/upgrading.md#exact-sibling-pins-and-dependency-floors).
