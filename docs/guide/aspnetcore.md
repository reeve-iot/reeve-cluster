# ASP.NET Core

`Reeve.Cluster.AspNetCore` puts the replica's state in front of every request and hub invocation. A replica that is
not serving refuses work with a status a client can act on, and a write is acknowledged only when every serving
replica has applied it. It also provides the readiness check, the status and skip endpoints and the drain on shutdown.

## Registration

```csharp
builder.Services.AddReeveCluster(ports, options => ...);
builder.Services.AddReeveClusterAspNetCore();                        // after AddReeveCluster, before any application IHubFilter
builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);

var app = builder.Build();
app.UseReeveCluster();                                               // first in the pipeline
app.UseAuthentication();
app.UseAuthorization();

app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") })
    .AllowWhenFenced().SkipReadIndex();
app.MapPut("/flags/{name}", UpdateFlag).AcknowledgeWrites();
```

`AddReeveClusterAspNetCore` makes `ProxyReadTimeout` and `ActionTimeBudget` required and checks that
`ReadIndexTimeout + ActionTimeBudget + AckTimeout` is shorter than `ProxyReadTimeout`
([configuration](../operations/configuration.md)). `UseReeveCluster()` adds the scope, routing and the fence middleware:

- Call it first. It adds routing but not authentication or authorization, so call those after it.
- Middleware after it sees only requests the replica admits, so a `503` from `UseCors` placed after it has no CORS
  headers. Static files, health checks mapped as middleware and remote-authentication callbacks have no endpoint to
  mark: map them as endpoints.
- Register `AddReeveClusterAspNetCore` before your own `IHubFilter`, since filters run in registration order.

## Admission

Every request and hub invocation passes one check. A request that is refused did not run, so a client may retry it
with any method. Each answer carries `Retry-After: 1`.

| Answer | When |
|---|---|
| `503 replica_not_serving` | The replica is starting, loading, catching up, activated but not serving yet, or draining |
| `503 replica_fenced` | The replica is fencing or fenced |
| `503 replica_behind` | The replica did not reach the current state within `ReadIndexTimeout` |

The body is `application/problem+json` with an `errorCode`. The `ReeveClusterHttpOptions.WriteError` hook writes the
body of every answer of the package in your own error format, and `HeaderPrefix` (default `Reeve-`) renames its
headers. Before the first load finishes the process answers no request at all.

## Reads see current configuration

Before a request runs, the library waits until the replica has applied everything acknowledged so far (the read
index), then pins that state for the whole request. A request never sees configuration older than what
another request already saw, and never a mix of two states. `replica_behind` bounds the wait. Turn it off for an
endpoint that does not need it, such as health, with `SkipReadIndex()` or `[SkipReadIndex]`.

## Endpoints that serve while fenced

`AllowWhenFenced()` (or `[AllowWhenFenced]` on a controller, action or hub) marks an endpoint that is served in every
phase, not only `Serving`. Use it for what an operator needs when the fleet is unhealthy: sign-in, health probes,
metrics, version, status and the skip endpoint. Its writes commit in every phase and are not awaited, so the
acknowledgement guarantee below does not cover them. On a replica that is not serving it reads state as of the fence.

## Writes and acknowledgement

A write that appends configuration is answered only after every serving replica has applied it. A global MVC result
filter, a hub filter and `AcknowledgeWrites()` on a minimal-API endpoint or group do the wait before the response
starts. Without `AcknowledgeWrites()` a minimal-API endpoint gets the right status and headers but keeps its own body.

The answer depends on what happened:

| Answer | Meaning | Client |
|---|---|---|
| The action's own status | The write was acknowledged. `Reeve-Skipped` names a type under an open skip | Done |
| `202 write_unconfirmed`, `Reeve-Seq` | The write may be committed, but not every replica confirmed it in time (`AckTimeout`), or this replica fenced or drained while waiting | Read before writing again |
| `503 write_refused` | The replica fenced during the request, so no configuration write took effect, but the handler ran | Retry only idempotent work |
| The action's `4xx` or `5xx` with `Reeve-Replication: unconfirmed` | The action failed after a write that is unconfirmed | Per the status |

With `replica_fenced` nothing ran. With `write_refused` the handler ran, and anything it did outside configuration may
have landed. An exception thrown after a refused commit is answered the same way, never as a `500`: the package
registers an `IExceptionHandler` ahead of yours for it, and yours see every other exception.

A scope can opt out of the wait with `IChangeAck.AcknowledgeAtCommit()`. Its answer carries
`Reeve-Replication: at-commit`, and other replicas may not have applied the write yet.

A hub invocation follows the same rules. A refusal reaches the client as a `HubException` whose message is the error
code, and the return value is lost.

## SignalR and long-lived connections

The request that carries a hub connection is admitted by the serving check alone. Each hub invocation, and
`OnConnectedAsync`, is admitted on its own, and `OnConnectedAsync` does not wait for the read index.

A fencing replica aborts open connections through an `IFenceSink`. When you map a hub, name `AspNetCoreFenceSink` in
`ClusterPorts`, or the start is refused: `new ClusterPorts(reconcilers, typeof(AspNetCoreFenceSink))`.

Hub connections and raw WebSockets are tracked for you. Track a server-sent-events or other streaming response
yourself, or a fence will not abort it: `using var tracked = connections.Track(httpContext);` with the
`ClusterConnections` service.

SignalR clients with automatic reconnect return through the balancer to a replica that serves. A long-polling hub
connection is closed without a reconnect, so the client application restarts it. A connection keeps the state it
pinned at connect, so make fresh reads in a new request or invocation.

## Graceful shutdown

On SIGTERM a replica drains. New requests and hub invocations get `503 replica_not_serving`. Requests already running
finish and their acknowledgements run normally. The replica waits for them for at most `ProxyReadTimeout`, aborts open
connections, answers pending acknowledgements as unconfirmed, writes `draining` and exits. A request still running
after `ProxyReadTimeout` is cut with the process.

The `503` starts when SIGTERM arrives, before the balancer stops routing to the pod. A `preStop` sleep covers that,
and the termination grace period must exceed it plus the drain
([deployment](../operations/deployment.md#stopping-prestop-and-termination-grace)). A stop that overruns the grace
period never writes `draining`, so writers wait as after a crash, up to `LeaseTtl`. A hub invocation running at the
stop is not waited for, and its client does not learn the write's outcome.

## Readiness

`AddReeveClusterReadiness()` reports healthy while the replica is serving and no reason is raised. Otherwise it names
the phase and the reasons: `joining.refused`, `joining.unanswered`, `loading.failed`, `activation.failing`,
`activation.version-floor`, `activation.protocol-floor` and `activation.upgrade-path`. An open skip appears in the
details without changing the status. Map `/health/live` with a predicate that runs no check, so a replica the library
took out of service is not restarted ([probes](../operations/deployment.md#probes-and-the-start)).

## Status and skip endpoints

Both are marked `AllowWhenFenced().SkipReadIndex()` and add no authorization. Chain your own on the builder they
return, and keep sign-in exempt so an operator can reach them on a fenced fleet.

```csharp
app.MapReeveClusterStatus("/status").RequireAuthorization();
app.MapReeveClusterSkips("/ops/skips").RequireAuthorization();
```

`MapReeveClusterStatus(pattern = "/reeve/status")` maps a `GET` that returns every member with its state, lease,
version, sections and recent fences. A read that fails or exceeds `ActionTimeBudget` answers
`503 status_unavailable`. The body names operators and versions, so protect it.

`MapReeveClusterSkips(pattern = "/reeve/skips")` opens and closes skips, the way past an entry a reconciler cannot
apply ([troubleshooting](../operations/troubleshooting.md#a-reconciler-cannot-get-past-an-entry)). A group with no
authorization metadata and no fallback policy answers `403 skip_endpoint_unprotected` and writes nothing. On an
authenticated request the operator is the signed-in user.

## What the package does not cover

- Writes of exempt requests, and reads outside a request or hub invocation (a connection handler, a stream body, a
  WebSocket after connect), are not covered by the guarantees above.
- If a replica dies after a commit and before its response starts, the client does not learn the outcome and a balancer
  may resend an idempotent method. The client never sees a false success
  ([balancer and ingress](../operations/balancer-and-ingress.md)).
