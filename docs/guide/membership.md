# Membership

Every replica is a member of the cluster and holds a lease in PostgreSQL. This page describes the phases a replica goes
through, what your application sees in each, how a replica stops and rejoins, and how the version gate protects rolling
upgrades.

## Phases

A replica decides about itself by its local phase. `IReplicaPhase.Current` returns it.

| Phase | What it means for the application |
|---|---|
| `Starting` | Joining the cluster. The host's start waits here, so no HTTP request is answered yet |
| `Loading` | The first full load: every reconciler runs over one snapshot |
| `CatchingUp` | Applying the configuration log up to its head |
| `Activated` | Active in the cluster, applying up to the head it read at activation. Not serving yet |
| `Serving` | Open to client traffic. Units run, claims are granted, readiness is healthy |
| `Draining` | A graceful stop is under way |
| `Fencing`, `Fenced` | The replica took itself out of service and is waiting to rejoin |
| `Exited` | The process is leaving |

`Serving` is the only phase open to ordinary client traffic. A replica enters it once per join, after it has applied
the whole log. Peers see a separate row state in the database (`joining`, `active`, `fenced`, `draining`), which
decides who a write waits for. You do not act on it.

The library checks the phase, and that its own clock is before its fence deadline, immediately before every action
that depends on serving. Do the same: never cache "I am serving". Pin a generation for each action, or call
`EnsureOwner()` before an effect, and treat a refusal as the answer.

## Readiness

`AddReeveClusterReadiness` adds a health check that is healthy only while the replica is serving, and otherwise
names the phase and each reason it is not ready. Map it as an endpoint marked `AllowWhenFenced().SkipReadIndex()`, as
in the [getting started guide](getting-started.md#wire-the-pipeline-and-readiness), and use it as the readiness probe.

Reasons you may meet are `loading.failed` (a full load failed or ran past `ReconcileTimeout`), `activation.failing`,
`activation.version-floor`, `activation.protocol-floor` and `activation.upgrade-path`. An open skip shows as a critical
note on an otherwise healthy answer. The host starts only after the first load, so a replica that never loads shows
its reason in logs and metrics, not on an endpoint. Give the startup probe a window longer than `LeaseTtl` plus the
first load. [Troubleshooting](../operations/troubleshooting.md#a-replica-never-becomes-ready) maps each reason.

## The fence middleware

`UseReeveCluster()` adds middleware that admits a request only while the replica is serving. For other requests it
answers `503` with an error code in the body:

| Code | Meaning | Client action |
|---|---|---|
| `replica_not_serving` | The replica is starting, loading, catching up, activated but not serving yet, or draining. Nothing ran | Retry; any method |
| `replica_fenced` | The replica is fenced. Nothing ran | Retry; any method |
| `replica_behind` | The replica has not applied everything committed before the request. Nothing ran | Retry; any method |

A request already running when a fence begins may finish. A commit it tries after that is refused, and the request
answers `503 write_refused` or `202 write_unconfirmed`, as the
[configuration log guide](configuration-log.md#write-and-wait-for-the-acknowledgement) describes.

An endpoint marked `[AllowWhenFenced]` (or `.AllowWhenFenced()` on minimal APIs) is served in every phase. Use it for
probes, sign-in, metrics and recovery actions. Its writes are not awaited, and on a replica that is not serving it
reads state as of the fence or the catch-up point. A balancer must resend only requests that are safe to
resend: see [balancer and ingress](../operations/balancer-and-ingress.md).

Middleware after `UseReeveCluster()` sees only admitted requests. `UseCors` placed after it leaves a `503` without CORS
headers. Static files and remote-authentication callbacks have no endpoint to mark, so map them as endpoints.

## Fencing

A replica fences when it cannot prove its lease: it cannot renew, a reconciler stalls past its limit, it finds a new
database timeline, or its deadline passes while it is paused. What the application sees:

1. The replica stops serving. Readiness fails, and the fence middleware refuses new requests.
2. Commits of client requests are refused. Units, background services and the applier still commit.
3. Open connections are aborted through the fence sink, in `AspNetCoreFenceSink`'s case a hub connection's transport. A
   SignalR client with automatic reconnection reconnects to a serving replica.
4. Owned units get `stop` cancelled and `EnsureOwner()` throws ([owned work](owned-work.md)).
5. The replica rejoins with a full load, or the process exits when a stuck reconciler left it no clean way back.

A fence is not an error to fix. A restart, a network blip and a database failover end in it. Let the orchestrator
restart a process that exits. A replica that fences again soon after rejoining waits `RejoinBackoff` (60 s) first.
The status endpoint (`MapReeveClusterStatus`) lists each replica's recent fences and their cause.

To react to a phase change in your own component, call `IReplicaPhase.WaitForChangeAsync(seen, ct)`. It is
level-triggered, so a fence followed by a rejoin can read as `Serving` twice. It is a signal, not a check.

## Draining and shutdown

On SIGTERM the replica drains, in this order:

1. It stops taking requests. New ones get `503 replica_not_serving`, and the placer assigns it nothing. Running
   requests finish, and their writes are still acknowledged.
2. Its units stop and release their rows. Units of an `AnyEligible` keyed kind are adopted by another replica.
3. It waits for the requests that were running, at most `ProxyReadTimeout`. Then it aborts open connections, marks
   its row `draining` so writers stop waiting for it, and exits.

Set the pod's termination grace period above `preStop + StopBudget + ProxyReadTimeout + 2 x RenewTimeout`, or the
`draining` write is never made and writers wait for the lease to expire, up to `LeaseTtl`. A `preStop` sleep of a
few seconds keeps requests routed to the replica while the balancer catches up from getting a `503`. See
[deployment](../operations/deployment.md#stopping-prestop-and-termination-grace).

## Rejoin and replica ids

After a fence, a replica reads its own row and rejoins as a new epoch, with a full load of configuration. One process
per `ReplicaId` may run at a time. A second process with the same id is refused while the first's lease is valid. If it
takes over an expired row, the first learns of it on its next renewal, fences and exits. Use the pod name so a
restarted pod continues under its id.

## Versions and rolling upgrades

Members of different releases run side by side during a rolling update. Each advertises its application version and the
library's protocol number. Set a SemVer 2.0 version in your build, because the library reads the entry assembly's
informational version, and a version that is not SemVer refuses the start.

The cluster holds a version floor and a protocol floor. A replica below a floor cannot activate. It stays
`joining`, never serves, and names the floor in its readiness. A feature that must not run before every replica
has the new code asks the gate:

```csharp
if (await gate.RequireVersionAsync("1.2.0", ct))   // IClusterVersionGate
{
    UseTheNewFormat();
}
```

`RequireVersionAsync` answers true when the floor is already at or above the version, or when every live member
runs at least that version and the floor could be raised. A true answer is durable: afterwards an older binary cannot
join. It answers false, and writes nothing, while an older member holds a lease, so ask again later. The check on an
already raised floor takes no lock, so a feature may ask on every call. A raise takes a lock that every
configuration write waits for, bounded by `RenewTimeout + 1 s`. A raise above the calling binary's own version
throws `ArgumentException`.

`AllMembersAtLeastAsync(version)` only observes the members. Use it for status and decisions you can undo.
`RequireProtocolAsync` and `AllMembersSpeakAsync` do the same for the library's protocol number.

The library keeps one release of overlap in its own tables and messages. A rolling update by one release is
supported: run the migrator or let the new replicas run it, then roll the replicas. An upgrade that skips a release,
or a release whose CHANGELOG entry says so, is a full stop. Rolling back below a raised floor leaves the old build
unable to join until an operator lowers the floor with `IClusterFloorCommand`. See
[upgrading](../operations/upgrading.md).

## Capabilities

A replica advertises capabilities: a name per registered unit kind, plus any you set with
`IClusterCapabilities.SetAsync`. Units use them for eligibility. Capabilities are not readiness: a replica can
advertise one and not serve.
