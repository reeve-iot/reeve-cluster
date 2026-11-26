# Testing

Every store is tested against real PostgreSQL and Redis in containers. Pure decision functions (the member
state machine, renewal outcomes, the options validator, the acknowledgement wait set, the owned-row and
durable-command state machines, each primitive's fail-closed outcome) are unit tested directly.

## Test projects

| Project | Namespace prefix | What it holds | Needs |
|---|---|---|---|
| `tests/Reeve.Cluster.Tests` | `Unit.` | Decision functions, stores against fakes, options validation, metrics through a `MeterListener` | Nothing |
| `tests/Reeve.Cluster.ArchitectureTests` | `Arch.` | Rules on the library's own source and SQL: banned tokens, no public types in the migrations, token draws, fault-point and stimulus catalogues, status records | Nothing |
| `tests/Reeve.Cluster.IntegrationTests` | `Int.` | Every store, the migrations, capture, HTTP integration and Redis, against containers | Docker |
| `tests/Reeve.Cluster.Harness` | | The multi-replica harness (a library, not tests; see [The harness](#the-harness)) | |
| `tests/Reeve.Cluster.Harness.Tests` | `Harness.` | Multi-replica histories with injected faults, schedule replays, the linearizability workload, mixed-version runs and the harness's self-tests (`Harness.SelfTests`) | Docker; Linux for process mode |
| `tests/linearizability-checker` | | A Go program (Porcupine) that checks a recorded history, with fixtures in `testdata/` | Go |

## Lanes

| Lane | What runs | In CI | Locally |
|---|---|---|---|
| Pull request | Build, `Unit.` and `Arch.`. No containers. | `CI` workflow, on every pull request and every push to `main` | `dotnet test tests/Reeve.Cluster.Tests -c Release`, and the same for `tests/Reeve.Cluster.ArchitectureTests` |
| Linearizability checker | `go test` over the fixtures | `CI` workflow | `cd tests/linearizability-checker && go test ./...` |
| Integration | `Int.` and `Harness.`, `Category!=MixedVersion`, with the Go checker built and passed in `REEVE_LINEARIZABILITY_CHECKER` | `Integration` workflow: nightly, on demand, and before every release (the `Release` workflow runs it first) | One or two named classes (below) |
| Nightly | Tests marked `Category=Nightly`: the linearizability workload at its larger run count and the Redis Cluster run | The `nightly` job of the `Integration` workflow, on the schedule or when a manual run sets the `nightly` input. It sets `REEVE_NIGHTLY=true`. | `REEVE_NIGHTLY=true dotnet test <project> --filter Category=Nightly` |
| Mixed-version | Two releases of the library on one PostgreSQL and one Redis | The `mixed-version` job of the `Integration` workflow | [Mixed-version runs](#mixed-version-runs) |

The integration lane does not run for each pull request.

### Running the container suites locally

The integration and harness suites start PostgreSQL 18 (a primary and, for failover tests, a standby), Redis,
Toxiproxy, nginx and up to five replicas as child .NET processes. With the test host that is about 4 GB. Both test
projects cap xUnit at 2 parallel threads in `xunit.runner.json`. Raise that only if you have the memory.

Run a narrow filter, one container suite at a time:

```bash
dotnet test tests/Reeve.Cluster.IntegrationTests -c Release \
  --filter "FullyQualifiedName~Int.Log.AckWaitTests"
dotnet test tests/Reeve.Cluster.Harness.Tests -c Release \
  --filter "FullyQualifiedName~Harness.Slice.SliceTwoReplicaTests"
```

Testcontainers reaches Docker through its socket. If the tests themselves run in a container that uses a mounted
socket, the containers they start are siblings, not children, and their published ports may not be reachable on
`localhost`. The symptom is "connection refused" on localhost, not a missing daemon. Set `TESTCONTAINERS_HOST_OVERRIDE`
to an address that reaches them.

On a slow or overloaded machine, a test that could not check its property because the test process was not scheduled
does not fail. It prints `HARNESS-DEGRADED: <test> <reason>` and appends the line to
`artifacts/harness-degraded/degraded.txt`. The number of those lines is the coverage a slow run lost.

### Environment variables

| Variable | Effect |
|---|---|
| `REEVE_HARNESS_SEED` | Replays a harness run with the seed its report printed |
| `REEVE_NIGHTLY=true` | Lets a `[NightlyFact]` run. Without it the test skips itself. |
| `REEVE_LINEARIZABILITY_RUNS` | Run count of the nightly linearizability workload (default 6) |
| `REEVE_LINEARIZABILITY_CHECKER` | A built checker binary. Without it the harness builds and runs the checker with the local Go toolchain. |
| `REEVE_MIXED_OLD_REF`, `REEVE_MIXED_NEW_REF`, `REEVE_MIXED_SCHEMA_DELTA`, `REEVE_MIXED_BUILD_DIR` | See [Mixed-version runs](#mixed-version-runs) |
| `TESTCONTAINERS_HOST_OVERRIDE` | The host Testcontainers publishes ports on |

## Tools

| Script | What it does |
|---|---|
| `tools/previous-release.sh` | Prints the releases a version must run beside in the mixed-version lane |
| `tools/check-mixed-junit.sh <junit.xml>` | Fails unless every mixed-version test is in the file and none was skipped |

## The harness

`tests/Reeve.Cluster.Harness` runs N library instances in one test process against one PostgreSQL, with fault
injection. It can also run the sample application as N child processes behind nginx. It is a test library and is not
shipped.

| Piece | What it does |
|---|---|
| `Stack/*` | PostgreSQL 18, Redis and Toxiproxy on one Docker network, shared through a collection fixture. A Toxiproxy proxy per process in front of PostgreSQL and Redis can cut, hold (a frozen process as the network sees it) or delay the link. `PostgresPair` adds a streaming standby in mode `async`, `FIRST 1` or `ANY 1`, with `FailOverAsync` and `SwitchOverAsync`. A test that promotes gets a stack of its own. `DatabaseFaults` stops, restarts and pauses the database. |
| `Processes/*` | Process mode: the sample as child OS processes, one per replica id, behind nginx configured like the balancer in the operations guide (`proxy_next_upstream error http_503`, read timeout = `ProxyReadTimeout`). `SampleProcess.Signal` sends a real `SIGTERM`, `SIGKILL`, `SIGSTOP` or `SIGCONT`. Linux only. |
| `Replicas/*` | One in-process replica with its own host, DI container, clock, fault points and reconciler script, plus `PauseAsync`, `ResumeAsync`, `CrashAsync` and `StopAsync`. A `HarnessRun` is one test's seed, log, database and replicas. |
| `Faults/*`, `Time/*` | Fault points (`Hold`, `Delay`, `Throw`, `Observe`), whole-replica pauses and death, reconcilers that succeed, throw, take time or hang, and a replica clock with a rate, a shift, a wall step and a pause |
| `Timing/*` | The stall-tolerant timing profile tests use by default, a tight one for tests about a fence, a lease or a stall, and a probe that measures how long the test process was not scheduled |
| `Stimuli/*` | One stimulus per membership action, and the operations the table names: pause, crash, restart, stop, operator delete, floor raise, clock shift, scripted reconciler faults, and the offline restore (`TakeBackup`, `RestoreDatabase`) |
| `Observation/*` | The phase timeline, the database observer (member rows, the wait set, `fence_history`), views of the model-event log and a report that starts with the run's seed |
| `Linearizability/*` | The workload and recorder of [the linearizability check](#the-linearizability-check) |

How tests use it:

- A test reaches an interleaving by holding a fault point (`Hold(point)`, `WaitReachedAsync()`, `Release()`), never by
  sleeping.
- Every process has its own proxies and every run its own database, so one test never reaches another.
- `REEVE_HARNESS_SEED` fixes the harness's own decisions, not the order in which the library's threads reach their next
  point. Hold a schedule that matters with a fault point.

### Schedule replays

`Harness.Replay.*`, `Harness.Slice.SliceReplayTests`, `Harness.AckRace` and `Harness.Stall` replay recorded
failure schedules against the code, using stimuli and fault points for each step. Each replay passes on the
current code and fails when the defect it records is put back.
The membership replays run with one and with two replicas, since a one-replica deployment must follow the same rules.

## Mixed-version runs

`Harness.Mixed.MixedVersionTests` (`Category=MixedVersion`) runs two releases of the library on one PostgreSQL and one
Redis: old (`o...`) and new (`n...`) replicas of the sample, driving Ask, owned units, claims, slots, the version gate
and reads across the two.

- `SampleBuilds.FromRefAsync(tag)` exports the release's library source with `git archive`, lays the working tree's
  `samples/Reeve.Cluster.Sample` over it (current sample, old library) and publishes it once to a directory outside
  the repository (`REEVE_MIXED_BUILD_DIR`, default the temp directory). A release that targets the test's framework is
  published for it. An older one is published for its own framework and runs on the test's runtime with
  `DOTNET_ROLL_FORWARD=Major`. It needs the tags, so a shallow clone does not work.
- `ProcessCluster.StartProcessAsync(id, build:)` runs a process on that build (`migrate: false` leaves the schema to
  the first process). The tests drive the sample's `/mixed/*` surface, which is on with `Sample:MixedFleet`. Reads
  there skip the read index's wait. `dotnet Reeve.Cluster.Sample.dll --lower-floor <version>` is the new build's
  floor command, run once.
- `REEVE_MIXED_OLD_REF` names the older release (the tests skip without it) and `REEVE_MIXED_NEW_REF` the newer one
  (default: the working tree). `REEVE_MIXED_SCHEMA_DELTA=none` says the two releases have the same migrations, so the
  schema test expects the new build to leave the schema as it found it. When unset, the schema must have changed.
- The sample may use only API that every compared release has, and the whole sample must build against each compared
  release's package set. A sample change that needs a newer API turns the lane red at the next tag.

```bash
REEVE_MIXED_OLD_REF=$(tools/previous-release.sh --head | tail -1) \
  dotnet test tests/Reeve.Cluster.Harness.Tests -c Release --filter Category=MixedVersion
```

`tools/previous-release.sh` decides which releases a version runs beside. For `vX.Y.Z` it prints the highest release
below `vX.Y.0` (the previous minor or, for `X.0.0`, the previous major) and, for a patch, also the highest release
below `vX.Y.Z`. Only a minor or major release changes tables or messages, so that is the unit of overlap. Every printed
release is strictly below the asked version, because equal versions say nothing about a mixed fleet. Tags above it,
prereleases and tags that are not releases change nothing.

| Form | Meaning |
|---|---|
| `tools/previous-release.sh vX.Y.Z` | The releases that tag runs beside |
| `--head` | The same for the `<Version>` in `Directory.Build.props`. A props line a tag already carries is a released line, and that tag is not an old ref. |
| `--require-below OLD NEW` | Exits 1 unless OLD's version is strictly below NEW's (a release tag names its version, another ref is read from its `Directory.Build.props`) |
| `--self-test` | Checks the rules against a table of cases in a scratch repository |

The `mixed-version` job of the `Integration` workflow runs nightly, on a manual run and before every release. On a
release tag it asks `tools/previous-release.sh` for the releases that tag runs beside. Otherwise it asks for the
releases below the `<Version>` line. For each release it checks with `--require-below` that the release is strictly
below HEAD, runs the MixedVersion tests, and checks the junit output with `tools/check-mixed-junit.sh`. It sets
`REEVE_MIXED_SCHEMA_DELTA=none` when no migration was added or removed between the two releases. An edit inside a
merged migration file is not a schema change. When there is no earlier release the job does nothing. The `Release`
workflow runs it before publishing ([Releasing](building-and-releasing.md#releasing)).

## The linearizability check

The check records concurrent reads and writes of configuration through the sample and checks the history against a
sequential model, as Jepsen does.

- `ConfigWorkload` runs writers, readers and hub readers in closed loops, with choices from the run's seed. It sends
  `GET`/`POST /config` and the hub's `GetConfig` through `ConfigClient` and `HubConfigReader`. `HistoryRecorder` writes
  one line per operation (format `reeve-history/1`: call and return from one monotonic clock; `ok`, `unknown` or
  `failed`).
- `LinearizabilityChecker` runs the Go checker on the file. Exit 0 means linearizable, 1 not, 2 timed out, 3 bad
  input. A run fails on anything but 0. Histories and pictures go to `artifacts/linearizability`. Use at most 12
  clients, since the checker's cost follows the operations in flight.
- `Harness.Linearizability.CheckerSelfTests` runs the checker's fixtures (a torn read, a stale read after an
  acknowledgement, new-then-old across replicas, an unknown write that flaps, a refused write a read saw, skips that
  excuse only the registers they name) and histories it must refuse.
- `ConfigWorkloadTests` runs two replicas behind nginx with no fault. `LinearizabilityUnderFaultsTests` runs three
  replicas over a primary with a `FIRST 1` standby while a seeded `FaultSchedule` kills one replica, drains one,
  freezes one, cuts one's links and fails the primary over, one fault at a time. It then checks the history once it
  holds enough writes, reads, hub reads, a request that met a fault and writes acknowledged after the failover. The
  nightly lane runs it many times (`G2_holds_over_many_runs_with_many_faults`).
- Exempt requests, a hub's connection handler and long-lived connections are outside the read guarantee and outside the
  workload. A restore is not among the faults, because it rewinds what a reader saw (a stated residual). An unknown
  write can excuse a stale read that shows its value.

## What the suites cover

The harness and integration suites exercise these scenarios:

- Outages and faults: cut links to PostgreSQL or Redis, pauses, a drifting clock, reconcilers that throw or hang,
  database restarts of 5 to 30 s, a primary failover with synchronous replication, and an offline restore.
- A lost suffix after a failover: with the standby cut, a captured write, an `Append`, an adoption and a skip open each
  commit with their wait cancelled, then the standby is promoted. Every replica fences on the new timeline, reloads in
  full and serves no lost entry, and tokens stay above anything the lost suffix drew.
- Cancelled commits: a cancelled renewal, grant, record or activation is never counted as done.
- Ownership: no two owners' fenced writes interleave under any fault, a handover stays within the declared failover
  bound, and a connection that sets `transaction_timeout` is refused.
- Phases and stops: readiness, `draining` on `SIGTERM`, and a stuck reconciler ending the process at fence step 6 at
  its start + `ReconcileTimeout` + `ApplyStallLimit`.
- HTTP answers on a fenced replica: `503 write_refused`, `202 write_unconfirmed` (never resent by the balancer),
  and `Reeve-Replication: at-commit` on an opted-out write.
- Generations, tokens, Redis, skips, the version floor and capture: reads of one generation, a delayed Redis `EXEC`
  landing only within the fence key's TTL, a skip closed without another write converging every member, and a save
  that would capture in a transaction the library did not begin throwing.
- Metrics: each instrument is driven on a fake clock and its value and tags are asserted through a `MeterListener`.
