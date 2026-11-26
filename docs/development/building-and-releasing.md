# Building and releasing

## Requirements

| Tool | Needed for |
|---|---|
| .NET 11 SDK | Everything. The projects target `net11.0`. |
| Docker | The integration and harness tests, which start PostgreSQL 18, Redis, Toxiproxy and nginx through Testcontainers |
| Go | The linearizability checker in `tests/linearizability-checker`, unless `REEVE_LINEARIZABILITY_CHECKER` names a built one |

## Build

```bash
dotnet build Reeve.Cluster.slnx -c Release
dotnet test tests/Reeve.Cluster.Tests -c Release --no-build              # unit tests
dotnet test tests/Reeve.Cluster.ArchitectureTests -c Release --no-build  # rules on the library's own source
```

Every pull request runs these two test projects and `go test` of the linearizability checker (the `CI` workflow).
[Testing](testing.md) describes the other lanes.

Every project targets `net11.0`, set once in `Directory.Build.props`, and builds against EF Core 11 and Npgsql 11.
`dotnet tool restore` installs `dotnet-ef` from `.config/dotnet-tools.json`, for [migrations](migrations.md).

## The packages

Every project under `src/` is a shipped package. The tests, the sample and the harness live outside `src/` and are not
packable.

| Package | Contents |
|---|---|
| `Reeve.Cluster.Abstractions` | Interfaces and result types. No dependencies. |
| `Reeve.Cluster` | Membership with self-fencing, the configuration log, the read index, owned units, Ask, the PostgreSQL primitives and the migrations |
| `Reeve.Cluster.Redis` | Wake-ups, the Ask relay transport, Redis primitives and `FenceRedis` |
| `Reeve.Cluster.EntityFrameworkCore` | The capture interceptor and the fenced-write helper |
| `Reeve.Cluster.AspNetCore` | The readiness check, fence middleware, acknowledgement filter, read index and the skip and status endpoints |

`src/Directory.Build.props` marks each as `ReeveShippedPackage`, which turns on XML documentation, exact sibling pins
and the public API analyzer. Every public member needs a `///` comment (a missing one is warning CS1591). Each package
carries `lib/net11.0`, the repository `README.md`, a symbol package (`.snupkg`) and SourceLink information.
`ContinuousIntegrationBuild` is on when `CI` is `true`, as it is on GitHub Actions.

## Analyzers

Analyzer rules are warnings, and the build is expected to stay at zero warnings. `.editorconfig` sets:

| Rule | Severity | Where | Notes |
|---|---|---|---|
| CA2007 (`ConfigureAwait`) | warning | `src/` | A library must not capture the caller's synchronisation context |
| CA2007 | off | `tests/`, `samples/` | Test and host code has no context to deadlock against |
| CA1063, CA2016 | warning | everywhere | Incorrect `Dispose`; a `CancellationToken` not forwarded |
| RS0016, RS0017, RS0036, RS0037 | warning | shipped packages | Public API baselines: a symbol missing from or stale in `PublicAPI.*.txt`, missing nullability, a file without `#nullable enable` |
| CA1068, CA1062, CA1848, CA1727 | off | | Public signatures are frozen, the packages are nullable-enabled, and the library does not log at a volume that needs `LoggerMessage` |

CA2007 cannot see an `await using` declaration, which has no place for `ConfigureAwait(false)`. A file that needs one
suppresses CA2007 with a file-level `#pragma`.

## Public API baselines

Each shipped package lists its public surface in `PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt` next to its
project file, checked by `Microsoft.CodeAnalysis.PublicApiAnalyzers`.

1. New or changed public members go in `PublicAPI.Unshipped.txt`. RS0016 names the missing symbol, and its code fix
   writes the line.
2. A removed member keeps its `PublicAPI.Shipped.txt` line and gets a `*REMOVED*` line in `PublicAPI.Unshipped.txt`.
3. A release moves the `PublicAPI.Unshipped.txt` lines into `PublicAPI.Shipped.txt`.

Both files start with `#nullable enable`. The analyzer needs that line.

## Dependency floors

`Directory.Packages.props` pins every version centrally. A dependency's version is a minimum: the host's current major
at its `.0` release, so an application is not forced to upgrade to use the library. Raise one only for a feature the
library needs, and say which in the CHANGELOG.

- The migrations are an EF Core migration of an internal context, so the Npgsql EF Core provider ships with the
  library.
- `Microsoft.EntityFrameworkCore.Design` is build-time only (`PrivateAssets="all"` in `Reeve.Cluster`) and is held at
  the EF Core floor that `Npgsql.EntityFrameworkCore.PostgreSQL` brings, not at the newest patch. Above that floor the
  shipped `Reeve.Cluster.dll` references an EF Core assembly version its own dependencies do not guarantee, which
  shows as MSB3277 warnings in the Redis and ASP.NET Core projects.
- Moving to EF Core 12 means changing the EF Core, Npgsql provider and Npgsql versions in `Directory.Packages.props`
  and the Renovate cap in `renovate.json`.
- The Microsoft.Extensions.* abstractions (dependency injection, hosting, logging, options) come from the .NET 11
  shared framework, so no package depends on them.
- `Reeve.Cluster.Redis` sets `RedisMinServerVersion` to 6.2, so the analyzer suggests only commands a Redis 6.2 can
  run.
- Test-only packages (Testcontainers and the like) are never a dependency of a shipped package.

## Packing

```bash
dotnet pack Reeve.Cluster.slnx -c Release -o artifacts/nupkg /p:Version=<version>
```

The shipped packages depend on each other at the exact same version, `[X.Y.Z]`. A plain `ProjectReference` packs as
`>= X`, which would let a consumer restore `Reeve.Cluster.Redis` 1.0.0 next to `Reeve.Cluster` 1.1.0. The adapters use
core internals through `InternalsVisibleTo`, so only a matched set is safe.

`Directory.Build.targets` wraps each sibling's version in brackets after NuGet's private `_GetProjectReferenceVersions`
target collects it. `PackageReference` ranges are untouched. Because the hook runs after a private target, the
`ReeveCheckExactSiblingPins` target reads the generated nuspec and fails the pack when a `Reeve.Cluster*` dependency
is not an exact range. If that happens, a newer SDK has probably renamed the private target. Update the hook in
`Directory.Build.targets`.

## Versions

The `<Version>` line in `Directory.Build.props` is the version local builds and packs carry. The `Release` workflow
takes the published version from the tag instead: `v1.2.3` publishes `1.2.3`.

## Releasing

1. Set `<Version>` in `Directory.Build.props`, add the version's section to `CHANGELOG.md` and move the
   `PublicAPI.Unshipped.txt` lines into `PublicAPI.Shipped.txt`.
2. Merge to `main`.
3. Push the tag `vX.Y.Z`.

The `Release` workflow runs on a `v*` tag with two jobs:

| Job | What it does |
|---|---|
| `tests` | Runs the `Integration` workflow ([Testing](testing.md#lanes)): the integration job and the mixed-version job against the releases `tools/previous-release.sh` names for the tag ([Mixed-version runs](testing.md#mixed-version-runs)). It does not run the nightly job. `publish` waits for it. |
| `publish` | Builds with the version from the tag, runs the unit and architecture tests, packs the solution and pushes the packages to nuget.org with trusted publishing, then creates the GitHub Release for the tag with the version's `CHANGELOG.md` section as its notes. `NuGet/login` exchanges the workflow's OIDC token for a short-lived API key for the account in the `NUGET_USER` Actions variable, so no API key is stored. |

A release that deliberately breaks compatibility with its predecessor (a major version with no mixed fleet) removes the
mixed-version run from the `Integration` workflow's release path in the same change that announces it.
