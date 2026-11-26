# Contributing

Thanks for helping with reeve-cluster. Bug reports and pull requests are welcome.

## Building

You need the .NET 11 SDK. The integration tests also need Docker, and the linearizability checker needs Go.

```bash
dotnet build Reeve.Cluster.slnx -c Release
```

## Testing

```bash
dotnet test tests/Reeve.Cluster.Tests -c Release --no-build              # unit tests
dotnet test tests/Reeve.Cluster.ArchitectureTests -c Release --no-build  # rules on the library's own source
```

These two projects need nothing else and are what every pull request runs. The integration and multi-replica
harness tests start PostgreSQL, Redis, Toxiproxy and nginx in containers, so they need Docker and about 4 GB of
memory. Run a narrow `--filter` locally; the `Integration` workflow runs the whole suite nightly and before every
release. The [testing guide](docs/development/testing.md) describes the lanes, the harness and the environment
variables.

## Public API

Each shipped package lists its public surface in `PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt` next to its
project file. Put new or changed public members in `PublicAPI.Unshipped.txt` (the RS0016 code fix writes the line).
A removed member keeps its `PublicAPI.Shipped.txt` line and gets a `*REMOVED*` line in `PublicAPI.Unshipped.txt`.
Details are in [Building and releasing](docs/development/building-and-releasing.md#public-api-baselines).

## Pull requests

Open pull requests against `main`. The `CI` workflow builds the solution, runs the unit and architecture tests and
runs the Go tests of the linearizability checker. Add a CHANGELOG entry for a change users will notice.

## Releasing

1. Set `<Version>` in `Directory.Build.props`, add the version's section to `CHANGELOG.md` and move the
   `PublicAPI.Unshipped.txt` lines into `PublicAPI.Shipped.txt`.
2. Merge to `main`.
3. Push the tag `vX.Y.Z`.

The `Release` workflow runs the `Integration` workflow, then builds, tests and packs with the version taken from the
tag, publishes the packages to nuget.org through trusted publishing and creates the GitHub Release with the version's
CHANGELOG section.
