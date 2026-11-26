# Development

How to build, test, change and release reeve-cluster. [CONTRIBUTING.md](../../CONTRIBUTING.md) has the short version.
The library's guarantees are in [the architecture overview](../architecture.md).

| Page | What it covers |
|---|---|
| [Building and releasing](building-and-releasing.md) | Requirements, the build, the packages, analyzers, public API baselines, dependency floors, versions and the release workflow |
| [Testing](testing.md) | The test projects and lanes, how to run each locally, the multi-replica harness, mixed-version runs, the linearizability check |
| [Migrations](migrations.md) | The schema's EF Core migrations: how the runner works and how to add one |

## Repository layout

| Path | What it holds |
|---|---|
| `src/` | The five shipped packages |
| `tests/` | Unit, architecture, integration and harness tests, the harness itself and the Go linearizability checker |
| `samples/Reeve.Cluster.Sample` | The sample application, which the harness also runs as child processes |
| `tools/` | Scripts for mixed-version release selection, and its junit check |
