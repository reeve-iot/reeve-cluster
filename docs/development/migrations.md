# Migrations

The schema `reeve_cluster` is created by EF Core migrations of `ClusterMigrationsDbContext`, in
`src/Reeve.Cluster/Migrations/`. The context is internal, is never registered in dependency injection and is never
queried. The library reads and writes the schema with raw Npgsql, and EF Core is used only in that folder (an
architecture test enforces it), so nothing a host registers can change the schema.

Operators run the migrations as described in [Upgrading](../operations/upgrading.md). This page is for changing them.

## How the runner works

`ClusterMigrationRunner` builds the context from `ClusterMigrationOptions.ConnectionString` and applies the pending
migrations.

- The history is kept in `reeve_cluster."__ClusterMigrationsHistory"`, so a host's `__EFMigrationsHistory` is untouched.
- Runners that start together migrate one at a time. The runner takes a transaction-scoped advisory lock (key
  `REEVMIGR` in ASCII, distinct from the change-log lock) on a separate connection that reads and writes no table.
  The second runner then finds the history complete. The lock is on its own connection because a migration that drops
  or alters a table would otherwise wait for the `ACCESS SHARE` locks of the tables the lock connection read. Both
  connections use TCP keepalive, so a dead migrator releases its locks promptly.
- Each migration is one transaction. Commits run on a data source without a `Command Timeout`, because a commit waits
  for the synchronous standby and a cancelled commit has an unknown outcome. Every other statement keeps the
  connection string's `Command Timeout`, and a cancel of one of those fails the run before the migration in progress
  commits.
- EF Core's `MigrateAsync` always runs `CREATE TABLE IF NOT EXISTS` on its history table, so the role needs `CREATE` on
  the schema, and on the database for the schema's first creation.
- Nothing is written on a server older than PostgreSQL 18.
- `ClusterInitialBaseline` is the whole baseline schema. Tables, keys, checks, indexes and sequences come from the
  model. The functions, the two triggers and the two seed rows are `migrationBuilder.Sql`, because EF Core cannot model
  them.

## Adding a migration

Change the model in `ClusterMigrationsDbContext.cs`, then run, from the repository root:

```bash
dotnet tool restore
dotnet tool run dotnet-ef migrations add <Name> --project src/Reeve.Cluster --context ClusterMigrationsDbContext
```

The design-time factory (`ClusterMigrationsDesignTimeFactory`) points the context at a host that never resolves
(`.invalid`), so `dotnet ef` cannot reach a database. `Microsoft.EntityFrameworkCore.Design` is build-time only and is
held at the EF Core floor that `Npgsql.EntityFrameworkCore.PostgreSQL` brings (see
[Dependency floors](building-and-releasing.md#dependency-floors)). New migrations stamp that version as their
`ProductVersion`.

Then:

- Make the generated classes `internal`: the migration, its `.Designer.cs` and the model snapshot. EF Core generates
  them `public`, which would put them in the library's API, and an architecture test fails on a public type in
  `Reeve.Cluster.Migrations`. EF Core finds non-public migrations.
- Check the generated SQL for what EF Core does not express. Functions, triggers, comments and seed rows go in
  `migrationBuilder.Sql`. A sequence is created `CACHE 1`, so a value is never lost to a session's cache across a
  restart. No index is built `CONCURRENTLY` (`Arch.SourceRules` bans `IsCreatedConcurrently`, and each migration is
  one transaction). A sequence the library draws from may be drawn only where `Arch.TokenDrawRules` allow.
- `Unit.Migrations.ClusterMigrationsModelTests` fails when the model has a change no migration covers.

## Rules

- Never edit or renumber a merged migration, because the history records its id. A schema change is a new migration.
  Ids are timestamps, so two branches that each add a migration merge without a conflict and apply in timestamp order.
  The model snapshot is the file that conflicts. The branch that rebases second regenerates its migration on the new
  snapshot (`dotnet tool run dotnet-ef migrations remove`, then `add`).
- Give tables, columns, indexes, functions and sequences explicit names in the model. Constraint names that PostgreSQL
  derives are its own.
- A migration after the baseline only adds: a table, a column that is nullable or has a default, an index, a function
  with a new name. A build one release older then keeps running against a database a newer build migrated, and the
  runner leaves history rows it does not know alone. `Unit.Migrations.OverlapRuleTests` scans the SQL EF Core
  generates for each migration after the baseline. A migration that must break the rule (the second release of a
  two-release change) is named in that test's exception list with the statement and the reason. The list is empty.
- Use only transaction-scoped advisory locks.
