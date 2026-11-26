using System.Diagnostics.CodeAnalysis;

namespace Reeve.Cluster;

/// <summary>
/// Applies the library's migrations to the database <see cref="ClusterMigrationOptions.ConnectionString"/> names: the schema
/// <c>reeve_cluster</c> and its history table. The migrations are EF Core migrations of an internal context that the host never
/// sees. The application runs the runner at start-up, with or without a lease: a migration-only run registers it through
/// <c>AddReeveClusterMigrations</c>, which registers nothing that needs a lease or Redis. EF Core does not support migrations
/// under trimming or NativeAOT, so <see cref="MigrateAsync"/> runs in a JIT-compiled process.
/// </summary>
public interface IClusterMigrationRunner
{
    /// <summary>
    /// Applies every migration this build knows and the history does not, under an advisory lock of its own, so runners that start
    /// together migrate one at a time and the later ones find nothing left to do. Each migration is one transaction.
    /// Nothing is written on a PostgreSQL below 18, which fails with <see cref="ClusterConfigurationReason.PostgreSqlTooOld"/>. A run that has nothing to apply changes nothing.
    /// </summary>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled. A commit may have landed on the primary without reaching the standby, so the
    /// outcome is unknown; the next run reads the history.
    /// </exception>
    /// <exception cref="ClusterConfigurationException">
    /// The server is below PostgreSQL 18, a standby, or sets <c>transaction_timeout</c> or <c>statement_timeout</c>.
    /// </exception>
    [RequiresUnreferencedCode("The schema migrations are EF Core migrations, which EF Core does not support under trimming or NativeAOT. Run them from a JIT-compiled process.")]
    [RequiresDynamicCode("The schema migrations are EF Core migrations, which EF Core does not support under trimming or NativeAOT. Run them from a JIT-compiled process.")]
    Task<ClusterMigrationResult> MigrateAsync(CancellationToken cancellationToken = default);
}

/// <summary>What one run of <see cref="IClusterMigrationRunner"/> did.</summary>
/// <param name="AppliedMigrations">The ids of the migrations this run applied, in order; empty when the schema was already current.</param>
/// <param name="LastMigration">
/// The id of the last migration the history records after the run, or <see langword="null"/> when it records none. It is not one of this
/// build's when a newer build has migrated the database first; the runner leaves such rows alone, because a migration only adds.
/// </param>
public sealed record ClusterMigrationResult(IReadOnlyList<string> AppliedMigrations, string? LastMigration);

/// <summary>The settings of the migration runner.</summary>
public sealed class ClusterMigrationOptions
{
    /// <summary>
    /// Connection string of the primary of the database that holds the <c>reeve_cluster</c> schema. Required. As for
    /// <see cref="ClusterOptions.ConnectionString"/>: with one host set no target session attribute, with several hosts
    /// set <c>Target Session Attributes=primary</c>.
    /// </summary>
    public string? ConnectionString { get; set; }
}
