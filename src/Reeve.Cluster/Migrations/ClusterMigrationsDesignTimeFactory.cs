using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Npgsql;

namespace Reeve.Cluster.Migrations;

internal sealed class ClusterMigrationsDesignTimeFactory : IDesignTimeDbContextFactory<ClusterMigrationsDbContext>
{
    private const string NeverDialled = "Host=cluster-design-time.invalid;Database=cluster;Username=none;Password=none";

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Only the dotnet-ef design-time tooling calls this factory, in a JIT-compiled process.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only the dotnet-ef design-time tooling calls this factory, in a JIT-compiled process.")]
    public ClusterMigrationsDbContext CreateDbContext(string[] args) => new(Options(NeverDialled));

    internal static DbContextOptions<ClusterMigrationsDbContext> Options(string connectionString) =>
        new DbContextOptionsBuilder<ClusterMigrationsDbContext>()
            .UseNpgsql(connectionString, npgsql => npgsql.MigrationsHistoryTable(ClusterMigrationsDbContext.HistoryTable, ClusterMigrationsDbContext.Schema))
            .Options;

    internal static DbContextOptions<ClusterMigrationsDbContext> Options(NpgsqlDataSource dataSource, int statementTimeout) =>
        new DbContextOptionsBuilder<ClusterMigrationsDbContext>()
            .UseNpgsql(dataSource, npgsql =>
            {
                npgsql.MigrationsHistoryTable(ClusterMigrationsDbContext.HistoryTable, ClusterMigrationsDbContext.Schema);
                if (statementTimeout > 0)
                {
                    npgsql.CommandTimeout(statementTimeout);
                }
            })
            .ConfigureWarnings(w => w.Log(RelationalEventId.PendingModelChangesWarning))
            .Options;
}
