using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Reeve.Cluster.Migrations;

namespace Unit.Migrations;

public sealed class ClusterMigrationsModelTests
{
    private static ClusterMigrationsDbContext Context() => new ClusterMigrationsDesignTimeFactory().CreateDbContext([]);

    [Fact]
    public void The_shipped_migrations_cover_every_change_in_the_model()
    {
        using var context = Context();

        Assert.False(context.Database.HasPendingModelChanges());
    }

    [Fact]
    public void The_chain_begins_with_the_baseline_and_keeps_its_history_in_the_clusters_own_table()
    {
        using var context = Context();

        var migrations = context.GetService<IMigrationsAssembly>().Migrations.Keys.Order(StringComparer.Ordinal).ToList();

        Assert.EndsWith("_ClusterInitialBaseline", migrations[0], StringComparison.Ordinal);
        var history = context.GetService<IHistoryRepository>();
        Assert.Contains("reeve_cluster.\"__ClusterMigrationsHistory\"", history.GetCreateIfNotExistsScript(), StringComparison.Ordinal);
    }
}
