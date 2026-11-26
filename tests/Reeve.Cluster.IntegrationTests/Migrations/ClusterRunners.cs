using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Migrations;

namespace Int.Migrations;

internal static class ClusterRunners
{
    public static ClusterMigrationRunner For(string connectionString) =>
        new(Options.Create(new ClusterMigrationOptions { ConnectionString = connectionString }), NullLogger<ClusterMigrationRunner>.Instance);
}
