using Reeve.Cluster.Log;
using Reeve.Cluster.Migrations;

namespace Unit.Core;

public sealed class AdvisoryKeyTests
{
    [Fact]
    public void The_change_log_key_is_REEVCLOG()
    {
        Assert.Equal(5928220721301704519L, ChangeLogLock.Key);
        Assert.Equal(0x52454556434C4F47L, ChangeLogLock.Key);
    }

    [Fact]
    public void The_migration_runner_key_is_REEVMIGR()
    {
        Assert.Equal(5928220721469278034L, ClusterMigrationRunner.AdvisoryLockKey);
        Assert.Equal(0x524545564D494752L, ClusterMigrationRunner.AdvisoryLockKey);
    }
}
