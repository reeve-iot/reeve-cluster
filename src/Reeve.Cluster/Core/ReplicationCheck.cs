namespace Reeve.Cluster.Core;

internal enum ReplicationOutcome
{
    NoStandby,

    Synchronous,

    NotSynchronous,

    Unreadable,
}

internal readonly record struct ReplicationSettings(int Walsenders, string StandbyNames, string SynchronousCommit);

internal readonly record struct ReplicationFinding(ReplicationOutcome Outcome, ReplicationSettings Settings);

internal static class ReplicationCheck
{
    public const int NotSynchronousReads = 3;

    public const string Sql = """
        SELECT (SELECT count(*) FROM pg_stat_replication r JOIN pg_stat_activity a ON a.pid = r.pid AND a.datid IS NULL)::int,
               current_setting('synchronous_standby_names'),
               current_setting('synchronous_commit')
        """;

    public static ReplicationFinding Evaluate(ReplicationSettings settings)
    {
        var outcome = settings.Walsenders == 0 ? ReplicationOutcome.NoStandby
            : !string.IsNullOrWhiteSpace(settings.StandbyNames) && settings.SynchronousCommit is "on" or "remote_write" or "remote_apply" ? ReplicationOutcome.Synchronous
            : ReplicationOutcome.NotSynchronous;
        return new ReplicationFinding(outcome, settings);
    }
}
