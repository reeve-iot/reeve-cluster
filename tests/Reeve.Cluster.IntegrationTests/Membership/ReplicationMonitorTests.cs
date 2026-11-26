using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class ReplicationMonitorTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public ReplicationMonitorTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_single_instance_passes_and_the_section_shows_it()
    {
        var section = await CheckAsync(postgres.ConnectionString);

        Assert.Equal(nameof(ReplicationOutcome.NoStandby), section["outcome"]);
        Assert.Equal(0, section["walsenders"]);
        Assert.Null(section["warning"]);
    }

    [Fact]
    public async Task A_role_without_pg_read_all_stats_sees_a_physical_walsender_and_the_settings()
    {
        var limited = await RoleAsync("limited");
        var local = await RoleAsync("local_commit");
        await Execute("ALTER ROLE limited SET synchronous_commit = on");
        await Execute("ALTER ROLE local_commit SET synchronous_commit = local");
        Assert.False(await Scalar<bool>("SELECT pg_has_role('limited', 'pg_read_all_stats', 'USAGE')"));

        var started = await postgres.ExecAsync(["sh", "-c", "pg_receivewal -D /tmp -U postgres --no-loop >/dev/null 2>&1 &"]);
        Assert.Equal(0, started.ExitCode);
        try
        {
            await Poll.UntilAsync(async () => await Scalar<long>("SELECT count(*) FROM pg_stat_replication") == 1, "pg_receivewal to stream");

            var asynchronous = await CheckAsync(limited);
            Assert.Equal(nameof(ReplicationOutcome.NotSynchronous), asynchronous["outcome"]);
            Assert.Equal(1, asynchronous["walsenders"]);
            Assert.Equal(string.Empty, asynchronous["synchronousStandbyNames"]);
            Assert.Equal("on", asynchronous["synchronousCommit"]);

            await SetStandbyNamesAsync("FIRST 1 (nobody)");
            try
            {
                Assert.Equal(nameof(ReplicationOutcome.Synchronous), (await CheckAsync(limited))["outcome"]);

                var localCommit = await CheckAsync(local);
                Assert.Equal(nameof(ReplicationOutcome.NotSynchronous), localCommit["outcome"]);
                Assert.Equal("local", localCommit["synchronousCommit"]);
            }
            finally
            {
                await SetStandbyNamesAsync(null);
            }
        }
        finally
        {
            await Execute("SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_replication");
            await Poll.UntilAsync(async () => await Scalar<long>("SELECT count(*) FROM pg_stat_replication") == 0, "pg_receivewal to stop");
        }
    }

    [Fact]
    public async Task A_database_that_revoked_the_view_from_public_is_unreadable_and_warns()
    {
        await Execute("REVOKE SELECT ON pg_stat_replication FROM PUBLIC");
        try
        {
            var section = await CheckAsync(await RoleAsync("revoked"));

            Assert.Equal(nameof(ReplicationOutcome.Unreadable), section["outcome"]);
            Assert.Equal(nameof(ReplicationOutcome.Unreadable), section["warning"]);
        }
        finally
        {
            await Execute("GRANT SELECT ON pg_stat_replication TO PUBLIC");
        }
    }

    private async Task SetStandbyNamesAsync(string? names)
    {
        await Execute(names is null ? "ALTER SYSTEM RESET synchronous_standby_names" : $"ALTER SYSTEM SET synchronous_standby_names = '{names}'");
        await Execute("SELECT pg_reload_conf()");
        await Poll.UntilAsync(async () => await Scalar<string>("SELECT current_setting('synchronous_standby_names')") == (names ?? string.Empty), "the reload");
    }

    private async Task<string> RoleAsync(string name)
    {
        await Execute($"CREATE ROLE {name} LOGIN PASSWORD '{name}'");
        return new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Username = name, Password = name }.ConnectionString;
    }

    private async Task Execute(string sql)
    {
        await using var admin = await postgres.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(sql, admin);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<T> Scalar<T>(string sql)
    {
        await using var admin = await postgres.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(sql, admin);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<IReadOnlyDictionary<string, object?>> CheckAsync(string connectionString)
    {
        var options = Options.Create(new ClusterOptions { ConnectionString = connectionString, ReplicaId = "r1" });
        await using var source = new ClusterDataSource(options.Value);
        var monitor = new ReplicationMonitor(source, options, TimeProvider.System, NullLogger<ReplicationMonitor>.Instance);

        await monitor.CheckAsync(CancellationToken.None);

        return (await monitor.CollectAsync(CancellationToken.None)).Values;
    }
}
