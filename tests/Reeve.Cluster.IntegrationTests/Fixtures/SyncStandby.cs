using Npgsql;
using Reeve.Cluster.Harness.Containers;

namespace Int;

/// <summary>
/// The server's synchronous standby that never connects, for the tests of a commit that waits for the standby: every
/// step waits on the server's state, never on a fixed sleep, so a runner that stalls for seconds only makes a test slower.
/// </summary>
/// <remarks>
/// The container runs with <c>synchronous_commit=off</c> on its command line; a test sets the database's own value first, and the
/// connections opened after it commit synchronously.
/// </remarks>
internal static class SyncStandby
{
    private const string Ghost = "reeve_ghost_standby";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A standby that never connects: <c>ALTER SYSTEM</c> and a reload, then a returns-once-it-holds wait. A reload reaches the
    /// checkpointer, which publishes the setting to the backends a moment later; a commit made before that does not wait. The wait
    /// commits a probe from a session of its own until one of them is held for the standby, so no test starts its clock earlier.
    /// </summary>
    public static async Task StallAsync(PostgresFixture postgres)
    {
        await SetAsync(postgres, Ghost);
        await UntilItHoldsAsync(postgres);
    }

    /// <summary>The standby requirement dropped (empty) or set; the reload is sent, nothing is waited for.</summary>
    public static async Task SetAsync(PostgresFixture postgres, string names)
    {
        await using var admin = await postgres.OpenConnectionAsync();
        await using (var alter = new NpgsqlCommand($"ALTER SYSTEM SET synchronous_standby_names = '{names}'", admin))
        {
            await alter.ExecuteNonQueryAsync();
        }

        await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", admin);
        await reload.ExecuteNonQueryAsync();
    }

    /// <summary>The backends of <paramref name="database"/> that wait for the standby now.</summary>
    public static async Task<long> WaitersAsync(PostgresFixture postgres, string database)
    {
        await using var admin = await postgres.OpenConnectionAsync();
        return await WaitersAsync(admin, database);
    }

    /// <summary>Returns once a backend of <paramref name="database"/> waits for the standby (a bounded poll of the server's own view).</summary>
    public static async Task UntilAWaiterAsync(PostgresFixture postgres, string database, TimeSpan? bound = null)
    {
        var until = DateTime.UtcNow + (bound ?? Bound);
        await using var admin = await postgres.OpenConnectionAsync();
        while (await WaitersAsync(admin, database) == 0)
        {
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException($"No backend of {database} waited for the synchronous standby.");
            }

            await Task.Delay(10);
        }
    }

    private static async Task<long> WaitersAsync(NpgsqlConnection admin, string database)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE datname = @db AND wait_event = 'SyncRep'", admin);
        command.Parameters.AddWithValue("db", database);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    // Commits a transactional message with synchronous_commit on until a commit is held: the requirement is then in force for
    // every backend. The held probe is cancelled from a second session; a cancelled wait for the standby ends in a warning and
    // the commit has landed, so the probe returns.
    private static async Task UntilItHoldsAsync(PostgresFixture postgres)
    {
        var until = DateTime.UtcNow + Bound;
        await using var probe = await postgres.OpenConnectionAsync();
        await using var watcher = await postgres.OpenConnectionAsync();
        await using (var on = new NpgsqlCommand("SET synchronous_commit = on", probe))
        {
            await on.ExecuteNonQueryAsync();
        }

        int pid;
        await using (var self = new NpgsqlCommand("SELECT pg_backend_pid()", probe))
        {
            pid = (int)(await self.ExecuteScalarAsync())!;
        }

        while (true)
        {
            await using var commit = new NpgsqlCommand("SELECT pg_logical_emit_message(true, 'reeve-probe', 'x')", probe);
            var running = commit.ExecuteScalarAsync();
            var held = false;
            while (!running.IsCompleted && !held)
            {
                await using var look = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE pid = @pid AND wait_event = 'SyncRep'", watcher);
                look.Parameters.AddWithValue("pid", pid);
                held = (long)(await look.ExecuteScalarAsync())! > 0;
                if (!held && DateTime.UtcNow > until)
                {
                    throw new TimeoutException("The synchronous standby never took effect.");
                }

                if (!held)
                {
                    await Task.Delay(10);
                }
            }

            if (held)
            {
                await using var cancel = new NpgsqlCommand("SELECT pg_cancel_backend(@pid)", watcher);
                cancel.Parameters.AddWithValue("pid", pid);
                await cancel.ExecuteScalarAsync();
                await running;
                return;
            }

            await running; // it committed without waiting: the setting is not in force yet
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException("The synchronous standby never took effect.");
            }

            await Task.Delay(20);
        }
    }
}
