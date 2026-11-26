using Int.Claims;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class PrimitiveRetentionTests : IClassFixture<PostgresFixture>
{
    private const string Live = "clock_timestamp() + interval '1 hour'";
    private const string InsideOwned = "clock_timestamp() - interval '59 minutes'";
    private const string PastOwned = "clock_timestamp() - interval '61 minutes'";
    private const string InsideOnce = "clock_timestamp() - interval '599 minutes'";
    private const string PastOnce = "clock_timestamp() - interval '601 minutes'";

    private readonly PostgresFixture postgres;

    public PrimitiveRetentionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task Claims_a_live_one_and_one_inside_the_horizon_survive_and_one_past_it_goes()
    {
        await using var rig = await RigAsync();
        await InsertClaimAsync(rig, "live", Live);
        await InsertClaimAsync(rig, "inside", InsideOwned);
        await InsertClaimAsync(rig, "past", PastOwned);

        Assert.Equal(1, await rig.Claims.Retention.DeleteClaimsAsync(10, CancellationToken.None));

        Assert.NotNull(await rig.Claims.RowAsync("live"));
        Assert.NotNull(await rig.Claims.RowAsync("inside"));
        Assert.Null(await rig.Claims.RowAsync("past"));
    }

    [Fact]
    public async Task Claims_leave_the_once_and_the_flight_records_to_their_own_retention()
    {
        await using var rig = await RigAsync();
        await InsertOnceAsync(rig, "reeve.once:o", PastOwned, "at_most_once", complete: true);
        await InsertFlightAsync(rig, "reeve.flight:f", PastOwned, resultEnds: null);

        Assert.Equal(0, await rig.Claims.Retention.DeleteClaimsAsync(10, CancellationToken.None));

        Assert.Equal(2, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task A_key_whose_row_retention_deleted_is_granted_again()
    {
        await using var rig = await RigAsync();
        var a = await rig.Claims.AddReplicaAsync("a");
        await InsertClaimAsync(rig, "k", PastOwned);
        Assert.Equal(1, await rig.Claims.Retention.DeleteClaimsAsync(10, CancellationToken.None));

        Assert.IsType<ClaimResult.Won>(await a.Claims.ClaimAsync("k", TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Claims_delete_waits_for_an_open_claim_fenced_transaction_then_deletes_the_ended_row()
    {
        await using var rig = await RigAsync();
        await InsertClaimAsync(rig, "k", PastOwned);
        await using var holder = await rig.Claims.Db.OpenAsync();
        await using var held = await holder.BeginTransactionAsync();
        await using (var share = new NpgsqlCommand("SELECT 1 FROM reeve_cluster.claim WHERE key = 'k' FOR SHARE", holder, held))
        {
            _ = await share.ExecuteScalarAsync();
        }

        var deletion = rig.Claims.Retention.DeleteClaimsAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        Assert.False(deletion.IsCompleted);
        await held.CommitAsync();

        Assert.Equal(1, await deletion);
    }

    [Fact]
    public async Task Once_a_valid_record_and_one_inside_the_once_horizon_survive_although_the_owned_row_retention_passed()
    {
        await using var rig = await RigAsync();
        await InsertOnceAsync(rig, "reeve.once:valid", Live, "at_most_once", complete: false);
        await InsertOnceAsync(rig, "reeve.once:inside", PastOwned, "at_most_once", complete: true);
        await InsertOnceAsync(rig, "reeve.once:inside-edge", InsideOnce, "at_most_once", complete: true);

        Assert.Equal(0, await rig.Claims.Retention.DeleteOnceAsync(10, CancellationToken.None));

        Assert.Equal(3, await rig.Claims.CountAsync());
    }

    [Theory]
    [InlineData("at_most_once", true)]
    [InlineData("at_most_once", false)]
    [InlineData("at_least_once", true)]
    public async Task Once_a_record_past_the_horizon_goes_and_the_key_can_run_again(string mode, bool complete)
    {
        await using var rig = await RigAsync();
        await InsertOnceAsync(rig, "reeve.once:k", PastOnce, mode, complete);

        Assert.Equal(1, await rig.Claims.Retention.DeleteOnceAsync(10, CancellationToken.None));

        Assert.Null(await rig.Claims.RowAsync("reeve.once:k"));
    }

    [Fact]
    public async Task Once_an_incomplete_at_least_once_record_survives_whatever_its_age()
    {
        await using var rig = await RigAsync();
        await InsertOnceAsync(rig, "reeve.once:owed", "clock_timestamp() - interval '100 days'", "at_least_once", complete: false);

        Assert.Equal(0, await rig.Claims.Retention.DeleteOnceAsync(10, CancellationToken.None));

        Assert.NotNull(await rig.Claims.RowAsync("reeve.once:owed"));
    }

    [Fact]
    public async Task Once_deletes_oldest_first_at_most_the_limit_and_leaves_plain_and_flight_rows()
    {
        await using var rig = await RigAsync();
        for (var i = 0; i < 4; i++)
        {
            await InsertOnceAsync(rig, $"reeve.once:k{i}", $"clock_timestamp() - interval '{700 + i} minutes'", "at_most_once", complete: true);
        }

        await InsertClaimAsync(rig, "plain", "clock_timestamp() - interval '100 days'");
        await InsertFlightAsync(rig, "reeve.flight:f", "clock_timestamp() - interval '100 days'", resultEnds: null);

        Assert.Equal(2, await rig.Claims.Retention.DeleteOnceAsync(2, CancellationToken.None));

        Assert.Null(await rig.Claims.RowAsync("reeve.once:k3"));
        Assert.Null(await rig.Claims.RowAsync("reeve.once:k2"));
        Assert.NotNull(await rig.Claims.RowAsync("reeve.once:k1"));
        Assert.Equal(4, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task Once_a_retake_that_locked_the_row_first_keeps_it_when_the_delete_has_waited()
    {
        await using var rig = await RigAsync();
        await InsertOnceAsync(rig, "reeve.once:k", PastOnce, "at_most_once", complete: false);
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var update = new NpgsqlCommand(
            "UPDATE reeve_cluster.claim SET token = gen_random_uuid(), expires_at = clock_timestamp() + interval '1 hour' WHERE key = 'reeve.once:k'", connection, transaction))
        {
            _ = await update.ExecuteNonQueryAsync();
        }

        var deletion = rig.Claims.Retention.DeleteOnceAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        await transaction.CommitAsync();

        Assert.Equal(0, await deletion);
        Assert.NotNull(await rig.Claims.RowAsync("reeve.once:k"));
    }

    [Fact]
    public async Task Once_nothing_is_deleted_while_the_retention_is_unset()
    {
        await using var rig = await RigAsync();
        rig.Claims.Options.Value.OnceRetention = null;
        await InsertOnceAsync(rig, "reeve.once:old", "clock_timestamp() - interval '1000 days'", "at_most_once", complete: true);
        await InsertOnceAsync(rig, "reeve.once:doubt", "clock_timestamp() - interval '1000 days'", "at_most_once", complete: false);

        Assert.Equal(0, await rig.Claims.Retention.DeleteOnceAsync(10, CancellationToken.None));

        Assert.Equal(2, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task Flights_a_valid_claim_and_a_live_result_survive_a_claim_that_ended_beyond_the_horizon()
    {
        await using var rig = await RigAsync();
        await InsertFlightAsync(rig, "reeve.flight:valid", Live, resultEnds: null);
        await InsertFlightAsync(rig, "reeve.flight:result", "clock_timestamp() - interval '5 hours'", resultEnds: Live);
        await InsertFlightAsync(rig, "reeve.flight:result-inside", "clock_timestamp() - interval '5 hours'", resultEnds: InsideOwned);

        Assert.Equal(0, await rig.Claims.Retention.DeleteFlightsAsync(10, CancellationToken.None));

        Assert.Equal(3, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task Flights_past_both_the_claim_and_the_result_horizon_go()
    {
        await using var rig = await RigAsync();
        await InsertFlightAsync(rig, "reeve.flight:no-result", PastOwned, resultEnds: null);
        await InsertFlightAsync(rig, "reeve.flight:both", "clock_timestamp() - interval '5 hours'", resultEnds: PastOwned);
        await InsertClaimAsync(rig, "plain", PastOwned);

        Assert.Equal(2, await rig.Claims.Retention.DeleteFlightsAsync(10, CancellationToken.None));

        Assert.Null(await rig.Claims.RowAsync("reeve.flight:no-result"));
        Assert.Null(await rig.Claims.RowAsync("reeve.flight:both"));
        Assert.NotNull(await rig.Claims.RowAsync("plain"));
    }

    [Fact]
    public async Task Flights_a_takeover_that_locked_the_row_first_keeps_it_when_the_delete_has_waited()
    {
        await using var rig = await RigAsync();
        var a = await rig.Claims.AddReplicaAsync("a");
        await InsertFlightAsync(rig, "reeve.flight:k", PastOwned, resultEnds: null);
        var seen = await rig.Claims.RowAsync("reeve.flight:k");
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var takeover = new NpgsqlCommand(FlightStore.TakeoverSql, connection, transaction))
        {
            takeover.Parameters.AddWithValue("key", "reeve.flight:k");
            takeover.Parameters.AddWithValue("expected", seen!.Token);
            takeover.Parameters.AddWithValue("replica", a.Id);
            takeover.Parameters.AddWithValue("epoch", a.Epoch);
            takeover.Parameters.AddWithValue("ttl", TimeSpan.FromHours(1));
            Assert.NotNull(await takeover.ExecuteScalarAsync());
        }

        var deletion = rig.Claims.Retention.DeleteFlightsAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        await transaction.CommitAsync();

        Assert.Equal(0, await deletion);
        Assert.NotNull(await rig.Claims.RowAsync("reeve.flight:k"));
    }

    [Fact]
    public async Task Budgets_the_current_window_and_one_that_ended_inside_the_horizon_survive()
    {
        await using var rig = await RigAsync();
        var a = await rig.Claims.AddReplicaAsync("a");
        Assert.Equal(new BudgetResult.Granted(1), await rig.BudgetOf(a).ConsumeAsync("k", "i", 5, TimeSpan.FromHours(1)));
        await InsertBudgetAsync(rig, "inside", InsideOwned, ids: 2);

        Assert.Equal(new BudgetRetained(0, 0), await rig.Claims.Retention.DeleteBudgetsAsync(10, CancellationToken.None));

        Assert.Equal(2, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget"));
        Assert.Equal(3, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget_id"));
    }

    [Fact]
    public async Task Budgets_a_window_past_the_horizon_goes_with_its_ids_and_no_other_windows_ids()
    {
        await using var rig = await RigAsync();
        await InsertBudgetAsync(rig, "old", PastOwned, ids: 3);
        await InsertBudgetAsync(rig, "old", InsideOwned, ids: 2);
        await InsertBudgetAsync(rig, "other", InsideOwned, ids: 4);

        var retained = await rig.Claims.Retention.DeleteBudgetsAsync(10, CancellationToken.None);

        Assert.Equal(new BudgetRetained(1, 3), retained);
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget WHERE key = 'old'"));
        Assert.Equal(2, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget_id WHERE key = 'old'"));
        Assert.Equal(4, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget_id WHERE key = 'other'"));
    }

    [Fact]
    public async Task Budgets_a_window_with_more_ids_than_a_pass_deletes_goes_when_its_ids_are_gone()
    {
        await using var rig = await RigAsync();
        await InsertBudgetAsync(rig, "old", PastOwned, ids: 25);

        var first = await rig.Claims.Retention.DeleteBudgetsAsync(1, CancellationToken.None);
        Assert.Equal(new BudgetRetained(0, 10), first);
        var second = await rig.Claims.Retention.DeleteBudgetsAsync(1, CancellationToken.None);
        Assert.Equal(new BudgetRetained(0, 10), second);
        var third = await rig.Claims.Retention.DeleteBudgetsAsync(1, CancellationToken.None);
        Assert.Equal(new BudgetRetained(1, 5), third);
        Assert.Equal(0, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget"));
    }

    [Fact]
    public async Task Budgets_a_consume_records_the_end_of_its_window_and_a_longer_period_keeps_the_later_end()
    {
        await using var rig = await RigAsync();
        var a = await rig.Claims.AddReplicaAsync("a");

        _ = await rig.BudgetOf(a).ConsumeAsync("hour", "i1", 5, TimeSpan.FromHours(1));
        Assert.Equal(TimeSpan.FromHours(1), await WindowLengthAsync(rig, "hour"));

        var eighty = TimeSpan.FromDays(29200);
        var hundred = TimeSpan.FromDays(36500);
        _ = await rig.BudgetOf(a).ConsumeAsync("k", "i1", 5, eighty);
        Assert.Equal(eighty, await WindowLengthAsync(rig, "k"));
        _ = await rig.BudgetOf(a).ConsumeAsync("k", "i2", 5, hundred);
        Assert.Equal(hundred, await WindowLengthAsync(rig, "k"));
        _ = await rig.BudgetOf(a).ConsumeAsync("k", "i3", 5, eighty);
        Assert.Equal(hundred, await WindowLengthAsync(rig, "k"));
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget WHERE key = 'k'"));
    }

    [Fact]
    public async Task Budgets_a_row_written_without_the_end_is_kept_whatever_its_age()
    {
        await using var rig = await RigAsync();
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.budget (key, window_start, used) VALUES ('legacy', to_timestamp(0), 1)");
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.budget_id (key, window_start, id) VALUES ('legacy', to_timestamp(0), 'i')");

        Assert.Equal(new BudgetRetained(0, 0), await rig.Claims.Retention.DeleteBudgetsAsync(10, CancellationToken.None));

        Assert.False(await rig.Claims.ScalarAsync<bool>("SELECT isfinite(window_end) FROM reeve_cluster.budget WHERE key = 'legacy'"));
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget_id WHERE key = 'legacy'"));
    }

    [Fact]
    public async Task Budgets_a_window_a_consume_extended_while_the_delete_waited_is_kept_whatever_its_start()
    {
        await using var rig = await RigAsync();
        await rig.Claims.ExecAsync(
            $"INSERT INTO reeve_cluster.budget (key, window_start, used, window_end) VALUES ('k', clock_timestamp() - interval '3 hours', 1, {PastOwned})");
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var extend = new NpgsqlCommand("UPDATE reeve_cluster.budget SET window_end = clock_timestamp() + interval '1 hour' WHERE key = 'k'", connection, transaction))
        {
            _ = await extend.ExecuteNonQueryAsync();
        }

        var deletion = rig.Claims.Retention.DeleteBudgetsAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        await transaction.CommitAsync();

        Assert.Equal(new BudgetRetained(0, 0), await deletion);
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget WHERE key = 'k'"));
    }

    [Fact]
    public async Task Budgets_a_window_without_ids_survives_until_its_horizon()
    {
        await using var rig = await RigAsync();
        await rig.Claims.ExecAsync(
            "INSERT INTO reeve_cluster.budget (key, window_start, used, window_end) VALUES "
            + "('current', clock_timestamp() - interval '30 minutes', 1, clock_timestamp() + interval '30 minutes'), "
            + $"('inside', clock_timestamp() - interval '2 hours', 1, {InsideOwned}), "
            + $"('past', clock_timestamp() - interval '3 hours', 1, {PastOwned})");

        Assert.Equal(new BudgetRetained(1, 0), await rig.Claims.Retention.DeleteBudgetsAsync(10, CancellationToken.None));

        Assert.Equal(0, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget WHERE key = 'past'"));
        Assert.Equal(2, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.budget"));
    }

    [Fact]
    public async Task Budgets_a_day_long_window_ends_a_day_after_its_start_in_a_time_zone_with_daylight_saving()
    {
        await using var rig = await RigAsync();
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SET TimeZone = 'Europe/Warsaw'; WITH fresh AS (SELECT timestamptz '2027-03-27 23:00+00' AS window_start) SELECT {BudgetStore.WindowEndSql} - fresh.window_start FROM fresh",
            connection);
        command.Parameters.AddWithValue("period", TimeSpan.FromDays(1));

        Assert.Equal(TimeSpan.FromDays(1), await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Counters_are_never_deleted_while_the_retention_is_unset()
    {
        await using var rig = await RigAsync();
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.counter (key, value, updated_at) VALUES ('c', 7, clock_timestamp() - interval '1000 days')");

        Assert.Equal(0, await rig.Claims.Retention.DeleteCountersAsync(10, CancellationToken.None));

        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.counter"));
    }

    [Fact]
    public async Task Counters_idle_beyond_the_retention_go_and_a_touched_one_survives()
    {
        await using var rig = await RigAsync();
        rig.Claims.Options.Value.CounterRetention = TimeSpan.FromHours(1);
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.counter (key, value, updated_at) VALUES ('idle', 7, " + PastOwned + "), ('inside', 7, " + InsideOwned + ")");
        _ = await rig.CounterOf().IncrementAsync("touched");

        Assert.Equal(1, await rig.Claims.Retention.DeleteCountersAsync(10, CancellationToken.None));

        Assert.Equal(2, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.counter"));
        Assert.Equal(0, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.counter WHERE key = 'idle'"));
    }

    [Fact]
    public async Task Counters_an_increment_refreshes_the_idle_time_of_an_old_counter()
    {
        await using var rig = await RigAsync();
        rig.Claims.Options.Value.CounterRetention = TimeSpan.FromHours(1);
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.counter (key, value, updated_at) VALUES ('c', 7, " + PastOwned + ")");

        _ = await rig.CounterOf().IncrementAsync("c");

        Assert.Equal(0, await rig.Claims.Retention.DeleteCountersAsync(10, CancellationToken.None));
        Assert.Equal(8, await rig.Claims.ScalarAsync<long>("SELECT value FROM reeve_cluster.counter WHERE key = 'c'"));
    }

    [Fact]
    public async Task Counters_an_increment_that_locked_the_row_first_keeps_it_when_the_delete_has_waited()
    {
        await using var rig = await RigAsync();
        rig.Claims.Options.Value.CounterRetention = TimeSpan.FromHours(1);
        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.counter (key, value, updated_at) VALUES ('c', 7, " + PastOwned + ")");
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var update = new NpgsqlCommand("UPDATE reeve_cluster.counter SET value = value + 1, updated_at = clock_timestamp() WHERE key = 'c'", connection, transaction))
        {
            _ = await update.ExecuteNonQueryAsync();
        }

        var deletion = rig.Claims.Retention.DeleteCountersAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        await transaction.CommitAsync();

        Assert.Equal(0, await deletion);
        Assert.Equal(8, await rig.Claims.ScalarAsync<long>("SELECT value FROM reeve_cluster.counter WHERE key = 'c'"));
    }

    [Fact]
    public async Task Slots_a_live_row_and_one_inside_the_horizon_survive_and_one_past_it_goes()
    {
        await using var rig = await RigAsync();
        await InsertSlotAsync(rig, "live", Live);
        await InsertSlotAsync(rig, "inside", InsideOwned);
        await InsertSlotAsync(rig, "past", PastOwned);

        Assert.Equal(1, await rig.Claims.Retention.DeleteSlotsAsync(10, CancellationToken.None));

        Assert.Equal(2, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.slot"));
        Assert.Equal(0, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.slot WHERE key = 'past'"));
    }

    [Fact]
    public async Task Slots_one_run_deletes_at_most_the_limit_oldest_first()
    {
        await using var rig = await RigAsync();
        await InsertSlotAsync(rig, "m64", "clock_timestamp() - interval '64 minutes'");
        await InsertSlotAsync(rig, "m62", "clock_timestamp() - interval '62 minutes'");
        await InsertSlotAsync(rig, "m63", "clock_timestamp() - interval '63 minutes'");
        await InsertSlotAsync(rig, "m61", "clock_timestamp() - interval '61 minutes'");

        Assert.Equal(2, await rig.Claims.Retention.DeleteSlotsAsync(2, CancellationToken.None));

        Assert.Equal("m61,m62", await rig.Claims.ScalarAsync<string>("SELECT string_agg(key, ',' ORDER BY key) FROM reeve_cluster.slot"));
    }

    [Fact]
    public async Task Slots_delete_waits_for_an_open_slot_fenced_transaction_then_deletes_the_ended_row()
    {
        await using var rig = await RigAsync();
        await InsertSlotAsync(rig, "k", PastOwned);
        await using var holder = await rig.Claims.Db.OpenAsync();
        await using var held = await holder.BeginTransactionAsync();
        await using (var share = new NpgsqlCommand("SELECT 1 FROM reeve_cluster.slot WHERE key = 'k' FOR KEY SHARE", holder, held))
        {
            _ = await share.ExecuteScalarAsync();
        }

        var deletion = rig.Claims.Retention.DeleteSlotsAsync(10, CancellationToken.None);
        await rig.Claims.Db.WaitForALockWaiterAsync();
        Assert.False(deletion.IsCompleted);
        await held.CommitAsync();

        Assert.Equal(1, await deletion);
    }

    [Fact]
    public async Task The_migration_adds_the_columns_with_defaults_and_the_index()
    {
        await using var rig = await RigAsync();

        Assert.Equal(1, await rig.Claims.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'reeve_cluster' AND table_name = 'budget' AND column_name = 'window_end' AND is_nullable = 'NO'"));
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'reeve_cluster' AND table_name = 'counter' AND column_name = 'updated_at' AND is_nullable = 'NO'"));
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM pg_indexes WHERE schemaname = 'reeve_cluster' AND indexname = 'budget_window_end'"));
        Assert.Equal(1, await rig.Claims.ScalarAsync<long>(
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'reeve_cluster' AND indexname = 'claim_expires_plain' AND indexdef LIKE '%once_mode IS NULL%'"));

        await rig.Claims.ExecAsync("INSERT INTO reeve_cluster.budget (key, window_start, used) VALUES ('legacy', to_timestamp(0), 1)");
        Assert.False(await rig.Claims.ScalarAsync<bool>("SELECT isfinite(window_end) FROM reeve_cluster.budget WHERE key = 'legacy'"));
    }

    [Fact]
    public async Task A_backlog_larger_than_the_pass_limit_drains_in_one_pass()
    {
        await using var rig = await RigAsync();
        await rig.Claims.ExecAsync(
            "INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at) "
            + $"SELECT 'k' || g, 'x', 1, gen_random_uuid(), {PastOwned} - g * interval '1 second' FROM generate_series(1, 35) g");
        var unit = new PrimitiveRetention(rig.Claims.Retention, rig.Claims.Options, TimeProvider.System, NullLogger<PrimitiveRetention>.Instance, passLimit: 10);

        Assert.Equal(35, await unit.PassAsync(CancellationToken.None));

        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Theory]
    [InlineData("claims", "claim_expires_plain")]
    [InlineData("flights", "claim_expires_plain")]
    [InlineData("once", "claim_expires_at")]
    [InlineData("budget ids", "budget_window_end")]
    [InlineData("budget windows", "budget_window_end")]
    [InlineData("commands", "command_finished")]
    public async Task Plans_the_horizon_is_an_index_condition_and_not_a_volatile_filter(string statement, string index)
    {
        await using var rig = await RigAsync();

        await rig.Claims.ExecAsync(
            "INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at, once_mode, once_effect, once_ttl) "
            + "SELECT 'reeve.once:' || g, 'x', 1, gen_random_uuid(), clock_timestamp() - interval '100 days' - g * interval '1 second', 'at_most_once', 'e', interval '1 minute' "
            + "FROM generate_series(1, 3000) g");
        await rig.Claims.ExecAsync("ANALYZE reeve_cluster.claim");
        var sql = statement switch
        {
            "claims" => RetentionStore.ClaimsSql,
            "flights" => RetentionStore.FlightsSql,
            "once" => RetentionStore.OnceSql,
            "budget ids" => RetentionStore.BudgetIdsSql,
            "budget windows" => RetentionStore.BudgetWindowsSql,
            _ => Reeve.Cluster.Commands.CommandStore.RetentionSql,
        };
        await using var connection = await rig.Claims.Db.OpenAsync();
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", connection))
        {
            _ = await off.ExecuteNonQueryAsync();
        }

        await using var explain = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection);
        explain.Parameters.AddWithValue("retention", TimeSpan.FromHours(1));
        explain.Parameters.AddWithValue("limit", 10);
        if (statement == "budget ids")
        {
            explain.Parameters.AddWithValue("idLimit", 100);
        }

        var lines = new List<string>();
        await using (var reader = await explain.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                lines.Add(reader.GetString(0));
            }
        }

        var plan = string.Join('\n', lines);
        Assert.DoesNotContain("clock_timestamp", plan);
        Assert.Contains(index, plan);
        Assert.Matches(@"Index Cond: .*(expires_at|window_end|completed_at) < \(now\(\)", plan);
    }

    private async Task<PrimitivesRig> RigAsync()
    {
        var rig = await PrimitivesRig.CreateAsync(postgres);
        rig.Claims.Options.Value.OwnedRowRetention = TimeSpan.FromHours(1);
        rig.Claims.Options.Value.OnceRetention = TimeSpan.FromHours(10);
        return rig;
    }

    private static Task InsertClaimAsync(PrimitivesRig rig, string key, string expires) =>
        rig.Claims.ExecAsync(
            $"INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at) VALUES (@key, 'x', 1, gen_random_uuid(), {expires})", ("key", key));

    private static Task InsertOnceAsync(PrimitivesRig rig, string key, string expires, string mode, bool complete) =>
        rig.Claims.ExecAsync(
            $"INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at, once_mode, once_effect, once_ttl, completed_at) "
            + $"VALUES (@key, 'x', 1, gen_random_uuid(), {expires}, @mode, 'e', interval '1 minute', {(complete ? "clock_timestamp()" : "NULL")})",
            ("key", key), ("mode", mode));

    private static Task InsertFlightAsync(PrimitivesRig rig, string key, string expires, string? resultEnds) =>
        rig.Claims.ExecAsync(
            $"INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at, result, result_expires_at) "
            + $"VALUES (@key, 'x', 1, gen_random_uuid(), {expires}, {(resultEnds is null ? "NULL" : "'\\x01'::bytea")}, {resultEnds ?? "NULL"})", ("key", key));

    private static Task InsertBudgetAsync(PrimitivesRig rig, string key, string windowEnd, int ids) =>
        rig.Claims.ExecAsync(
            $"""
            WITH w AS (SELECT {windowEnd} AS we),
            b AS (
                INSERT INTO reeve_cluster.budget (key, window_start, used, window_end)
                SELECT @key, we - interval '1 hour', @ids, we FROM w
                RETURNING window_start)
            INSERT INTO reeve_cluster.budget_id (key, window_start, id)
            SELECT @key, b.window_start, g::text FROM b, generate_series(1, @ids) g
            """,
            ("key", key), ("ids", ids));

    private static Task InsertSlotAsync(PrimitivesRig rig, string key, string expires) =>
        rig.Claims.ExecAsync(
            $"INSERT INTO reeve_cluster.slot (key, id, holder_replica, holder_epoch, token, expires_at) VALUES (@key, 'i', 'x', 1, gen_random_uuid(), {expires})", ("key", key));

    private static async Task<TimeSpan> WindowLengthAsync(PrimitivesRig rig, string key) =>
        TimeSpan.FromSeconds(Math.Round((double)await rig.Claims.ScalarAsync<decimal>(
            "SELECT extract(epoch FROM max(window_end - window_start)) FROM reeve_cluster.budget WHERE key = @key", ("key", key))));
}
