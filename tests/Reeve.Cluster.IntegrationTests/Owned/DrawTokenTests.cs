using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Schema;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class DrawTokenTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public DrawTokenTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I25_owned_token_is_a_CACHE_1_bigint_sequence()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        Assert.Equal(1L, await ScalarAsync<long>(connection, "SELECT cache_size FROM pg_sequences WHERE schemaname = 'reeve_cluster' AND sequencename = 'owned_token'"));
        Assert.Equal("bigint", await ScalarAsync<string>(connection, "SELECT data_type::text FROM pg_sequences WHERE schemaname = 'reeve_cluster' AND sequencename = 'owned_token'"));
        await SchemaAssertions.EverySequenceIsCacheOneAsync(connection);
    }

    [Fact]
    public async Task I10_a_token_is_the_servers_timeline_times_2_pow_40_plus_the_sequences_value()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        var token = await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");

        Assert.Equal(1L << 40, TimelineUnit);
        Assert.Equal(db.Timeline, TimelineOf(token));
        Assert.Equal(await db.LastTokenAsync(), SequenceValueOf(token));
        Assert.True(token >= db.Timeline * TimelineUnit + 1);
    }

    [Fact]
    public async Task Server_timeline_reads_the_first_eight_hex_digits_of_the_current_WAL_file_name()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        var walFile = await ScalarAsync<string>(connection, "SELECT pg_walfile_name(pg_current_wal_insert_lsn())");
        Assert.Equal(Convert.ToInt64(walFile[..8], 16), db.Timeline);
        Assert.True(db.Timeline >= 1);
    }

    [Fact]
    public async Task I10_draws_on_one_timeline_increase()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        var tokens = new List<long>();
        for (var i = 0; i < 20; i++)
        {
            tokens.Add(await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()"));
        }

        Assert.Equal(tokens.Order(), tokens);
        Assert.Equal(tokens.Count, tokens.Distinct().Count());
        Assert.All(tokens, t => Assert.Equal(db.Timeline, TimelineOf(t)));
    }

    [Fact]
    public async Task I10_a_draw_that_rolls_back_costs_only_its_value()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();
        var first = await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");

        long rolledBack;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            rolledBack = await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");
            await transaction.RollbackAsync();
        }

        var next = await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");
        Assert.True(first < rolledBack && rolledBack < next, $"{first} < {rolledBack} < {next}: a rolled-back draw's value must not come back.");
    }

    [Fact]
    public async Task I4_a_draw_locks_nothing_and_writes_nothing()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await ExecAsync(connection, "SELECT reeve_cluster.draw_token()");

        Assert.True(await db.CanLockClusterAsync(), "The draw holds a lock on the cluster row.");
        Assert.Equal(0L, await ScalarAsync<long>(
            connection,
            "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype IN ('relation', 'tuple') AND relation IN (SELECT oid FROM pg_class WHERE relnamespace = 'reeve_cluster'::regnamespace AND relkind = 'r')"));
        Assert.Equal(0L, await ScalarAsync<long>(
            connection,
            "SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint FROM pg_stat_xact_user_tables WHERE schemaname = 'reeve_cluster'"));
    }

    [Fact]
    public async Task A_draw_reads_no_cluster_row()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();
        await ExecAsync(connection, "DELETE FROM reeve_cluster.cluster");

        var token = await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");

        Assert.Equal(db.Timeline, TimelineOf(token));
    }

    [Fact]
    public async Task The_cluster_table_has_no_timeline_column()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        Assert.Equal(0L, await ScalarAsync<long>(
            connection,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'reeve_cluster' AND table_name = 'cluster' AND column_name = 'timeline'"));
    }

    [Fact]
    public async Task I10_tokens_increase_along_a_keys_history_across_release_sweep_and_re_creation()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        await db.AddMemberAsync("b");
        await using var connection = await db.OpenAsync();

        var created = await db.CreateOwnedAsync("unit", "k", "a");

        await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k' AND token = @t", ("t", created));
        Assert.Equal(created, await ScalarAsync<long>(connection, "SELECT token FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k'"));
        long adopted;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            adopted = (await AdoptAsync(connection, "unit", "k", created, "b"))!.Value;
            await transaction.CommitAsync();
        }

        var other = await db.CreateOwnedAsync("unit", "other", "a");

        await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k' AND token = @t", ("t", adopted));
        Assert.Equal(1, await ExecAsync(connection, "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' AND owner_replica IS NULL AND token = @t", ("t", adopted)));
        var recreated = await db.CreateOwnedAsync("unit", "k", "a");

        Assert.True(created < adopted, $"{created} < {adopted}");
        Assert.True(adopted < other, $"{adopted} < {other}");
        Assert.True(other < recreated, $"{other} < {recreated}: the re-created key's token must be above every token drawn before it");
    }

    [Fact]
    public async Task The_ownership_functions_are_volatile()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using var connection = await db.OpenAsync();

        foreach (var function in new[] { "draw_token", "server_timeline", "assert_owner", "owner_is" })
        {
            Assert.Equal('v', await ScalarAsync<char>(connection, "SELECT provolatile FROM pg_proc WHERE oid = to_regproc(@f)", ("f", "reeve_cluster." + function)));
        }
    }
}
