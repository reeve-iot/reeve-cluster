using System.Data;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class OwnershipEdgeTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public OwnershipEdgeTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Theory]
    [InlineData("assert_owner")]
    [InlineData("owner_is")]
    public async Task I32_RC002_is_raised_before_any_lock_even_while_a_change_of_owner_holds_the_row(string function)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var holder = await db.OpenAsync();
        await using var held = await holder.BeginTransactionAsync();
        await ExecAsync(holder, "SELECT 1 FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' FOR UPDATE");

        await using var caller = await db.OpenAsync();
        await ExecAsync(caller, "SET lock_timeout = '2s'");
        var state = await SqlStateOfAsync(() => ExecAsync(caller, $"SELECT reeve_cluster.{function}('unit', 'k', @t, @tl)", ("t", token), ("tl", db.PreviousTimeline)));

        Assert.Equal("RC002", state);
    }

    [Fact]
    public async Task I11_assert_owner_under_REPEATABLE_READ_behind_a_committing_change_of_owner_fails_with_40001_not_RC001()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        await db.AddMemberAsync("b");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        Assert.NotNull(await AdoptAsync(adopter, "unit", "k", token, "b"));

        await using var owner = await db.OpenAsync();
        var ownerPid = owner.ProcessID;
        var fenced = Task.Run(async () =>
        {
            await using var transaction = await owner.BeginTransactionAsync(IsolationLevel.RepeatableRead);
            await ExecAsync(owner, "SET LOCAL transaction_timeout = '30s'");
            await ExecAsync(owner, "SELECT reeve_cluster.assert_owner('unit', 'k', @t, @tl)", ("t", token), ("tl", db.Timeline));
            await transaction.CommitAsync();
        });
        await db.WaitUntilBlockedAsync(ownerPid, TimeSpan.FromSeconds(30));
        await adoption.CommitAsync();

        var ex = await Assert.ThrowsAsync<PostgresException>(() => fenced);
        Assert.Equal(PostgresErrorCodes.SerializationFailure, ex.SqlState);
    }

    [Fact]
    public async Task I11_the_lease_is_judged_on_clock_timestamp_not_on_the_transactions_start()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var connection = await db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '2 seconds' WHERE replica_id = 'a'");
        Assert.True(await ScalarAsync<bool>(connection, "SELECT now() < lease_expires_at FROM reeve_cluster.member WHERE replica_id = 'a'"), "Precondition: the transaction started inside the lease.");
        await ExecAsync(connection, "SELECT pg_sleep(3)");
        Assert.True(await ScalarAsync<bool>(connection, "SELECT clock_timestamp() > lease_expires_at FROM reeve_cluster.member WHERE replica_id = 'a'"), "Precondition: the lease has expired.");

        Assert.False(await ScalarAsync<bool>(connection, "SELECT reeve_cluster.owner_is('unit', 'k', @t, @tl)", ("t", token), ("tl", db.Timeline)));
    }

    [Fact]
    public async Task Null_arguments_are_refused()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        await using var c = await db.OpenAsync();

        Assert.Equal("RC001", await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.assert_owner('unit', 'k', NULL::bigint, @tl)", ("tl", db.Timeline))));
        Assert.False(await ScalarAsync<bool>(c, "SELECT reeve_cluster.owner_is('unit', 'k', NULL::bigint, @tl)", ("tl", db.Timeline)));
        Assert.Equal("RC002", await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.assert_owner('unit', 'k', @t, NULL::bigint)", ("t", token))));
        Assert.Equal("RC002", await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.owner_is('unit', 'k', @t, NULL::bigint)", ("t", token))));
        Assert.Equal("RC001", await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.assert_owner(NULL::text, 'k', @t, @tl)", ("t", token), ("tl", db.Timeline))));
    }

    [Fact]
    public async Task Every_update_of_an_owned_row_sets_updated_at_to_the_time_of_the_write()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        await using var c = await db.OpenAsync();
        const string read = "SELECT updated_at FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k'";

        var created = await ScalarAsync<DateTime>(c, read);
        await ExecAsync(c, "SELECT pg_sleep(0.05)");
        Assert.Equal(1, await ExecAsync(c, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k' AND token = @t", ("t", token)));
        var released = await ScalarAsync<DateTime>(c, read);
        Assert.True(released > created, $"{released:O} > {created:O}");

        await ExecAsync(c, "UPDATE reeve_cluster.owned SET updated_at = '2000-01-01' WHERE kind = 'unit' AND key = 'k'");
        Assert.True(await ScalarAsync<DateTime>(c, read) >= released, "An update set updated_at back past the time of its own write.");

        Assert.Equal(0, await ExecAsync(c, "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' AND owner_replica IS NULL AND token = @t AND updated_at < clock_timestamp() - interval '1 hour'", ("t", token)));
    }

    [Fact]
    public async Task I11_a_fenced_transaction_behind_an_adoption_that_rolls_back_passes()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        await db.AddMemberAsync("b");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var adopter = await db.OpenAsync();
        var adoption = await adopter.BeginTransactionAsync();
        Assert.NotNull(await AdoptAsync(adopter, "unit", "k", token, "b"));

        await using var owner = await db.OpenAsync();
        var ownerPid = owner.ProcessID;
        var fenced = Task.Run(async () =>
        {
            await using var transaction = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, TimeSpan.FromSeconds(30));
            await transaction.CommitAsync();
        });
        await db.WaitUntilBlockedAsync(ownerPid, TimeSpan.FromSeconds(30));
        Assert.False(fenced.IsCompleted);

        await adoption.RollbackAsync();
        await adoption.DisposeAsync();
        await fenced;
    }

    [Fact]
    public async Task I11_an_open_fenced_transaction_blocks_the_sweeps_delete_and_one_behind_a_committed_delete_raises_RC001()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        const string delete = "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' AND token = @t";

        await using var owner = await db.OpenAsync();
        await using (var transaction = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, TimeSpan.FromSeconds(30)))
        {
            await using var blocked = await db.OpenAsync();
            await ExecAsync(blocked, "SET lock_timeout = '500ms'");
            Assert.Equal(PostgresErrorCodes.LockNotAvailable, await SqlStateOfAsync(() => ExecAsync(blocked, delete, ("t", token))));
            await transaction.CommitAsync();
        }

        await using var sweeper = await db.OpenAsync();
        var sweep = await sweeper.BeginTransactionAsync();
        Assert.Equal(1, await ExecAsync(sweeper, delete, ("t", token)));

        var ownerPid = owner.ProcessID;
        var fenced = Task.Run(async () =>
        {
            await using var transaction = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, TimeSpan.FromSeconds(30));
            await transaction.CommitAsync();
        });
        await db.WaitUntilBlockedAsync(ownerPid, TimeSpan.FromSeconds(30));
        await sweep.CommitAsync();
        await sweep.DisposeAsync();

        var ex = await Assert.ThrowsAsync<PostgresException>(() => fenced);
        Assert.Equal("RC001", ex.SqlState);
    }

    [Fact]
    public async Task I11_owner_is_keeps_the_share_lock_when_it_answers_false()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var c = await db.OpenAsync();
        await using var transaction = await c.BeginTransactionAsync();
        Assert.False(await ScalarAsync<bool>(c, "SELECT reeve_cluster.owner_is('unit', 'k', @t, @tl)", ("t", token + 1_000), ("tl", db.Timeline)));
        Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"), "owner_is answered false and holds FOR SHARE.");
    }

    [Fact]
    public async Task S5_1_release_request_has_no_foreign_key_to_owned_which_would_break_the_sweeps_order()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        await using var c = await db.OpenAsync();
        Assert.Equal(0L, await ScalarAsync<long>(c, "SELECT count(*) FROM pg_constraint WHERE conrelid = 'reeve_cluster.release_request'::regclass AND contype = 'f'"));
        await ExecAsync(c, "ALTER TABLE reeve_cluster.release_request ADD FOREIGN KEY (kind, key) REFERENCES reeve_cluster.owned (kind, key)");

        await using var owner = await db.OpenAsync();
        await using (var fenced = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, TimeSpan.FromSeconds(30)))
        {
            await using var placer = await db.OpenAsync();
            await ExecAsync(placer, "SET lock_timeout = '1s'");
            Assert.Equal(1, await ExecAsync(placer, "INSERT INTO reeve_cluster.release_request (kind, key, token, requested_at) VALUES ('unit', 'k', @t, clock_timestamp())", ("t", token)));
            await fenced.CommitAsync();
        }

        await ExecAsync(c, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k'");
        await using var sweep = await c.BeginTransactionAsync();
        Assert.Equal(PostgresErrorCodes.ForeignKeyViolation, await SqlStateOfAsync(() => ExecAsync(c, "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' AND owner_replica IS NULL AND token = @t", ("t", token))));
    }
}
