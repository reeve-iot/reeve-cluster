using Npgsql;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class AssertOwnerTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public AssertOwnerTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private static async Task<bool> OwnerIsAsync(NpgsqlConnection connection, string kind, string key, long token, long timeline) =>
        await ScalarAsync<bool>(connection, "SELECT reeve_cluster.owner_is(@kind, @key, @token, @timeline)", ("kind", kind), ("key", key), ("token", token), ("timeline", timeline));

    private static Task AssertOwnerAsync(NpgsqlConnection connection, string kind, string key, long token, long timeline) =>
        ExecAsync(connection, "SELECT reeve_cluster.assert_owner(@kind, @key, @token, @timeline)", ("kind", kind), ("key", key), ("token", token), ("timeline", timeline));

    [Fact]
    public async Task I11_assert_owner_passes_for_the_current_tenure_and_holds_the_row_FOR_SHARE_until_the_transaction_ends()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var connection = await db.OpenAsync();
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await AssertOwnerAsync(connection, "unit", "k", token, db.Timeline);

            Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
            Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR UPDATE"));
            Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR SHARE"));
            Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR KEY SHARE"));

            await transaction.CommitAsync();
        }

        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
    }

    [Fact]
    public async Task I11_owner_is_answers_true_for_the_current_tenure_and_takes_the_same_lock()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var connection = await db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        Assert.True(await OwnerIsAsync(connection, "unit", "k", token, db.Timeline));
        Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR SHARE"));
    }

    public static TheoryData<string> NotTheOwner =>
    [
        "another token",
        "a token of the key's previous tenure",
        "no row",
        "a free row",
        "an expired lease",
        "a fenced member",
        "a draining member",
        "a joining member",
        "another epoch",
        "no member row",
    ];

    private static async Task<long> ArrangeAsync(OwnedDatabase db, string @case)
    {
        await using var connection = await db.OpenAsync();
        switch (@case)
        {
            case "another token":
                await db.AddMemberAsync("a");
                return await db.CreateOwnedAsync("unit", "k", "a") + 1_000;
            case "a token of the key's previous tenure":
            {
                await db.AddMemberAsync("a");
                await db.AddMemberAsync("b");
                var old = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k'");
                await using var transaction = await connection.BeginTransactionAsync();
                await AdoptAsync(connection, "unit", "k", old, "b");
                await transaction.CommitAsync();
                return old;
            }

            case "no row":
                await db.AddMemberAsync("a");
                return await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()");
            case "a free row":
            {
                await db.AddMemberAsync("a");
                var token = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k'");
                return token;
            }

            case "an expired lease":
            {
                await db.AddMemberAsync("a");
                var token = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'");
                return token;
            }

            case "a fenced member":
            case "a draining member":
            case "a joining member":
            {
                await db.AddMemberAsync("a");
                var token = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "UPDATE reeve_cluster.member SET state = @state WHERE replica_id = 'a'", ("state", @case.Split(' ')[1]));
                return token;
            }

            case "another epoch":
            {
                await db.AddMemberAsync("a");
                var token = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'a'");
                return token;
            }

            case "no member row":
            {
                await db.AddMemberAsync("a");
                var token = await db.CreateOwnedAsync("unit", "k", "a");
                await ExecAsync(connection, "DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");
                return token;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(@case), @case, null);
        }
    }

    [Theory]
    [MemberData(nameof(NotTheOwner))]
    public async Task I11_assert_owner_raises_RC001_and_owner_is_answers_false_when_the_caller_is_not_the_owner(string @case)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        var token = await ArrangeAsync(db, @case);

        await using var connection = await db.OpenAsync();
        var ex = await Assert.ThrowsAsync<PostgresException>(() => AssertOwnerAsync(connection, "unit", "k", token, db.Timeline));
        Assert.Equal("RC001", ex.SqlState);
        Assert.Equal("ownership_lost", ex.MessageText);

        Assert.False(await OwnerIsAsync(connection, "unit", "k", token, db.Timeline));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(+1)]
    public async Task I32_assert_owner_and_owner_is_raise_RC002_on_another_timeline_with_a_valid_token_and_lease(int direction)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        var other = direction < 0 ? db.PreviousTimeline : db.LaterTimeline;

        await using var connection = await db.OpenAsync();
        var ex = await Assert.ThrowsAsync<PostgresException>(() => AssertOwnerAsync(connection, "unit", "k", token, other));
        Assert.Equal("RC002", ex.SqlState);
        Assert.Equal("timeline_changed", ex.MessageText);
        Assert.Equal("RC002", await SqlStateOfAsync(() => OwnerIsAsync(connection, "unit", "k", token, other)));

        Assert.Equal("RC002", await SqlStateOfAsync(() => AssertOwnerAsync(connection, "unit", "k", token + 1_000, other)));
        Assert.Equal("RC002", await SqlStateOfAsync(() => AssertOwnerAsync(connection, "unit", "missing", token, other)));

        await AssertOwnerAsync(connection, "unit", "k", token, db.Timeline);
        Assert.True(await OwnerIsAsync(connection, "unit", "k", token, db.Timeline));
    }

    [Fact]
    public async Task I11_a_fenced_transaction_that_starts_while_the_owner_changes_waits_for_the_change_and_then_raises_RC001()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        await db.AddMemberAsync("b");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        var newToken = await AdoptAsync(adopter, "unit", "k", token, "b");
        Assert.NotNull(newToken);

        await using var owner = await db.OpenAsync();
        var ownerPid = owner.ProcessID;
        var fenced = Task.Run(async () =>
        {
            await using var transaction = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, TimeSpan.FromSeconds(30));
            await transaction.CommitAsync();
        });
        await db.WaitUntilBlockedAsync(ownerPid, TimeSpan.FromSeconds(30));
        Assert.False(fenced.IsCompleted);

        await adoption.CommitAsync();
        var ex = await Assert.ThrowsAsync<PostgresException>(() => fenced);
        Assert.Equal("RC001", ex.SqlState);

        await using var b = await db.OpenAsync();
        await using var bTx = await BeginFencedAsync(b, "unit", "k", newToken!.Value, db.Timeline, TimeSpan.FromSeconds(30));
        await bTx.CommitAsync();
    }
}
