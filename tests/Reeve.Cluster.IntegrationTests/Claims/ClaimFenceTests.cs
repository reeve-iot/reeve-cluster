using System.Diagnostics;
using Int.Owned;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Claims;

public sealed class ClaimFenceTests : IClassFixture<PostgresFixture>
{
    private const string AssertSql = "SELECT reeve_cluster.assert_claim(@key, @token, @timeline)";

    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan OwnershipTimeout = Ftt + TimeSpan.FromSeconds(1);

    private readonly PostgresFixture postgres;

    public ClaimFenceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private static async Task<string?> SqlStateOfAsync(NpgsqlConnection connection, string key, Guid token, long timeline)
    {
        await using var transaction = await connection.BeginTransactionAsync();
        try
        {
            await OwnedDatabase.ScalarAsync<object>(connection, AssertSql, ("key", key), ("token", token), ("timeline", timeline));
            return null;
        }
        catch (PostgresException e)
        {
            return e.SqlState;
        }
        finally
        {
            await transaction.RollbackAsync();
        }
    }

    private static string? SqlStateIn(Exception? exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is PostgresException pg)
            {
                return pg.SqlState;
            }
        }

        return null;
    }

    [Fact]
    public async Task I11_assert_claim_passes_for_a_valid_claim_and_holds_its_row_FOR_SHARE_to_the_transactions_end()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();

        await using var transaction = await connection.BeginTransactionAsync();
        await OwnedDatabase.ScalarAsync<object>(connection, AssertSql, ("key", "k"), ("token", token), ("timeline", rig.Db.Timeline));

        Assert.False(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
        Assert.False(await rig.CanLockAsync("k", "FOR UPDATE"));
        Assert.True(await rig.CanLockAsync("k", "FOR SHARE"));

        await transaction.CommitAsync();
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
    }

    [Theory]
    [InlineData("wrong token")]
    [InlineData("ttl")]
    [InlineData("lease")]
    [InlineData("fenced")]
    [InlineData("epoch")]
    [InlineData("deleted member")]
    [InlineData("missing row")]
    public async Task assert_claim_raises_RC003_unless_the_claim_is_valid(string reason)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k", reason == "ttl" ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromMinutes(1));
        var asked = token;
        switch (reason)
        {
            case "wrong token":
                asked = Guid.NewGuid();
                break;
            case "ttl":
                await rig.UntilInvalidAsync("k", token);
                break;
            case "lease":
                await rig.EndLeaseAsync("a");
                break;
            case "fenced":
                await rig.SetStateAsync("a", "fenced");
                break;
            case "epoch":
                await rig.ExecAsync("UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'a'");
                break;
            case "deleted member":
                await rig.ExecAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");
                break;
            case "missing row":
                Assert.True(await a.Claims.ReleaseAsync("k", token));
                break;
        }

        await using var connection = await rig.Db.OpenAsync();

        Assert.Equal("RC003", await SqlStateOfAsync(connection, "k", asked, rig.Db.Timeline));
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task I32_assert_claim_raises_RC002_on_another_timeline_before_it_looks_at_the_claim(int offset)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();

        Assert.Equal("RC002", await SqlStateOfAsync(connection, "k", token, rig.Db.Timeline + offset));
        Assert.Equal("RC002", await SqlStateOfAsync(connection, "k", Guid.NewGuid(), rig.Db.Timeline + offset));
        Assert.Equal("RC002", await SqlStateOfAsync(connection, "absent", Guid.NewGuid(), rig.Db.Timeline + offset));
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
    }

    [Fact]
    public async Task claim_valid_judges_the_token_the_time_to_live_and_the_holders_member_row_and_takes_no_lock()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();

        await using var transaction = await connection.BeginTransactionAsync();
        Assert.True(await OwnedDatabase.ScalarAsync<bool>(connection, "SELECT reeve_cluster.claim_valid(@key, @token)", ("key", "k"), ("token", token)));
        Assert.False(await OwnedDatabase.ScalarAsync<bool>(connection, "SELECT reeve_cluster.claim_valid(@key, @token)", ("key", "k"), ("token", Guid.NewGuid())));
        Assert.False(await OwnedDatabase.ScalarAsync<bool>(connection, "SELECT reeve_cluster.claim_valid(@key, @token)", ("key", "absent"), ("token", token)));
        Assert.True(await rig.CanLockAsync("k", "FOR UPDATE"));
    }

    [Fact]
    public async Task claim_is_is_claim_valid_as_a_function_that_raises_RC002_on_another_timeline_and_takes_no_lock()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        const string Sql = "SELECT reeve_cluster.claim_is(@key, @token, @timeline)";

        await using (await connection.BeginTransactionAsync())
        {
            Assert.True(await OwnedDatabase.ScalarAsync<bool>(connection, Sql, ("key", "k"), ("token", token), ("timeline", rig.Db.Timeline)));
            Assert.False(await OwnedDatabase.ScalarAsync<bool>(connection, Sql, ("key", "k"), ("token", Guid.NewGuid()), ("timeline", rig.Db.Timeline)));
            Assert.True(await rig.CanLockAsync("k", "FOR UPDATE"));
        }

        var other = await Assert.ThrowsAsync<PostgresException>(() =>
            OwnedDatabase.ScalarAsync<bool>(connection, Sql, ("key", "k"), ("token", token), ("timeline", rig.Db.PreviousTimeline)));
        Assert.Equal("RC002", other.SqlState);
    }

    [Fact]
    public async Task A_claim_fenced_transaction_runs_under_FencedTransactionTimeout_and_commits_its_effect()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY, who text NOT NULL)");

        await using (var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", token, connection))
        {
            Assert.Equal("2s", await ScalarAsync<string>(fenced, "SHOW transaction_timeout"));
            await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1, 'a')");
            await fenced.CommitAsync();
        }

        Assert.Equal(1L, await rig.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
    }

    [Fact]
    public async Task A_claim_fenced_transaction_that_is_disposed_without_a_commit_rolls_back_and_lets_go_of_the_row()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY)");

        await using (await a.Claims.BeginClaimFencedTransactionAsync("k", token, connection))
        {
            await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1)");
        }

        Assert.Equal(0L, await rig.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
        Assert.True(await a.Claims.ReleaseAsync("k", token).WaitAsync(ClaimsRig.Calm));
    }

    [Fact]
    public async Task A_stale_claim_holders_effect_is_refused_at_the_row_after_a_takeover()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var stale = await a.WinAsync("k");
        await rig.EndLeaseAsync("a");
        var current = await b.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY)");

        var refused = await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", stale, connection));

        Assert.Equal("RC003", SqlStateIn(refused));
        Assert.Equal(0L, await rig.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.True(await rig.ValidAsync("k", current));
        Assert.Empty(a.OtherTimelines);
    }

    [Fact]
    public async Task A_claim_that_was_released_or_deleted_refuses_a_begin_and_holds_no_lock()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await rig.ExecAsync("DELETE FROM reeve_cluster.claim WHERE key = 'k'");

        var refused = await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));

        Assert.Equal("RC003", SqlStateIn(refused));
        await using var again = await connection.BeginTransactionAsync();
    }

    [Fact]
    public async Task I32_a_begin_on_another_timeline_than_the_join_is_refused_as_ClaimLostException_and_starts_the_fence()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        a.Timeline = rig.Db.PreviousTimeline;

        var refused = await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));

        Assert.Equal("RC002", SqlStateIn(refused));
        Assert.Equal([a.Epoch], a.OtherTimelines);
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
    }

    [Fact]
    public async Task C2_a_takeover_waits_for_an_open_claim_fenced_transaction_then_wins_with_a_new_token()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY, who text NOT NULL)");
        var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", first, connection);
        await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1, 'a')");

        await rig.EndLeaseAsync("a");
        var takeover = b.Claims.ClaimAsync("k", TimeSpan.FromMinutes(1));
        await rig.Db.WaitForALockWaiterAsync();
        Assert.False(takeover.IsCompleted);
        Assert.Equal(first, (await rig.RowAsync("k"))!.Token);

        await fenced.CommitAsync();
        await fenced.DisposeAsync();
        var result = await takeover.WaitAsync(ClaimsRig.Calm);

        var second = Assert.IsType<ClaimResult.Won>(result).Token;
        Assert.NotEqual(first, second);
        Assert.Equal(1L, await rig.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.Equal(new ClaimRow("b", b.Epoch, second, Valid: true), await rig.RowAsync("k"));
    }

    [Theory]
    [InlineData("lease")]
    [InlineData("fenced")]
    [InlineData("draining")]
    public async Task C3_a_fenced_holders_claim_fenced_begin_is_refused_within_the_time_to_live(string end)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k", TimeSpan.FromMinutes(10));
        if (end == "lease")
        {
            await rig.EndLeaseAsync("a");
        }
        else
        {
            await rig.SetStateAsync("a", end);
        }

        await using var connection = await rig.Db.OpenAsync();
        var refused = await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));

        Assert.Equal("RC003", SqlStateIn(refused));
        Assert.True((await rig.RowAsync("k"))!.Valid, "the claim's own time to live had not passed");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task C4_a_takeover_that_starts_just_after_a_claim_fenced_transactions_SET_LOCAL_completes_without_25P04(int afterMilliseconds)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k");
        await using var holder = await rig.Db.OpenAsync();

        var warm = await a.WinAsync("warm");
        await rig.EndLeaseAsync("a");
        await rig.Store.TakeoverAsync("warm", warm, "b", b.Epoch, TimeSpan.FromMinutes(1), CancellationToken.None);

        await rig.SetLeaseAsync("a", "1500 milliseconds");
        var sinceBeforeBegin = Stopwatch.StartNew();
        await using var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", first, holder);
        var sinceHoldersSet = Stopwatch.StartNew();
        while (sinceHoldersSet.Elapsed < TimeSpan.FromMilliseconds(afterMilliseconds))
        {
            Thread.SpinWait(10);
        }

        var second = await rig.Store.TakeoverAsync("k", first, "b", b.Epoch, TimeSpan.FromMinutes(1), CancellationToken.None);

        Assert.NotNull(second);
        Assert.NotEqual(first, second);
        Assert.InRange(sinceBeforeBegin.Elapsed, Ftt, (5 * OwnershipTimeout) + TimeSpan.FromSeconds(10));
        Assert.Equal("25P04", SqlStateIn(await Record.ExceptionAsync(() => fenced.CommitAsync())));
    }

    private static async Task<T> ScalarAsync<T>(ClaimFencedTransaction fenced, string sql)
    {
        await using var command = new NpgsqlCommand(sql, fenced.Connection, fenced.Transaction);
        return (T)(await command.ExecuteScalarAsync())!;
    }
}
