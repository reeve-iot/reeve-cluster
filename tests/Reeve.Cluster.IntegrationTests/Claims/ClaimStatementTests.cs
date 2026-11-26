using System.Diagnostics;
using Int.Owned;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Claims;

public sealed class ClaimStatementTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan OwnershipTimeout = Ftt + TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public ClaimStatementTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public static TheoryData<string> LockingStatements() => ["insert", "takeover", "release", "retention"];

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

    [Theory]
    [MemberData(nameof(LockingStatements))]
    public async Task Each_statement_runs_under_its_bound_and_after_25P04_runs_again_on_a_new_connection(string statement)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt, retention: TimeSpan.FromMilliseconds(1));
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        Guid token = default;
        if (statement != "insert")
        {
            token = await a.WinAsync("k");
            if (statement is "takeover" or "retention")
            {
                await rig.EndLeaseAsync("a");
                await rig.ExpireClaimAsync("k");
            }
        }

        Task<object?> Run() => statement switch
        {
            "insert" => rig.Store.InsertAsync("k", "b", b.Epoch, Minute, CancellationToken.None).ContinueWith(t => (object?)t.Result, TaskScheduler.Default),
            "takeover" => rig.Store.TakeoverAsync("k", token, "b", b.Epoch, Minute, CancellationToken.None).ContinueWith(t => (object?)t.Result, TaskScheduler.Default),
            "release" => rig.Store.ReleaseAsync("k", token, CancellationToken.None).ContinueWith(t => (object?)t.Result, TaskScheduler.Default),
            _ => rig.Retention.DeleteClaimsAsync(10, CancellationToken.None).ContinueWith(t => (object?)t.Result, TaskScheduler.Default),
        };

        await using var holder = await rig.Db.OpenAsync();
        var held = await holder.BeginTransactionAsync();
        await OwnedDatabase.ExecAsync(holder, statement == "insert"
            ? "INSERT INTO reeve_cluster.claim (key, holder_replica, holder_epoch, token, expires_at) VALUES ('k', 'x', 1, gen_random_uuid(), clock_timestamp() + interval '1 hour')"
            : "SELECT 1 FROM reeve_cluster.claim WHERE key = 'k' FOR SHARE");

        var elapsed = Stopwatch.StartNew();
        var ended = await Record.ExceptionAsync(() => Run().WaitAsync((5 * OwnershipTimeout) + TimeSpan.FromSeconds(10)));
        Assert.Equal("25P04", SqlStateIn(ended));
        Assert.InRange(elapsed.Elapsed, OwnershipTimeout - TimeSpan.FromMilliseconds(100), (5 * OwnershipTimeout) + TimeSpan.FromSeconds(10));

        await held.RollbackAsync();
        Assert.NotNull(await Run());
    }

    [Theory]
    [InlineData("takeover")]
    [InlineData("release")]
    public async Task A_statement_that_waited_for_a_concurrent_update_lands_under_a_repeatable_read_default(string statement)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await using (var connection = await db.OpenAsync())
        {
            await OwnedDatabase.ExecAsync(connection, $"ALTER DATABASE \"{connection.Database}\" SET default_transaction_isolation = 'repeatable read'");
        }

        await using var rig = ClaimsRig.Open(db);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.WinAsync("k");
        await rig.EndLeaseAsync("a");
        await using var writer = await db.OpenAsync();
        var writing = await writer.BeginTransactionAsync();
        await OwnedDatabase.ExecAsync(writer, "UPDATE reeve_cluster.claim SET holder_epoch = holder_epoch WHERE key = 'k'");

        Task<bool> run = statement == "takeover"
            ? rig.Store.TakeoverAsync("k", token, "b", b.Epoch, Minute, CancellationToken.None).ContinueWith(t => t.Result is not null, TaskScheduler.Default)
            : rig.Store.ReleaseAsync("k", token, CancellationToken.None);
        await db.WaitForALockWaiterAsync();

        await writing.CommitAsync();

        Assert.True(await run.WaitAsync(ClaimsRig.Calm));
    }

    [Fact]
    public async Task A_takeover_of_a_row_that_a_concurrent_takeover_changed_matches_nothing()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var c = await rig.AddReplicaAsync("c");
        var first = await a.WinAsync("k");
        await rig.EndLeaseAsync("a");

        var second = await rig.Store.TakeoverAsync("k", first, "b", b.Epoch, Minute, CancellationToken.None);
        var third = await rig.Store.TakeoverAsync("k", first, "c", c.Epoch, Minute, CancellationToken.None);

        Assert.NotNull(second);
        Assert.Null(third);
        Assert.Equal("b", (await rig.RowAsync("k"))!.Holder);
    }

    [Fact]
    public async Task A_takeover_of_a_claim_that_is_still_valid_matches_nothing()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k");

        var taken = await rig.Store.TakeoverAsync("k", first, "b", b.Epoch, Minute, CancellationToken.None);

        Assert.Null(taken);
        Assert.Equal(first, (await rig.RowAsync("k"))!.Token);
    }

    [Fact]
    public async Task Retention_deletes_claims_that_ended_longer_ago_than_the_retention_and_nothing_else()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, retention: TimeSpan.FromHours(1));
        var a = await rig.AddReplicaAsync("a");
        var live = await a.WinAsync("live");
        await a.WinAsync("just-ended");
        await a.WinAsync("long-ended");
        await rig.ExpireClaimAsync("just-ended");
        await rig.ExecAsync("UPDATE reeve_cluster.claim SET expires_at = clock_timestamp() - interval '2 hours' WHERE key = 'long-ended'");

        var deleted = await rig.Retention.DeleteClaimsAsync(10, CancellationToken.None);

        Assert.Equal(1, deleted);
        Assert.Null(await rig.RowAsync("long-ended"));
        Assert.NotNull(await rig.RowAsync("just-ended"));
        Assert.Equal(live, (await rig.RowAsync("live"))!.Token);
    }

    [Fact]
    public async Task Retention_deletes_at_most_the_limit_oldest_first()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, retention: TimeSpan.FromMilliseconds(1));
        var a = await rig.AddReplicaAsync("a");
        for (var i = 0; i < 5; i++)
        {
            await a.WinAsync($"k{i}");
            await rig.ExecAsync($"UPDATE reeve_cluster.claim SET expires_at = clock_timestamp() - interval '{i + 1} hours' WHERE key = 'k{i}'");
        }

        Assert.Equal(2, await rig.Retention.DeleteClaimsAsync(2, CancellationToken.None));

        Assert.Null(await rig.RowAsync("k4"));
        Assert.Null(await rig.RowAsync("k3"));
        Assert.NotNull(await rig.RowAsync("k2"));
        Assert.Equal(3, await rig.CountAsync());
    }

    [Fact]
    public async Task Retention_rechecks_expiry_after_waiting_for_a_takeover()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, retention: TimeSpan.FromMilliseconds(1));
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await rig.EndLeaseAsync("a");
        await rig.ExecAsync("UPDATE reeve_cluster.claim SET expires_at = clock_timestamp() - interval '2 hours' WHERE key = 'k'");

        await using var connection = await rig.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = new NpgsqlCommand("SELECT key FROM reeve_cluster.claim WHERE key = 'k' FOR NO KEY UPDATE", connection, transaction))
        {
            _ = await command.ExecuteScalarAsync();
        }
        var renewed = Guid.NewGuid();
        await using (var command = new NpgsqlCommand("UPDATE reeve_cluster.claim SET token = @token, expires_at = clock_timestamp() + interval '1 hour' WHERE key = 'k'", connection, transaction))
        {
            command.Parameters.AddWithValue("token", renewed);
            await command.ExecuteNonQueryAsync();
        }

        var deletion = rig.Retention.DeleteClaimsAsync(10, CancellationToken.None);
        await rig.Db.WaitForALockWaiterAsync();
        await transaction.CommitAsync();

        Assert.Equal(0, await deletion);
        Assert.Equal(renewed, (await rig.RowAsync("k"))!.Token);
        Assert.NotEqual(token, renewed);
    }

    [Fact]
    public async Task A_holder_whose_row_the_retention_deleted_is_refused_like_one_whose_claim_ended()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, retention: TimeSpan.FromMilliseconds(1));
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await rig.ExpireClaimAsync("k");
        await Task.Delay(10);
        Assert.Equal(1, await rig.Retention.DeleteClaimsAsync(10, CancellationToken.None));
        await using var connection = await rig.Db.OpenAsync();

        await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));
        Assert.False(await rig.ValidAsync("k", token));
    }
}
