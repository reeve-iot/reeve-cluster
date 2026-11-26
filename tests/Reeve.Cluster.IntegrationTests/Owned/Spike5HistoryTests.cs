using System.Diagnostics;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class Spike5HistoryTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan OwnershipTimeout = Ftt + TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Margin = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Calm = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public Spike5HistoryTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private static async Task<OwnedDatabase> WithBusinessRowsAsync(OwnedDatabase db)
    {
        await using var connection = await db.OpenAsync();
        await ExecAsync(connection, "CREATE TABLE public.biz (id int PRIMARY KEY, v int NOT NULL DEFAULT 0); INSERT INTO public.biz (id) VALUES (1), (2)");
        return db;
    }

    private static async Task<int> BizAsync(OwnedDatabase db, int id)
    {
        await using var connection = await db.OpenAsync();
        return await ScalarAsync<int>(connection, "SELECT v FROM public.biz WHERE id = @id", ("id", id));
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
    public async Task I11_an_adoption_waits_for_an_open_fenced_transaction_and_completes_within_FencedTransactionTimeout_plus_a_margin()
    {
        var db = await WithBusinessRowsAsync(await OwnedDatabase.CreateAsync(postgres));
        await db.AddMemberAsync("a");
        await db.AddMemberAsync("b");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var owner = await db.OpenAsync();
        await using var admin = await db.OpenAsync();
        await using var adopter = await db.OpenAsync();
        var sinceOwnersSet = Stopwatch.StartNew();
        var fenced = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, Ftt);
        await ExecAsync(owner, "UPDATE public.biz SET v = v + 1 WHERE id = 1");

        await ExecAsync(admin, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'");

        var adoption = Task.Run(async () =>
        {
            await using var transaction = await adopter.BeginTransactionAsync();
            await ExecAsync(adopter, $"SET LOCAL transaction_timeout = '{(long)OwnershipTimeout.TotalMilliseconds}ms'");
            var adopted = await AdoptAsync(adopter, "unit", "k", token, "b");
            await transaction.CommitAsync();
            return (adopted, sinceOwnersSet.Elapsed);
        });
        var (newToken, doneAt) = await adoption;

        Assert.NotNull(newToken);
        Assert.True(newToken > token);
        Assert.True(doneAt >= Ftt - TimeSpan.FromMilliseconds(100), $"The adoption finished {doneAt} after the owner's SET LOCAL, before its FencedTransactionTimeout ended it.");

        var limit = (5 * OwnershipTimeout) + TimeSpan.FromSeconds(10);
        Assert.True(doneAt <= limit, $"The adoption finished {doneAt} after the owner's SET LOCAL; the bound is {OwnershipTimeout}.");

        var commit = await Record.ExceptionAsync(() => fenced.CommitAsync());
        Assert.Equal("25P04", SqlStateIn(commit));
        Assert.Equal(0, await BizAsync(db, 1));
    }

    [Fact]
    public async Task I11_a_paused_owners_commit_fails_with_25P04_and_its_write_does_not_land()
    {
        var db = await WithBusinessRowsAsync(await OwnedDatabase.CreateAsync(postgres));
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await using var owner = await db.OpenAsync();
        var ownerPid = owner.ProcessID;
        var sinceOwnersSet = Stopwatch.StartNew();
        var fenced = await BeginFencedAsync(owner, "unit", "k", token, db.Timeline, Ftt);
        await ExecAsync(owner, "UPDATE public.biz SET v = v + 1 WHERE id = 1");

        await db.WaitUntilGoneAsync(ownerPid, Ftt + Margin);
        Assert.True(sinceOwnersSet.Elapsed >= Ftt - TimeSpan.FromMilliseconds(100), $"The server ended the owner {sinceOwnersSet.Elapsed} after its SET LOCAL, before FencedTransactionTimeout {Ftt}.");
        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));

        var commit = await Record.ExceptionAsync(() => fenced.CommitAsync());
        Assert.Equal("25P04", SqlStateIn(commit));
        Assert.Equal(0, await BizAsync(db, 1));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_deadlock_between_two_fenced_transactions_on_business_rows_is_40P01_never_RC001(bool sameKey)
    {
        var db = await WithBusinessRowsAsync(await OwnedDatabase.CreateAsync(postgres));
        await db.AddMemberAsync("a");
        var first = await db.CreateOwnedAsync("unit", "k1", "a");
        var second = sameKey ? first : await db.CreateOwnedAsync("unit", "k2", "a");
        var secondKey = sameKey ? "k1" : "k2";

        var firstWrote = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondWrote = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<string> Worker(string key, long token, int mine, int theirs, TaskCompletionSource wrote, TaskCompletionSource other)
        {
            await using var connection = await db.OpenAsync();
            try
            {
                await using var transaction = await BeginFencedAsync(connection, "unit", key, token, db.Timeline, TimeSpan.FromSeconds(30));
                await ExecAsync(connection, "UPDATE public.biz SET v = v + 1 WHERE id = @id", ("id", mine));
                wrote.SetResult();
                await other.Task;
                await ExecAsync(connection, "UPDATE public.biz SET v = v + 1 WHERE id = @id", ("id", theirs));
                await transaction.CommitAsync();
                return "committed";
            }
            catch (PostgresException e)
            {
                wrote.TrySetResult();
                return e.SqlState;
            }
        }

        var results = await Task.WhenAll(
            Worker("k1", first, 1, 2, firstWrote, secondWrote),
            Worker(secondKey, second, 2, 1, secondWrote, firstWrote));

        Assert.Equal(["40P01", "committed"], results.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task S5_1_a_release_request_upsert_commits_at_once_while_fenced_transactions_overlap_without_a_gap()
    {
        var db = await WithBusinessRowsAsync(await OwnedDatabase.CreateAsync(postgres));
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        using var stop = new CancellationTokenSource();
        var handOffs = 0;
        var overlap = Task.Run(async () =>
        {
            NpgsqlConnection[] connections = [await db.OpenAsync(), await db.OpenAsync()];
            try
            {
                var current = 0;
                var transaction = await BeginFencedAsync(connections[current], "unit", "k", token, db.Timeline, Calm);
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(30);
                    var next = 1 - current;
                    var nextTransaction = await BeginFencedAsync(connections[next], "unit", "k", token, db.Timeline, Calm);
                    await ExecAsync(connections[current], "UPDATE public.biz SET v = v + 1 WHERE id = 1");
                    await transaction.CommitAsync();
                    await transaction.DisposeAsync();
                    (current, transaction) = (next, nextTransaction);
                    Interlocked.Increment(ref handOffs);
                }

                await transaction.CommitAsync();
                await transaction.DisposeAsync();
            }
            finally
            {
                foreach (var connection in connections)
                {
                    await connection.DisposeAsync();
                }
            }
        });

        try
        {
            while (Volatile.Read(ref handOffs) < 3)
            {
                Assert.False(overlap.IsCompleted, "The overlapping fenced transactions stopped.");
                await Task.Delay(10);
            }

            await using (var admin = await db.OpenAsync())
            await using (var control = await db.OpenAsync())
            await using (var transaction = await control.BeginTransactionAsync())
            {
                var before = Volatile.Read(ref handOffs);
                await ExecAsync(control, "SET LOCAL statement_timeout = '60s'");
                var controlWrite = SqlStateOfAsync(() => ExecAsync(control, "UPDATE reeve_cluster.owned SET updated_at = clock_timestamp() WHERE kind = 'unit' AND key = 'k' AND token = @t", ("t", token)));
                await Poll.UntilAsync(() => Volatile.Read(ref handOffs) - before >= 3 || controlWrite.IsCompleted, "three hand-offs while the control waited");
                Assert.False(controlWrite.IsCompleted, "The control's write got its lock while the fenced transactions overlapped.");
                await ExecAsync(admin, "SELECT pg_cancel_backend(@pid)", ("pid", control.ProcessID));
                Assert.Equal(PostgresErrorCodes.QueryCanceled, await controlWrite);
            }

            await using var placer = await db.OpenAsync();
            await using (var transaction = await placer.BeginTransactionAsync())
            {
                await ExecAsync(placer, $"SET LOCAL transaction_timeout = '{(long)Calm.TotalMilliseconds}ms'");
                Assert.Equal(1, await ExecAsync(
                    placer,
                    """
                    INSERT INTO reeve_cluster.release_request (kind, key, token, requested_at) VALUES ('unit', 'k', @t, clock_timestamp())
                    ON CONFLICT (kind, key) DO UPDATE SET token = excluded.token, requested_at = excluded.requested_at
                    WHERE release_request.token < excluded.token
                    """,
                    ("t", token)));
                await transaction.CommitAsync();
            }

            Assert.False(overlap.IsCompleted, "The overlap ended before the request; the request did not run against it.");
        }
        finally
        {
            await stop.CancelAsync();
            await overlap;
        }

        await using var read = await db.OpenAsync();
        Assert.Equal(token, await ScalarAsync<long>(read, "SELECT r.token FROM reeve_cluster.owned o JOIN reeve_cluster.release_request r USING (kind, key, token) WHERE o.kind = 'unit' AND o.key = 'k'"));
    }
}
