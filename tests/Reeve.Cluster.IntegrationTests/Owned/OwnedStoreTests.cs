using System.Diagnostics;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class OwnedStoreTests : IClassFixture<PostgresFixture>
{
    private const string Kind = "unit";
    private const string Key = "k";

    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan Calm = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan OwnershipTimeout = Ftt + TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Margin = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Retention = TimeSpan.FromMilliseconds(1);

    private readonly PostgresFixture postgres;

    public OwnedStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly OwnedDataSource data;

        private Rig(OwnedDatabase db, OwnedDataSource data, OwnedStore store, long epochA, long epochB)
        {
            Db = db;
            this.data = data;
            Store = store;
            EpochA = epochA;
            EpochB = epochB;
        }

        public OwnedDatabase Db { get; }

        public OwnedStore Store { get; }

        public long EpochA { get; }

        public long EpochB { get; }

        public static async Task<Rig> CreateAsync(PostgresFixture postgres, TimeSpan? fencedTransactionTimeout = null)
        {
            var db = await OwnedDatabase.CreateAsync(postgres);
            var epochA = await db.AddMemberAsync("a");
            var epochB = await db.AddMemberAsync("b");
            return Open(db, epochA, epochB, fencedTransactionTimeout);
        }

        public static Rig Open(OwnedDatabase db, long epochA, long epochB, TimeSpan? fencedTransactionTimeout = null)
        {
            var options = Options.Create(new ClusterOptions
            {
                ConnectionString = db.ConnectionString,
                FencedTransactionTimeout = fencedTransactionTimeout ?? Calm,
                OwnedRowRetention = Retention,
            });
            var data = new OwnedDataSource(options);
            return new Rig(db, data, new OwnedStore(data, options), epochA, epochB);
        }

        public Task<OwnedRead> ReadAsync() => Store.ReadAsync(Kind, Key, CancellationToken.None);

        public async Task<long> InsertAAsync() =>
            await Store.InsertAsync(Kind, Key, "a", EpochA, CancellationToken.None) ?? throw new InvalidOperationException("The insert conflicted.");

        public async Task ExpireAAsync()
        {
            await using var connection = await Db.OpenAsync();
            await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'");
        }

        public async Task ArrangeAsync(OwnedCondition condition)
        {
            if (condition == OwnedCondition.Absent)
            {
                return;
            }

            var token = await InsertAAsync();
            switch (condition)
            {
                case OwnedCondition.Free:
                    Assert.True(await Store.ReleaseAsync(Kind, Key, "a", EpochA, token, CancellationToken.None));
                    break;
                case OwnedCondition.ReleaseRequested:
                    Assert.True(await Store.RequestReleaseAsync(Kind, Key, token, CancellationToken.None));
                    break;
                case OwnedCondition.Orphaned:
                    await ExpireAAsync();
                    break;
            }

            Assert.Equal(condition, (await ReadAsync()).Condition);
        }

        public async Task<bool> RunAsync(OwnedStatement statement)
        {
            var read = await ReadAsync();
            var ct = CancellationToken.None;
            return statement switch
            {
                OwnedStatement.Insert => await Store.InsertAsync(Kind, Key, "b", EpochB, ct) is not null,
                OwnedStatement.Take => await Store.TakeAsync(Kind, Key, read.Row!.Value, "b", EpochB, ct) is not null,
                OwnedStatement.RequestRelease => await Store.RequestReleaseAsync(Kind, Key, read.Row!.Value.Token, ct),
                OwnedStatement.Release => await Store.ReleaseAsync(Kind, Key, "a", EpochA, read.Row!.Value.Token, ct),
                OwnedStatement.Clear => await Store.ClearAsync(Kind, Key, read.Row!.Value, ct),
                OwnedStatement.Sweep => await SweepAfterRetentionAsync(read.Row!.Value.Token),
                OwnedStatement.RefuseAlreadyOwned => await Store.InsertAsync(Kind, Key, "b", EpochB, ct) is null,
                _ => throw new ArgumentOutOfRangeException(nameof(statement), statement, null),
            };
        }

        public async Task<bool> SweepAfterRetentionAsync(long token)
        {
            await Task.Delay(Retention * 20);
            return await Store.SweepAsync(Kind, Key, token, CancellationToken.None);
        }

        public async Task SetLeaseAAsync(string lease)
        {
            await using var connection = await Db.OpenAsync();
            await ExecAsync(connection, $"UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '{lease}' WHERE replica_id = 'a'");
        }

        public async Task<long?> StoredRequestAsync()
        {
            await using var connection = await Db.OpenAsync();
            await using var command = Command(connection, "SELECT token FROM reeve_cluster.release_request WHERE kind = @kind AND key = @key", ("kind", Kind), ("key", Key));
            return (long?)await command.ExecuteScalarAsync();
        }

        public async Task WaitForALockWaiterAsync()
        {
            await using var connection = await Db.OpenAsync();
            var deadline = DateTime.UtcNow + Calm;
            while (!await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock')"))
            {
                Assert.True(DateTime.UtcNow < deadline, "No statement blocked on a lock.");
                await Task.Delay(20);
            }
        }

        public ValueTask DisposeAsync() => data.DisposeAsync();
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

    public static TheoryData<int, string> WriterCells()
    {
        var cells = new TheoryData<int, string>();
        for (var i = 0; i < OwnedStateMachine.Table.Count; i++)
        {
            if (OwnedStateMachine.Table[i] is { Writer: { } writer } c)
            {
                cells.Add(i, $"{c.RowId} {writer} {c.From}");
            }
        }

        return cells;
    }

    [Theory]
    [MemberData(nameof(WriterCells))]
    public async Task Each_cell_of_the_table_leads_from_its_condition_to_its_target(int index, string cell)
    {
        var (rowId, from, writer, statement, to) = OwnedStateMachine.Table[index];
        await using var rig = await Rig.CreateAsync(postgres);
        await rig.ArrangeAsync(from);
        var before = await rig.ReadAsync();

        Assert.True(await rig.RunAsync(statement), $"{cell}: {statement} matched nothing ({rowId}, {writer}).");

        var after = await rig.ReadAsync();
        Assert.Equal(to, after.Condition);
        if (statement is OwnedStatement.Insert or OwnedStatement.Take)
        {
            Assert.Equal(("b", rig.EpochB), (after.Row!.Value.OwnerReplica, after.Row.Value.OwnerEpoch));
            Assert.True(before.Row is null || after.Row.Value.Token > before.Row.Value.Token);
        }
    }

    [Fact]
    public async Task O07_a_hold_that_lapses_orphans_the_row_with_no_write()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        await rig.ArrangeAsync(OwnedCondition.ReleaseRequested);

        await rig.ExpireAAsync();

        Assert.Equal(OwnedCondition.Orphaned, (await rig.ReadAsync()).Condition);
    }

    [Theory]
    [InlineData(nameof(OwnedCondition.Owned))]
    [InlineData(nameof(OwnedCondition.ReleaseRequested))]
    public async Task I9_a_held_row_is_neither_taken_nor_cleared_nor_released_by_another(string held)
    {
        await using var rig = await Rig.CreateAsync(postgres);
        await rig.ArrangeAsync(Enum.Parse<OwnedCondition>(held));
        var read = await rig.ReadAsync();
        var row = read.Row!.Value;

        Assert.Null(await rig.Store.InsertAsync(Kind, Key, "b", rig.EpochB, CancellationToken.None));
        Assert.Null(await rig.Store.TakeAsync(Kind, Key, row, "b", rig.EpochB, CancellationToken.None));
        Assert.False(await rig.Store.ClearAsync(Kind, Key, row, CancellationToken.None));
        Assert.False(await rig.Store.ReleaseAsync(Kind, Key, "b", rig.EpochB, row.Token, CancellationToken.None));
        Assert.False(await rig.Store.ReleaseAsync(Kind, Key, "a", rig.EpochA + 100, row.Token, CancellationToken.None));
        Assert.False(await rig.SweepAfterRetentionAsync(row.Token));

        Assert.Equal(read, await rig.ReadAsync());
    }

    [Fact]
    public async Task I9_a_take_or_a_clear_on_a_stale_read_changes_nothing()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        await rig.ArrangeAsync(OwnedCondition.Orphaned);
        var stale = (await rig.ReadAsync()).Row!.Value;
        var taken = await rig.Store.TakeAsync(Kind, Key, stale, "b", rig.EpochB, CancellationToken.None);
        Assert.NotNull(taken);

        await rig.ExpireAAsync();
        Assert.Null(await rig.Store.TakeAsync(Kind, Key, stale, "a", rig.EpochA, CancellationToken.None));
        Assert.False(await rig.Store.ClearAsync(Kind, Key, stale, CancellationToken.None));
        Assert.Null(await rig.Store.TakeAsync(Kind, Key, stale with { OwnerReplica = null, OwnerEpoch = null }, "a", rig.EpochA, CancellationToken.None));

        Assert.Equal(new OwnedRow("b", rig.EpochB, taken.Value), (await rig.ReadAsync()).Row);
    }

    [Fact]
    public async Task I10_tokens_increase_across_take_release_end_sweep_and_re_creation()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        var ct = CancellationToken.None;
        var tokens = new List<long> { await rig.InsertAAsync() };

        Assert.True(await rig.Store.ReleaseAsync(Kind, Key, "a", rig.EpochA, tokens[^1], ct));
        Assert.Equal(tokens[^1], (await rig.ReadAsync()).Row!.Value.Token);
        tokens.Add((await rig.Store.TakeAsync(Kind, Key, (await rig.ReadAsync()).Row!.Value, "b", rig.EpochB, ct))!.Value);

        Assert.True(await rig.Store.ReleaseAsync(Kind, Key, "b", rig.EpochB, tokens[^1], ct));
        Assert.True(await rig.SweepAfterRetentionAsync(tokens[^1]));
        Assert.Equal(OwnedCondition.Absent, (await rig.ReadAsync()).Condition);

        tokens.Add((await rig.Store.InsertAsync(Kind, Key, "a", rig.EpochA, ct))!.Value);
        await rig.ExpireAAsync();
        tokens.Add((await rig.Store.TakeAsync(Kind, Key, (await rig.ReadAsync()).Row!.Value, "b", rig.EpochB, ct))!.Value);

        Assert.Equal(tokens.Order(), tokens);
        Assert.Equal(tokens.Count, tokens.Distinct().Count());
    }

    [Fact]
    public async Task O02_a_creator_whose_row_was_swept_meanwhile_takes_nothing_and_inserts_again()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        await rig.ArrangeAsync(OwnedCondition.Free);
        var ct = CancellationToken.None;
        Assert.Null(await rig.Store.InsertAsync(Kind, Key, "b", rig.EpochB, ct));
        var read = (await rig.ReadAsync()).Row!.Value;

        Assert.True(await rig.SweepAfterRetentionAsync(read.Token));

        Assert.Null(await rig.Store.TakeAsync(Kind, Key, read, "b", rig.EpochB, ct));
        var token = await rig.Store.InsertAsync(Kind, Key, "b", rig.EpochB, ct);
        Assert.True(token > read.Token);
        Assert.Equal(OwnedCondition.Owned, (await rig.ReadAsync()).Condition);
    }

    [Fact]
    public async Task S5_1_the_release_deletes_its_request_and_the_sweep_a_void_one_of_its_token()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        var ct = CancellationToken.None;
        var token = await rig.InsertAAsync();
        Assert.True(await rig.Store.RequestReleaseAsync(Kind, Key, token, ct));

        Assert.True(await rig.Store.ReleaseAsync(Kind, Key, "a", rig.EpochA, token, ct));
        Assert.Null(await rig.StoredRequestAsync());

        Assert.True(await rig.Store.RequestReleaseAsync(Kind, Key, token, ct));
        Assert.Equal(OwnedCondition.Free, (await rig.ReadAsync()).Condition);
        Assert.True(await rig.SweepAfterRetentionAsync(token));
        Assert.Null(await rig.StoredRequestAsync());
    }

    [Fact]
    public async Task I10_a_request_whose_tenure_ended_before_it_commits_is_void()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        var ct = CancellationToken.None;
        var token = await rig.InsertAAsync();
        var readByPlacer = (await rig.ReadAsync()).Row!.Value.Token;

        await rig.ExpireAAsync();
        var next = await rig.Store.TakeAsync(Kind, Key, (await rig.ReadAsync()).Row!.Value, "b", rig.EpochB, ct);
        Assert.True(next > token);

        Assert.True(await rig.Store.RequestReleaseAsync(Kind, Key, readByPlacer, ct));
        Assert.Equal(OwnedCondition.Owned, (await rig.ReadAsync()).Condition);
    }

    [Fact]
    public async Task An_upsert_with_a_lower_token_than_the_stored_one_changes_nothing()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        var ct = CancellationToken.None;
        var token = await rig.InsertAAsync();
        Assert.True(await rig.Store.RequestReleaseAsync(Kind, Key, token, ct));
        await using var connection = await rig.Db.OpenAsync();
        const string Stored = "SELECT token::text || '@' || requested_at::text FROM reeve_cluster.release_request WHERE kind = 'unit' AND key = 'k'";
        var stored = await ScalarAsync<string>(connection, Stored);

        Assert.False(await rig.Store.RequestReleaseAsync(Kind, Key, token - 1, ct));
        Assert.False(await rig.Store.RequestReleaseAsync(Kind, Key, token, ct));

        Assert.Equal(stored, await ScalarAsync<string>(connection, Stored));
        Assert.Equal(OwnedCondition.ReleaseRequested, (await rig.ReadAsync()).Condition);
    }

    [Fact]
    public async Task A_take_draws_its_token_after_its_lock()
    {
        await using var rig = await Rig.CreateAsync(postgres);
        var token = await rig.InsertAAsync();
        await using var owner = await rig.Db.OpenAsync();
        await using var fenced = await BeginFencedAsync(owner, Kind, Key, token, rig.Db.Timeline, TimeSpan.FromSeconds(30));
        await rig.ExpireAAsync();

        var take = rig.Store.TakeAsync(Kind, Key, new OwnedRow("a", rig.EpochA, token), "b", rig.EpochB, CancellationToken.None);
        await rig.WaitForALockWaiterAsync();
        var drawnMeanwhile = await rig.Store.InsertAsync(Kind, "other", "b", rig.EpochB, CancellationToken.None);
        await fenced.RollbackAsync();

        Assert.True(await take > drawnMeanwhile);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task An_adoption_that_starts_just_after_a_paused_owners_SET_LOCAL_completes_without_25P04(int afterMilliseconds)
    {
        await using var rig = await Rig.CreateAsync(postgres, Ftt);
        var token = await rig.InsertAAsync();
        await using var owner = await rig.Db.OpenAsync();

        var warmed = await rig.Store.InsertAsync(Kind, "warm", "a", rig.EpochA, CancellationToken.None) ?? throw new InvalidOperationException("The insert conflicted.");
        await rig.ExpireAAsync();
        await rig.Store.TakeAsync(Kind, "warm", new OwnedRow("a", rig.EpochA, warmed), "b", rig.EpochB, CancellationToken.None);
        await rig.SetLeaseAAsync("1500 milliseconds");
        await using var fenced = await BeginFencedAsync(owner, Kind, Key, token, rig.Db.Timeline, Ftt);
        var sinceOwnersSet = Stopwatch.StartNew();
        while (sinceOwnersSet.Elapsed < TimeSpan.FromMilliseconds(afterMilliseconds))
        {
            Thread.SpinWait(10);
        }

        var adopted = await rig.Store.TakeAsync(Kind, Key, new OwnedRow("a", rig.EpochA, token), "b", rig.EpochB, CancellationToken.None);

        Assert.True(adopted > token);
        Assert.InRange(sinceOwnersSet.Elapsed, Ftt - TimeSpan.FromMilliseconds(100), (5 * (Ftt + Margin)) + TimeSpan.FromSeconds(10));
        Assert.Equal("25P04", SqlStateIn(await Record.ExceptionAsync(() => fenced.CommitAsync())));
    }

    public static TheoryData<string> LockingStatements() =>
        [nameof(OwnedStatement.Insert), nameof(OwnedStatement.Take), nameof(OwnedStatement.RequestRelease), nameof(OwnedStatement.Release),
         nameof(OwnedStatement.Clear), nameof(OwnedStatement.Sweep)];

    [Theory]
    [MemberData(nameof(LockingStatements))]
    public async Task Each_statement_runs_under_its_bound_and_after_25P04_runs_again_on_a_new_connection(string name)
    {
        var statement = Enum.Parse<OwnedStatement>(name);
        await using var rig = await Rig.CreateAsync(postgres, Ftt);
        var from = statement switch
        {
            OwnedStatement.Insert => OwnedCondition.Absent,
            OwnedStatement.Take or OwnedStatement.Clear => OwnedCondition.Orphaned,
            OwnedStatement.Sweep => OwnedCondition.Free,
            _ => OwnedCondition.Owned,
        };
        await rig.ArrangeAsync(from);
        var before = await rig.ReadAsync();

        await using var holder = await rig.Db.OpenAsync();
        var held = await holder.BeginTransactionAsync();
        await ExecAsync(holder, statement switch
        {
            OwnedStatement.Insert => "INSERT INTO reeve_cluster.owned (kind, key, token) VALUES ('unit', 'k', 0)",
            OwnedStatement.RequestRelease => "INSERT INTO reeve_cluster.release_request VALUES ('unit', 'k', 0, clock_timestamp())",
            _ => "SELECT 1 FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' FOR SHARE",
        });

        var elapsed = Stopwatch.StartNew();
        Assert.Equal("25P04", SqlStateIn(await Record.ExceptionAsync(() => rig.RunAsync(statement).WaitAsync((5 * OwnershipTimeout) + TimeSpan.FromSeconds(10)))));
        Assert.InRange(elapsed.Elapsed, OwnershipTimeout - TimeSpan.FromMilliseconds(100), (5 * OwnershipTimeout) + TimeSpan.FromSeconds(10));

        await held.RollbackAsync();
        Assert.Equal(before, await rig.ReadAsync());
        Assert.True(await rig.RunAsync(statement));
    }

    [Fact]
    public async Task A_take_that_waited_for_a_concurrent_update_lands_under_a_repeatable_read_default()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        var epochA = await db.AddMemberAsync("a");
        var epochB = await db.AddMemberAsync("b");
        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, $"ALTER DATABASE \"{connection.Database}\" SET default_transaction_isolation = 'repeatable read'");
        }

        await using var rig = Rig.Open(db, epochA, epochB);
        await rig.ArrangeAsync(OwnedCondition.Orphaned);
        var read = (await rig.ReadAsync()).Row!.Value;

        await using var writer = await db.OpenAsync();
        var writing = await writer.BeginTransactionAsync();
        await ExecAsync(writer, "UPDATE reeve_cluster.owned SET key = key WHERE kind = 'unit' AND key = 'k'");
        var take = rig.Store.TakeAsync(Kind, Key, read, "b", epochB, CancellationToken.None);
        await rig.WaitForALockWaiterAsync();

        await writing.CommitAsync();

        Assert.NotNull(await take);
    }
}
