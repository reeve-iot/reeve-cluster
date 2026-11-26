using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class OwnedStoreClauseTests : IClassFixture<PostgresFixture>
{
    private const string Kind = "unit";
    private const string Key = "k";

    private static readonly CancellationToken Ct = CancellationToken.None;

    private readonly PostgresFixture postgres;

    public OwnedStoreClauseTests(PostgresFixture postgres)
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

        public static async Task<Rig> CreateAsync(PostgresFixture postgres, TimeSpan retention, bool repeatableReadDefault = false)
        {
            var db = await OwnedDatabase.CreateAsync(postgres);
            var epochA = await db.AddMemberAsync("a");
            var epochB = await db.AddMemberAsync("b");
            if (repeatableReadDefault)
            {
                await using var connection = await db.OpenAsync();
                await ExecAsync(connection, $"ALTER DATABASE \"{connection.Database}\" SET default_transaction_isolation = 'repeatable read'");
            }

            var options = Options.Create(new ClusterOptions
            {
                ConnectionString = db.ConnectionString,
                FencedTransactionTimeout = TimeSpan.FromSeconds(30),
                OwnedRowRetention = retention,
            });
            var data = new OwnedDataSource(options);
            return new Rig(db, data, new OwnedStore(data, options), epochA, epochB);
        }

        public async Task<OwnedRow?> RowAsync() => (await Store.ReadAsync(Kind, Key, Ct)).Row;

        public async Task<OwnedCondition> ConditionAsync() => (await Store.ReadAsync(Kind, Key, Ct)).Condition;

        public async Task<long> InsertAAsync() =>
            await Store.InsertAsync(Kind, Key, "a", EpochA, Ct) ?? throw new InvalidOperationException("The insert conflicted.");

        public async Task ExpireAsync(string replica)
        {
            await using var connection = await Db.OpenAsync();
            await ExecAsync(
                connection,
                "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = @replica",
                ("replica", replica));
        }

        public async Task<long?> StoredRequestAsync()
        {
            await using var connection = await Db.OpenAsync();
            await using var command = Command(connection, "SELECT token FROM reeve_cluster.release_request WHERE kind = @kind AND key = @key", ("kind", Kind), ("key", Key));
            return (long?)await command.ExecuteScalarAsync();
        }

        public async Task<DateTime> RequestedAtAsync()
        {
            await using var connection = await Db.OpenAsync();
            return await ScalarAsync<DateTime>(connection, "SELECT requested_at FROM reeve_cluster.release_request WHERE kind = @kind AND key = @key", ("kind", Kind), ("key", Key));
        }

        public async Task WaitForALockWaiterAsync()
        {
            await using var connection = await Db.OpenAsync();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock')"))
            {
                Assert.True(DateTime.UtcNow < deadline, "No statement blocked on a lock.");
                await Task.Delay(20);
            }
        }

        public ValueTask DisposeAsync() => data.DisposeAsync();
    }

    [Fact]
    public async Task a_take_or_a_clear_of_a_row_read_owned_and_since_released_changes_nothing()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1));
        var t0 = await p.InsertAAsync();
        var readOwned = (await p.RowAsync())!.Value;
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct));

        Assert.Null(await p.Store.TakeAsync(Kind, Key, readOwned, "b", p.EpochB, Ct));
        Assert.False(await p.Store.ClearAsync(Kind, Key, readOwned, Ct));

        Assert.Equal(new OwnedRow(null, null, t0), await p.RowAsync());
    }

    [Fact]
    public async Task an_ended_tenure_of_the_same_owner_and_epoch_is_neither_released_nor_taken_nor_cleared()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1));
        var t0 = await p.InsertAAsync();
        var first = (await p.RowAsync())!.Value;
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct));
        var t1 = await p.Store.TakeAsync(Kind, Key, (await p.RowAsync())!.Value, "a", p.EpochA, Ct);
        Assert.NotNull(t1);

        Assert.False(await p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct));

        await p.ExpireAsync("a");
        Assert.Null(await p.Store.TakeAsync(Kind, Key, first, "b", p.EpochB, Ct));
        Assert.False(await p.Store.ClearAsync(Kind, Key, first, Ct));

        Assert.Equal(new OwnedRow("a", p.EpochA, t1.Value), await p.RowAsync());
    }

    [Fact]
    public async Task the_sweep_leaves_a_free_row_younger_than_the_retention()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromHours(1));
        var t0 = await p.InsertAAsync();
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct));

        Assert.False(await p.Store.SweepAsync(Kind, Key, t0, Ct));

        Assert.Equal(new OwnedRow(null, null, t0), await p.RowAsync());
    }

    [Fact]
    public async Task the_sweep_of_a_stale_read_leaves_a_row_freed_under_a_later_token()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1));
        var t0 = await p.InsertAAsync();
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct));
        var t1 = await p.Store.TakeAsync(Kind, Key, (await p.RowAsync())!.Value, "b", p.EpochB, Ct);
        Assert.NotNull(t1);
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "b", p.EpochB, t1.Value, Ct));
        await Task.Delay(50);

        Assert.False(await p.Store.SweepAsync(Kind, Key, t0, Ct));

        Assert.Equal(new OwnedRow(null, null, t1.Value), await p.RowAsync());
    }

    [Fact]
    public async Task a_request_for_a_later_tenure_replaces_a_void_one()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1));
        var t0 = await p.InsertAAsync();
        Assert.True(await p.Store.RequestReleaseAsync(Kind, Key, t0, Ct));
        var firstAt = await p.RequestedAtAsync();
        await p.ExpireAsync("a");
        var t1 = await p.Store.TakeAsync(Kind, Key, (await p.RowAsync())!.Value, "b", p.EpochB, Ct);
        Assert.NotNull(t1);
        Assert.Equal(OwnedCondition.Owned, await p.ConditionAsync());

        Assert.True(await p.Store.RequestReleaseAsync(Kind, Key, t1.Value, Ct));

        Assert.Equal(t1, await p.StoredRequestAsync());
        Assert.True(await p.RequestedAtAsync() > firstAt);
        Assert.Equal(OwnedCondition.ReleaseRequested, await p.ConditionAsync());
    }

    [Fact]
    public async Task the_sweep_deletes_a_void_request_below_its_token()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1));
        var t0 = await p.InsertAAsync();
        Assert.True(await p.Store.RequestReleaseAsync(Kind, Key, t0, Ct));
        await p.ExpireAsync("a");
        var t1 = await p.Store.TakeAsync(Kind, Key, (await p.RowAsync())!.Value, "b", p.EpochB, Ct);
        Assert.NotNull(t1);
        Assert.True(await p.Store.ReleaseAsync(Kind, Key, "b", p.EpochB, t1.Value, Ct));
        Assert.Equal(t0, await p.StoredRequestAsync());
        await Task.Delay(50);

        Assert.True(await p.Store.SweepAsync(Kind, Key, t1.Value, Ct));

        Assert.Null(await p.StoredRequestAsync());
    }

    [Fact]
    public async Task a_release_that_waited_for_a_concurrent_update_lands_under_a_repeatable_read_default()
    {
        await using var p = await Rig.CreateAsync(postgres, TimeSpan.FromMilliseconds(1), repeatableReadDefault: true);
        var t0 = await p.InsertAAsync();

        await using var writer = await p.Db.OpenAsync();
        var writing = await writer.BeginTransactionAsync();
        await ExecAsync(writer, "UPDATE reeve_cluster.owned SET key = key WHERE kind = 'unit' AND key = 'k'");
        var release = p.Store.ReleaseAsync(Kind, Key, "a", p.EpochA, t0, Ct);
        await p.WaitForALockWaiterAsync();
        await writing.CommitAsync();

        Assert.True(await release);
        Assert.Equal(OwnedCondition.Free, await p.ConditionAsync());
    }
}
