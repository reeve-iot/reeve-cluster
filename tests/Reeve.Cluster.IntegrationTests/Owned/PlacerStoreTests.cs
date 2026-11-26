using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class PlacerStoreTests : IClassFixture<PostgresFixture>
{
    private const string Kind = "widget";
    private static readonly TimeSpan Calm = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public PlacerStoreTests(PostgresFixture postgres) => this.postgres = postgres;

    private sealed class Rig
    {
        public required OwnedDatabase Db { get; init; }

        public required OwnedStore Store { get; init; }

        public required FencedOwner Placer { get; init; }

        public required long EpochA { get; init; }

        public required long EpochB { get; init; }

        public PlacementMember A => new("a", EpochA, ["unit:widget"]);

        public static async Task<Rig> CreateAsync(PostgresFixture postgres)
        {
            var db = await OwnedDatabase.CreateAsync(postgres);
            await db.AddMemberAsync("p");
            var epochA = await db.AddMemberAsync("a");
            var epochB = await db.AddMemberAsync("b");
            await using (var connection = await db.OpenAsync())
            {
                await OwnedDatabase.ExecAsync(connection, "UPDATE reeve_cluster.member SET serving_since = clock_timestamp(), capabilities = '{unit:widget}'");
            }

            var placerToken = await db.CreateOwnedAsync(Placement.PlacerKind, Placement.PlacerKind, "p");
            var options = Options.Create(new ClusterOptions { ConnectionString = db.ConnectionString, FencedTransactionTimeout = Calm });
            return new Rig
            {
                Db = db,
                Store = new OwnedStore(new OwnedDataSource(options), options),
                Placer = new FencedOwner(Placement.PlacerKind, Placement.PlacerKind, placerToken, db.Timeline),
                EpochA = epochA,
                EpochB = epochB,
            };
        }

        public Task<long?> AssignAsync(string key, OwnedRow? expected, PlacementMember? target = null, EligibilityFilter? filter = null) =>
            Store.AssignAsync(Placer, new Assignment(Kind, key, expected, target ?? A), filter ?? EligibilityFilter.Any, CancellationToken.None);

        public async Task<OwnedRow?> RowAsync(string key) => (await Store.ReadAsync(Kind, key, CancellationToken.None)).Row;

        public async Task ExecAsync(string sql)
        {
            await using var connection = await Db.OpenAsync();
            await OwnedDatabase.ExecAsync(connection, sql);
        }
    }

    [Fact]
    public async Task An_absent_row_is_inserted_owned_by_the_target_and_a_free_one_is_taken_as_read()
    {
        var rig = await Rig.CreateAsync(postgres);

        var inserted = await rig.AssignAsync("w", null);
        Assert.Equal(new OwnedRow("a", rig.EpochA, inserted!.Value), await rig.RowAsync("w"));

        Assert.True(await rig.Store.ReleaseAsync(Kind, "w", "a", rig.EpochA, inserted.Value, CancellationToken.None));
        var taken = await rig.AssignAsync("w", new OwnedRow(null, null, inserted.Value), new PlacementMember("b", rig.EpochB, ["unit:widget"]));
        Assert.True(taken > inserted);
        Assert.Equal(new OwnedRow("b", rig.EpochB, taken!.Value), await rig.RowAsync("w"));
    }

    [Fact]
    public async Task An_orphaned_row_is_taken_and_a_row_whose_hold_is_valid_is_not_whatever_the_pass_read()
    {
        var rig = await Rig.CreateAsync(postgres);
        var token = await rig.Db.CreateOwnedAsync(Kind, "w", "b");
        var asRead = new OwnedRow("b", rig.EpochB, token);

        Assert.Null(await rig.AssignAsync("w", asRead));
        Assert.Equal(asRead, await rig.RowAsync("w"));

        await rig.ExecAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'b'");
        Assert.NotNull(await rig.AssignAsync("w", asRead));
        Assert.Equal("a", (await rig.RowAsync("w"))!.Value.OwnerReplica);
    }

    public static TheoryData<string, string> Unfit => new()
    {
        { "drain step 1, or a delay between activation and Serving", "UPDATE reeve_cluster.member SET serving_since = NULL WHERE replica_id = 'a'" },
        { "draining", "UPDATE reeve_cluster.member SET state = 'draining', serving_since = NULL WHERE replica_id = 'a'" },
        { "fenced", "UPDATE reeve_cluster.member SET state = 'fenced', serving_since = NULL WHERE replica_id = 'a'" },
        { "lease gone", "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'" },
        { "rejoined under a new epoch", "UPDATE reeve_cluster.member SET epoch = epoch + 100 WHERE replica_id = 'a'" },
        { "no unit:widget", "UPDATE reeve_cluster.member SET capabilities = '{}' WHERE replica_id = 'a'" },
    };

    [Theory]
    [MemberData(nameof(Unfit))]
    public async Task A_target_that_no_longer_qualifies_when_the_statement_runs_is_refused(string why, string change)
    {
        var rig = await Rig.CreateAsync(postgres);
        var freed = await rig.Db.CreateOwnedAsync(Kind, "free", "b");
        await rig.ExecAsync($"UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE key = 'free'");
        await rig.ExecAsync(change);

        Assert.Null(await rig.AssignAsync("absent", null));
        Assert.Null(await rig.AssignAsync("free", new OwnedRow(null, null, freed)));
        Assert.Null(await rig.RowAsync("absent"));
        Assert.Equal(new OwnedRow(null, null, freed), await rig.RowAsync("free"));
        _ = why;
    }

    [Fact]
    public async Task The_kind_eligibility_filter_is_checked_inside_the_write()
    {
        var rig = await Rig.CreateAsync(postgres);
        await rig.ExecAsync("UPDATE reeve_cluster.member SET capabilities = '{unit:widget,gpu,degraded}' WHERE replica_id = 'a'");

        Assert.Null(await rig.AssignAsync("w", null, filter: EligibilityFilter.Any.Require("gpu").Forbid("degraded")));
        Assert.Null(await rig.AssignAsync("w", null, filter: EligibilityFilter.Any.Require("tpu")));
        Assert.NotNull(await rig.AssignAsync("w", null, filter: EligibilityFilter.Any.Require("gpu")));
    }

    [Fact]
    public async Task A_placer_whose_tenure_ended_is_refused_and_writes_nothing()
    {
        var rig = await Rig.CreateAsync(postgres);
        var token = await rig.Db.CreateOwnedAsync(Kind, "owned", "b");
        await rig.ExecAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'p'");

        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.AssignAsync("w", null));
        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.Store.RequestReleaseAsync(rig.Placer, Kind, "owned", token, CancellationToken.None));
        Assert.Null(await rig.RowAsync("w"));
        Assert.Null((await rig.Store.ReadAsync(Kind, "owned", CancellationToken.None)).RequestToken);
    }

    [Fact]
    public async Task A_placer_on_another_timeline_fences_and_writes_nothing()
    {
        var rig = await Rig.CreateAsync(postgres);
        var placer = new FencedOwner(Placement.PlacerKind, Placement.PlacerKind, rig.Placer.Token, rig.Db.PreviousTimeline);

        await Assert.ThrowsAsync<OwnershipLostException>(() =>
            rig.Store.AssignAsync(placer, new Assignment(Kind, "w", null, rig.A), EligibilityFilter.Any, CancellationToken.None));
        Assert.Equal(1, placer.OtherTimelines);
        Assert.Null(await rig.RowAsync("w"));
    }

    [Fact]
    public async Task A_placer_the_library_is_stopping_writes_nothing()
    {
        var rig = await Rig.CreateAsync(postgres);
        rig.Placer.Refusal = new OwnershipLostException("stopping");

        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.AssignAsync("w", null));
        Assert.Null(await rig.RowAsync("w"));
    }

    [Fact]
    public async Task The_request_does_not_wait_for_the_owner_fenced_transaction_and_the_take_holds_the_placer_share_lock()
    {
        var rig = await Rig.CreateAsync(postgres);
        var token = await rig.Db.CreateOwnedAsync(Kind, "w", "b");
        await using var owner = await rig.Db.OpenAsync();
        await using var fenced = await BeginFencedAsync(owner, Kind, "w", token, rig.Db.Timeline, Calm);

        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        Assert.True(await rig.Store.RequestReleaseAsync(rig.Placer, Kind, "w", token, bound.Token));

        await rig.ExecAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'b'");
        var take = rig.AssignAsync("w", new OwnedRow("b", rig.EpochB, token));
        await rig.Db.WaitForALockWaiterAsync();
        Assert.False(await rig.Db.CanLockOwnedAsync(Placement.PlacerKind, Placement.PlacerKind, "FOR NO KEY UPDATE"));
        Assert.False(take.IsCompleted);

        await fenced.CommitAsync();
        Assert.NotNull(await take.WaitAsync(bound.Token));
        Assert.True(await rig.Db.CanLockOwnedAsync(Placement.PlacerKind, Placement.PlacerKind, "FOR NO KEY UPDATE"));
    }
}
