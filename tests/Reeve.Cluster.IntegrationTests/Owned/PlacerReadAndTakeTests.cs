using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;

namespace Int.Owned;

public sealed class PlacerReadAndTakeTests : IClassFixture<PostgresFixture>
{
    private const string Kind = "widget";
    private static readonly TimeSpan Calm = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public PlacerReadAndTakeTests(PostgresFixture postgres) => this.postgres = postgres;

    private static OwnedStore Store(OwnedDatabase db)
    {
        var options = Options.Create(new ClusterOptions { ConnectionString = db.ConnectionString, FencedTransactionTimeout = Calm });
        return new OwnedStore(new OwnedDataSource(options), options);
    }

    private static async Task ExecAsync(OwnedDatabase db, string sql)
    {
        await using var connection = await db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, sql);
    }

    [Fact]
    public async Task The_pass_reads_the_serving_members_and_judges_each_hold_on_the_owners_epoch_state_and_lease()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("serving");
        await db.AddMemberAsync("activated");
        await db.AddMemberAsync("expired");
        await db.AddMemberAsync("draining", state: "draining");
        var rejoinedEpoch = await db.AddMemberAsync("rejoined");

        await db.CreateOwnedAsync(Kind, "held", "serving");
        await db.CreateOwnedAsync(Kind, "of-activated", "activated");
        await db.CreateOwnedAsync(Kind, "of-expired", "expired");
        await db.CreateOwnedAsync(Kind, "of-draining", "draining");
        await db.CreateOwnedAsync(Kind, "of-rejoined", "rejoined");
        await db.CreateOwnedAsync(Kind, "free", "serving");
        await db.CreateOwnedAsync(Kind, "of-gone", "serving");
        await db.CreateOwnedAsync("other", "x", "serving");

        await ExecAsync(db, "UPDATE reeve_cluster.member SET serving_since = clock_timestamp(), capabilities = '{unit:widget}' WHERE replica_id IN ('serving', 'expired', 'rejoined')");
        await ExecAsync(db, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'expired'");
        await ExecAsync(db, "UPDATE reeve_cluster.member SET epoch = epoch + 100 WHERE replica_id = 'rejoined'");
        await ExecAsync(db, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'widget' AND key = 'free'");
        await ExecAsync(db, "UPDATE reeve_cluster.owned SET owner_replica = 'gone' WHERE kind = 'widget' AND key = 'of-gone'");

        var read = await Store(db).ReadPlacementAsync([Kind], CancellationToken.None);

        Assert.Equal(new[] { "rejoined", "serving" }, read.Members.Select(m => m.ReplicaId).OrderBy(id => id, StringComparer.Ordinal).ToArray());
        Assert.Equal(rejoinedEpoch + 100, read.Members.Single(m => m.ReplicaId == "rejoined").Epoch);
        Assert.Equal(new[] { "unit:widget" }, read.Members.Single(m => m.ReplicaId == "serving").Capabilities.ToArray());
        Assert.All(read.Rows, r => Assert.Equal(Kind, r.Kind));
        Assert.Equal(
            new[] { "free=False", "held=True", "of-activated=True", "of-draining=False", "of-expired=False", "of-gone=False", "of-rejoined=False" },
            read.Rows.Select(r => $"{r.Key}={r.HoldValid}").OrderBy(s => s, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task A_free_row_is_taken_only_under_the_token_the_pass_read()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("p");
        var epochA = await db.AddMemberAsync("a");
        var epochB = await db.AddMemberAsync("b");
        await ExecAsync(db, "UPDATE reeve_cluster.member SET serving_since = clock_timestamp(), capabilities = '{unit:widget}'");
        var placerToken = await db.CreateOwnedAsync(Placement.PlacerKind, Placement.PlacerKind, "p");
        var store = Store(db);
        var placer = new FencedOwner(Placement.PlacerKind, Placement.PlacerKind, placerToken, db.Timeline);
        var a = new PlacementMember("a", epochA, ["unit:widget"]);
        var b = new PlacementMember("b", epochB, ["unit:widget"]);

        var first = await store.AssignAsync(placer, new Assignment(Kind, "w", null, a), EligibilityFilter.Any, CancellationToken.None);
        Assert.NotNull(first);
        Assert.True(await store.ReleaseAsync(Kind, "w", "a", epochA, first.Value, CancellationToken.None));
        var second = await store.AssignAsync(placer, new Assignment(Kind, "w", new OwnedRow(null, null, first.Value), a), EligibilityFilter.Any, CancellationToken.None);
        Assert.NotNull(second);
        Assert.True(await store.ReleaseAsync(Kind, "w", "a", epochA, second.Value, CancellationToken.None));

        Assert.Null(await store.AssignAsync(placer, new Assignment(Kind, "w", new OwnedRow(null, null, first.Value), b), EligibilityFilter.Any, CancellationToken.None));
        Assert.Equal(new OwnedRow(null, null, second.Value), (await store.ReadAsync(Kind, "w", CancellationToken.None)).Row);
    }

    [Fact]
    public async Task The_pass_reads_the_requests_in_force_past_ReleaseGrace()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var overdue = await db.CreateOwnedAsync("other", "late", "a");
        var young = await db.CreateOwnedAsync(Kind, "young", "a");
        var moved = await db.CreateOwnedAsync(Kind, "moved", "a");
        var released = await db.CreateOwnedAsync(Kind, "released", "a");
        await ExecAsync(db, $"""
            INSERT INTO reeve_cluster.release_request VALUES
              ('other', 'late', {overdue}, clock_timestamp() - interval '1 minute'),
              ('widget', 'young', {young}, clock_timestamp()),
              ('widget', 'moved', {moved}, clock_timestamp() - interval '1 minute'),
              ('widget', 'released', {released}, clock_timestamp() - interval '1 minute');
            UPDATE reeve_cluster.owned SET token = token + 1000 WHERE key = 'moved';
            UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE key = 'released';
            """);

        var read = await Store(db).ReadPlacementAsync([Kind], CancellationToken.None);

        var late = Assert.Single(read.Overdue);
        Assert.Equal(("other", "late", overdue, "a"), (late.Kind, late.Key, late.Token, late.Owner));
    }
}
