using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class FenceStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public FenceStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync() => await db.DisposeAsync();

    private Task<List<FenceHistoryRow>> ReadHistoryAsync(string id) => db.ReadFenceHistoryAsync(id);

    private Task RecordAsync(long epoch, string trigger = "fence-start", double spanSeconds = 3, string id = "r1", double ageSeconds = 0) =>
        db.Fences.RecordFenceAsync(new FenceRecord(id, epoch, trigger, TimeSpan.FromSeconds(spanSeconds)), TimeSpan.FromSeconds(ageSeconds), CancellationToken.None);

    [Theory]
    [InlineData("joining")]
    [InlineData("active")]
    public async Task Step_3_writes_its_own_row_fenced_with_the_lease_ended_and_serving_since_cleared(string state)
    {
        await db.InsertRowAsync("r1", epoch: 7, state, leaseSeconds: 30);
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET serving_since = clock_timestamp()");

        Assert.True(await db.Fences.ReleaseLeaseAsync("r1", 7, CancellationToken.None));

        var row = await db.ReadRowAsync("r1");
        Assert.Equal(7, row.Epoch);
        Assert.Equal("fenced", row.State);
        Assert.True(row.LeaseLeft <= TimeSpan.Zero, $"the lease has {row.LeaseLeft} left");
        Assert.Null(row.ServingSince);
    }

    [Theory]
    [InlineData(8, "active")]
    [InlineData(6, "active")]
    [InlineData(7, "draining")]
    [InlineData(7, "fenced")]
    public async Task Step_3_writes_nothing_on_a_row_of_another_epoch_or_one_no_longer_joining_or_active(long rowEpoch, string state)
    {
        await db.InsertRowAsync("r1", rowEpoch, state, leaseSeconds: 30);

        Assert.False(await db.Fences.ReleaseLeaseAsync("r1", 7, CancellationToken.None));

        var row = await db.ReadRowAsync("r1");
        Assert.Equal(rowEpoch, row.Epoch);
        Assert.Equal(state, row.State);
        Assert.True(row.LeaseLeft > TimeSpan.FromSeconds(20));
    }

    [Fact]
    public async Task Step_3_on_an_absent_row_writes_nothing()
    {
        Assert.False(await db.Fences.ReleaseLeaseAsync("r1", 7, CancellationToken.None));
    }

    [Fact]
    public async Task The_fence_history_record_carries_the_trigger_the_time_and_the_span()
    {
        var before = await db.ClockAsync();
        await RecordAsync(7, "timeline", spanSeconds: 11.5);
        var after = await db.ClockAsync();

        var (epoch, trigger, span, _) = Assert.Single(await ReadHistoryAsync("r1"));
        Assert.Equal(7, epoch);
        Assert.Equal("timeline", trigger);
        Assert.Equal(TimeSpan.FromSeconds(11.5), span);
        var fencedAt = await db.ReadFencedAtAsync("r1", 7);
        Assert.InRange(fencedAt, before, after);
    }

    [Fact]
    public async Task The_record_carries_the_time_of_step_0_on_the_databases_clock()
    {
        var before = await db.ClockAsync();
        await RecordAsync(7, ageSeconds: 40);
        var after = await db.ClockAsync();

        Assert.Single(await ReadHistoryAsync("r1"));
        var fencedAt = await db.ReadFencedAtAsync("r1", 7);
        Assert.InRange(fencedAt, before - TimeSpan.FromSeconds(40), after - TimeSpan.FromSeconds(40));
    }

    [Fact]
    public async Task A_second_record_of_one_epoch_changes_nothing()
    {
        await RecordAsync(7, "fence-start", spanSeconds: 3);

        await RecordAsync(7, "fatal-fault", spanSeconds: 9);

        var (_, trigger, span, _) = Assert.Single(await ReadHistoryAsync("r1"));
        Assert.Equal("fence-start", trigger);
        Assert.Equal(TimeSpan.FromSeconds(3), span);
    }

    [Fact]
    public async Task The_record_trims_the_replicas_history_to_its_newest_rows_and_leaves_other_replicas_alone()
    {
        for (var epoch = 1; epoch <= FenceStore.HistoryRows + 4; epoch++)
        {
            await RecordAsync(epoch);
        }

        await RecordAsync(3, id: "r2");

        Assert.Equal(
            Enumerable.Range(5, FenceStore.HistoryRows).Select(e => (long)e),
            (await ReadHistoryAsync("r1")).Select(r => r.Epoch));
        Assert.Single(await ReadHistoryAsync("r2"));
    }

    [Fact]
    public async Task The_rejoin_read_finds_no_row_for_a_new_replica()
    {
        Assert.Equal(OwnRowObservation.Absent, await db.Fences.ReadRejoinStateAsync("r1", CancellationToken.None));
    }

    [Fact]
    public async Task The_rejoin_read_returns_the_own_row_whatever_fence_history_holds()
    {
        await db.InsertRowAsync("r1", epoch: 9, "fenced", leaseSeconds: 30);
        await RecordAsync(9, spanSeconds: 4);
        await db.InsertRowAsync("r2", epoch: 3, "active", leaseSeconds: -1);

        Assert.Equal(new OwnRowObservation(true, 9, MemberRowState.Fenced, true), await db.Fences.ReadRejoinStateAsync("r1", CancellationToken.None));
        Assert.Equal(new OwnRowObservation(true, 3, MemberRowState.Active, false), await db.Fences.ReadRejoinStateAsync("r2", CancellationToken.None));
    }
}
