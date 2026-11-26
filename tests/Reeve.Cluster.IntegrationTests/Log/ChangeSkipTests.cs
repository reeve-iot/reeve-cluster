using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Log;

public sealed class ChangeSkipTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public ChangeSkipTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I15_the_close_writes_its_columns_appends_a_repair_entry_and_fills_until_seq_with_its_seq_so_the_range_excludes_it()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", seq));

        var repair = await rig.Skip.CloseAsync(opened.SkipId, "closer");

        Assert.True(repair > seq);
        Assert.Equal(repair, await rig.Log.ScalarAsync<long>($"SELECT until_seq FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
        Assert.Equal("closer", await rig.Log.ScalarAsync<string>($"SELECT closed_by FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
        Assert.True(await rig.Log.ScalarAsync<bool>($"SELECT closed_at IS NOT NULL FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
        Assert.Equal("widget", await rig.Log.ScalarAsync<string>($"SELECT aggregate_type FROM reeve_cluster.change_log WHERE seq = {repair} AND aggregate_key IS NULL"));

        Assert.Empty(await rig.RecordAsync(repair, repair));
        Assert.Equal(["widget"], await rig.RecordAsync(seq, seq));

        var close = Assert.Single(rig.EventsOf(LogActions.SkipClose));
        Assert.Equal(repair, close.State![ObservationKeys.RepairSeq]);
    }

    [Fact]
    public async Task I4_the_close_locks_its_row_before_the_append_and_holds_both_to_its_commit()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));

        using var before = rig.Faults.HoldNext(LogFaultPoints.CloseBeforeRepairAppend);
        using var after = rig.Faults.HoldNext(LogFaultPoints.CloseAfterRepairAppend);
        var close = rig.Skip.CloseAsync(opened.SkipId, "closer");

        await before.WaitReachedAsync();
        Assert.False(await rig.SkipRowFreeAsync(opened.SkipId));
        Assert.True(await rig.ChangeLogLockFreeAsync());
        Assert.True(await rig.AckRowFreeAsync());
        before.Release();

        await after.WaitReachedAsync();
        Assert.False(await rig.SkipRowFreeAsync(opened.SkipId));
        Assert.False(await rig.ChangeLogLockFreeAsync());
        after.Release();

        await close;
        Assert.True(await rig.ChangeLogLockFreeAsync());
    }

    [Fact]
    public async Task A_second_close_appends_nothing_and_returns_the_same_repair_seq()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));
        var repair = await rig.Skip.CloseAsync(opened.SkipId, "closer");
        var entries = await rig.LogCountAsync();

        Assert.Equal(repair, await rig.Skip.CloseAsync(opened.SkipId, "someone else"));
        Assert.Equal(entries, await rig.LogCountAsync());
        Assert.Equal("closer", await rig.Log.ScalarAsync<string>($"SELECT closed_by FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
    }

    [Fact]
    public async Task A_close_of_an_unknown_skip_throws_KeyNotFoundException_and_appends_nothing()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);

        await Assert.ThrowsAsync<KeyNotFoundException>(() => rig.Skip.CloseAsync(42, "closer"));
        Assert.Equal(0L, await rig.LogCountAsync());
    }

    [Fact]
    public async Task I4_an_open_locks_change_ack_inserts_its_range_and_appends_nothing()
    {
        await using var rig = await SkipRig.CreateAsync(postgres, fencedTransactionTimeout: TimeSpan.FromSeconds(60));
        var entries = await rig.LogCountAsync();

        using var insert = rig.Faults.HoldNext(LogFaultPoints.OpenAfterInsert);
        var open = rig.OpenAsync("widget", 1);

        await insert.WaitReachedAsync();
        Assert.False(await rig.AckRowFreeAsync());
        Assert.True(await rig.ChangeLogLockFreeAsync());
        insert.Release();

        var opened = Assert.IsType<SkipOpenResult.Opened>(await open);
        Assert.True(await rig.AckRowFreeAsync());
        Assert.Equal(entries, await rig.LogCountAsync());
        Assert.True(await rig.Log.ScalarAsync<bool>($"SELECT until_seq IS NULL AND closed_at IS NULL FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));

        var points = rig.Faults.Reached(LogFaultPoints.OpenBeforeUpsert);
        Assert.Equal(1, points);
        var lockEvent = Assert.Single(rig.EventsOf(LogActions.OpenLock));
        Assert.Equal("1min", lockEvent.State![ObservationKeys.TransactionTimeout]);
        Assert.Equal(opened.SkipId, Assert.Single(rig.EventsOf(LogActions.OpenCommit)).State![ObservationKeys.SkipId]);
    }

    [Fact]
    public async Task The_skip_table_has_no_lifted_from_version_column()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);

        Assert.Equal(0L, await rig.Log.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'reeve_cluster' AND table_name = 'change_skip' AND column_name = 'lifted_from_version'"));
    }

    [Fact]
    public async Task An_open_stores_its_request_and_never_raises_from_seq()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        await rig.WriteAsync("widget");

        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.Skip.OpenAsync(new SkipRequest("widget", 7, "poison entry", "alice")));

        Assert.Equal(
            "widget|7|poison entry|alice|t|t",
            await rig.Log.ScalarAsync<string>(
                $"SELECT concat_ws('|', aggregate_type, from_seq, reason, opened_by, until_seq IS NULL, opened_at IS NOT NULL) FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
    }

    [Fact]
    public async Task Two_concurrent_closes_of_one_skip_append_one_repair_entry_and_answer_the_same_seq()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));
        var entries = await rig.LogCountAsync();
        using var hold = rig.Faults.HoldNext(LogFaultPoints.CloseBeforeRepairAppend);

        var first = rig.Skip.CloseAsync(opened.SkipId, "first");
        await hold.WaitReachedAsync();
        var second = rig.Skip.CloseAsync(opened.SkipId, "second");
        await rig.WaitUntilASessionWaitsForALockAsync();
        Assert.False(second.IsCompleted);
        hold.Release();

        var repair = await first.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(repair, await second.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(entries + 1, await rig.LogCountAsync());
        Assert.Equal(repair, await rig.Log.ScalarAsync<long>($"SELECT until_seq FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
        Assert.Equal("first", await rig.Log.ScalarAsync<string>($"SELECT closed_by FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
    }

    [Fact]
    public async Task A_stalled_close_is_ended_by_its_FencedTransactionTimeout_and_writes_nothing()
    {
        await using var rig = await SkipRig.CreateAsync(postgres, fencedTransactionTimeout: TimeSpan.FromSeconds(1));
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));
        var entries = await rig.LogCountAsync();
        using var stall = rig.Faults.HoldNext(LogFaultPoints.CloseAfterRepairAppend);

        var close = rig.Skip.CloseAsync(opened.SkipId, "closer");

        await Task.WhenAny(stall.WaitReachedAsync(), close);
        await rig.WaitUntilNoSessionIsIdleInATransactionAsync();
        stall.Release();

        await Assert.ThrowsAnyAsync<NpgsqlException>(() => close);
        Assert.True(await rig.ChangeLogLockFreeAsync());
        Assert.Equal(entries, await rig.LogCountAsync());
        Assert.True(await rig.Log.ScalarAsync<bool>(
            $"SELECT until_seq IS NULL AND closed_by IS NULL FROM reeve_cluster.change_skip WHERE id = {opened.SkipId}"));
    }

    [Fact]
    public async Task A_close_publishes_the_appliers_wakeup_for_its_repair_entry_and_an_open_publishes_none()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));
        Assert.Empty(rig.EventsOf(LogActions.WCommit));

        var closeSeq = await rig.Skip.CloseAsync(opened.SkipId, "closer");

        Assert.Equal<long>([closeSeq], rig.EventsOf(LogActions.WCommit).Select(e => (long)e.State![ObservationKeys.Seq]!));
    }
}
