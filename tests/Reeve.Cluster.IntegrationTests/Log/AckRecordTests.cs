using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Log;

public sealed class AckRecordTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public AckRecordTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task The_migration_creates_the_one_change_ack_row_at_0_and_an_empty_change_skip()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);

        Assert.Equal(0L, await rig.AckedSeqAsync());
        Assert.Equal(1L, await rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_ack"));
        Assert.Equal(0L, await rig.SkipCountAsync());
        Assert.Equal(1L, await rig.Log.ScalarAsync<long>(
            "SELECT cache_size FROM pg_sequences WHERE schemaname = 'reeve_cluster' AND sequencename = 'change_skip_id_seq'"));
    }

    [Fact]
    public async Task I30_a_record_raises_acked_seq_to_its_bound_with_greatest_and_never_lowers_it()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var first = await rig.WriteAsync("widget");
        var second = await rig.WriteAsync("widget");

        Assert.Empty(await rig.RecordAsync(first, second + 5));
        Assert.Equal(second + 5, await rig.AckedSeqAsync());

        Assert.Empty(await rig.RecordAsync(second, second));
        Assert.Equal(second + 5, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task I30_after_a_record_an_open_from_its_bound_or_below_of_any_type_is_refused_and_writes_nothing()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");
        await rig.RecordAsync(seq, seq);
        var entries = await rig.LogCountAsync();

        foreach (var (type, from) in new[] { ("widget", seq), ("gadget", seq), ("gadget", 1L) })
        {
            var refused = Assert.IsType<SkipOpenResult.Refused>(await rig.OpenAsync(type, from));
            Assert.Equal(seq, refused.AckedSeq);
        }

        Assert.Equal(0L, await rig.SkipCountAsync());
        Assert.Equal(entries, await rig.LogCountAsync());
        Assert.Equal(3, rig.EventsOf(LogActions.OpenRefuse).Count);
        Assert.Empty(rig.EventsOf(LogActions.OpenCommit));

        Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("gadget", seq + 1));
    }

    [Fact]
    public async Task I30_an_open_committed_before_the_record_makes_its_skip_read_name_the_type_whatever_the_write_touched()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");
        Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("gadget", seq));

        Assert.Equal(["gadget"], await rig.RecordAsync(seq, seq));

        var read = Assert.Single(rig.EventsOf(LogActions.AckSkipRead));
        Assert.Equal<IEnumerable<string>>(["gadget"], (IEnumerable<string>)read.State![ObservationKeys.Skipped]!);
    }

    [Fact]
    public async Task I30_the_skip_read_names_ranges_from_at_or_below_the_bound_that_are_open_or_end_above_the_awaited_seq()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var first = await rig.WriteAsync("widget");

        var closed = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("closed", first));
        var repair = await rig.Skip.CloseAsync(closed.SkipId, "operator");

        var later = await rig.WriteAsync("widget");
        Assert.True(later > repair);

        Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("above", later + 10));

        Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("between", later + 1));

        Assert.Equal(["between"], await rig.RecordAsync(later, later + 1));
        Assert.Empty(await rig.RecordAsync(repair, repair));
        Assert.Equal(["closed"], await rig.RecordAsync(first, first));
    }

    [Fact]
    public async Task A_record_with_a_remainder_under_1_ms_runs_with_transaction_timeout_1_ms()
    {
        await using var rig = await SkipRig.CreateAsync(postgres, time: new SteppedTime());
        var seq = await rig.WriteAsync("widget");

        using var hold = rig.Faults.HoldNext(LogFaultPoints.RecordBetweenUpsertAndSkipRead);
        var record = rig.RecordAsync(seq, seq, TimeSpan.FromTicks(3));
        if (await Task.WhenAny(hold.WaitReachedAsync(), record) != record)
        {
            Assert.Equal("1ms", Assert.Single(rig.EventsOf(LogActions.AckRecord)).State![ObservationKeys.TransactionTimeout]);

            await rig.WaitUntilNoSessionIsIdleInATransactionAsync();
            hold.Release();
        }

        await Assert.ThrowsAnyAsync<NpgsqlException>(() => record);
        Assert.Equal(0L, await rig.AckedSeqAsync());

        Assert.Empty(await rig.RecordAsync(seq, seq, TimeSpan.FromSeconds(30)));
        Assert.Equal(seq, await rig.AckedSeqAsync());
        Assert.Equal("30s", rig.EventsOf(LogActions.AckRecord)[^1].State![ObservationKeys.TransactionTimeout]);
    }

    [Fact]
    public async Task I30_the_record_holds_the_change_ack_row_from_its_upsert_to_its_commit()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");

        using var before = rig.Faults.HoldNext(LogFaultPoints.RecordBeforeUpsert);
        using var between = rig.Faults.HoldNext(LogFaultPoints.RecordBetweenUpsertAndSkipRead);
        using var commit = rig.Faults.HoldNext(LogFaultPoints.RecordBeforeCommit);
        var record = rig.RecordAsync(seq, seq);

        await before.WaitReachedAsync();
        Assert.True(await rig.AckRowFreeAsync());
        before.Release();

        await between.WaitReachedAsync();
        Assert.False(await rig.AckRowFreeAsync());
        Assert.True(await rig.ChangeLogLockFreeAsync());
        between.Release();

        await commit.WaitReachedAsync();
        Assert.False(await rig.AckRowFreeAsync());
        commit.Release();

        Assert.Empty(await record);
        Assert.True(await rig.AckRowFreeAsync());
    }

    [Fact]
    public async Task A_stalled_open_delays_a_record_only_until_its_FencedTransactionTimeout_ends_it_and_writes_nothing()
    {
        await using var rig = await SkipRig.CreateAsync(postgres, fencedTransactionTimeout: TimeSpan.FromSeconds(1));
        var seq = await rig.WriteAsync("widget");

        using var stall = rig.Faults.HoldNext(LogFaultPoints.OpenAfterUpsert);
        var open = rig.OpenAsync("gadget", seq);
        await stall.WaitReachedAsync();
        Assert.Equal("1s", Assert.Single(rig.EventsOf(LogActions.OpenLock)).State![ObservationKeys.TransactionTimeout]);

        var record = rig.RecordAsync(seq, seq);
        Assert.Empty(await record.WaitAsync(TimeSpan.FromSeconds(20)));
        Assert.Equal(seq, await rig.AckedSeqAsync());

        stall.Release();
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => open);
        Assert.Equal(0L, await rig.SkipCountAsync());
    }

    [Fact]
    public async Task A_stalled_record_delays_an_open_only_until_its_transaction_timeout_ends_it()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");

        using var stall = rig.Faults.HoldNext(LogFaultPoints.RecordHold);
        var record = rig.RecordAsync(seq, seq, TimeSpan.FromSeconds(1));
        await stall.WaitReachedAsync();

        var open = rig.OpenAsync("gadget", seq);
        Assert.IsType<SkipOpenResult.Opened>(await open.WaitAsync(TimeSpan.FromSeconds(20)));
        Assert.Equal(0L, Assert.Single(rig.EventsOf(LogActions.OpenLock)).State![ObservationKeys.AckedSeq]);

        stall.Release();
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => record);
        Assert.Equal(0L, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task A_record_takes_a_bound_at_or_above_a_positive_awaited_seq_and_runs_only_before_its_deadline()
    {
        await using var rig = await SkipRig.CreateAsync(postgres);
        var seq = await rig.WriteAsync("widget");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => rig.RecordAsync(0, 1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => rig.RecordAsync(5, 4));
        await Assert.ThrowsAsync<TimeoutException>(() => rig.RecordAsync(seq, seq, TimeSpan.Zero));
        await Assert.ThrowsAsync<TimeoutException>(() => rig.RecordAsync(seq, seq, TimeSpan.FromSeconds(-1)));
        Assert.Equal(0L, await rig.AckedSeqAsync());
        Assert.Empty(rig.EventsOf(LogActions.AckRecord));
    }

    [Fact]
    public async Task A_record_takes_its_remainder_after_the_wait_for_a_connection_not_before()
    {
        var time = new SteppedTime();
        await using var rig = await SkipRig.CreateAsync(postgres, time: time);
        var seq = await rig.WriteAsync("widget");

        using (rig.Faults.Observe(LogFaultPoints.RecordBeforeUpsert, _ =>
        {
            time.Advance(TimeSpan.FromSeconds(31));
            return ValueTask.CompletedTask;
        }))
        {
            await Assert.ThrowsAsync<TimeoutException>(() => rig.RecordAsync(seq, seq, TimeSpan.FromSeconds(30)));
            Assert.Equal(0L, await rig.AckedSeqAsync());

            Assert.Empty(await rig.RecordAsync(seq, seq, TimeSpan.FromSeconds(41)));
        }

        Assert.Equal("10s", Assert.Single(rig.EventsOf(LogActions.AckRecord)).State![ObservationKeys.TransactionTimeout]);
    }

    private sealed class SteppedTime : TimeProvider
    {
        private long now;

        public override long GetTimestamp() => Interlocked.Read(ref now);

        public void Advance(TimeSpan by) =>
            Interlocked.Add(ref now, (long)(by.Ticks * (decimal)TimestampFrequency / TimeSpan.TicksPerSecond));
    }
}
