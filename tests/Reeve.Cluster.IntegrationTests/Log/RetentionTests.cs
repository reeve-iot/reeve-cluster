using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;

namespace Int.Log;

public sealed class RetentionTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public RetentionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task Horizon_seq_stays_0_while_a_run_deletes_nothing()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var first = await rig.WriteAsync();
        await rig.WriteAsync();

        Assert.Equal(new RetentionRun(null, 0), await retention.RunAsync());
        await rig.MemberAsync("a");
        Assert.Equal(new RetentionRun(0, 0), await retention.RunAsync());
        await rig.ApplyAsync("a", first, notice: false);
        Assert.Equal(new RetentionRun(first, 0), await retention.RunAsync());

        Assert.Equal(0L, await HorizonAsync(rig));
        Assert.Equal(2L, await rig.Skip.LogCountAsync());
        Assert.Empty(rig.EventsOf(LogActions.Retention));
    }

    [Fact]
    public async Task I5_a_run_deletes_below_the_minimum_over_the_leased_joining_and_active_members_and_records_it_as_the_horizon()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var seqs = new List<long>();
        for (var i = 0; i < 6; i++)
        {
            seqs.Add(await rig.WriteAsync());
        }

        await Member(rig, "active", "active", seqs[4]);
        await Member(rig, "joining", "joining", seqs[2]);
        await Member(rig, "expired", "active", seqs[0], TimeSpan.FromMinutes(-1));
        await Member(rig, "fenced", "fenced", seqs[0]);
        await Member(rig, "draining", "draining", seqs[1]);

        Assert.Equal(new RetentionRun(seqs[2], 2), await retention.RunAsync());
        Assert.Equal(seqs[2..], await rig.Log.SeqsAsync());
        Assert.Equal(seqs[2], await HorizonAsync(rig));
        Assert.True(await LeasedCursorsValidAsync(rig));
        var recorded = Assert.Single(rig.EventsOf(LogActions.Retention));
        Assert.Equal(seqs[2], recorded.State![ObservationKeys.Horizon]);
        Assert.Equal(2L, recorded.State[ObservationKeys.Deleted]);

        await rig.ApplyAsync("joining", seqs[5], notice: false);
        Assert.Equal(new RetentionRun(seqs[4], 2), await retention.RunAsync());
        Assert.Equal(seqs[4..], await rig.Log.SeqsAsync());
        Assert.Equal(seqs[4], await HorizonAsync(rig));

        Assert.Equal(new RetentionRun(seqs[4], 0), await retention.RunAsync());
        Assert.Equal(seqs[4..], await rig.Log.SeqsAsync());
        Assert.Equal(seqs[4], await HorizonAsync(rig));
        Assert.Equal(2, rig.EventsOf(LogActions.Retention).Count);
    }

    [Fact]
    public async Task Without_a_leased_member_nothing_runs_and_nothing_is_deleted()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var first = await rig.WriteAsync();
        var last = await rig.WriteAsync();
        await Member(rig, "gone", "active", last, TimeSpan.FromMinutes(-1));
        await Member(rig, "left", "fenced", last, TimeSpan.FromMinutes(-1));

        Assert.Equal(new RetentionRun(null, 0), await retention.RunAsync());
        Assert.Equal([first, last], await rig.Log.SeqsAsync());
        Assert.Equal(0L, await HorizonAsync(rig));
    }

    [Fact]
    public async Task I5_cursors_that_advance_between_the_minimum_and_the_delete_keep_their_rows()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var seqs = new List<long>();
        for (var i = 0; i < 4; i++)
        {
            seqs.Add(await rig.WriteAsync());
        }

        await Member(rig, "a", "active", seqs[1]);
        await Member(rig, "b", "joining", seqs[2]);

        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RetentionBetweenMinimumAndDelete);
        var run = retention.RunAsync();
        await hold.WaitReachedAsync();

        seqs.Add(await rig.WriteAsync());
        await rig.ApplyAsync("a", seqs[4], notice: false);
        await rig.ApplyAsync("b", seqs[3], notice: false);
        hold.Release();

        Assert.Equal(new RetentionRun(seqs[1], 1), await run);
        Assert.Equal(seqs[1..], await rig.Log.SeqsAsync());
        Assert.Equal(seqs[1], await HorizonAsync(rig));
        Assert.True(await LeasedCursorsValidAsync(rig));
    }

    [Fact]
    public async Task The_delete_and_the_horizon_commit_together_under_no_change_log_lock()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var first = await rig.WriteAsync();
        var second = await rig.WriteAsync();
        await Member(rig, "a", "active", second);

        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RetentionBeforeHorizonUpdate);
        var run = retention.RunAsync();
        await hold.WaitReachedAsync();

        Assert.Equal([first, second], await rig.Log.SeqsAsync());
        Assert.Equal(0L, await HorizonAsync(rig));
        Assert.True(await rig.Skip.ChangeLogLockFreeAsync());
        var third = await rig.WriteAsync().WaitAsync(Poll.Bound);
        hold.Release();

        Assert.Equal(new RetentionRun(second, 1), await run);
        Assert.Equal([second, third], await rig.Log.SeqsAsync());
        Assert.Equal(second, await HorizonAsync(rig));
    }

    [Fact]
    public async Task Two_concurrent_runs_delete_each_entry_once_and_only_the_run_that_deleted_records_the_horizon()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        await rig.WriteAsync();
        await rig.WriteAsync();
        var third = await rig.WriteAsync();
        await Member(rig, "a", "active", third);

        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RetentionBeforeHorizonUpdate);
        var held = retention.RunAsync();
        await hold.WaitReachedAsync();

        var other = retention.RunAsync();
        await rig.Skip.WaitUntilASessionWaitsForALockAsync();
        Assert.False(other.IsCompleted);
        hold.Release();

        Assert.Equal(new RetentionRun(third, 2), await held);
        Assert.Equal(new RetentionRun(third, 0), await other);
        Assert.Equal([third], await rig.Log.SeqsAsync());
        Assert.Equal(third, await HorizonAsync(rig));
        Assert.Single(rig.EventsOf(LogActions.Retention));
    }

    [Fact]
    public async Task A_member_whose_lease_expired_finds_its_cursor_below_the_horizon_and_invalid()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var first = await rig.WriteAsync();
        var second = await rig.WriteAsync();
        await Member(rig, "a", "active", second);
        await Member(rig, "away", "active", first, TimeSpan.FromMinutes(-1));
        await Member(rig, "fresh", "joining", 0, TimeSpan.FromMinutes(-1));

        Assert.Equal(new RetentionRun(second, 1), await retention.RunAsync());

        Assert.False(await CursorValidAsync(rig, "away"));
        Assert.False(await CursorValidAsync(rig, "fresh"));
        Assert.True(await CursorValidAsync(rig, "a"));
    }

    [Fact]
    public async Task The_hosted_service_runs_retention_every_RetentionInterval()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig, TimeSpan.FromMilliseconds(100));
        await rig.WriteAsync();
        var second = await rig.WriteAsync();
        await Member(rig, "a", "active", second);

        await retention.StartAsync(CancellationToken.None);
        try
        {
            await Poll.UntilAsync(async () => await HorizonAsync(rig) != 0, "the horizon moved by the hosted service");

            Assert.Equal(second, await HorizonAsync(rig));
            Assert.Equal([second], await rig.Log.SeqsAsync());
        }
        finally
        {
            await retention.StopAsync(CancellationToken.None);
            retention.Dispose();
        }
    }

    [Fact]
    public async Task A_run_whose_commit_fails_changes_nothing_and_records_no_event()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var first = await rig.WriteAsync();
        var second = await rig.WriteAsync();
        await Member(rig, "a", "active", second);

        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                """
                CREATE FUNCTION public.fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'the commit fails'; END $$;
                CREATE CONSTRAINT TRIGGER fail_commit AFTER UPDATE ON reeve_cluster.cluster
                    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.fail_commit();
                """, connection);
            await command.ExecuteNonQueryAsync();
        }

        await Assert.ThrowsAnyAsync<NpgsqlException>(() => retention.RunAsync());

        Assert.Equal([first, second], await rig.Log.SeqsAsync());
        Assert.Equal(0L, await HorizonAsync(rig));
        Assert.Empty(rig.EventsOf(LogActions.Retention));
    }

    [Fact]
    public async Task The_hosted_service_keeps_running_after_a_failed_run()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig, TimeSpan.FromMilliseconds(100));
        await rig.WriteAsync();
        var second = await rig.WriteAsync();
        await Member(rig, "a", "active", second);

        using var fail = rig.Skip.Faults.Throw(LogFaultPoints.RetentionBetweenMinimumAndDelete, () => new InvalidOperationException("a failed run"));
        await retention.StartAsync(CancellationToken.None);
        try
        {
            await Poll.UntilAsync(async () => await HorizonAsync(rig) != 0, "the horizon moved by the hosted service");

            Assert.Equal(second, await HorizonAsync(rig));
            Assert.True(rig.Skip.Faults.Reached(LogFaultPoints.RetentionBetweenMinimumAndDelete) >= 2);
        }
        finally
        {
            await retention.StopAsync(CancellationToken.None);
            retention.Dispose();
        }
    }

    [Fact]
    public async Task A_member_that_becomes_leased_between_the_minimum_and_the_delete_finds_its_cursor_invalid_and_the_horizon_stays()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var seqs = new List<long>();
        for (var i = 0; i < 4; i++)
        {
            seqs.Add(await rig.WriteAsync());
        }

        await Member(rig, "a", "active", seqs[2]);
        await Member(rig, "back", "fenced", seqs[0], TimeSpan.FromMinutes(-1));

        using var hold = rig.Skip.Faults.HoldNext(LogFaultPoints.RetentionBetweenMinimumAndDelete);
        var run = retention.RunAsync();
        await hold.WaitReachedAsync();

        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                """
                UPDATE reeve_cluster.member SET state = 'joining', lease_expires_at = clock_timestamp() + interval '5 minutes'
                WHERE replica_id = 'back'
                """, connection);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        hold.Release();

        Assert.Equal(new RetentionRun(seqs[2], 2), await run);
        Assert.False(await CursorValidAsync(rig, "back"));
        Assert.True(await CursorValidAsync(rig, "a"));
        Assert.Equal(seqs[2], await HorizonAsync(rig));

        Assert.Equal(new RetentionRun(seqs[0], 0), await retention.RunAsync());
        Assert.Equal(seqs[2], await HorizonAsync(rig));
        Assert.Equal(seqs[2..], await rig.Log.SeqsAsync());
        Assert.Single(rig.EventsOf(LogActions.Retention));
    }

    [Fact]
    public async Task I5_a_leased_cursor_above_the_head_never_takes_the_head_row()
    {
        await using var rig = await AckRig.CreateAsync(postgres);
        var retention = Retention(rig);
        var seqs = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            seqs.Add(await rig.WriteAsync());
        }

        await rig.MemberAsync("a", "joining");
        await rig.ApplyAsync("a", seqs[2] + 5, Guid.NewGuid(), notice: false);
        await rig.MemberAsync("b", "joining");
        await rig.ApplyAsync("b", seqs[2] + 7, Guid.NewGuid(), notice: false);

        Assert.Equal(new RetentionRun(seqs[2], 2), await retention.RunAsync());
        Assert.Equal(seqs[2..], await rig.Log.SeqsAsync());
        Assert.Equal(seqs[2], await HorizonAsync(rig));
    }

    private static Retention Retention(AckRig rig, TimeSpan? interval = null) => new(
        rig.Data,
        Options.Create(new ClusterOptions
        {
            ConnectionString = rig.Skip.ConnectionString,
            ReplicaId = "r1",
            RetentionInterval = interval ?? TimeSpan.FromSeconds(60),
        }),
        TimeProvider.System,
        rig.Skip.Faults,
        rig.Skip.Events,
        NullLogger<Retention>.Instance);

    private static async Task Member(AckRig rig, string id, string state, long seq, TimeSpan? leaseFor = null)
    {
        await rig.MemberAsync(id, state, leaseFor: leaseFor);
        if (seq > 0)
        {
            await rig.ApplyAsync(id, seq, notice: false);
        }
    }

    private static Task<long> HorizonAsync(AckRig rig) => rig.Log.ScalarAsync<long>("SELECT horizon_seq FROM reeve_cluster.cluster");

    private static Task<bool> CursorValidAsync(AckRig rig, string id) => rig.Log.ScalarAsync<bool>(
        $"SELECT reeve_cluster.cursor_valid(applied_seq, applied_token) FROM reeve_cluster.member WHERE replica_id = '{id}'");

    private static Task<bool> LeasedCursorsValidAsync(AckRig rig) => rig.Log.ScalarAsync<bool>(
        """
        SELECT bool_and(reeve_cluster.cursor_valid(applied_seq, applied_token)) FROM reeve_cluster.member
        WHERE state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()
        """);
}
