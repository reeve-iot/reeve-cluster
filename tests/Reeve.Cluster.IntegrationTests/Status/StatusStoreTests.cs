using Int.Membership;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;

namespace Int.Status;

public sealed class StatusStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly CancellationToken Never = CancellationToken.None;

    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public StatusStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync() => await db.DisposeAsync();

    private static ReportedSection Section(string name, int n) => new(name, $$"""{"n":{{n}}}""");

    private async Task<long> CountAsync(string where = "true")
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM reeve_cluster.member_status WHERE {where}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task The_migration_creates_member_status_keyed_by_replica_epoch_and_section()
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var columns = new NpgsqlCommand(
            "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'reeve_cluster' AND table_name = 'member_status'",
            connection);
        await using var key = new NpgsqlCommand(
            """
            SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey::int2[], a.attnum))
            FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
            WHERE i.indrelid = 'reeve_cluster.member_status'::regclass AND i.indisprimary
            """,
            connection);

        Assert.Equal(
            "replica_id text, epoch bigint, section text, payload jsonb, updated_at timestamp with time zone",
            (string)(await columns.ExecuteScalarAsync())!);
        Assert.Equal("replica_id, epoch, section", (string)(await key.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task A_report_writes_a_row_per_section_and_the_next_replaces_it()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);

        var first = await db.Status.ReportAsync("r1", 7, [Section("phase", 1), Section("lease", 1)], Never);
        await db.ExecuteAsync("UPDATE reeve_cluster.member_status SET updated_at = clock_timestamp() - interval '5 seconds'");
        var second = await db.Status.ReportAsync("r1", 7, [Section("phase", 2), Section("lease", 2)], Never);

        Assert.Equal((2, 2), (first, second));
        Assert.Equal(2, await CountAsync());
        var member = Assert.Single((await db.Status.ReadAsync(Never)).Members);
        Assert.Equal(["lease", "phase"], member.Sections.Select(s => s.Name));
        Assert.All(member.Sections, s =>
        {
            Assert.Equal(2, s.Values.GetProperty("n").GetInt32());
            Assert.True(s.Age < TimeSpan.FromSeconds(4), $"the second report replaced the first: age {s.Age}");
        });
    }

    [Theory]
    [InlineData("another epoch")]
    [InlineData("no member row")]
    [InlineData("past the window")]
    public async Task A_report_of_another_epoch_or_past_the_window_writes_nothing(string case_)
    {
        switch (case_)
        {
            case "another epoch": await db.InsertRowAsync("r1", 8, "active", leaseSeconds: 30); break;
            case "past the window": await db.InsertRowAsync("r1", 7, "fenced", leaseSeconds: -(int)db.LeaseTtl.TotalSeconds - 2); break;
        }

        var written = await db.Status.ReportAsync("r1", 7, [Section("phase", 1)], Never);

        Assert.Equal(0, written);
        Assert.Equal(0, await CountAsync());
    }

    [Fact]
    public async Task A_report_inside_the_window_after_the_lease_ended_is_written()
    {
        await db.InsertRowAsync("r1", 7, "fenced", leaseSeconds: -1);

        var written = await db.Status.ReportAsync("r1", 7, [Section("phase", 1)], Never);

        Assert.Equal(1, written);
    }

    [Fact]
    public async Task A_fenced_members_sections_are_read_until_LeaseTtl_after_its_lease_ended()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        await db.Status.ReportAsync("r1", 7, [Section("applier", 1)], Never);

        Assert.True(await db.Fences.ReleaseLeaseAsync("r1", 7, Never));
        var fenced = Assert.Single((await db.Status.ReadAsync(Never)).Members);

        Assert.Equal(("fenced", false), (fenced.State, fenced.LeaseValid));
        Assert.Equal(["applier"], fenced.Sections.Select(s => s.Name));

        await db.ExecuteAsync($"UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '{(int)db.LeaseTtl.TotalSeconds + 1} seconds'");
        var later = Assert.Single((await db.Status.ReadAsync(Never)).Members);

        Assert.Equal("r1", later.ReplicaId);
        Assert.Empty(later.Sections);
        Assert.Equal(1, await CountAsync());
    }

    [Fact]
    public async Task A_section_past_lease_plus_LeaseTtl_is_not_read_and_one_inside_it_is()
    {
        var ttl = (int)db.LeaseTtl.TotalSeconds;
        await db.InsertRowAsync("inside", 1, "fenced", leaseSeconds: -1);
        await db.InsertRowAsync("outside", 1, "fenced", leaseSeconds: -(ttl + 2));
        await db.ExecuteAsync("""
            INSERT INTO reeve_cluster.member_status (replica_id, epoch, section, payload)
            VALUES ('inside', 1, 'phase', '{"n":1}'), ('outside', 1, 'phase', '{"n":1}')
            """);

        var read = await db.Status.ReadAsync(Never);

        Assert.Equal(["inside", "outside"], read.Members.Select(m => m.ReplicaId));
        Assert.Equal([1, 0], read.Members.Select(m => m.Sections.Count));
    }

    [Fact]
    public async Task A_rejoin_hides_the_sections_of_the_old_epoch_and_the_next_report_sweeps_them()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        await db.Status.ReportAsync("r1", 7, [Section("phase", 1)], Never);
        await db.Fences.RecordFenceAsync(new FenceRecord("r1", 7, "fence-start", TimeSpan.FromSeconds(4)), TimeSpan.Zero, Never);

        await db.ExecuteAsync("UPDATE reeve_cluster.member SET epoch = 8, state = 'joining', lease_expires_at = clock_timestamp() + interval '30 seconds'");
        var rejoined = Assert.Single((await db.Status.ReadAsync(Never)).Members);

        Assert.Equal(8, rejoined.Epoch);
        Assert.Empty(rejoined.Sections);
        Assert.Equal(["fence-start"], rejoined.Fences.Select(f => f.Trigger));
        Assert.Equal(1, await CountAsync("epoch = 7"));

        await db.Status.ReportAsync("r1", 8, [Section("phase", 2)], Never);

        Assert.Equal(0, await CountAsync("epoch = 7"));
        Assert.Equal(1, await CountAsync("epoch = 8"));
    }

    [Fact]
    public async Task The_sweep_deletes_rows_without_a_member_in_the_window_and_keeps_the_rest()
    {
        var ttl = (int)db.LeaseTtl.TotalSeconds;
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        await db.InsertRowAsync("r2", 3, "fenced", leaseSeconds: -1);
        await db.InsertRowAsync("r3", 3, "fenced", leaseSeconds: -(ttl + 2));
        await db.ExecuteAsync("""
            INSERT INTO reeve_cluster.member_status (replica_id, epoch, section, payload)
            VALUES ('r2', 3, 'phase', '{}'), ('r3', 3, 'phase', '{}'), ('r4', 1, 'phase', '{}'), ('r2', 2, 'phase', '{}')
            """);

        await db.Status.ReportAsync("r1", 7, [Section("phase", 1)], Never);

        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT replica_id || ':' || epoch FROM reeve_cluster.member_status ORDER BY 1", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var left = new List<string>();
        while (await reader.ReadAsync())
        {
            left.Add(reader.GetString(0));
        }

        Assert.Equal(["r1:7", "r2:3"], left);
    }

    [Fact]
    public async Task The_aggregated_read_lists_the_fence_history_newest_first()
    {
        await db.InsertRowAsync("r1", 9, "active", leaseSeconds: 30);
        foreach (var (epoch, trigger) in new[] { (3L, "applier-stall"), (5L, "zero-row-renewal"), (4L, "fence-start") })
        {
            await db.Fences.RecordFenceAsync(new FenceRecord("r1", epoch, trigger, TimeSpan.FromSeconds(epoch)), TimeSpan.FromSeconds(1), Never);
        }

        var fences = Assert.Single((await db.Status.ReadAsync(Never)).Members).Fences;

        Assert.Equal([(5L, "zero-row-renewal", 5.0), (4L, "fence-start", 4.0), (3L, "applier-stall", 3.0)], fences.Select(f => (f.Epoch, f.Trigger, f.Span.TotalSeconds)));
        Assert.All(fences, f => Assert.True(f.FencedAt <= DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task The_fence_history_statement_selects_only_the_history_of_members()
    {
        await db.InsertRowAsync("r1", 9, "active", leaseSeconds: 30);
        await db.Fences.RecordFenceAsync(new FenceRecord("r1", 5, "fence-start", TimeSpan.FromSeconds(5)), TimeSpan.FromSeconds(1), Never);
        await db.Fences.RecordFenceAsync(new FenceRecord("gone", 3, "fence-start", TimeSpan.FromSeconds(3)), TimeSpan.FromSeconds(1), Never);

        await using var connection = new Npgsql.NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(StatusStore.FencesSql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var replicas = new List<string>();
        while (await reader.ReadAsync())
        {
            replicas.Add(reader.GetString(0));
        }

        Assert.Equal(["r1"], replicas);
    }

    [Fact]
    public async Task The_aggregated_read_returns_each_members_row_and_sections_with_their_age()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        await db.InsertRowAsync("r2", 4, "draining", leaseSeconds: 10);
        await db.Status.ReportAsync("r1", 7, [Section("phase", 1), Section("skips", 1)], Never);
        await db.Status.ReportAsync("r2", 4, [Section("phase", 2)], Never);
        await db.ExecuteAsync("UPDATE reeve_cluster.member_status SET updated_at = clock_timestamp() - interval '5 seconds' WHERE replica_id = 'r2'");

        var read = await db.Status.ReadAsync(Never);

        Assert.Equal(["r1", "r2"], read.Members.Select(m => m.ReplicaId));
        var (r1, r2) = (read.Members[0], read.Members[1]);
        Assert.Equal((7L, "active", true, "1.0.0", 1), (r1.Epoch, r1.State, r1.LeaseValid, r1.Version, r1.Protocol));
        Assert.Equal(("draining", true), (r2.State, r2.LeaseValid));
        Assert.Equal(["phase", "skips"], r1.Sections.Select(s => s.Name));
        Assert.All(r1.Sections, s => Assert.InRange(s.Age, TimeSpan.Zero, TimeSpan.FromSeconds(4)));
        Assert.InRange(r2.Sections[0].Age, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(9));
        Assert.Equal(2, r2.Sections[0].Values.GetProperty("n").GetInt32());
        Assert.True(read.ReadAt > DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task An_expired_lease_reads_LeaseValid_false_through_IClusterStatus()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: -5);
        await db.InsertRowAsync("r2", 8, "active", leaseSeconds: 30);
        await db.InsertRowAsync("r3", 9, "joining", leaseSeconds: -1);
        await db.InsertRowAsync("r4", 10, "active", leaseSeconds: 30);
        Assert.True(await db.Fences.ReleaseLeaseAsync("r4", 10, Never));
        IClusterStatus status = new ClusterStatus(db.Status, Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ActionTimeBudget = TimeSpan.FromSeconds(30) }), TimeProvider.System);

        var read = await status.ReadAsync(Never);

        Assert.Equal(
            [("r1", "active", false), ("r2", "active", true), ("r3", "joining", false), ("r4", "fenced", false)],
            read.Members.Select(m => (m.ReplicaId, m.State, m.LeaseValid)));
        Assert.All(read.Members.Where(m => !m.LeaseValid), m => Assert.True(m.LeaseExpiresAt <= read.ReadAt));
        Assert.True(read.Members.Single(m => m.ReplicaId == "r2").LeaseExpiresAt > read.ReadAt);
    }

    [Fact]
    public async Task A_read_held_by_a_table_lock_past_ActionTimeBudget_ends_with_a_TimeoutException()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        IClusterStatus status = new ClusterStatus(db.Status, Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ActionTimeBudget = TimeSpan.FromMilliseconds(200) }), TimeProvider.System);
        await using var holder = new NpgsqlConnection(db.ConnectionString);
        await holder.OpenAsync();
        await using (var transaction = await holder.BeginTransactionAsync())
        {
            await using (var lockTable = new NpgsqlCommand("LOCK TABLE reeve_cluster.member IN ACCESS EXCLUSIVE MODE", holder, transaction))
            {
                await lockTable.ExecuteNonQueryAsync();
            }

            var thrown = await Assert.ThrowsAsync<TimeoutException>(() => status.ReadAsync(Never));
            Assert.IsAssignableFrom<OperationCanceledException>(thrown.InnerException);
            await transaction.RollbackAsync();
        }

        Assert.Equal("r1", Assert.Single((await status.ReadAsync(Never)).Members).ReplicaId);
    }

    [Fact]
    public async Task A_report_with_no_section_still_sweeps()
    {
        await db.InsertRowAsync("r1", 7, "active", leaseSeconds: 30);
        await db.ExecuteAsync("INSERT INTO reeve_cluster.member_status (replica_id, epoch, section, payload) VALUES ('gone', 1, 'phase', '{}')");

        Assert.Equal(0, await db.Status.ReportAsync("r1", 7, [], Never));

        Assert.Equal(0, await CountAsync());
    }
}
