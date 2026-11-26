using Int.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Migrations;
using Reeve.Cluster.Status;

namespace Int.Membership;

public sealed class MemberStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly MemberIdentity Identity = new(Guid.NewGuid(), "1.2.3", MemberIdentity.CurrentProtocol, ["unit:widget"]);

    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public MemberStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync() => await db.DisposeAsync();

    private Task<JoinAnswer?> Join(string id = "r1") =>
        db.Store.JoinAsync(new JoinRequest(id, TestProfile.LeaseTtl, Identity), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(30));

    [Fact]
    public async Task M01_a_join_on_an_absent_row_writes_a_joining_row_with_a_lease_and_answers_its_epoch_and_timeline()
    {
        var before = await db.ClockAsync();
        var answer = await Join();
        var after = await db.ClockAsync();

        Assert.NotNull(answer);
        Assert.Equal(1, answer.Value.Timeline);
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(answer.Value.Epoch, row.Epoch);
        Assert.Equal("joining", row.State);
        Assert.InRange(await db.ReadExpiryAsync("r1"), before + TestProfile.LeaseTtl, after + TestProfile.LeaseTtl);
        Assert.Equal(Identity.ProcessId, row.ProcessId);
        Assert.Equal("1.2.3", row.Version);
        Assert.Equal(MemberIdentity.CurrentProtocol, row.Protocol);
        Assert.Equal(["unit:widget"], row.Capabilities);
        Assert.Null(row.ServingSince);
        Assert.Null(row.ActivatedAtSeq);
    }

    [Theory]
    [InlineData("joining")]
    [InlineData("active")]
    public async Task M02_a_join_is_refused_while_a_row_holds_a_valid_lease_and_changes_nothing(string state)
    {
        await db.InsertRowAsync("r1", epoch: 40, state, leaseSeconds: 30);

        Assert.Null(await Join());

        var row = await db.ReadRowAsync("r1");
        Assert.Equal(40, row.Epoch);
        Assert.Equal(state, row.State);
    }

    [Theory]
    [InlineData("fenced", 30)]
    [InlineData("draining", 30)]
    [InlineData("joining", -1)]
    [InlineData("active", -1)]
    public async Task M01_a_join_takes_over_a_fenced_or_draining_row_or_an_expired_lease_with_a_new_epoch(string state, int leaseSeconds)
    {
        await db.InsertRowAsync("r1", epoch: 40, state, leaseSeconds);
        await db.ExecuteAsync("SELECT setval('reeve_cluster.member_epoch', 40)");

        var answer = await Join();

        Assert.NotNull(answer);
        Assert.True(answer.Value.Epoch > 40);
        var row = await db.ReadRowAsync("r1");
        Assert.Equal("joining", row.State);
        Assert.Equal(Identity.ProcessId, row.ProcessId);
        Assert.True(row.LeaseLeft > TimeSpan.Zero);
    }

    [Fact]
    public async Task I13_a_joins_epoch_is_the_sequences_next_value_over_an_absent_row_an_existing_row_and_after_an_operators_delete()
    {
        await db.ExecuteAsync("SELECT setval('reeve_cluster.member_epoch', 40)");

        Assert.Equal(41, (await Join())!.Value.Epoch);
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced'");
        Assert.Equal(42, (await Join())!.Value.Epoch);
        await db.ExecuteAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'r1'");
        Assert.Equal(43, (await Join())!.Value.Epoch);

        await db.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced', epoch = 100");
        Assert.Equal(44, (await Join())!.Value.Epoch);
        Assert.Equal(44, await db.ReadEpochSequenceAsync());
    }

    [Fact]
    public async Task Two_processes_start_with_one_replica_id_at_once_and_exactly_one_joins()
    {
        for (var round = 0; round < 10; round++)
        {
            await db.ExecuteAsync("DELETE FROM reeve_cluster.member");
            using var start = new Barrier(2);
            var a = Task.Run(() => { start.SignalAndWait(); return Join(); });
            var b = Task.Run(() => { start.SignalAndWait(); return Join(); });

            var answers = await Task.WhenAll(a, b);

            Assert.Single(answers, x => x is not null);
            Assert.Equal(answers.Single(x => x is not null)!.Value.Epoch, (await db.ReadRowAsync("r1")).Epoch);
        }
    }

    [Fact]
    public async Task I4_a_join_waits_only_for_its_own_member_row()
    {
        Assert.NotNull(await Join("r1"));
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced' WHERE replica_id = 'r1'");
        await using var holder = new NpgsqlConnection(db.ConnectionString);
        await holder.OpenAsync();
        await using var transaction = await holder.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand("SELECT 1 FROM reeve_cluster.member WHERE replica_id = 'r1' FOR UPDATE", holder, transaction))
        {
            await hold.ExecuteScalarAsync();
        }

        var blocked = Join("r1");
        await db.UntilALockWaitsAsync();
        var other = Join("r2");
        var otherInTime = await Task.WhenAny(other, Task.Delay(Poll.Bound)) == other;
        var blockedWhileOtherAnswered = !blocked.IsCompleted;
        await transaction.CommitAsync();
        var r1 = await blocked;
        var r2 = await other;

        Assert.True(otherInTime, "the join of r2 waited for another replica's row: the join takes more than its member row");
        Assert.True(blockedWhileOtherAnswered, "the join of r1 did not wait for its row's lock");
        Assert.NotNull(r1);
        Assert.NotNull(r2);
        Assert.NotEqual(r1.Value.Epoch, r2.Value.Epoch);
    }

    [Fact]
    public async Task The_renewal_of_its_own_leased_row_returns_one_row_with_the_servers_timeline_and_extends_the_lease()
    {
        var joined = await Join();
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '2 seconds'");
        var before = await db.ReadExpiryAsync("r1");

        var answer = await db.Store.RenewAsync("r1", joined!.Value.Epoch, TestProfile.LeaseTtl, CancellationToken.None);

        Assert.Equal(new RenewalAnswer(1, joined.Value.Timeline), answer);
        Assert.True(await db.ReadExpiryAsync("r1") - before > TimeSpan.FromSeconds(5));
    }

    [Theory]
    [InlineData("older-epoch")]
    [InlineData("takeover")]
    [InlineData("delete")]
    [InlineData("fenced")]
    [InlineData("draining")]
    [InlineData("expired")]
    public async Task I13_I23_a_renewal_whose_row_is_no_longer_this_processs_answers_zero_rows(string change)
    {
        var epoch = (await Join())!.Value.Epoch;
        await db.ExecuteAsync(change switch
        {
            "older-epoch" => "UPDATE reeve_cluster.member SET epoch = epoch - 1",
            "takeover" => "UPDATE reeve_cluster.member SET epoch = epoch + 1, process_id = gen_random_uuid()",
            "delete" => "DELETE FROM reeve_cluster.member",
            "fenced" => "UPDATE reeve_cluster.member SET state = 'fenced'",
            "draining" => "UPDATE reeve_cluster.member SET state = 'draining'",
            _ => "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond'",
        });

        var answer = await db.Store.RenewAsync("r1", epoch, TestProfile.LeaseTtl, CancellationToken.None);

        Assert.Equal(new RenewalAnswer(0, null), answer);
    }

    [Fact]
    public async Task M14_the_draining_write_is_a_compare_and_set_on_the_epoch()
    {
        var epoch = (await Join())!.Value.Epoch;

        Assert.False(await db.Store.WriteDrainingAsync("r1", epoch - 1, CancellationToken.None));
        Assert.True(await db.Store.WriteDrainingAsync("r1", epoch, CancellationToken.None));
        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);
    }

    [Theory]
    [InlineData("older-epoch")]
    [InlineData("fenced")]
    [InlineData("draining")]
    [InlineData("own-active")]
    public async Task I2_drain_step_1_clears_serving_since_by_a_compare_and_set_on_the_epoch_and_the_state(string change)
    {
        var epoch = (await Join())!.Value.Epoch;
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'active', serving_since = clock_timestamp()");
        if (change is "fenced" or "draining")
        {
            await db.ExecuteAsync($"UPDATE reeve_cluster.member SET state = '{change}'");
        }

        var cleared = await db.Store.ClearServingSinceAsync("r1", change == "older-epoch" ? epoch - 1 : epoch, CancellationToken.None);

        var row = await db.ReadRowAsync("r1");
        Assert.Equal(change == "own-active", cleared);
        Assert.Equal(change == "own-active", row.ServingSince is null);
    }
}

internal sealed class MemberDatabase : IAsyncDisposable
{
    private readonly ClusterDataSource data;
    private readonly RenewalDataSource renewal;
    private readonly MemberWriteDataSource writes;
    private readonly ClusterOptions options;

    private MemberDatabase(string connectionString)
    {
        ConnectionString = connectionString;
        options = new ClusterOptions { ConnectionString = connectionString, ReplicaId = "r1" };
        TestProfile.Apply(options);
        data = new ClusterDataSource(options);
        renewal = new RenewalDataSource(Options.Create(options));
        writes = new MemberWriteDataSource(Options.Create(options));
        Store = new MemberStore(renewal, writes);
        Fences = new FenceStore(data, renewal, writes);
        Capabilities = new CapabilityStore(writes);
    }

    public string ConnectionString { get; }

    public MemberStore Store { get; }

    public CapabilityStore Capabilities { get; }

    public FenceStore Fences { get; }

    public StatusStore Status => new(data, data, Options.Create(options));

    public TimeSpan LeaseTtl => options.LeaseTtl;

    public ActivationStore Activation(IFaultPoints? points = null, TimeSpan? renewTimeout = null)
    {
        var own = options;
        if (renewTimeout is { } timeout)
        {
            own = new ClusterOptions { ConnectionString = ConnectionString, ReplicaId = "r1" };
            TestProfile.Apply(own);
            own.RenewTimeout = timeout;
        }

        return new ActivationStore(data, writes, Options.Create(own), points ?? NoFaultPoints.Instance);
    }

    public FloorStore Floors(IFaultPoints? points = null, TimeSpan? renewTimeout = null)
    {
        var own = options;
        if (renewTimeout is { } timeout)
        {
            own = new ClusterOptions { ConnectionString = ConnectionString, ReplicaId = "r1" };
            TestProfile.Apply(own);
            own.RenewTimeout = timeout;
        }

        return new FloorStore(data, writes, Options.Create(own), points ?? NoFaultPoints.Instance);
    }

    public ClusterVersionGate VersionGate(FloorStore? floors = null) =>
        new(floors ?? Floors(), Options.Create(options), new MemberIdentity(Guid.NewGuid(), "99.0.0", 99, []));

    public ClusterFloorCommand FloorCommand(FloorStore? floors = null) => new(floors ?? Floors());

    public async Task UntilALockWaitsAsync()
    {
        await using var watcher = new NpgsqlConnection(ConnectionString);
        await watcher.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", watcher);
        await Poll.UntilAsync(async () => (long)(await command.ExecuteScalarAsync())! > 0, "a statement waiting for a lock");
    }

    public static async Task<MemberDatabase> CreateAsync(PostgresFixture postgres, Func<string, string>? connectionString = null)
    {
        var migrated = await postgres.CreateDatabaseAsync();
        var runner = ClusterRunners.For(migrated);
        await runner.MigrateAsync();
        return new MemberDatabase(connectionString is null ? migrated : connectionString(migrated));
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<long> ReadEpochSequenceAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT last_value FROM reeve_cluster.member_epoch", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public Task InsertRowAsync(string id, long epoch, string state, int leaseSeconds) => ExecuteAsync($"""
        INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, version, protocol, process_id)
        VALUES ('{id}', {epoch}, '{state}', clock_timestamp() + interval '{leaseSeconds} seconds', '1.0.0', 1, gen_random_uuid())
        """);

    public async Task<MemberRow> ReadRowAsync(string id)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("""
            SELECT epoch, state, lease_expires_at - clock_timestamp(), process_id, version, protocol, capabilities, serving_since, activated_at_seq
            FROM reeve_cluster.member WHERE replica_id = @id
            """, connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"no member row for {id}");
        return new MemberRow(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.GetTimeSpan(2),
            reader.GetGuid(3),
            reader.GetString(4),
            reader.GetInt32(5),
            reader.GetFieldValue<string[]>(6),
            reader.IsDBNull(7) ? null : reader.GetDateTime(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8));
    }

    public async Task<DateTime> ClockAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    public async Task<DateTime> ReadFencedAtAsync(string id, long epoch)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT fenced_at FROM reeve_cluster.fence_history WHERE replica_id = @id AND epoch = @epoch", connection);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("epoch", epoch);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    public async Task<DateTime> ReadExpiryAsync(string id)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT lease_expires_at FROM reeve_cluster.member WHERE replica_id = @id", connection);
        command.Parameters.AddWithValue("id", id);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    public async Task<List<FenceHistoryRow>> ReadFenceHistoryAsync(string id)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT epoch, trigger, span, clock_timestamp() - fenced_at FROM reeve_cluster.fence_history WHERE replica_id = @id ORDER BY epoch",
            connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<FenceHistoryRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new FenceHistoryRow(reader.GetInt64(0), reader.GetString(1), reader.GetTimeSpan(2), reader.GetTimeSpan(3)));
        }

        return rows;
    }

    public async ValueTask DisposeAsync()
    {
        await writes.DisposeAsync();
        await renewal.DisposeAsync();
        await data.DisposeAsync();
    }
}

internal sealed record FenceHistoryRow(long Epoch, string Trigger, TimeSpan Span, TimeSpan Age);

internal sealed record MemberRow(
    long Epoch,
    string State,
    TimeSpan LeaseLeft,
    Guid ProcessId,
    string Version,
    int Protocol,
    string[] Capabilities,
    DateTime? ServingSince,
    long? ActivatedAtSeq);
