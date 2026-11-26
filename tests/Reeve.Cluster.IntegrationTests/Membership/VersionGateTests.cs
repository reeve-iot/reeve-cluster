using System.Globalization;
using Int.Log;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class VersionGateTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public VersionGateTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync() => await db.DisposeAsync();

    private Task AddMemberAsync(string id, string state = "active", int protocol = MemberIdentity.CurrentProtocol, string version = "1.0.0", int leaseSeconds = 600) =>
        db.ExecuteAsync(string.Create(CultureInfo.InvariantCulture, $"""
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, version, protocol, process_id)
            VALUES ('{id}', 1, '{state}', clock_timestamp() + interval '{leaseSeconds} seconds', '{version}', {protocol}, gen_random_uuid())
            """));

    private Task SetFloorsAsync(int? protocol = null, string? version = null) => db.ExecuteAsync(
        $"UPDATE reeve_cluster.cluster SET protocol_floor = {protocol?.ToString(CultureInfo.InvariantCulture) ?? "protocol_floor"}, " +
        $"version_floor = {(version is null ? "version_floor" : $"'{version}'")}");

    private async Task<long> JoinAsync(string version, int protocol = MemberIdentity.CurrentProtocol)
    {
        var identity = new MemberIdentity(Guid.NewGuid(), version, protocol, []);
        var answer = await db.Store.JoinAsync(new JoinRequest("r1", TimeSpan.FromMinutes(10), identity), CancellationToken.None);
        return answer!.Value.Epoch;
    }

    private async Task<IAsyncDisposable> HoldChangeLogLockAsync()
    {
        var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        var transaction = await connection.BeginTransactionAsync();
        await using (var take = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({ChangeLogLock.Key})", connection, transaction))
        {
            await take.ExecuteNonQueryAsync();
        }

        return new LockHolder(connection, transaction);
    }

    private sealed class LockHolder(NpgsqlConnection connection, NpgsqlTransaction transaction) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await transaction.RollbackAsync();
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_raise_sets_the_protocol_floor_and_records_who_moved_it()
    {
        await AddMemberAsync("a", "active", protocol: 2);
        await AddMemberAsync("b", "joining", protocol: 3);

        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));

        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal(2, floors.ProtocolFloor);
        Assert.Equal("raise:r1", floors.MovedBy);
        Assert.NotNull(floors.MovedAt);
    }

    [Theory]
    [InlineData("active")]
    [InlineData("joining")]
    public async Task I14_a_raise_is_refused_by_a_leased_member_below_it_and_writes_nothing(string state)
    {
        await AddMemberAsync("new", "active", protocol: 2);
        await AddMemberAsync("old", state, protocol: 1);

        Assert.False(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));

        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((0, (string?)null), (floors.ProtocolFloor, floors.MovedBy));
    }

    [Theory]
    [InlineData("fenced", 600)]
    [InlineData("draining", 600)]
    [InlineData("active", -5)]
    [InlineData("joining", -5)]
    public async Task I14_a_member_without_a_valid_lease_pins_nothing(string state, int leaseSeconds)
    {
        await AddMemberAsync("new", "active", protocol: 2);
        await AddMemberAsync("gone", state, protocol: 1, leaseSeconds: leaseSeconds);

        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
        Assert.Equal(2, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);
    }

    [Fact]
    public async Task I14_after_a_raise_an_old_binary_is_refused_and_a_lowering_admits_it_without_a_restart()
    {
        await AddMemberAsync("new", "active", protocol: 2);
        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
        var epoch = await JoinAsync("1.5.0", protocol: 1);
        var request = new ActivationRequest("r1", epoch, "1.5.0", 1);

        var refused = await db.Activation().ActivateAsync(request, () => true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ActivationOutcome.RefusedByProtocolFloor, refused.Outcome);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);

        Assert.Equal(FloorLowering.Lowered, await db.FloorCommand().LowerProtocolFloorAsync(1, "operator").WaitAsync(Wait));

        var admitted = await db.Activation().ActivateAsync(request, () => true, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(ActivationOutcome.Committed, admitted.Outcome);
    }

    [Fact]
    public async Task I14_after_a_version_raise_an_old_binary_is_refused_by_SemVer()
    {
        await AddMemberAsync("new", "active", version: "1.10.0");
        Assert.True(await db.VersionGate().RequireVersionAsync("1.10.0").WaitAsync(Wait));
        var epoch = await JoinAsync("1.9.0");

        var refused = await db.Activation().ActivateAsync(new ActivationRequest("r1", epoch, "1.9.0", 1), () => true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ActivationOutcome.RefusedByVersionFloor, refused.Outcome);
        Assert.Equal("1.10.0", refused.VersionFloor);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task A_floor_already_at_or_above_answers_true_without_the_lock_or_a_write()
    {
        await AddMemberAsync("old", "active", protocol: 1, version: "0.9.0");
        await SetFloorsAsync(protocol: 3, version: "2.0.0");

        await using (await HoldChangeLogLockAsync())
        {
            Assert.True(await db.VersionGate().RequireProtocolAsync(3).WaitAsync(Wait));
            Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
            Assert.True(await db.VersionGate().RequireVersionAsync("1.10.0").WaitAsync(Wait));
            Assert.True(await db.VersionGate().RequireVersionAsync("2.0.0").WaitAsync(Wait));
        }

        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((3, "2.0.0", (string?)null), (floors.ProtocolFloor, floors.VersionFloor, floors.MovedBy));
    }

    [Fact]
    public async Task The_library_never_lowers_a_floor_even_when_asked_for_less()
    {
        await SetFloorsAsync(protocol: 5, version: "3.0.0");

        Assert.True(await db.VersionGate().RequireProtocolAsync(1).WaitAsync(Wait));
        Assert.True(await db.VersionGate().RequireVersionAsync("1.0.0").WaitAsync(Wait));

        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((5, "3.0.0"), (floors.ProtocolFloor, floors.VersionFloor));
    }

    [Fact]
    public async Task I14_a_version_raise_compares_by_SemVer_precedence_not_by_text()
    {
        await AddMemberAsync("a", "active", version: "1.9.0");

        Assert.False(await db.VersionGate().RequireVersionAsync("1.10.0").WaitAsync(Wait));
        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
        Assert.True(await db.VersionGate().RequireVersionAsync("1.9.0").WaitAsync(Wait));
        Assert.Equal("1.9.0", (await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);

        await db.ExecuteAsync("UPDATE reeve_cluster.member SET version = '1.10.0'");
        Assert.True(await db.VersionGate().RequireVersionAsync("1.10.0").WaitAsync(Wait));
        Assert.Equal("1.10.0", (await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
    }

    [Fact]
    public async Task I14_a_leased_member_whose_version_is_not_SemVer_refuses_a_version_raise()
    {
        await AddMemberAsync("odd", "active", version: "nightly");

        Assert.False(await db.VersionGate().RequireVersionAsync("1.0.0").WaitAsync(Wait));
        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
    }

    [Fact]
    public async Task I14_a_stored_floor_that_is_not_SemVer_answers_false_and_is_not_overwritten()
    {
        await SetFloorsAsync(version: "garbage");

        Assert.False(await db.VersionGate().RequireVersionAsync("1.0.0").WaitAsync(Wait));
        Assert.Equal("garbage", (await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1.0")]
    [InlineData("latest")]
    public async Task A_version_that_is_not_SemVer_is_refused_before_any_statement(string version)
    {
        await Assert.ThrowsAsync<ArgumentException>(() => db.VersionGate().RequireVersionAsync(version));
        await Assert.ThrowsAsync<ArgumentException>(() => db.VersionGate().AllMembersAtLeastAsync(version));
        await Assert.ThrowsAsync<ArgumentException>(() => db.FloorCommand().LowerVersionFloorAsync(version, "operator"));
        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).MovedBy);
    }

    [Fact]
    public async Task A_negative_protocol_is_refused()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => db.VersionGate().RequireProtocolAsync(-1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => db.VersionGate().AllMembersSpeakAsync(-1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => db.FloorCommand().LowerProtocolFloorAsync(-1, "operator"));
    }

    [Fact]
    public async Task I14_a_raise_that_the_management_command_overtook_answers_false_and_writes_nothing()
    {
        await SetFloorsAsync(version: "1.0.0");
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.FloorRaiseAfterMembersRead);
        var raise = db.VersionGate(db.Floors(probe)).RequireVersionAsync("2.0.0");
        await probe.Arrived.WaitAsync(Wait);
        await db.ExecuteAsync("UPDATE reeve_cluster.cluster SET version_floor = '0.5.0'");
        probe.Release();

        Assert.False(await raise.WaitAsync(Wait));
        Assert.Equal("0.5.0", (await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
    }

    [Fact]
    public async Task A_raise_cancelled_while_waiting_for_the_lock_changes_nothing_and_holds_nothing()
    {
        await AddMemberAsync("a", "active", protocol: 2);
        using var cancel = new CancellationTokenSource();
        Task<bool> raise;
        await using (await HoldChangeLogLockAsync())
        {
            raise = db.VersionGate().RequireProtocolAsync(2, cancel.Token);
            await db.UntilALockWaitsAsync();
            await cancel.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => raise.WaitAsync(Wait));
        }

        Assert.Equal(0, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);

        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
    }

    [Fact]
    public async Task A_raise_that_waits_for_the_lock_longer_than_its_bound_is_ended_by_the_server_and_changes_nothing()
    {
        await AddMemberAsync("a", "active", protocol: 2);
        var gate = db.VersionGate(db.Floors(renewTimeout: TimeSpan.FromMilliseconds(1)));
        await using (await HoldChangeLogLockAsync())
        {
            var error = await Assert.ThrowsAnyAsync<Exception>(() => gate.RequireProtocolAsync(2).WaitAsync(Wait));

            Assert.Equal("25P04", Assert.IsAssignableFrom<PostgresException>(error).SqlState);
        }

        Assert.Equal(0, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);
        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
    }

    [Fact]
    public async Task A_raise_stuck_behind_a_session_that_holds_the_cluster_row_is_ended_by_its_bound_and_lets_the_appends_through()
    {
        await AddMemberAsync("a", "active", protocol: 2);
        await using var retention = new NpgsqlConnection(db.ConnectionString);
        await retention.OpenAsync();
        await using var holding = await retention.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand("UPDATE reeve_cluster.cluster SET horizon_seq = horizon_seq", retention, holding))
        {
            await hold.ExecuteNonQueryAsync();
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        var raise = db.VersionGate(db.Floors(renewTimeout: TimeSpan.FromMilliseconds(1))).RequireProtocolAsync(2);
        await db.UntilALockWaitsAsync();
        await using var appender = new NpgsqlConnection(db.ConnectionString);
        await appender.OpenAsync();
        await using var append = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k')", appender);
        var appending = append.ExecuteNonQueryAsync();

        var error = await Assert.ThrowsAnyAsync<Exception>(() => raise.WaitAsync(Wait));
        started.Stop();
        Assert.Equal("25P04", Assert.IsAssignableFrom<PostgresException>(error).SqlState);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(10), $"the raise ended after {started.Elapsed}, not at its bound");
        Assert.Equal(1, await appending.WaitAsync(Wait));
        Assert.Equal(0, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);

        await holding.RollbackAsync();
        Assert.True(await db.VersionGate().RequireProtocolAsync(2).WaitAsync(Wait));
    }

    [Fact]
    public async Task The_raise_takes_the_lock_after_the_point_before_it_and_holds_it_to_the_commit()
    {
        await AddMemberAsync("a", "active", protocol: 2);
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.FloorRaiseBeforeCommit);
        var raise = db.VersionGate(db.Floors(probe)).RequireProtocolAsync(2);
        await probe.Arrived.WaitAsync(Wait);

        await using var appender = new NpgsqlConnection(db.ConnectionString);
        await appender.OpenAsync();
        await using var append = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k')", appender);
        var appending = append.ExecuteNonQueryAsync();
        await db.UntilALockWaitsAsync();
        Assert.False(appending.IsCompleted);
        Assert.Equal(0, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);

        probe.Release();
        Assert.True(await raise.WaitAsync(Wait));
        await appending.WaitAsync(Wait);
        Assert.Equal(2, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);
    }

    [Fact]
    public async Task The_observations_count_leased_joining_and_active_members_only()
    {
        await AddMemberAsync("a", "active", protocol: 2, version: "2.0.0");
        await AddMemberAsync("b", "joining", protocol: 2, version: "2.1.0");
        await AddMemberAsync("c-fenced", "fenced", protocol: 1, version: "1.0.0");
        await AddMemberAsync("d-draining", "draining", protocol: 1, version: "1.0.0");
        await AddMemberAsync("e-expired", "active", protocol: 1, version: "1.0.0", leaseSeconds: -5);
        var gate = db.VersionGate();

        await using (await HoldChangeLogLockAsync())
        {
            Assert.True(await gate.AllMembersSpeakAsync(2).WaitAsync(Wait));
            Assert.False(await gate.AllMembersSpeakAsync(3).WaitAsync(Wait));
            Assert.True(await gate.AllMembersAtLeastAsync("2.0.0").WaitAsync(Wait));
            Assert.False(await gate.AllMembersAtLeastAsync("2.1.0").WaitAsync(Wait));
        }

        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).MovedBy);
    }

    [Fact]
    public async Task The_observations_hold_for_no_member_and_fail_closed_on_a_version_that_is_not_SemVer()
    {
        Assert.True(await db.VersionGate().AllMembersSpeakAsync(9).WaitAsync(Wait));
        Assert.True(await db.VersionGate().AllMembersAtLeastAsync("9.0.0").WaitAsync(Wait));

        await AddMemberAsync("odd", "active", version: "nightly");
        Assert.False(await db.VersionGate().AllMembersAtLeastAsync("0.0.1").WaitAsync(Wait));
    }

    [Fact]
    public async Task The_lowering_records_who_lowered_it_and_only_lowers()
    {
        await SetFloorsAsync(protocol: 3, version: "2.0.0");
        var command = db.FloorCommand();

        Assert.Equal(FloorLowering.NotAbove, await command.LowerProtocolFloorAsync(3, "alice").WaitAsync(Wait));
        Assert.Equal(FloorLowering.NotAbove, await command.LowerProtocolFloorAsync(9, "alice").WaitAsync(Wait));
        Assert.Equal(FloorLowering.NotAbove, await command.LowerVersionFloorAsync("2.0.0", "alice").WaitAsync(Wait));
        Assert.Equal(FloorLowering.NotAbove, await command.LowerVersionFloorAsync("3.0.0", "alice").WaitAsync(Wait));
        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).MovedBy);

        Assert.Equal(FloorLowering.Lowered, await command.LowerProtocolFloorAsync(1, "alice").WaitAsync(Wait));
        var afterProtocol = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((1, "2.0.0", "lower:alice"), (afterProtocol.ProtocolFloor, afterProtocol.VersionFloor, afterProtocol.MovedBy));
        Assert.NotNull(afterProtocol.MovedAt);

        Assert.Equal(FloorLowering.Lowered, await command.LowerVersionFloorAsync("1.10.0", "bob").WaitAsync(Wait));
        var afterVersion = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((1, "1.10.0", "lower:bob"), (afterVersion.ProtocolFloor, afterVersion.VersionFloor, afterVersion.MovedBy));

        Assert.Equal(FloorLowering.Lowered, await command.LowerVersionFloorAsync(null, "carol").WaitAsync(Wait));
        Assert.Null((await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
        Assert.Equal(FloorLowering.NotAbove, await command.LowerVersionFloorAsync(null, "carol").WaitAsync(Wait));
    }

    [Fact]
    public async Task The_lowering_replaces_a_stored_floor_that_is_not_SemVer()
    {
        await SetFloorsAsync(version: "garbage");

        Assert.Equal(FloorLowering.Lowered, await db.FloorCommand().LowerVersionFloorAsync("1.0.0", "alice").WaitAsync(Wait));
        Assert.Equal("1.0.0", (await db.Floors().ReadAsync(CancellationToken.None)).VersionFloor);
    }

    [Fact]
    public async Task The_lowering_names_who_lowered_it()
    {
        await SetFloorsAsync(protocol: 2);

        await Assert.ThrowsAsync<ArgumentException>(() => db.FloorCommand().LowerProtocolFloorAsync(1, " "));
        await Assert.ThrowsAsync<ArgumentException>(() => db.FloorCommand().LowerVersionFloorAsync("1.0.0", ""));
        Assert.Equal(2, (await db.Floors().ReadAsync(CancellationToken.None)).ProtocolFloor);
    }

    [Fact]
    public async Task A_lowering_that_a_raise_or_another_lowering_overtook_writes_nothing()
    {
        await SetFloorsAsync(version: "2.0.0");
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.FloorLowerBeforeWrite);
        var lowering = db.FloorCommand(db.Floors(probe)).LowerVersionFloorAsync("1.0.0", "alice");
        await probe.Arrived.WaitAsync(Wait);
        await db.ExecuteAsync("UPDATE reeve_cluster.cluster SET version_floor = '1.5.0'");
        probe.Release();

        Assert.Equal(FloorLowering.Changed, await lowering.WaitAsync(Wait));
        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal(("1.5.0", (string?)null), (floors.VersionFloor, floors.MovedBy));
    }

    [Fact]
    public async Task The_guards_read_finds_a_leased_active_member_below_the_declared_protocol_other_than_the_asker()
    {
        await AddMemberAsync("old-active", "active", protocol: 1);
        await AddMemberAsync("new-active", "active", protocol: 2);
        var store = db.Floors();

        var blocker = await store.FindOlderActiveMemberAsync("r1", 2, CancellationToken.None);

        Assert.Equal(new OlderActiveMember("old-active", 1), blocker);
        Assert.Null(await store.FindOlderActiveMemberAsync("r1", 1, CancellationToken.None));
        Assert.Null(await store.FindOlderActiveMemberAsync("old-active", 2, CancellationToken.None));
    }

    [Theory]
    [InlineData("joining", 600)]
    [InlineData("fenced", 600)]
    [InlineData("draining", 600)]
    [InlineData("active", -5)]
    public async Task The_guards_read_ignores_a_member_that_is_not_leased_and_active(string state, int leaseSeconds)
    {
        await AddMemberAsync("old", state, protocol: 1, leaseSeconds: leaseSeconds);

        Assert.Null(await db.Floors().FindOlderActiveMemberAsync("r1", 2, CancellationToken.None));
    }
}
