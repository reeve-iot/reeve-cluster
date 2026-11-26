using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;

namespace Harness.SelfTests;

[Collection(StackCollection.Name)]
public sealed class DatabaseObservationTests(StackFixture fixture)
{
    private readonly ClusterStack stack = fixture.Stack;

    private static async Task ExecuteAsync(HarnessRun run, string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync();
    }

    private static Task InsertMemberAsync(HarnessRun run, string id, string state, double leaseSeconds, long? activatedAt, long epoch = 1) =>
        ExecuteAsync(
            run,
            """
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, activated_at_seq, applied_seq, version, protocol, process_id)
            VALUES (@id, @epoch, @state, clock_timestamp() + make_interval(secs => @lease), @activated, 0, '0.1.0', 1, gen_random_uuid())
            """,
            ("id", id),
            ("epoch", epoch),
            ("state", state),
            ("lease", leaseSeconds),
            ("activated", (object?)activatedAt ?? DBNull.Value));

    [Fact]
    public async Task A_run_applies_the_migration_chain_to_its_own_database_and_reads_its_member_rows()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();

        await InsertMemberAsync(run, "r1", "active", 30, 0);
        var (members, now) = await run.Observer.MembersAsync();

        var member = Assert.Single(members);
        Assert.Equal(("r1", 1L, MemberRowState.Active), (member.ReplicaId, member.Epoch, member.State));
        Assert.True(member.LeaseExpiresAt > now);
    }

    [Fact]
    public async Task The_wait_set_is_what_the_database_says_by_the_predicate_the_wait_uses()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();
        await InsertMemberAsync(run, "active-old", "active", 30, 0);
        await InsertMemberAsync(run, "active-late", "active", 30, 9);
        await InsertMemberAsync(run, "expired", "active", -1, 0);
        await InsertMemberAsync(run, "fenced", "fenced", 30, 0);
        await InsertMemberAsync(run, "joining", "joining", 30, null);

        var waitSet = await run.Observer.WaitSetAsync(5);

        Assert.True(waitSet.SetEquals(["active-old"]));
        Assert.True((await run.Observer.WaitSetAsync(10)).SetEquals(["active-old", "active-late"]));
    }

    [Fact]
    public async Task Fence_history_is_read_per_replica_oldest_epoch_first()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();
        await ExecuteAsync(run, "INSERT INTO reeve_cluster.fence_history (replica_id, epoch, trigger, span) VALUES ('r1', 2, 'ZeroRowRenewal', interval '3 seconds'), ('r1', 1, 'FenceStart', interval '9 seconds'), ('r2', 1, 'ApplierStall', interval '1 second')");

        var r1 = await run.Observer.FenceHistoryAsync("r1");
        var all = await run.Observer.FenceHistoryAsync();

        Assert.Equal([1L, 2L], r1.Select(f => f.Epoch));
        Assert.Equal(["FenceStart", "ZeroRowRenewal"], r1.Select(f => f.Trigger));
        Assert.Equal(TimeSpan.FromSeconds(9), r1[0].Span);
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task The_operator_delete_stimulus_removes_the_row_and_the_zero_row_stimulus_moves_its_epoch_on()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();
        await InsertMemberAsync(run, "gone", "active", 30, 0);
        await InsertMemberAsync(run, "taken-over", "active", 30, 0, epoch: 4);

        var deleted = await HarnessOperations.OperatorDelete(run, "gone");
        var changed = await HarnessOperations.MutateMemberRow(run, "taken-over");

        Assert.Equal(1, deleted);
        Assert.Equal(1, changed);
        Assert.Equal([EnvironmentActions.OperatorDelete], run.Events.Snapshot().Select(e => e.Action));
        Assert.Equal("gone", run.Events.Snapshot()[0].State![EnvironmentActions.ReplicaKey]);
        Assert.Null(await run.Observer.MemberAsync("gone"));
        Assert.Equal(5, (await run.Observer.MemberAsync("taken-over"))!.Epoch);
        Assert.Equal(0, await HarnessOperations.OperatorDelete(run, "gone"));
        Assert.Single(run.Events.Snapshot());
    }

    [Fact]
    public async Task A_renewal_by_the_spec_predicate_matches_no_row_after_the_takeover_stimulus()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();
        await InsertMemberAsync(run, "r1", "active", 30, 0, epoch: 7);
        const string Renewal = "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '8 seconds' WHERE replica_id = 'r1' AND epoch = 7 AND state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()";
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var before = new NpgsqlCommand(Renewal, connection);
        Assert.Equal(1, await before.ExecuteNonQueryAsync());

        await HarnessOperations.MutateMemberRow(run, "r1");

        await using var after = new NpgsqlCommand(Renewal, connection);
        Assert.Equal(0, await after.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task The_wait_set_of_an_empty_member_table_is_empty()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();

        var (members, now) = await run.Observer.MembersAsync();

        Assert.Empty(members);
        Assert.Null(now);
        Assert.Empty(await run.Observer.WaitSetAsync(1));
    }

    [Fact]
    public async Task The_floor_raise_stimulus_sets_the_clusters_protocol_floor()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        await run.MigrateAsync();

        await HarnessOperations.RaiseProtocolFloor(run, 2);
        await Assert.ThrowsAsync<InvalidOperationException>(() => HarnessOperations.RaiseProtocolFloor(run, 1));

        Assert.Equal([EnvironmentActions.FloorRaise], run.Events.Snapshot().Select(e => e.Action));
        Assert.Equal(2, run.Events.Snapshot()[0].State!["floor"]);
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT protocol_floor FROM reeve_cluster.cluster", connection);
        Assert.Equal(2, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task The_network_stimulus_cuts_a_replicas_proxies()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var replica = await run.StartReplicaAsync("r1");

        await HarnessOperations.CutNetwork(replica);

        Assert.True(replica.Link!.Postgres.IsCut);
        Assert.True(replica.Link.Redis.IsCut);
    }

    [Fact]
    public async Task The_database_outage_stimulus_restarts_the_database_for_the_downtime_given()
    {
        await using var run = await HarnessRun.BeginAsync(_ => { }, stack);
        var started = System.Diagnostics.Stopwatch.StartNew();

        await HarnessOperations.DatabaseOutage(run, TimeSpan.FromSeconds(2));

        Assert.True(started.Elapsed >= TimeSpan.FromSeconds(1.95), $"took {started.Elapsed}");
        await using var connection = await stack.OpenObserverConnectionAsync();
    }
}
