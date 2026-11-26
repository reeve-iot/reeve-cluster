using System.Collections.Concurrent;
using Int.Log;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class ActivationTransactionTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;
    private long epoch;

    public ActivationTransactionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync()
    {
        db = await MemberDatabase.CreateAsync(postgres);
        var identity = new MemberIdentity(Guid.NewGuid(), "1.5.0", MemberIdentity.CurrentProtocol, []);
        var answer = await db.Store.JoinAsync(new JoinRequest("r1", TimeSpan.FromMinutes(10), identity), CancellationToken.None);
        epoch = answer!.Value.Epoch;
    }

    public async Task DisposeAsync() => await db.DisposeAsync();

    private ActivationRequest Request(string version = "1.5.0", int protocol = MemberIdentity.CurrentProtocol) => new("r1", epoch, version, protocol);

    private Task<ActivationAnswer> ActivateAsync(ActivationStore? store = null, ActivationRequest? request = null, Func<bool>? mayCommit = null) =>
        (store ?? db.Activation()).ActivateAsync(request ?? Request(), mayCommit ?? (() => true), CancellationToken.None).WaitAsync(Wait);

    private static (ActivationOutcome Outcome, long Head) Of(ActivationAnswer answer) => (answer.Outcome, answer.Head);

    private async Task<long> AppendAsync(int count = 1)
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        return await AppendAsync(connection, count);
    }

    private static async Task<long> AppendAsync(NpgsqlConnection connection, int count = 1)
    {
        long seq = 0;
        for (var i = 0; i < count; i++)
        {
            await using var command = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k') RETURNING seq", connection);
            seq = (long)(await command.ExecuteScalarAsync())!;
        }

        return seq;
    }

    private static async Task<DateTime> AppendTimedAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k') RETURNING clock_timestamp()", connection);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    private async Task<DateTime> LockHolderTransactionStartAsync()
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT a.xact_start FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND a.pid <> pg_backend_pid()",
            connection);
        return (DateTime)(await command.ExecuteScalarAsync())!;
    }

    private async Task<Cursor> CursorAtAsync(long seq)
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT entry_token FROM reeve_cluster.change_log WHERE seq = @s", connection);
        command.Parameters.AddWithValue("s", seq);
        return new Cursor(seq, (Guid)(await command.ExecuteScalarAsync())!);
    }

    private Task SetFloorsAsync(int? protocol = null, string? version = null, bool clearVersion = false) => db.ExecuteAsync(
        $"UPDATE reeve_cluster.cluster SET protocol_floor = {protocol?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "protocol_floor"}, " +
        $"version_floor = {(clearVersion ? "NULL" : version is null ? "version_floor" : $"'{version}'")}");

    [Fact]
    public async Task M06_the_activation_sets_the_row_active_at_the_head_read_under_the_lock()
    {
        var head = await AppendAsync(3);

        var answer = await ActivateAsync();

        Assert.Equal(ActivationOutcome.Committed, answer.Outcome);
        Assert.Equal(head, answer.Head);
        Assert.Equal((0, (string?)null), (answer.ProtocolFloor, answer.VersionFloor));
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(("active", head), (row.State, row.ActivatedAtSeq));
        Assert.Null(row.ServingSince);
    }

    [Fact]
    public async Task M06_on_an_empty_log_the_head_is_0()
    {
        var answer = await ActivateAsync();

        Assert.Equal((ActivationOutcome.Committed, 0L), (answer.Outcome, answer.Head));
        Assert.Equal(0L, (await db.ReadRowAsync("r1")).ActivatedAtSeq);
    }

    [Theory]
    [InlineData("1.9.0", "1.10.0")]
    [InlineData("1.0.0-rc.1", "1.0.0")]
    [InlineData("0.9.99", "0.10.0")]
    public async Task I14_a_version_below_the_floor_by_SemVer_is_refused_without_an_UPDATE_and_stays_joining(string version, string floor)
    {
        await SetFloorsAsync(version: floor);

        var answer = await ActivateAsync(request: Request(version));

        Assert.Equal(ActivationOutcome.RefusedByVersionFloor, answer.Outcome);
        Assert.Equal(floor, answer.VersionFloor);
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(("joining", (long?)null), (row.State, row.ActivatedAtSeq));
    }

    [Theory]
    [InlineData("1.10.0", "1.9.0")]
    [InlineData("1.0.0", "1.0.0-rc.1")]
    [InlineData("1.0.0", "1.0.0")]
    public async Task I14_a_version_at_or_above_the_floor_by_SemVer_activates_where_text_order_would_refuse_it(string version, string floor)
    {
        await SetFloorsAsync(version: floor);

        var answer = await ActivateAsync(request: Request(version));

        Assert.Equal(ActivationOutcome.Committed, answer.Outcome);
        Assert.Equal("active", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task I14_a_protocol_below_the_floor_leaves_zero_rows_and_is_refused_by_the_protocol_floor()
    {
        await SetFloorsAsync(protocol: MemberIdentity.CurrentProtocol + 1);

        var answer = await ActivateAsync();

        Assert.Equal(ActivationOutcome.RefusedByProtocolFloor, answer.Outcome);
        Assert.Equal(MemberIdentity.CurrentProtocol + 1, answer.ProtocolFloor);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task A_protocol_at_the_floor_activates()
    {
        await SetFloorsAsync(protocol: MemberIdentity.CurrentProtocol);

        Assert.Equal(ActivationOutcome.Committed, (await ActivateAsync()).Outcome);
    }

    [Theory]
    [InlineData(null, "1.0.0")]
    [InlineData("1.0.0", null)]
    [InlineData("1.0.0", "0.9.0")]
    [InlineData("1.0.0", "1.2.0")]
    public async Task I14_a_version_floor_changed_between_the_read_and_the_update_leaves_zero_rows(string? read, string? changedTo)
    {
        if (read is not null)
        {
            await SetFloorsAsync(version: read);
        }

        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateAfterHeadRead);
        var activation = ActivateAsync(db.Activation(probe));
        await probe.Arrived.WaitAsync(Wait);
        await SetFloorsAsync(version: changedTo, clearVersion: changedTo is null);
        probe.Release();

        var answer = await activation;

        Assert.Equal(ActivationOutcome.Abandoned, answer.Outcome);
        Assert.Equal(read, answer.VersionFloor);
        Assert.Null(ActivationRules.ReasonCode(answer.Outcome));
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(("joining", (long?)null), (row.State, row.ActivatedAtSeq));
    }

    [Fact]
    public async Task Zero_rows_after_fence_step_3_abandons_the_activation_and_the_row_stays_fenced()
    {
        Assert.True(await db.Fences.ReleaseLeaseAsync("r1", epoch, CancellationToken.None));

        var answer = await ActivateAsync();

        Assert.Equal(ActivationOutcome.Abandoned, answer.Outcome);
        Assert.Equal("fenced", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task Zero_rows_after_a_takeover_abandons_the_activation_and_never_writes_the_new_epochs_row()
    {
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET epoch = epoch + 1");

        var answer = await ActivateAsync();

        Assert.Equal(ActivationOutcome.Abandoned, answer.Outcome);
        var row = await db.ReadRowAsync("r1");
        Assert.Equal(("joining", epoch + 1), (row.State, row.Epoch));
    }

    [Theory]
    [InlineData("membership.activate.after-update")]
    [InlineData("membership.activate.before-commit")]
    public async Task Phases_fence_step_3_during_the_activation_leaves_the_row_fenced(string point)
    {
        var probe = new PointProbe();
        probe.Hold(point == MembershipFaultPoints.ActivateAfterUpdate.Name ? MembershipFaultPoints.ActivateAfterUpdate : MembershipFaultPoints.ActivateBeforeCommit);
        var activation = ActivateAsync(db.Activation(probe));
        await probe.Arrived.WaitAsync(Wait);
        var release = db.Fences.ReleaseLeaseAsync("r1", epoch, CancellationToken.None);
        await db.UntilALockWaitsAsync();
        Assert.False(release.IsCompleted, "step 3 must wait for the activation's row lock");
        probe.Release();

        await activation;
        Assert.True(await release.WaitAsync(Wait));
        var row = await db.ReadRowAsync("r1");
        Assert.Equal("fenced", row.State);
        Assert.Null(row.ServingSince);
    }

    [Fact]
    public async Task Phases_a_phase_that_left_CatchingUp_before_the_commit_rolls_the_activation_back()
    {
        var answer = await ActivateAsync(mayCommit: () => false);

        Assert.Equal(ActivationOutcome.LeftCatchingUp, answer.Outcome);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
        Assert.True(await db.Fences.ReleaseLeaseAsync("r1", epoch, CancellationToken.None));
        Assert.Equal("fenced", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task A_cancel_before_the_commit_rolls_the_activation_back()
    {
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateBeforeCommit);
        using var cancel = new CancellationTokenSource();
        var activation = db.Activation(probe).ActivateAsync(Request(), () => true, cancel.Token);
        await probe.Arrived.WaitAsync(Wait);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => activation.WaitAsync(Wait));
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task An_activation_whose_process_stops_between_its_statements_is_ended_by_its_transaction_timeout()
    {
        var bound = TimeSpan.FromSeconds(6);
        var paused = new PausedAt(MembershipFaultPoints.ActivateBeforeCommit.Name);
        await using var writer = new NpgsqlConnection(db.ConnectionString);
        await writer.OpenAsync();
        var activation = db.Activation(paused, bound - TimeSpan.FromSeconds(1)).ActivateAsync(Request(), () => true, CancellationToken.None);
        await paused.Arrived.WaitAsync(Wait);

        var holderStart = await LockHolderTransactionStartAsync();
        var append = AppendTimedAsync(writer);
        var limit = (5 * bound) + TimeSpan.FromSeconds(10);
        var completed = await Task.WhenAny(append, Task.Delay(limit)) == append;
        paused.Resume();

        var error = await Record.ExceptionAsync(() => activation.WaitAsync(Wait));
        var lockedAt = await append.WaitAsync(Wait);
        Assert.True(completed, $"an append waited more than {limit} behind an activation whose process stopped between its statements");
        var held = lockedAt - holderStart;
        Assert.True(
            held >= bound - TimeSpan.FromMilliseconds(100) && held <= bound + TimeSpan.FromSeconds(2),
            $"the activation held the lock for {held} by the server's clock; its transaction_timeout is RenewTimeout + 1 s = {bound}");
        Assert.Equal("25P04", Assert.IsAssignableFrom<PostgresException>(error).SqlState);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task I22_an_append_holding_the_lock_when_the_activation_starts_is_at_or_below_H()
    {
        await using var writer = new NpgsqlConnection(db.ConnectionString);
        await writer.OpenAsync();
        await using var transaction = await writer.BeginTransactionAsync();
        await using var append = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type) VALUES ('t') RETURNING seq", writer, transaction);
        var seq = (long)(await append.ExecuteScalarAsync())!;

        var activation = ActivateAsync();
        await db.UntilALockWaitsAsync();
        Assert.False(activation.IsCompleted, "the activation must wait for the change-log lock");
        await transaction.CommitAsync();

        var answer = await activation;
        Assert.Equal(ActivationOutcome.Committed, answer.Outcome);
        Assert.True(answer.Head >= seq, $"H = {answer.Head} misses the write at {seq} that committed before the activation's head read");
    }

    [Fact]
    public async Task I22_an_append_after_the_activation_took_the_lock_waits_for_its_commit_and_its_writer_then_sees_the_row_active()
    {
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateBeforeCommit);
        var activation = ActivateAsync(db.Activation(probe));
        await probe.Arrived.WaitAsync(Wait);

        var write = Task.Run(async () =>
        {
            await using var writer = new NpgsqlConnection(db.ConnectionString);
            await writer.OpenAsync();
            await using var transaction = await writer.BeginTransactionAsync();
            await using var append = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type) VALUES ('t') RETURNING seq", writer, transaction);
            var seq = (long)(await append.ExecuteScalarAsync())!;
            await transaction.CommitAsync();
            await using var read = new NpgsqlCommand("SELECT state FROM reeve_cluster.member WHERE replica_id = 'r1'", writer);
            return (seq, (string)(await read.ExecuteScalarAsync())!);
        });
        await db.UntilALockWaitsAsync();
        Assert.False(write.IsCompleted, "the append must wait for the activation's change-log lock");
        probe.Release();

        var answer = await activation;
        var (seq, seen) = await write.WaitAsync(Wait);
        Assert.Equal(ActivationOutcome.Committed, answer.Outcome);
        Assert.True(seq > answer.Head, $"the append at {seq} must be after H = {answer.Head}");
        Assert.Equal("active", seen);
    }

    [Fact]
    public async Task I22_under_a_continuous_stream_of_writes_every_writer_either_excludes_the_replica_or_sees_it_active()
    {
        using var stop = new CancellationTokenSource();
        var writes = new ConcurrentBag<(long Seq, string Seen)>();
        var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            await using var writer = new NpgsqlConnection(db.ConnectionString);
            await writer.OpenAsync();
            while (!stop.IsCancellationRequested)
            {
                await using var transaction = await writer.BeginTransactionAsync();
                await using var append = new NpgsqlCommand("INSERT INTO reeve_cluster.change_log (aggregate_type) VALUES ('t') RETURNING seq", writer, transaction);
                var seq = (long)(await append.ExecuteScalarAsync())!;
                await transaction.CommitAsync();
                await using var read = new NpgsqlCommand("SELECT state FROM reeve_cluster.member WHERE replica_id = 'r1'", writer);
                writes.Add((seq, (string)(await read.ExecuteScalarAsync())!));
            }
        })).ToArray();

        await Poll.UntilAsync(() => !writes.IsEmpty, "a write before the activation");
        var answer = await ActivateAsync();
        await Poll.UntilAsync(() => writes.Any(w => w.Seq > answer.Head), "a write after the activation");
        await stop.CancelAsync();
        await Task.WhenAll(writers).WaitAsync(Wait);

        Assert.Equal(ActivationOutcome.Committed, answer.Outcome);
        Assert.Contains(writes, w => w.Seq <= answer.Head);
        Assert.Contains(writes, w => w.Seq > answer.Head);
        var neither = writes.Where(w => w.Seq > answer.Head && w.Seen != "active").ToList();
        Assert.True(neither.Count == 0, $"writes after H = {answer.Head} whose writer saw the row joining: {string.Join(", ", neither.Select(w => w.Seq))}");
    }

    [Fact]
    public async Task The_backlog_counts_the_entries_after_the_cursor_up_to_one_past_the_threshold_and_checks_its_validity()
    {
        var head = await AppendAsync(5);
        var store = db.Activation();
        var at2 = await CursorAtAsync(head - 3);

        Assert.Equal(new Backlog(true, 3), await store.ReadBacklogAsync(at2, threshold: 10, CancellationToken.None));
        Assert.Equal(new Backlog(true, 3), await store.ReadBacklogAsync(at2, threshold: 3, CancellationToken.None));
        Assert.Equal(new Backlog(true, 2), await store.ReadBacklogAsync(at2, threshold: 1, CancellationToken.None));
        Assert.Equal(new Backlog(true, 5), await store.ReadBacklogAsync(Cursor.Empty, threshold: 5, CancellationToken.None));
        Assert.Equal(new Backlog(false, 3), await store.ReadBacklogAsync(at2 with { Token = Guid.NewGuid() }, threshold: 10, CancellationToken.None));
    }

    [Fact]
    public async Task M07_serving_since_is_set_once_the_cursor_is_valid_and_at_or_above_the_activation_head()
    {
        var head = await AppendAsync(3);
        var store = db.Activation();
        Assert.Equal(ActivationOutcome.Committed, (await ActivateAsync(store)).Outcome);

        Assert.False(await store.SetServingSinceAsync("r1", epoch, await CursorAtAsync(head - 1), CancellationToken.None));
        Assert.Null((await db.ReadRowAsync("r1")).ServingSince);
        Assert.False(await store.SetServingSinceAsync("r1", epoch, new Cursor(head, Guid.NewGuid()), CancellationToken.None));
        Assert.Null((await db.ReadRowAsync("r1")).ServingSince);
        Assert.False(await store.SetServingSinceAsync("r1", epoch + 1, await CursorAtAsync(head), CancellationToken.None));

        Assert.True(await store.SetServingSinceAsync("r1", epoch, await CursorAtAsync(head), CancellationToken.None));
        var first = (await db.ReadRowAsync("r1")).ServingSince;
        Assert.NotNull(first);
        await Task.Delay(20);
        Assert.True(await store.SetServingSinceAsync("r1", epoch, await CursorAtAsync(head), CancellationToken.None));
        Assert.Equal(first, (await db.ReadRowAsync("r1")).ServingSince);
    }

    [Fact]
    public async Task M07_serving_since_is_never_set_on_a_joining_or_fenced_row()
    {
        var head = await AppendAsync(1);
        var store = db.Activation();
        Assert.False(await store.SetServingSinceAsync("r1", epoch, await CursorAtAsync(head), CancellationToken.None));

        Assert.Equal(ActivationOutcome.Committed, (await ActivateAsync(store)).Outcome);
        Assert.True(await db.Fences.ReleaseLeaseAsync("r1", epoch, CancellationToken.None));
        Assert.False(await store.SetServingSinceAsync("r1", epoch, await CursorAtAsync(head), CancellationToken.None));
        Assert.Null((await db.ReadRowAsync("r1")).ServingSince);
    }

    private sealed class PausedAt(string held) : IFaultPoints
    {
        private readonly TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Arrived => arrived.Task;

        public void Resume() => resumed.TrySetResult();

        public async ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
        {
            if (point == held)
            {
                arrived.TrySetResult();
                await resumed.Task;
            }
        }
    }
}
