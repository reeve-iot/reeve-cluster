using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;

namespace Int.Log;

public sealed class ChangeLogAppendTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public ChangeLogAppendTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<LogRig> RigAsync(IFaultPoints? points = null) => LogRig.CreateAsync(postgres, points);

    private static async Task<int> ValueAsync(LogRig rig, int id) =>
        (int)await rig.ScalarAsync<long>($"SELECT value FROM public.config WHERE id = {id}");

    [Fact]
    public async Task Append_records_the_seqs_in_the_ambient_scope_at_the_append_and_returns_the_highest()
    {
        var points = new PointProbe();
        points.Hold(LogFaultPoints.AppendAfter);
        var rig = await RigAsync(points);
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var append = rig.Log.AppendAsync(transaction, [new ChangeEntry("widget", "w1"), new ChangeEntry("widget", "w2"), new ChangeEntry("gadget")]);
        await points.Arrived.WaitAsync(Bound);

        var seqs = request.Scope.Seqs;
        Assert.Equal(3, seqs.Count);
        Assert.Equal(Enumerable.Range((int)seqs[0], 3).Select(s => (long)s), seqs);

        points.Release();
        Assert.Equal(seqs[^1], await append.WaitAsync(Bound));
        await transaction.CommitAsync();

        await using var read = new NpgsqlCommand("SELECT seq, aggregate_type, aggregate_key FROM reeve_cluster.change_log ORDER BY seq", connection);
        await using var reader = await read.ExecuteReaderAsync();
        var rows = new List<(long, string, string?)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt64(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        Assert.Equal([(seqs[0], "widget", "w1"), (seqs[1], "widget", "w2"), (seqs[2], "gadget", null)], rows);
    }

    [Fact]
    public async Task Append_reaches_its_points_around_the_statement_and_emits_WAppend_with_the_seqs_and_the_xid()
    {
        var points = new PointProbe();
        var rig = await RigAsync(points);
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var xidCommand = new NpgsqlCommand("SELECT pg_current_xact_id()::text", connection, transaction);
        var xid = (string)(await xidCommand.ExecuteScalarAsync())!;

        var highest = await rig.Log.AppendAsync(transaction, LogRig.Entries("a", "b"));

        Assert.Equal([LogFaultPoints.AppendBefore.Name, LogFaultPoints.AppendAfter.Name], points.Reached);
        var append = Assert.Single(rig.Events.All);
        Assert.Equal(LogActions.WAppend, append.Action);
        Assert.Equal(xid, append.State![ObservationKeys.Xid]);
        Assert.Equal(request.Scope.Seqs, (IEnumerable<long>)append.State[ObservationKeys.Seqs]!);
        Assert.Equal(highest, request.Scope.Seqs[^1]);
        Assert.Equal(((ClusterScope)request.Scope).Id, append.State[ObservationKeys.Request]);
        Assert.Equal(false, append.State[ObservationKeys.Refused]);
    }

    [Fact]
    public async Task The_trigger_assigns_the_seq_the_xid_and_the_entry_token_and_holds_the_change_log_lock_until_the_transaction_ends()
    {
        var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await using var insert = new NpgsqlCommand(
            """
            INSERT INTO reeve_cluster.change_log (seq, entry_token, xid, aggregate_type)
            VALUES (999999, '00000000-0000-0000-0000-000000000001', '1', 'a'), (999998, '00000000-0000-0000-0000-000000000001', '1', 'b')
            RETURNING seq, entry_token, xid = pg_current_xact_id()
            """, connection, transaction);
        var rows = new List<(long Seq, Guid Token, bool OwnXid)>();
        await using (var reader = await insert.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetInt64(0), reader.GetGuid(1), reader.GetBoolean(2)));
            }
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(rows[0].Seq + 1, rows[1].Seq);
        Assert.True(rows[1].Seq < 999998);
        Assert.All(rows, r => Assert.True(r.OwnXid));
        Assert.All(rows, r => Assert.NotEqual(Guid.Parse("00000000-0000-0000-0000-000000000001"), r.Token));
        Assert.NotEqual(rows[0].Token, rows[1].Token);

        var held = $"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND ((classid::bigint << 32) | objid::bigint) = {ChangeLogLock.Key}";
        Assert.Equal(1L, await rig.ScalarAsync<long>(held));

        await transaction.CommitAsync();
        Assert.Equal(0L, await rig.ScalarAsync<long>(held));
        Assert.Equal(0L, await rig.ScalarAsync<long>("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'"));
    }

    [Fact]
    public async Task A_transaction_rolled_back_after_Append_leaves_a_recorded_seq_and_a_gap()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();

        long rolledBack;
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            rolledBack = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
            await transaction.RollbackAsync();
        }

        Assert.Equal([rolledBack], request.Scope.Seqs);
        Assert.Empty(await rig.SeqsAsync());

        await using (var transaction = await connection.BeginTransactionAsync())
        {
            var next = await rig.Log.AppendAsync(transaction, LogRig.Entries("b"));
            await transaction.CommitAsync();
            Assert.True(next > rolledBack);
            Assert.Equal([next], await rig.SeqsAsync());
        }

        Assert.Equal(2, request.Scope.Seqs.Count);
    }

    [Fact]
    public async Task Append_without_a_transaction_throws_and_records_nothing()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);

        await Assert.ThrowsAsync<ArgumentNullException>(() => rig.Log.AppendAsync(null!, LogRig.Entries("a")));

        Assert.Empty(request.Scope.Seqs);
        Assert.Empty(await rig.SeqsAsync());
    }

    [Fact]
    public async Task Append_in_a_transaction_that_has_ended_throws_and_records_nothing()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        var transaction = await connection.BeginTransactionAsync();
        await transaction.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Log.AppendAsync(transaction, LogRig.Entries("a")));

        Assert.Empty(request.Scope.Seqs);
        Assert.Empty(await rig.SeqsAsync());
    }

    [Fact]
    public async Task Append_of_no_entry_or_an_entry_without_a_type_throws()
    {
        var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => rig.Log.AppendAsync(transaction, []));
        await Assert.ThrowsAsync<ArgumentException>(() => rig.Log.AppendAsync(transaction, [new ChangeEntry("")]));
        await Assert.ThrowsAsync<ArgumentException>(() => rig.Log.AppendAsync(transaction, [null!]));
    }

    [Fact]
    public async Task An_Append_outside_every_scope_appends_and_records_nowhere()
    {
        var rig = await RigAsync();
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await transaction.CommitAsync();

        Assert.Equal([seq], await rig.SeqsAsync());
        Assert.Null(rig.Scopes.Current);
    }

    [Fact]
    public async Task A_cancelled_append_rolls_the_transaction_back_so_its_business_writes_cannot_commit_without_recorded_seqs()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await LogRig.TouchAsync(transaction, 1);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            rig.Log.AppendAsync(transaction, LogRig.Entries("a"), new CancellationToken(canceled: true)));

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => transaction.CommitAsync());
        Assert.Empty(request.Scope.Seqs);
        Assert.Equal(0, await ValueAsync(rig, 1));
        Assert.Empty(await rig.SeqsAsync());
    }

    [Fact]
    public async Task An_Append_in_a_transaction_begun_through_EF_lists_it_in_the_scope_until_it_ends()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        rig.EfTransactions.Began(transaction);

        await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));

        Assert.Same(transaction, Assert.Single(request.Scope.OpenTransactions));

        await transaction.CommitAsync();
        rig.EfTransactions.Ended(transaction);
        Assert.Empty(request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task An_Append_in_a_raw_transaction_is_not_listed_as_open()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));

        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Single(request.Scope.Seqs);
    }

    [Fact]
    public async Task Before_fence_step_2_a_client_requests_Append_proceeds()
    {
        var rig = await RigAsync();
        rig.Gate.MoveTo(ReplicaPhase.Fencing);
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await transaction.CommitAsync();

        Assert.Equal([seq], request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
    }

    [Fact]
    public async Task After_fence_step_2_a_client_requests_Append_throws_records_the_refusal_and_rolls_the_write_back()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await LogRig.TouchAsync(transaction, 1);

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Log.AppendAsync(transaction, LogRig.Entries("a")));

        Assert.True(request.Scope.CommitRefused);
        Assert.Empty(request.Scope.Seqs);
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => transaction.CommitAsync());
        Assert.Equal(0, await ValueAsync(rig, 1));
        Assert.Empty(await rig.SeqsAsync());

        var refused = Assert.Single(rig.Events.All);
        Assert.Equal(LogActions.WAppend, refused.Action);
        Assert.Equal(true, refused.State![ObservationKeys.Refused]);
        Assert.Empty((IEnumerable<long>)refused.State[ObservationKeys.Seqs]!);
    }

    [Fact]
    public async Task After_fence_step_2_an_Append_in_a_flow_that_outlived_its_client_request_commits_and_records_nowhere()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        var requestEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        IClusterScope endedRequest;
        Task<long> loop;

        using (var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest))
        {
            endedRequest = request.Scope;
            loop = Task.Run(async () =>
            {
                await requestEnded.Task;
                await using var connection = await rig.OpenAsync();
                await using var transaction = await connection.BeginTransactionAsync();
                await LogRig.TouchAsync(transaction, 1);
                var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
                await transaction.CommitAsync();
                return seq;
            });
        }

        requestEnded.SetResult();
        var seq = await loop.WaitAsync(Bound);

        Assert.Equal([seq], await rig.SeqsAsync());
        Assert.Equal(1, await ValueAsync(rig, 1));
        Assert.Empty(endedRequest.Seqs);
        Assert.False(endedRequest.CommitRefused);
    }

    [Fact]
    public async Task An_Append_with_an_explicit_scope_that_has_ended_commits_after_step_2_and_records_nowhere()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        request.Dispose();
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var appended = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"), (IClusterScopeWriter)request.Scope, CancellationToken.None);
        await transaction.CommitAsync();

        Assert.Equal([appended.HighestSeq], await rig.SeqsAsync());
        Assert.Empty(request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
        Assert.Null(Assert.Single(rig.Events.All).State![ObservationKeys.Request]);
    }

    [Fact]
    public async Task A_refusal_after_an_earlier_Append_of_the_request_keeps_the_earlier_seq_recorded()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var first = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));

        await rig.RunFenceStep2Async();
        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Log.AppendAsync(transaction, LogRig.Entries("b")));

        Assert.Equal([first], request.Scope.Seqs);
        Assert.True(request.Scope.CommitRefused);
        Assert.Empty(await rig.SeqsAsync());
    }

    [Fact]
    public async Task A_fence_that_reaches_step_2_while_the_writer_is_held_before_its_append_refuses_it()
    {
        var points = new PointProbe();
        points.Hold(LogFaultPoints.AppendBefore);
        var rig = await RigAsync(points);
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var append = rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await points.Arrived.WaitAsync(Bound);
        await rig.RunFenceStep2Async();
        points.Release();

        await Assert.ThrowsAsync<InvalidOperationException>(() => append.WaitAsync(Bound));
        Assert.True(request.Scope.CommitRefused);
        Assert.Empty(await rig.SeqsAsync());
    }

    [Theory]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public async Task After_fence_step_2_an_Append_outside_a_non_exempt_client_request_commits(ClusterScopeKind kind)
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        using var scope = rig.Scopes.Begin(kind);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await LogRig.TouchAsync(transaction, 1);

        var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await transaction.CommitAsync();

        Assert.False(scope.Scope.CommitRefused);
        Assert.Equal([seq], await rig.SeqsAsync());
        Assert.Equal(1, await ValueAsync(rig, 1));

        Assert.Equal(kind == ClusterScopeKind.ExemptRequest ? [] : new List<long> { seq }, scope.Scope.Seqs);
    }

    [Fact]
    public async Task After_fence_step_2_an_Append_outside_every_scope_commits()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await transaction.CommitAsync();

        Assert.Equal([seq], await rig.SeqsAsync());
    }

    [Fact]
    public async Task A_unit_of_work_begun_inside_a_fenced_client_request_does_not_escape_the_refusal()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        using (rig.Scopes.BeginUnitOfWork())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Log.AppendAsync(transaction, LogRig.Entries("a")));
        }

        Assert.True(request.Scope.CommitRefused);
        Assert.Empty(await rig.SeqsAsync());
    }

    [Fact]
    public async Task A_rejoin_lifts_the_refusal()
    {
        var rig = await RigAsync();
        await rig.RunFenceStep2Async();
        rig.Gate.MoveTo(ReplicaPhase.Fenced);
        rig.Gate.MoveTo(ReplicaPhase.Starting);
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("a"));
        await transaction.CommitAsync();

        Assert.Equal([seq], request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
    }

    [Fact]
    public async Task A_client_request_running_at_step_2_is_refused_after_the_rejoin()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await rig.RunFenceStep2Async();
        rig.Gate.MoveTo(ReplicaPhase.Fenced);
        rig.Gate.MoveTo(ReplicaPhase.Starting);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await LogRig.TouchAsync(transaction, 1);

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Log.AppendAsync(transaction, LogRig.Entries("a")));

        Assert.True(request.Scope.CommitRefused);
        Assert.Equal(0, await ValueAsync(rig, 1));
        Assert.Empty(await rig.SeqsAsync());
    }
}
