using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClusterSingleFlightTests
{
    private static readonly Guid First = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid Second = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static readonly TimeSpan ClaimTtl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ResultTtl = TimeSpan.FromMinutes(2);
    private static readonly byte[] Mine = [1, 2, 3];
    private static readonly byte[] Theirs = [9, 9];

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedFlightRows rows = new();
    private readonly ScriptedGate gate = new();
    private readonly HookedPoints points = new();
    private readonly ClaimScopes scopes = new();
    private readonly List<CancellationToken> computed = [];
    private Func<CancellationToken, Task<byte[]>>? computeBody;

    private ClusterSingleFlight Flight() =>
        new(rows, gate, scopes, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points, NullLogger<ClusterSingleFlight>.Instance);

    private Task<byte[]> Compute(CancellationToken token)
    {
        computed.Add(token);
        return computeBody is null ? Task.FromResult(Mine) : computeBody(token);
    }

    private Task<SingleFlightResult> Run(CancellationToken token = default) => Flight().RunAsync("k", Compute, ClaimTtl, ResultTtl, token);

    private async Task<SingleFlightResult> Advancing(Task<SingleFlightResult> call)
    {
        for (var i = 0; i < 1000 && !call.IsCompleted; i++)
        {
            time.Advance(ClusterSingleFlight.PollInterval);
            await Task.Delay(1);
        }

        return await call.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task A_free_key_is_computed_under_the_granted_token_and_its_result_is_stored()
    {
        rows.Inserts.Enqueue(First);
        rows.Writes.Enqueue(true);

        var result = await Run();

        var computedResult = Assert.IsType<SingleFlightResult.Computed>(result);
        Assert.Equal(Mine, computedResult.Value);
        Assert.Single(computed);
        Assert.Equal(["insert", "write"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(("insert", FlightKeys.Store("k"), "r1", 4L), rows.Calls[0]);
        Assert.Equal((FlightKeys.Store("k"), First, 100L, ResultTtl), (rows.Calls[1].Key, rows.WrittenToken, rows.WrittenTimeline, rows.WrittenTtl));
        Assert.Equal(Mine, rows.Written);
        Assert.Equal(ClaimTtl, rows.InsertTtl);
    }

    [Fact]
    public async Task The_computation_receives_the_callers_token()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(First);
        rows.Writes.Enqueue(true);

        await Run(cancel.Token);

        Assert.Equal(cancel.Token, Assert.Single(computed));
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_computes_nothing_and_touches_no_row()
    {
        gate.Serving = false;

        Assert.IsType<SingleFlightResult.NotServing>(await Run());
        Assert.Empty(rows.Calls);
        Assert.Empty(computed);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_computes_nothing()
    {
        points.On(ClaimsFaultPoints.FlightBeforeInsert.Name, () => gate.Serving = false);
        rows.Inserts.Enqueue(First);

        Assert.IsType<SingleFlightResult.NotServing>(await Run());
        Assert.Empty(rows.Calls);
        Assert.Empty(computed);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_takeover_across_fence_start_takes_nothing_over()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: null));
        points.On(ClaimsFaultPoints.FlightBeforeTakeover.Name, () => gate.Serving = false);

        Assert.IsType<SingleFlightResult.NotServing>(await Run());
        Assert.Equal(["insert", "read"], rows.Calls.Select(c => c.Statement));
        Assert.Empty(computed);
    }

    [Fact]
    public async Task I1_a_replica_that_stopped_serving_during_the_computation_does_not_write_the_result()
    {
        rows.Inserts.Enqueue(First);
        computeBody = _ =>
        {
            gate.Serving = false;
            return Task.FromResult(Mine);
        };

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.ComputedRefused>(result).Value);
        Assert.Equal(["insert"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task A_stored_result_is_waited_for_not_computed_whatever_the_claims_validity()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: Theirs));

        var result = await Run();

        Assert.Equal(Theirs, Assert.IsType<SingleFlightResult.Waited>(result).Value);
        Assert.Empty(computed);
        Assert.Equal(["insert", "read"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task A_computation_in_flight_on_another_replica_is_polled_until_its_result_appears()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: true, Result: null));
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: true, Result: null));
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: Theirs));

        var result = await Advancing(Run());

        Assert.Equal(Theirs, Assert.IsType<SingleFlightResult.Waited>(result).Value);
        Assert.Empty(computed);
        Assert.Equal(3, rows.Calls.Count(c => c.Statement == "read"));
        Assert.DoesNotContain(rows.Calls, c => c.Statement is "takeover" or "write");
    }

    [Fact]
    public async Task A_waiter_checks_serving_before_each_poll_and_stops_when_the_replica_no_longer_serves()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: true, Result: null));
        points.On(ClaimsFaultPoints.FlightBeforeInsert.Name, () => gate.Serving = gate.Checks < 1);

        var result = await Advancing(Run());

        Assert.IsType<SingleFlightResult.NotServing>(result);
    }

    [Fact]
    public async Task A_waiter_the_caller_cancels_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: true, Result: null));

        var call = Run(cancel.Token);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Advancing(call));
        Assert.Empty(computed);
    }

    [Fact]
    public async Task A_claim_that_is_not_valid_and_holds_no_result_is_taken_over_under_the_token_read_and_computed()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: null));
        rows.Takeovers.Enqueue(Second);
        rows.Writes.Enqueue(true);

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.Computed>(result).Value);
        Assert.Equal(["insert", "read", "takeover", "write"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(("takeover", FlightKeys.Store("k"), "r1", 4L), rows.Calls[2]);
        Assert.Equal(First, rows.TakeoverExpected);
        Assert.Equal(Second, rows.WrittenToken);
        Assert.Equal(ClaimTtl, rows.TakeoverTtl);
    }

    [Fact]
    public async Task A_takeover_that_matched_no_row_polls_again_instead_of_computing()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: null));
        rows.Takeovers.Enqueue(null);
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r3", 9, Second, Valid: false, Result: Theirs));

        var result = await Advancing(Run());

        Assert.Equal(Theirs, Assert.IsType<SingleFlightResult.Waited>(result).Value);
        Assert.Empty(computed);
    }

    [Fact]
    public async Task A_row_that_is_gone_after_the_conflict_makes_the_call_insert_again_and_check_serving_again()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(null);
        rows.Inserts.Enqueue(First);
        rows.Writes.Enqueue(true);

        var result = await Run();

        Assert.IsType<SingleFlightResult.Computed>(result);
        Assert.Equal(["insert", "read", "insert", "write"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(3, gate.Checks);
    }

    [Fact]
    public async Task I33_a_grant_whose_commit_the_librarys_own_bound_cancelled_is_unknown_and_nothing_is_computed()
    {
        rows.OnInsert = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Inserts.Enqueue(First);

        Assert.IsType<SingleFlightResult.Unknown>(await Run());
        Assert.Empty(computed);
        Assert.Equal(["insert"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task I33_a_takeover_whose_commit_the_librarys_own_bound_cancelled_is_unknown_and_nothing_is_computed()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: false, Result: null));
        rows.OnTakeover = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Takeovers.Enqueue(Second);

        Assert.IsType<SingleFlightResult.Unknown>(await Run());
        Assert.Empty(computed);
    }

    [Fact]
    public async Task I33_a_plain_read_the_librarys_own_bound_cancelled_is_unknown_and_nothing_is_computed()
    {
        rows.Inserts.Enqueue(null);
        rows.OnRead = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Reads.Enqueue(new FlightRead("r2", 9, First, Valid: true, Result: Theirs));

        Assert.IsType<SingleFlightResult.Unknown>(await Run());
        Assert.Empty(computed);
    }

    [Fact]
    public async Task I33_a_grant_the_caller_cancelled_before_it_returned_throws_and_nothing_is_computed()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnInsert = () => cancel.Cancel();
        rows.Inserts.Enqueue(First);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(cancel.Token));
        Assert.Empty(computed);
    }

    [Fact]
    public async Task A_database_failure_at_the_grant_is_thrown_and_nothing_is_computed()
    {
        rows.InsertFailure = new DbFailure("the database is down");

        await Assert.ThrowsAsync<DbFailure>(() => Run());
        Assert.Empty(computed);
    }

    [Fact]
    public async Task A_result_write_that_matched_no_row_is_refused_and_the_computed_value_is_still_the_callers()
    {
        rows.Inserts.Enqueue(First);
        rows.Writes.Enqueue(false);

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.ComputedRefused>(result).Value);
        Assert.False(result is SingleFlightResult.Computed or SingleFlightResult.Waited);
    }

    [Fact]
    public async Task I33_a_result_write_the_librarys_own_bound_cancelled_is_unknown_although_it_updated_the_row()
    {
        rows.Inserts.Enqueue(First);
        rows.OnWrite = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Writes.Enqueue(true);

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.ComputedUnknown>(result).Value);
    }

    [Fact]
    public async Task A_database_failure_at_the_result_write_is_unknown_not_thrown_and_not_stored()
    {
        rows.Inserts.Enqueue(First);
        rows.WriteFailure = new DbFailure("the connection dropped");

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.ComputedUnknown>(result).Value);
    }

    [Fact]
    public async Task A_result_write_on_another_timeline_fences_the_replica_and_is_refused()
    {
        rows.Inserts.Enqueue(First);
        rows.WriteFailure = new PostgresException("timeline_changed", "ERROR", "ERROR", "RC002");

        var result = await Run();

        Assert.Equal(Mine, Assert.IsType<SingleFlightResult.ComputedRefused>(result).Value);
        Assert.Equal([4L], gate.OtherTimelines);
    }

    [Fact]
    public async Task A_result_write_the_caller_cancelled_before_it_returned_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(First);
        rows.OnWrite = () => cancel.Cancel();
        rows.Writes.Enqueue(true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(cancel.Token));
    }

    [Fact]
    public async Task A_computation_that_throws_propagates_releases_its_claim_and_writes_no_result()
    {
        rows.Inserts.Enqueue(First);
        rows.Releases.Enqueue(true);
        computeBody = _ => throw new InvalidOperationException("the computation failed");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Run());

        Assert.Equal(["insert", "release"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(First, rows.ReleasedToken);
    }

    [Fact]
    public async Task A_computation_the_caller_cancelled_still_releases_its_claim()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(First);
        rows.Releases.Enqueue(true);
        computeBody = async token =>
        {
            await cancel.CancelAsync();
            token.ThrowIfCancellationRequested();
            return Mine;
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(cancel.Token));

        Assert.Equal(["insert", "release"], rows.Calls.Select(c => c.Statement));
        Assert.False(rows.ReleaseToken.IsCancellationRequested);
    }

    [Fact]
    public async Task A_release_that_fails_does_not_hide_the_computations_exception()
    {
        rows.Inserts.Enqueue(First);
        rows.ReleaseFailure = new DbFailure("the database is down");
        computeBody = _ => throw new InvalidOperationException("the computation failed");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => Run());

        Assert.Equal("the computation failed", thrown.Message);
    }

    [Fact]
    public async Task A_call_on_a_key_with_an_open_local_claim_scope_is_refused_before_any_row_is_touched()
    {
        using var scope = scopes.Open(FlightKeys.Store("k"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => Run());
        Assert.Empty(rows.Calls);
        Assert.Empty(computed);
    }

    [Fact]
    public async Task A_call_needs_a_key_a_computation_and_positive_times_to_live()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Flight().RunAsync(null!, Compute, ClaimTtl, ResultTtl));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Flight().RunAsync("k", null!, ClaimTtl, ResultTtl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Flight().RunAsync("k", Compute, TimeSpan.Zero, ResultTtl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Flight().RunAsync("k", Compute, ClaimTtl, TimeSpan.Zero));
        Assert.Empty(rows.Calls);
        Assert.Equal(0, gate.Checks);
    }

    [Fact]
    public async Task The_fault_points_between_the_grant_and_the_computation_and_between_the_computation_and_the_write_are_reached_in_order()
    {
        var order = new List<string>();
        points.On(ClaimsFaultPoints.FlightAfterGrant.Name, () => order.Add("after-grant"));
        points.On(ClaimsFaultPoints.FlightBeforeResult.Name, () => order.Add("before-result"));
        computeBody = _ =>
        {
            order.Add("compute");
            return Task.FromResult(Mine);
        };
        rows.Inserts.Enqueue(First);
        rows.Writes.Enqueue(true);

        await Run();

        Assert.Equal(["after-grant", "compute", "before-result"], order);
    }

    private sealed class ScriptedFlightRows : IFlightRows
    {
        public Queue<Guid?> Inserts { get; } = new();

        public Queue<FlightRead?> Reads { get; } = new();

        public Queue<Guid?> Takeovers { get; } = new();

        public Queue<bool> Writes { get; } = new();

        public Queue<bool> Releases { get; } = new();

        public List<(string Statement, string Key, string Replica, long Epoch)> Calls { get; } = [];

        public Action? OnInsert { get; set; }

        public Action? OnRead { get; set; }

        public Action? OnTakeover { get; set; }

        public Action? OnWrite { get; set; }

        public Exception? InsertFailure { get; set; }

        public Exception? WriteFailure { get; set; }

        public Exception? ReleaseFailure { get; set; }

        public TimeSpan InsertTtl { get; private set; }

        public Guid TakeoverExpected { get; private set; }

        public TimeSpan TakeoverTtl { get; private set; }

        public Guid WrittenToken { get; private set; }

        public long WrittenTimeline { get; private set; }

        public byte[]? Written { get; private set; }

        public TimeSpan WrittenTtl { get; private set; }

        public Guid ReleasedToken { get; private set; }

        public CancellationToken ReleaseToken { get; private set; }

        public Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("insert", key, replica, epoch));
            InsertTtl = ttl;
            OnInsert?.Invoke();
            return InsertFailure is null ? Task.FromResult(Inserts.Dequeue()) : Task.FromException<Guid?>(InsertFailure);
        }

        public Task<FlightRead?> ReadAsync(string key, CancellationToken cancellationToken)
        {
            Calls.Add(("read", key, string.Empty, 0));
            OnRead?.Invoke();
            return Task.FromResult(Reads.Dequeue());
        }

        public Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("takeover", key, replica, epoch));
            TakeoverExpected = expected;
            TakeoverTtl = ttl;
            OnTakeover?.Invoke();
            return Task.FromResult(Takeovers.Dequeue());
        }

        public Task<bool> WriteResultAsync(string key, Guid token, long timeline, byte[] result, TimeSpan resultTtl, CancellationToken cancellationToken)
        {
            Calls.Add(("write", key, string.Empty, 0));
            (WrittenToken, WrittenTimeline, Written, WrittenTtl) = (token, timeline, result, resultTtl);
            OnWrite?.Invoke();
            return WriteFailure is null ? Task.FromResult(Writes.Dequeue()) : Task.FromException<bool>(WriteFailure);
        }

        public Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken)
        {
            Calls.Add(("release", key, string.Empty, 0));
            (ReleasedToken, ReleaseToken) = (token, cancellationToken);
            return ReleaseFailure is null ? Task.FromResult(Releases.Dequeue()) : Task.FromException<bool>(ReleaseFailure);
        }
    }
}
