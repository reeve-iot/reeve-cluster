using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClusterClaimsTests
{
    private static readonly Guid First = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid Second = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedRows rows = new();
    private readonly SwitchGate gate = new();
    private readonly HookPoints points = new();
    private readonly ClaimScopes scopes = new();

    private ClusterClaims Claims() =>
        new(rows, gate, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points, NullLogger<ClusterClaims>.Instance, scopes);

    [Fact]
    public async Task A_key_with_no_row_is_won_under_the_token_the_insert_drew()
    {
        rows.Inserts.Enqueue(First);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(new ClaimResult.Won(First), result);
        Assert.Equal([("insert", "k", "r1", 4L)], rows.Calls);
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_grants_nothing_and_touches_no_row()
    {
        gate.Serving = false;

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.NotServing>(result);
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_grant_on_a_key_with_an_open_local_fenced_scope_is_refused()
    {
        using var scope = scopes.Open("k");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Claims().ClaimAsync("k", Ttl));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_grants_nothing()
    {
        points.On(ClaimsFaultPoints.GrantBeforeInsert.Name, () => gate.Serving = false);
        rows.Inserts.Enqueue(First);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.NotServing>(result);
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_takeover_across_fence_start_takes_nothing_over()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: false));
        points.On(ClaimsFaultPoints.GrantBeforeTakeover.Name, () => gate.Serving = false);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.NotServing>(result);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "takeover");
    }

    [Fact]
    public async Task A_conflict_with_a_valid_claim_is_lost_to_its_holder_and_waits_for_nobody()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: true));

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(new ClaimResult.Lost("r2"), result);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "takeover");
    }

    [Fact]
    public async Task A_conflict_with_a_claim_that_is_not_valid_is_taken_over_under_a_new_token()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: false));
        rows.Takeovers.Enqueue(Second);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(new ClaimResult.Won(Second), result);
        Assert.Contains(rows.Calls, c => c.Statement == "takeover" && c.Key == "k");
        Assert.Equal(First, rows.TakeoverExpected);
    }

    [Fact]
    public async Task A_takeover_that_matched_no_row_answers_lost_to_the_holder_it_read()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: false));
        rows.Takeovers.Enqueue(null);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(new ClaimResult.Lost("r2"), result);
    }

    [Fact]
    public async Task A_row_that_is_gone_after_the_conflict_makes_the_grant_insert_again()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(null);
        rows.Inserts.Enqueue(First);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(new ClaimResult.Won(First), result);
        Assert.Equal(["insert", "read", "insert"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task The_serving_check_runs_before_every_statement_that_writes()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: false));
        rows.Takeovers.Enqueue(Second);

        await Claims().ClaimAsync("k", Ttl);

        Assert.Equal(2, gate.Checks);
    }

    [Fact]
    public async Task I33_a_grant_whose_commit_the_librarys_own_bound_cancelled_is_not_won_although_the_insert_returned_its_token()
    {
        rows.OnInsert = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Inserts.Enqueue(First);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.Unknown>(result);
    }

    [Fact]
    public async Task I33_a_takeover_whose_commit_the_librarys_own_bound_cancelled_is_not_won()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new ClaimRead("r2", 9, First, Valid: false));
        rows.OnTakeover = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Takeovers.Enqueue(Second);

        var result = await Claims().ClaimAsync("k", Ttl);

        Assert.IsType<ClaimResult.Unknown>(result);
    }

    [Fact]
    public async Task I33_a_grant_the_caller_cancelled_before_it_returned_throws_instead_of_answering_won()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnInsert = () => cancel.Cancel();
        rows.Inserts.Enqueue(First);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Claims().ClaimAsync("k", Ttl, cancel.Token));
    }

    [Fact]
    public async Task A_release_inside_an_open_claim_fenced_scope_of_this_process_is_refused_and_writes_nothing()
    {
        using var scope = scopes.Open("k");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Claims().ReleaseAsync("k", First));

        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_release_of_another_key_than_the_open_scopes_goes_through()
    {
        using var scope = scopes.Open("other");
        rows.Releases.Enqueue(true);

        Assert.True(await Claims().ReleaseAsync("k", First));
        Assert.Equal([("release", "k", "", 0L)], rows.Calls);
    }

    [Fact]
    public async Task A_release_after_the_scope_closed_goes_through()
    {
        scopes.Open("k").Dispose();
        rows.Releases.Enqueue(false);

        Assert.False(await Claims().ReleaseAsync("k", First));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_claim_needs_a_positive_time_to_live(int seconds)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Claims().ClaimAsync("k", TimeSpan.FromSeconds(seconds)));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_plain_claim_cannot_use_the_once_key_space_to_grant_release_or_fence()
    {
        var key = OnceKeys.Store("k");

        await Assert.ThrowsAsync<ArgumentException>(() => Claims().ClaimAsync(key, Ttl));
        await Assert.ThrowsAsync<ArgumentException>(() => Claims().ReleaseAsync(key, First));
        await Assert.ThrowsAsync<ArgumentException>(
            () => Claims().BeginFencedTransactionAsync(key, First, null!, System.Data.IsolationLevel.ReadCommitted, CancellationToken.None));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_plain_claim_cannot_use_the_single_flight_key_space_to_grant_release_or_fence()
    {
        var key = FlightKeys.Store("k");

        await Assert.ThrowsAsync<ArgumentException>(() => Claims().ClaimAsync(key, Ttl));
        await Assert.ThrowsAsync<ArgumentException>(() => Claims().ReleaseAsync(key, First));
        await Assert.ThrowsAsync<ArgumentException>(
            () => Claims().BeginFencedTransactionAsync(key, First, null!, System.Data.IsolationLevel.ReadCommitted, CancellationToken.None));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_claim_needs_a_key()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Claims().ClaimAsync(null!, Ttl));
    }

    private sealed class SwitchGate : IClaimGate
    {
        public bool Serving { get; set; } = true;

        public int Checks { get; private set; }

        public long? OtherTimelineEpoch { get; private set; }

        public ClaimSeat? CheckServing()
        {
            Checks++;
            return Serving ? new ClaimSeat("r1", 4, 100) : null;
        }

        public void OnOtherTimeline(long epoch) => OtherTimelineEpoch = epoch;
    }

    private sealed class HookPoints : IFaultPoints
    {
        private readonly Dictionary<string, Action> hooks = [];

        public void On(string point, Action action) => hooks[point] = action;

        public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
        {
            if (hooks.TryGetValue(point, out var action))
            {
                action();
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class ScriptedRows : IClaimRows
    {
        public Queue<Guid?> Inserts { get; } = new();

        public Queue<ClaimRead?> Reads { get; } = new();

        public Queue<Guid?> Takeovers { get; } = new();

        public Queue<bool> Releases { get; } = new();

        public List<(string Statement, string Key, string Replica, long Epoch)> Calls { get; } = [];

        public Action? OnInsert { get; set; }

        public Action? OnTakeover { get; set; }

        public Guid? TakeoverExpected { get; private set; }

        public Task<Guid?> InsertAsync(string key, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("insert", key, replica, epoch));
            OnInsert?.Invoke();
            return Task.FromResult(Inserts.Dequeue());
        }

        public Task<ClaimRead?> ReadAsync(string key, CancellationToken cancellationToken)
        {
            Calls.Add(("read", key, string.Empty, 0));
            return Task.FromResult(Reads.Dequeue());
        }

        public Task<Guid?> TakeoverAsync(string key, Guid expected, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("takeover", key, replica, epoch));
            TakeoverExpected = expected;
            OnTakeover?.Invoke();
            return Task.FromResult(Takeovers.Dequeue());
        }

        public Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken)
        {
            Calls.Add(("release", key, string.Empty, 0));
            return Task.FromResult(Releases.Dequeue());
        }
    }
}
