using System.Data.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClusterOnceTests
{
    private static readonly Guid First = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly IServiceScopeFactory Scopes = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedOnceRows rows = new();
    private readonly SwitchGate gate = new();
    private readonly HookPoints points = new();
    private readonly List<(string Key, CancellationToken Token)> ran = [];
    private Func<string, CancellationToken, Task>? effectBody;

    private ClusterOnce Once()
    {
        var effects = new OnceEffects([new OnceEffectRegistration("send", (_, key, token) =>
        {
            ran.Add((key, token));
            return effectBody is null ? Task.CompletedTask : effectBody(key, token);
        })]);
        return new ClusterOnce(
            rows, gate, effects, Scopes, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points, NullLogger<ClusterOnce>.Instance);
    }

    [Fact]
    public async Task A_free_key_runs_the_effect_under_the_granted_token_and_completes_the_record()
    {
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(true);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.Completed(), result);
        Assert.Equal(["k"], ran.Select(r => r.Key));
        Assert.Equal(["insert", "complete"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(("insert", OnceKeys.Store("k"), "r1", 4L), (rows.Calls[0].Statement, rows.Calls[0].Key, rows.Calls[0].Replica, rows.Calls[0].Epoch));
        Assert.Equal((OnceMode.AtMostOnce, "send", Ttl), rows.Inserted);
        Assert.Equal(First, rows.Completed);
    }

    [Fact]
    public async Task The_record_is_stored_under_the_once_key_space_and_the_effect_gets_the_applications_key()
    {
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(true);

        await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.StartsWith(OnceKeys.Prefix, rows.Calls[0].Key, StringComparison.Ordinal);
        Assert.Equal(OnceKeys.Store("k"), rows.Calls[1].Key);
        Assert.Equal("k", Assert.Single(ran).Key);
    }

    [Fact]
    public async Task The_effect_receives_the_callers_token()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(true);

        await Once().RunAsync("k", "send", OnceMode.AtLeastOnce, Ttl, cancel.Token);

        Assert.Equal(cancel.Token, Assert.Single(ran).Token);
        Assert.Equal((OnceMode.AtLeastOnce, "send", Ttl), rows.Inserted);
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_starts_nothing_and_touches_no_row()
    {
        gate.Serving = false;

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.IsType<OnceResult.NotServing>(result);
        Assert.Empty(rows.Calls);
        Assert.Empty(ran);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_grants_nothing()
    {
        points.On(ClaimsFaultPoints.OnceBeforeInsert.Name, () => gate.Serving = false);
        rows.Inserts.Enqueue(First);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.IsType<OnceResult.NotServing>(result);
        Assert.Empty(rows.Calls);
        Assert.Empty(ran);
    }

    [Fact]
    public async Task An_effect_that_is_not_registered_is_refused_before_any_row_is_touched()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => Once().RunAsync("k", "unknown", OnceMode.AtMostOnce, Ttl));

        Assert.Empty(rows.Calls);
        Assert.Empty(gate.CheckedAt);
    }

    [Fact]
    public async Task A_run_needs_a_key_a_positive_time_to_live_and_a_defined_mode()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Once().RunAsync(null!, "send", OnceMode.AtMostOnce, Ttl));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Once().RunAsync("k", "send", OnceMode.AtMostOnce, TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Once().RunAsync("k", "send", (OnceMode)7, Ttl));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_conflict_with_a_complete_run_is_already_completed_and_runs_nothing()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new OnceRead("r2", 9, First, Valid: false, Completed: true));

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.AlreadyCompleted(), result);
        Assert.Empty(ran);
    }

    [Fact]
    public async Task A_conflict_with_a_valid_run_is_running_under_its_executor_and_runs_nothing()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new OnceRead("r2", 9, First, Valid: true, Completed: false));

        var result = await Once().RunAsync("k", "send", OnceMode.AtLeastOnce, Ttl);

        Assert.Equal(new OnceResult.Running("r2"), result);
        Assert.Empty(ran);
    }

    [Theory]
    [InlineData(OnceMode.AtMostOnce)]
    [InlineData(OnceMode.AtLeastOnce)]
    public async Task A_conflict_with_a_run_that_is_not_valid_and_not_complete_is_in_doubt_and_is_not_repeated_by_the_caller(OnceMode mode)
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(new OnceRead("r2", 9, First, Valid: false, Completed: false));

        var result = await Once().RunAsync("k", "send", mode, Ttl);

        Assert.Equal(new OnceResult.InDoubt(), result);
        Assert.Empty(ran);
        Assert.Equal(["insert", "read"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task A_row_that_is_gone_after_the_conflict_makes_the_run_insert_again_and_check_serving_again()
    {
        rows.Inserts.Enqueue(null);
        rows.Reads.Enqueue(null);
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(true);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.Completed(), result);
        Assert.Equal(["insert", "read", "insert", "complete"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(2, gate.CheckedAt.Count);
    }

    [Fact]
    public async Task I33_a_grant_whose_commit_the_librarys_own_bound_cancelled_is_unknown_and_the_effect_is_not_started()
    {
        rows.OnInsert = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Inserts.Enqueue(First);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.Unknown(), result);
        Assert.Empty(ran);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "complete");
    }

    [Fact]
    public async Task I33_a_plain_read_the_librarys_own_bound_cancelled_is_in_doubt_and_runs_nothing()
    {
        rows.Inserts.Enqueue(null);
        rows.OnRead = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Reads.Enqueue(new OnceRead("r2", 9, First, Valid: true, Completed: false));

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.InDoubt(), result);
        Assert.Empty(ran);
    }

    [Fact]
    public async Task I33_a_grant_the_caller_cancelled_before_it_returned_throws_and_the_effect_is_not_started()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnInsert = () => cancel.Cancel();
        rows.Inserts.Enqueue(First);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl, cancel.Token));

        Assert.Empty(ran);
    }

    [Fact]
    public async Task A_database_failure_at_the_grant_is_thrown_and_the_effect_is_not_started()
    {
        rows.InsertFailure = new TestDbException("the database is down");

        await Assert.ThrowsAsync<TestDbException>(() => Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl));

        Assert.Empty(ran);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "complete");
    }

    [Fact]
    public async Task An_effect_that_throws_propagates_and_writes_no_completion()
    {
        effectBody = (_, _) => throw new InvalidOperationException("the effect failed");
        rows.Inserts.Enqueue(First);

        await Assert.ThrowsAsync<InvalidOperationException>(() => Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl));

        Assert.Equal(["insert"], rows.Calls.Select(c => c.Statement));
    }

    [Fact]
    public async Task A_completion_that_matched_no_row_because_the_record_no_longer_held_is_in_doubt()
    {
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(false);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.CompletionUnknown(), result);
        Assert.Single(ran);
    }

    [Fact]
    public async Task I33_a_completion_whose_commit_the_librarys_own_bound_cancelled_is_in_doubt_although_it_updated_the_row()
    {
        rows.Inserts.Enqueue(First);
        rows.OnComplete = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Completions.Enqueue(true);

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.CompletionUnknown(), result);
    }

    [Fact]
    public async Task A_database_failure_at_the_completion_is_in_doubt_not_thrown_and_not_completed()
    {
        rows.Inserts.Enqueue(First);
        rows.CompleteFailure = new InvalidOperationException("the connection dropped");

        var result = await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(new OnceResult.CompletionUnknown(), result);
        Assert.Single(ran);
    }

    [Fact]
    public async Task A_completion_the_caller_cancelled_before_it_returned_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.Inserts.Enqueue(First);
        rows.OnComplete = () => cancel.Cancel();
        rows.Completions.Enqueue(true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl, cancel.Token));
    }

    [Fact]
    public async Task The_fault_points_between_the_grant_and_the_effect_and_between_the_effect_and_the_completion_are_reached_in_order()
    {
        var order = new List<string>();
        points.On(ClaimsFaultPoints.OnceAfterGrant.Name, () => order.Add("after-grant"));
        points.On(ClaimsFaultPoints.OnceBeforeComplete.Name, () => order.Add("before-complete"));
        effectBody = (_, _) =>
        {
            order.Add("effect");
            return Task.CompletedTask;
        };
        rows.Inserts.Enqueue(First);
        rows.Completions.Enqueue(true);

        await Once().RunAsync("k", "send", OnceMode.AtMostOnce, Ttl);

        Assert.Equal(["after-grant", "effect", "before-complete"], order);
    }

    private sealed class SwitchGate : IClaimGate
    {
        public bool Serving { get; set; } = true;

        public List<int> CheckedAt { get; } = [];

        public ClaimSeat? CheckServing()
        {
            CheckedAt.Add(CheckedAt.Count);
            return Serving ? new ClaimSeat("r1", 4, 100) : null;
        }

        public void OnOtherTimeline(long epoch)
        {
        }
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

    private sealed class ScriptedOnceRows : IOnceRows
    {
        public Queue<Guid?> Inserts { get; } = new();

        public Queue<OnceRead?> Reads { get; } = new();

        public Queue<bool> Completions { get; } = new();

        public List<(string Statement, string Key, string Replica, long Epoch)> Calls { get; } = [];

        public Action? OnInsert { get; set; }

        public Action? OnRead { get; set; }

        public Action? OnComplete { get; set; }

        public Exception? InsertFailure { get; set; }

        public Exception? CompleteFailure { get; set; }

        public (OnceMode Mode, string Effect, TimeSpan Ttl)? Inserted { get; private set; }

        public Guid? Completed { get; private set; }

        public Task<Guid?> InsertAsync(string key, OnceMode mode, string effect, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("insert", key, replica, epoch));
            Inserted = (mode, effect, ttl);
            OnInsert?.Invoke();
            return InsertFailure is null ? Task.FromResult(Inserts.Dequeue()) : Task.FromException<Guid?>(InsertFailure);
        }

        public Task<OnceRead?> ReadAsync(string key, CancellationToken cancellationToken)
        {
            Calls.Add(("read", key, string.Empty, 0));
            OnRead?.Invoke();
            return Task.FromResult(Reads.Dequeue());
        }

        public Task<bool> CompleteAsync(string key, Guid token, CancellationToken cancellationToken)
        {
            Calls.Add(("complete", key, string.Empty, 0));
            Completed = token;
            OnComplete?.Invoke();
            return CompleteFailure is null ? Task.FromResult(Completions.Dequeue()) : Task.FromException<bool>(CompleteFailure);
        }

        public Task<IReadOnlyList<OnceCandidate>> ListRetakeableAsync(IReadOnlyCollection<string> effects, int limit, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("A caller's run never lists.");

        public Task<Guid?> RetakeAsync(string key, Guid expected, string replica, long epoch, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("A caller's run never re-takes.");
    }

    private sealed class TestDbException(string message) : DbException(message)
    {
    }
}
