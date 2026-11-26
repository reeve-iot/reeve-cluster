using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class OnceSweeperTests
{
    private static readonly Guid Old1 = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid Old2 = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static readonly Guid New1 = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid New2 = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly IServiceScopeFactory Scopes = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedRetakeRows rows = new();
    private readonly SwitchGate gate = new();
    private readonly HookPoints points = new();
    private readonly List<(string Effect, string Key, CancellationToken Token)> ran = [];
    private readonly Dictionary<string, Func<string, CancellationToken, Task>> bodies = [];
    private readonly List<string> registered = ["send", "mail"];

    private OnceSweeper Sweeper(int passLimit = 100)
    {
        var effects = new OnceEffects(registered.Select(name => new OnceEffectRegistration(name, (_, key, token) =>
        {
            ran.Add((name, key, token));
            return bodies.TryGetValue(key, out var body) ? body(key, token) : Task.CompletedTask;
        })));
        return new OnceSweeper(
            rows, gate, effects, Scopes, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points,
            NullLogger<OnceSweeper>.Instance, passLimit);
    }

    [Fact]
    public async Task A_replica_with_no_registered_effect_lists_nothing_and_touches_no_row()
    {
        registered.Clear();

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task The_pass_lists_only_the_effects_registered_here_and_at_most_its_limit()
    {
        await Sweeper(passLimit: 7).PassAsync(CancellationToken.None);

        Assert.Equal(["mail", "send"], rows.ListedEffects!.Order(StringComparer.Ordinal));
        Assert.Equal(7, rows.ListedLimit);
    }

    [Fact]
    public async Task A_candidate_for_an_unregistered_effect_is_not_retaken_or_completed()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "missing"));

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Equal(["list"], rows.Calls.Select(c => c.Statement));
        Assert.Empty(ran);
    }

    [Fact]
    public async Task A_candidate_is_retaken_by_this_replica_and_its_effect_runs_under_the_new_token_then_completes()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Retakes.Enqueue(New1);
        rows.Completions.Enqueue(true);

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal([("send", "k1")], ran.Select(r => (r.Effect, r.Key)));
        Assert.Equal(["list", "retake", "complete"], rows.Calls.Select(c => c.Statement));
        Assert.Equal(("retake", OnceKeys.Store("k1"), "r1", 4L), (rows.Calls[1].Statement, rows.Calls[1].Key, rows.Calls[1].Replica, rows.Calls[1].Epoch));
        Assert.Equal(Old1, rows.RetakeExpected);
        Assert.Equal(New1, rows.Completed);
    }

    [Fact]
    public async Task A_record_outside_the_once_key_space_is_left_alone()
    {
        rows.Candidates.Add(new OnceCandidate("plain", Old1, "send"));

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "retake");
        Assert.Empty(ran);
    }

    [Fact]
    public async Task Each_candidate_runs_the_effect_it_names()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k2"), Old2, "mail"));
        rows.Retakes.Enqueue(New1);
        rows.Retakes.Enqueue(New2);
        rows.Completions.Enqueue(true);
        rows.Completions.Enqueue(true);

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(2, count);
        Assert.Equal([("send", "k1"), ("mail", "k2")], ran.Select(r => (r.Effect, r.Key)));
    }

    [Fact]
    public async Task The_effect_runs_under_the_passs_token()
    {
        using var stop = new CancellationTokenSource();
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Retakes.Enqueue(New1);
        rows.Completions.Enqueue(true);

        await Sweeper().PassAsync(stop.Token);

        Assert.Equal(stop.Token, Assert.Single(ran).Token);
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_retakes_nothing_and_runs_nothing()
    {
        gate.Serving = false;
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "retake");
        Assert.Empty(ran);
    }

    [Fact]
    public async Task I1_a_sweeper_paused_before_a_retake_across_fence_start_retakes_nothing()
    {
        points.On(ClaimsFaultPoints.RetakeBeforeStatement.Name, () => gate.Serving = false);
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k2"), Old2, "send"));

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "retake");
        Assert.Empty(ran);
    }

    [Fact]
    public async Task A_retake_that_matched_no_row_runs_nothing_and_the_pass_goes_on()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k2"), Old2, "send"));
        rows.Retakes.Enqueue(null);
        rows.Retakes.Enqueue(New2);
        rows.Completions.Enqueue(true);

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal(["k2"], ran.Select(r => r.Key));
    }

    [Fact]
    public async Task I33_a_retake_whose_commit_the_librarys_own_bound_cancelled_is_not_run_although_it_wrote_a_token()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.OnRetake = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Retakes.Enqueue(New1);

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Empty(ran);
        Assert.DoesNotContain(rows.Calls, c => c.Statement == "complete");
    }

    [Fact]
    public async Task A_retake_the_stop_token_cancelled_before_it_returned_ends_the_pass_with_a_cancel()
    {
        using var stop = new CancellationTokenSource();
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.OnRetake = () => stop.Cancel();
        rows.Retakes.Enqueue(New1);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Sweeper().PassAsync(stop.Token));

        Assert.Empty(ran);
    }

    [Fact]
    public async Task An_effect_that_throws_leaves_the_record_for_the_next_sweep_and_the_pass_goes_on()
    {
        bodies["k1"] = (_, _) => throw new InvalidOperationException("the effect failed");
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k2"), Old2, "send"));
        rows.Retakes.Enqueue(New1);
        rows.Retakes.Enqueue(New2);
        rows.Completions.Enqueue(true);

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal(["k1", "k2"], ran.Select(r => r.Key));
        Assert.Equal(New2, rows.Completed);
        Assert.Single(rows.Calls, c => c.Statement == "complete");
    }

    [Fact]
    public async Task A_completion_that_matched_no_row_or_failed_does_not_stop_the_pass()
    {
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k2"), Old2, "send"));
        rows.Retakes.Enqueue(New1);
        rows.Retakes.Enqueue(New2);
        rows.Completions.Enqueue(false);
        rows.CompleteFailureFor = OnceKeys.Store("k2");

        var count = await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(2, count);
        Assert.Equal(["k1", "k2"], ran.Select(r => r.Key));
    }

    [Fact]
    public async Task A_database_failure_at_the_listing_is_thrown_to_the_loop_and_nothing_runs()
    {
        rows.ListFailure = new InvalidOperationException("the database is down");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Sweeper().PassAsync(CancellationToken.None));

        Assert.Empty(ran);
    }

    [Fact]
    public async Task The_fault_point_between_a_retake_and_its_effect_is_reached_after_the_commit()
    {
        var order = new List<string>();
        points.On(ClaimsFaultPoints.RetakeAfterCommit.Name, () => order.Add("after-commit"));
        bodies["k1"] = (_, _) =>
        {
            order.Add("effect");
            return Task.CompletedTask;
        };
        rows.Candidates.Add(new OnceCandidate(OnceKeys.Store("k1"), Old1, "send"));
        rows.Retakes.Enqueue(New1);
        rows.Completions.Enqueue(true);

        await Sweeper().PassAsync(CancellationToken.None);

        Assert.Equal(["after-commit", "effect"], order);
    }

    private sealed class SwitchGate : IClaimGate
    {
        public bool Serving { get; set; } = true;

        public ClaimSeat? CheckServing() => Serving ? new ClaimSeat("r1", 4, 100) : null;

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

    private sealed class ScriptedRetakeRows : IOnceRows
    {
        public List<OnceCandidate> Candidates { get; } = [];

        public Queue<Guid?> Retakes { get; } = new();

        public Queue<bool> Completions { get; } = new();

        public List<(string Statement, string Key, string Replica, long Epoch)> Calls { get; } = [];

        public Action? OnRetake { get; set; }

        public Exception? ListFailure { get; set; }

        public string? CompleteFailureFor { get; set; }

        public IReadOnlyCollection<string>? ListedEffects { get; private set; }

        public int ListedLimit { get; private set; }

        public Guid? RetakeExpected { get; private set; }

        public Guid? Completed { get; private set; }

        public Task<IReadOnlyList<OnceCandidate>> ListRetakeableAsync(IReadOnlyCollection<string> effects, int limit, CancellationToken cancellationToken)
        {
            Calls.Add(("list", string.Empty, string.Empty, 0));
            ListedEffects = effects;
            ListedLimit = limit;
            return ListFailure is null
                ? Task.FromResult<IReadOnlyList<OnceCandidate>>(Candidates)
                : Task.FromException<IReadOnlyList<OnceCandidate>>(ListFailure);
        }

        public Task<Guid?> RetakeAsync(string key, Guid expected, string replica, long epoch, CancellationToken cancellationToken)
        {
            Calls.Add(("retake", key, replica, epoch));
            RetakeExpected = expected;
            OnRetake?.Invoke();
            return Task.FromResult(Retakes.Dequeue());
        }

        public Task<bool> CompleteAsync(string key, Guid token, CancellationToken cancellationToken)
        {
            Calls.Add(("complete", key, string.Empty, 0));
            Completed = token;
            return key == CompleteFailureFor
                ? Task.FromException<bool>(new InvalidOperationException("the connection dropped"))
                : Task.FromResult(Completions.Dequeue());
        }

        public Task<Guid?> InsertAsync(string key, OnceMode mode, string effect, string replica, long epoch, TimeSpan ttl, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The sweeper never inserts.");

        public Task<OnceRead?> ReadAsync(string key, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The sweeper never reads a single record.");
    }
}
