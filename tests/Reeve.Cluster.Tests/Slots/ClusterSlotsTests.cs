using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Slots;

namespace Unit.Slots;

public sealed class ClusterSlotsTests
{
    private static readonly Guid First = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid Second = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider time = new();
    private readonly ScriptedRows rows = new();
    private readonly SwitchGate gate = new();
    private readonly HookPoints points = new();
    private readonly SlotScopes scopes = new();
    private readonly Observer observer = new();

    private ClusterSlots Slots() =>
        new(
            rows, gate, new SlotRegistry(time), scopes, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, points,
            NullLogger<ClusterSlots>.Instance, observer);

    [Fact]
    public async Task A_grant_answers_the_token_the_statement_drew_and_hands_the_seat_and_the_limit_to_it()
    {
        rows.Acquires.Enqueue(First);

        var result = await Slots().AcquireAsync("k", "i", 3, Ttl);

        Assert.Equal(new SlotAcquireResult.Acquired(First), result);
        Assert.Equal([("acquire", "k", "i", "r1", 4L, 100L, 3)], rows.Calls);
        Assert.Equal([result], observer.Acquires);
    }

    [Fact]
    public async Task A_key_that_is_full_answers_full()
    {
        rows.Acquires.Enqueue(null);

        Assert.IsType<SlotAcquireResult.Full>(await Slots().AcquireAsync("k", "i", 1, Ttl));
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_acquires_nothing_and_touches_no_row()
    {
        gate.Serving = false;

        Assert.IsType<SlotAcquireResult.NotServing>(await Slots().AcquireAsync("k", "i", 1, Ttl));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_acquire_across_fence_start_acquires_nothing()
    {
        points.On(SlotsFaultPoints.AcquireBeforeStatement.Name, () => gate.Serving = false);
        rows.Acquires.Enqueue(First);

        Assert.IsType<SlotAcquireResult.NotServing>(await Slots().AcquireAsync("k", "i", 1, Ttl));
        Assert.Empty(rows.Calls);
        Assert.Equal(1, gate.ServingChecks);
    }

    [Fact]
    public async Task The_statements_hold_between_the_delete_and_the_count_runs_the_fault_point()
    {
        var reached = 0;
        points.On(SlotsFaultPoints.AcquireAfterDelete.Name, () => reached++);
        rows.OnAcquire = async afterDelete => await afterDelete(CancellationToken.None);
        rows.Acquires.Enqueue(First);

        await Slots().AcquireAsync("k", "i", 1, Ttl);

        Assert.Equal(1, reached);
    }

    [Fact]
    public async Task I33_an_acquire_whose_commit_the_librarys_own_bound_cancelled_is_unknown_although_the_statement_returned_a_token()
    {
        rows.OnAcquireSync = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Acquires.Enqueue(First);
        var slots = Slots();

        var result = await slots.AcquireAsync("k", "i", 1, Ttl);

        Assert.IsType<SlotAcquireResult.Unknown>(result);
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Single(rows.Calls);
    }

    [Fact]
    public async Task I33_an_acquire_the_caller_cancelled_before_it_returned_throws_instead_of_answering_acquired()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnAcquireSync = () => cancel.Cancel();
        rows.Acquires.Enqueue(First);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Slots().AcquireAsync("k", "i", 1, Ttl, cancel.Token));
    }

    [Fact]
    public async Task I32_an_acquire_on_another_timeline_answers_not_serving_and_starts_the_fence()
    {
        rows.AcquireFailure = new PostgresException("timeline_changed", "ERROR", "ERROR", "RC002");

        Assert.IsType<SlotAcquireResult.NotServing>(await Slots().AcquireAsync("k", "i", 1, Ttl));
        Assert.Equal([4L], gate.OtherTimelines);
    }

    [Fact]
    public async Task Any_other_database_error_of_an_acquire_is_thrown()
    {
        rows.AcquireFailure = new PostgresException("serialization", "ERROR", "ERROR", "40001");

        await Assert.ThrowsAsync<PostgresException>(() => Slots().AcquireAsync("k", "i", 1, Ttl));
        Assert.Empty(gate.OtherTimelines);
    }

    [Fact]
    public async Task An_acquire_on_a_key_with_an_open_local_fenced_scope_of_any_of_its_slots_is_refused()
    {
        using var scope = scopes.Open("k", "other");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Slots().AcquireAsync("k", "i", 1, Ttl));
        Assert.Empty(rows.Calls);
    }

    [Theory]
    [InlineData("not serving")]
    [InlineData("full")]
    [InlineData("unknown")]
    public async Task An_acquire_that_answers_anything_but_acquired_leaves_the_existing_entry_of_the_slot_alone(string answer)
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        switch (answer)
        {
            case "not serving":
                gate.Serving = false;
                break;
            case "full":
                rows.Acquires.Enqueue(null);
                break;
            default:
                rows.OnAcquireSync = () => time.Advance(TimeSpan.FromSeconds(10));
                rows.Acquires.Enqueue(Second);
                break;
        }

        var result = await slots.AcquireAsync("k", "i", 1, Ttl);

        Assert.False(result.IsAcquired);
        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", Second, Ttl));
    }

    [Fact]
    public async Task A_fenced_begin_validates_its_connection_before_the_holder_check_that_can_start_a_fence()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => slots.BeginFencedTransactionAsync("k", "i", First, null!, System.Data.IsolationLevel.ReadCommitted, CancellationToken.None));
        Assert.Empty(gate.HolderChecks);
    }

    [Fact]
    public async Task A_renewal_runs_the_holder_check_and_hands_it_the_epoch_the_slot_was_granted_in()
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        var result = await slots.RenewAsync("k", "i", First, Ttl);

        Assert.Equal(SlotRenewResult.Renewed, result);
        Assert.Equal([4L], gate.HolderChecks);
        Assert.Equal(("renew", "k", "i", string.Empty, 0L, 100L, 0), rows.Calls[1]);
        Assert.Equal([SlotRenewResult.Renewed], observer.Renewals);
    }

    [Fact]
    public async Task A_draining_replica_renews_although_it_acquires_nothing()
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        gate.Serving = false;

        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.IsType<SlotAcquireResult.NotServing>(await slots.AcquireAsync("k", "i", 1, Ttl));
    }

    [Fact]
    public async Task A_token_that_is_not_in_the_registry_is_ended_and_no_statement_is_sent()
    {
        var result = await Slots().RenewAsync("k", "i", Second, Ttl);

        Assert.Equal(SlotRenewResult.Ended, result);
        Assert.Empty(rows.Calls);
        Assert.Equal([SlotRenewResult.Ended], observer.Renewals);
    }

    [Fact]
    public async Task A_slot_whose_local_deadline_passed_is_ended_without_a_statement()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        time.Advance(Ttl);

        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Single(rows.Calls);
    }

    [Fact]
    public async Task A_refused_holder_check_answers_not_serving_sends_nothing_and_ends_the_tenure()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        gate.Holder = false;

        Assert.Equal(SlotRenewResult.NotServing, await slots.RenewAsync("k", "i", First, Ttl));
        gate.Holder = true;
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Single(rows.Calls);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_renewal_across_fence_start_renews_nothing()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        points.On(SlotsFaultPoints.RenewBeforeStatement.Name, () => gate.Holder = false);

        Assert.Equal(SlotRenewResult.NotServing, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Single(rows.Calls);
    }

    [Fact]
    public async Task A_renewal_that_updated_no_row_is_ended_and_the_entry_leaves()
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(false);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Equal(2, rows.Calls.Count);
    }

    [Fact]
    public async Task A_renewal_moves_the_local_deadline_so_a_renewed_slot_outlives_its_first_time_to_live()
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(true);
        rows.Renewals.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        time.Advance(TimeSpan.FromSeconds(25));
        await slots.RenewAsync("k", "i", First, Ttl);
        time.Advance(TimeSpan.FromSeconds(25));

        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", First, Ttl));
    }

    [Fact]
    public async Task I33_a_renewal_the_librarys_bound_cancelled_is_unknown_and_keeps_the_entry()
    {
        rows.Acquires.Enqueue(First);
        rows.Renewals.Enqueue(true);
        rows.Renewals.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        rows.OnRenewSync = () => time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(SlotRenewResult.Unknown, await slots.RenewAsync("k", "i", First, Ttl));

        rows.OnRenewSync = null;
        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", First, Ttl));
    }

    [Fact]
    public async Task I32_a_renewal_on_another_timeline_answers_not_serving_and_starts_the_fence()
    {
        rows.Acquires.Enqueue(First);
        rows.RenewFailure = new PostgresException("timeline_changed", "ERROR", "ERROR", "RC002");
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        Assert.Equal(SlotRenewResult.NotServing, await slots.RenewAsync("k", "i", First, Ttl));
        Assert.Equal([4L], gate.OtherTimelines);
    }

    [Fact]
    public async Task A_release_runs_in_every_phase_with_no_serving_or_holder_check_and_ends_the_entry()
    {
        rows.Acquires.Enqueue(First);
        rows.Releases.Enqueue(true);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        gate.Serving = false;
        gate.Holder = false;

        Assert.True(await slots.ReleaseAsync("k", "i", First));

        Assert.Equal(("release", "k", "i", string.Empty, 0L, 0L, 0), rows.Calls[1]);
        Assert.Equal(First, rows.ReleasedToken);
        Assert.Equal(1, gate.ServingChecks);
        Assert.Empty(gate.HolderChecks);
        gate.Holder = true;
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
    }

    [Fact]
    public async Task A_release_without_a_token_asks_for_the_ids_slot_whatever_its_token()
    {
        rows.Releases.Enqueue(true);

        Assert.True(await Slots().ReleaseAsync("k", "i", null));
        Assert.Null(rows.ReleasedToken);
    }

    [Fact]
    public async Task A_late_release_of_an_earlier_tenure_does_not_end_the_current_tenures_entry()
    {
        rows.Acquires.Enqueue(Second);
        rows.Renewals.Enqueue(true);
        rows.Releases.Enqueue(false);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        Assert.False(await slots.ReleaseAsync("k", "i", First));

        Assert.Equal(SlotRenewResult.Renewed, await slots.RenewAsync("k", "i", Second, Ttl));
    }

    [Fact]
    public async Task A_release_of_a_slot_inside_an_open_fenced_scope_of_this_process_is_refused_and_a_neighbour_is_not()
    {
        using var scope = scopes.Open("k", "i");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Slots().ReleaseAsync("k", "i", First));
        Assert.Empty(rows.Calls);

        rows.Releases.Enqueue(false);
        Assert.False(await Slots().ReleaseAsync("k", "j", First));
    }

    [Fact]
    public async Task I33_a_release_the_librarys_bound_cancelled_throws_commit_outcome_unknown_and_still_ends_the_entry()
    {
        rows.Acquires.Enqueue(First);
        rows.Releases.Enqueue(true);
        rows.OnReleaseSync = () => time.Advance(TimeSpan.FromSeconds(30));
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => slots.ReleaseAsync("k", "i", First));

        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", First, Ttl));
    }

    [Theory]
    [InlineData("", "i", 1, 30)]
    [InlineData("k", "", 1, 30)]
    [InlineData("k", "i", 0, 30)]
    [InlineData("k", "i", -1, 30)]
    [InlineData("k", "i", 1, 3)]
    [InlineData("k", "i", 1, 0)]
    public async Task Arguments_are_refused_before_any_io(string key, string id, int limit, int seconds)
    {
        var slots = Slots();
        var ttl = TimeSpan.FromSeconds(seconds);

        var acquire = await Record.ExceptionAsync(() => slots.AcquireAsync(key, id, limit, ttl));

        Assert.IsAssignableFrom<ArgumentException>(acquire);
        Assert.Empty(rows.Calls);
        Assert.Equal(0, gate.ServingChecks);
    }

    [Theory]
    [InlineData("", "i", 30)]
    [InlineData("k", "", 30)]
    [InlineData("k", "i", 3)]
    public async Task A_renewal_refuses_its_arguments_before_any_io(string key, string id, int seconds)
    {
        var renew = await Record.ExceptionAsync(() => Slots().RenewAsync(key, id, First, TimeSpan.FromSeconds(seconds)));

        Assert.IsAssignableFrom<ArgumentException>(renew);
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task Null_keys_and_ids_are_refused()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Slots().AcquireAsync(null!, "i", 1, Ttl));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Slots().AcquireAsync("k", null!, 1, Ttl));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Slots().ReleaseAsync("k", null!, First));
    }

    [Fact]
    public async Task A_holder_read_answers_the_rows_of_the_statement_in_every_phase_with_no_serving_or_holder_check()
    {
        var holders = new[]
        {
            new SlotHolder { Id = "i", ReplicaId = "r2", Epoch = 7 },
            new SlotHolder { Id = "j", ReplicaId = "r3", Epoch = 9 },
        };
        rows.Holders.Enqueue(holders);
        gate.Serving = false;
        gate.Holder = false;

        var read = await Slots().ReadHoldersAsync("k");

        Assert.Equal(holders, read);
        Assert.Equal([("holders", "k", string.Empty, string.Empty, 0L, 0L, 0)], rows.Calls);
        Assert.Equal(0, gate.ServingChecks);
        Assert.Empty(gate.HolderChecks);
    }

    [Fact]
    public async Task A_holder_read_answers_the_database_and_not_this_processs_registry()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        rows.Holders.Enqueue([]);

        Assert.Empty(await slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task A_holder_read_hands_the_callers_token_to_the_statement()
    {
        using var cancel = new CancellationTokenSource();
        rows.Holders.Enqueue([]);

        await Slots().ReadHoldersAsync("k", cancel.Token);

        Assert.Equal(cancel.Token, rows.HoldersToken);
    }

    [Fact]
    public async Task A_holder_read_refuses_an_empty_or_null_key_before_any_io()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => Slots().ReadHoldersAsync(string.Empty));
        await Assert.ThrowsAsync<ArgumentNullException>(() => Slots().ReadHoldersAsync(null!));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task The_holder_read_of_a_foreign_slots_implementation_is_not_supported()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => ((IClusterSlots)new ForeignSlots()).ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task The_fenced_begin_of_a_foreign_slots_implementation_is_refused()
    {
        await Assert.ThrowsAsync<ArgumentException>(
            () => new ForeignSlots().BeginSlotFencedTransactionAsync("k", "i", First, null!));
    }

    [Fact]
    public async Task A_fenced_begin_with_a_token_that_is_not_in_the_registry_is_refused_before_any_io()
    {
        await Assert.ThrowsAsync<SlotLostException>(
            () => Slots().BeginFencedTransactionAsync("k", "i", Second, new NpgsqlConnection(), System.Data.IsolationLevel.ReadCommitted, CancellationToken.None));
        Assert.Empty(rows.Calls);
    }

    [Fact]
    public async Task A_fenced_begin_whose_holder_check_refuses_is_refused_before_any_io()
    {
        rows.Acquires.Enqueue(First);
        var slots = Slots();
        await slots.AcquireAsync("k", "i", 1, Ttl);
        gate.Holder = false;

        await Assert.ThrowsAsync<SlotLostException>(
            () => slots.BeginFencedTransactionAsync("k", "i", First, new NpgsqlConnection(), System.Data.IsolationLevel.ReadCommitted, CancellationToken.None));
        Assert.Equal([4L], gate.HolderChecks);
    }

    private sealed class ForeignSlots : IClusterSlots
    {
        public Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit, TimeSpan ttl, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    private sealed class SwitchGate : ISlotGate
    {
        public bool Serving { get; set; } = true;

        public bool Holder { get; set; } = true;

        public int ServingChecks { get; private set; }

        public List<long> HolderChecks { get; } = [];

        public List<long> OtherTimelines { get; } = [];

        public ClaimSeat? CheckServing()
        {
            ServingChecks++;
            return Serving ? new ClaimSeat("r1", 4, 100) : null;
        }

        public ClaimSeat? CheckHolder(long grantedEpoch)
        {
            HolderChecks.Add(grantedEpoch);
            return Holder ? new ClaimSeat("r1", grantedEpoch, 100) : null;
        }

        public void OnOtherTimeline(long epoch) => OtherTimelines.Add(epoch);
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

    private sealed class Observer : ISlotObserver
    {
        public List<SlotAcquireResult> Acquires { get; } = [];

        public List<SlotRenewResult> Renewals { get; } = [];

        public void Acquired(SlotAcquireResult result) => Acquires.Add(result);

        public void Renewed(SlotRenewResult result) => Renewals.Add(result);
    }

    private sealed class ScriptedRows : ISlotRows
    {
        public Queue<Guid?> Acquires { get; } = new();

        public Queue<bool> Renewals { get; } = new();

        public Queue<bool> Releases { get; } = new();

        public Queue<IReadOnlyList<SlotHolder>> Holders { get; } = new();

        public CancellationToken HoldersToken { get; private set; }

        public List<(string Statement, string Key, string Id, string Replica, long Epoch, long Timeline, int Limit)> Calls { get; } = [];

        public Func<Func<CancellationToken, ValueTask>, Task>? OnAcquire { get; set; }

        public Action? OnAcquireSync { get; set; }

        public Action? OnRenewSync { get; set; }

        public Action? OnReleaseSync { get; set; }

        public Exception? AcquireFailure { get; set; }

        public Exception? RenewFailure { get; set; }

        public Guid? ReleasedToken { get; private set; }

        public async Task<Guid?> AcquireAsync(
            string key, string id, string replica, long epoch, long timeline, int limit, TimeSpan ttl, Func<CancellationToken, ValueTask> afterDelete,
            CancellationToken cancellationToken)
        {
            Calls.Add(("acquire", key, id, replica, epoch, timeline, limit));
            if (AcquireFailure is not null)
            {
                throw AcquireFailure;
            }

            if (OnAcquire is not null)
            {
                await OnAcquire(afterDelete);
            }

            OnAcquireSync?.Invoke();
            return Acquires.Dequeue();
        }

        public Task<bool> RenewAsync(string key, string id, Guid token, long timeline, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Calls.Add(("renew", key, id, string.Empty, 0L, timeline, 0));
            if (RenewFailure is not null)
            {
                throw RenewFailure;
            }

            OnRenewSync?.Invoke();
            return Task.FromResult(Renewals.Dequeue());
        }

        public Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken)
        {
            Calls.Add(("release", key, id, string.Empty, 0, 0, 0));
            ReleasedToken = token;
            OnReleaseSync?.Invoke();
            return Task.FromResult(Releases.Dequeue());
        }

        public Task<IReadOnlyList<SlotHolder>> ReadHoldersAsync(string key, CancellationToken cancellationToken)
        {
            Calls.Add(("holders", key, string.Empty, string.Empty, 0, 0, 0));
            HoldersToken = cancellationToken;
            return Task.FromResult(Holders.Dequeue());
        }
    }
}
