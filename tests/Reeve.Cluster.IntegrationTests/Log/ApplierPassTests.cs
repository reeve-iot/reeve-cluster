using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ApplierPassTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan ReconcileTimeout = TimeSpan.FromMilliseconds(300);

    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public ApplierPassTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var rig in rigs)
        {
            await rig.DisposeAsync();
        }
    }

    private async Task<ApplierRig> RigAsync(string[] types, IFaultPoints? points = null, Action<ClusterOptions>? configure = null, TimeProvider? time = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, types, points, configure, time);
        rigs.Add(rig);
        return rig;
    }

    [Fact]
    public async Task The_first_start_loads_every_type_in_full_from_one_snapshot_writes_the_cursor_and_counts_the_entries()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("b", "k2"), ("other", "k3"));
        var head = await rig.HeadAsync();

        await rig.LoadAsync();

        var generation = rig.Store.Current!;
        Assert.Equal(head.Seq, generation.Head);
        Assert.Equal(head.Token, generation.HeadToken);
        Assert.Equal(head, await rig.RowCursorAsync());
        Assert.Equal(new PublishedPass(head, 3, Full: true), rig.Applier.LastPublished);
        foreach (var type in new[] { "a", "b" })
        {
            var call = Assert.Single(rig[type].Calls);
            Assert.True(call.FullLoad);
            Assert.Null(call.Hints);
            Assert.Null(call.Previous);
            Assert.Equal(1, rig.State(type).Value);
        }

        var actions = rig.Events.All.Select(e => e.Action).Where(a => a is LogActions.PassBegin or LogActions.Publish or LogActions.CursorWrite).ToList();
        Assert.Equal([LogActions.PassBegin, LogActions.Publish, LogActions.CursorWrite], actions);
    }

    [Fact]
    public async Task The_first_start_on_an_empty_log_stands_for_0()
    {
        var rig = await RigAsync(["a"]);

        await rig.LoadAsync();

        Assert.Equal(0, rig.Store.Current!.Head);
        Assert.Null(rig.Store.Current.HeadToken);
        Assert.Equal(Cursor.Empty, await rig.RowCursorAsync());
        Assert.Equal(0, rig.Applier.LastPublished!.Entries);
        Assert.Equal(PassOutcome.NothingNew, await rig.PassAsync());
    }

    [Fact]
    public async Task Writes_made_during_the_first_load_are_applied_by_the_next_pass()
    {
        var probe = new PointProbe();
        probe.Hold(LogFaultPoints.InsideFirstLoad);
        var rig = await RigAsync(["a", "b"], probe);
        var s = await rig.WriteAsync(("a", "k0"));
        await rig.JoinAsync();

        var loading = rig.PassAsync();
        await probe.Arrived.WaitAsync(Wait);
        var during = await rig.WriteAsync(("a", "k1"));
        probe.Release();
        Assert.Equal(PassOutcome.Published, await loading.WaitAsync(Wait));

        Assert.Equal(s, rig.Store.Current!.Head);
        Assert.Equal(1, rig.State("a").Value);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal(during, rig.Store.Current.Head);
        Assert.Equal(2, rig.State("a").Value);
        var second = rig["a"].Calls.Last();
        Assert.False(second.FullLoad);
        Assert.Equal(["k1"], second.Hints!);
        Assert.Single(rig["b"].Calls);
        Assert.Equal(new PublishedPass(await rig.HeadAsync(), 1, Full: false), rig.Applier.LastPublished);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task I6_a_write_of_two_types_committed_during_a_pass_is_in_no_generation_until_the_next_pass_has_both(bool ownSession)
    {
        var rig = await RigAsync(["a", "b"]);
        rig["b"].ReadThroughOwnSession = ownSession;
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "k"), ("b", "k"));
        var both = 0L;
        rig["a"].Before = async (_, _) =>
        {
            rig["a"].Before = null;
            both = await rig.WriteAsync(("a", "k"), ("b", "k"));
        };

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(1, rig.State("a").Value);
        Assert.Equal(1, rig.State("b").Value);
        Assert.True(rig.Store.Current!.Head < both);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal(2, rig.State("a").Value);
        Assert.Equal(2, rig.State("b").Value);
        Assert.Equal(both, rig.Store.Current.Head);
    }

    [Fact]
    public async Task I7_a_reconciler_attempt_that_times_out_and_returns_later_publishes_nothing()
    {
        var time = new FakeTimeProvider();
        var rig = await RigAsync(["a", "b"], configure: o => o.ReconcileTimeout = ReconcileTimeout, time: time);
        await rig.LoadAsync();
        var before = rig.Store.Current!;
        await rig.WriteAsync(("a", "k"));
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig["a"].Before = (_, _) =>
        {
            entered.TrySetResult();
            return hang.Task;
        };

        var stalling = rig.PassAsync();
        await entered.Task.WaitAsync(Wait);
        time.Advance(ReconcileTimeout);
        Assert.Equal(PassOutcome.Stalled, await stalling.WaitAsync(Wait));
        var unreturned = Assert.Single(rig.Store.Unreturned);
        Assert.Equal(AttemptOutcomeKind.TimedOut, unreturned.Why);
        Assert.Same(before, rig.Store.Current);

        rig["a"].Before = null;
        hang.SetResult();
        await unreturned.Attempt.Returned.WaitAsync(Wait);

        Assert.Same(before, rig.Store.Current);
        Assert.Equal(0, rig.Applier.LastPassEntries);
        Assert.Empty(rig.Store.Unreturned);
        Assert.Contains(rig.Events.All, e => e.Action == LogActions.AttemptHang);
        Assert.Contains(rig.Events.All, e => e.Action == LogActions.LateReturn);
        Assert.Single(rig.Events.All, e => e.Action == LogActions.Publish);
        Assert.Equal(before.Head, (await rig.RowCursorAsync()).Seq);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync(withoutHints: true));
        Assert.Null(rig["a"].Calls.Last().Hints);
        Assert.Equal(1, rig.State("a").Value);
    }

    [Theory]
    [InlineData("fails")]
    [InlineData("times-out")]
    public async Task The_entries_the_last_pass_applied_are_0_after_an_attempt_that_stalls_until_a_pass_publishes(string stall)
    {
        var time = new FakeTimeProvider();
        var rig = await RigAsync(["a"], configure: o => o.ReconcileTimeout = ReconcileTimeout, time: time);
        await rig.WriteAsync(("a", "k1"), ("a", "k2"));
        await rig.LoadAsync();
        var loaded = rig.Applier.LastPublished!;
        Assert.Equal((2L, 2L), (loaded.Entries, rig.Applier.LastPassEntries));
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig["a"].Before = stall == "fails"
            ? (_, _) => throw new InvalidOperationException("the test's reconciler fails")
            : (_, _) =>
            {
                entered.TrySetResult();
                return hang.Task;
            };
        await rig.WriteAsync(("a", "k3"));

        var stalling = rig.PassAsync();
        if (stall != "fails")
        {
            await entered.Task.WaitAsync(Wait);
            time.Advance(ReconcileTimeout);
        }

        Assert.Equal(PassOutcome.Stalled, await stalling.WaitAsync(Wait));
        Assert.Equal(0, rig.Applier.LastPassEntries);
        Assert.Same(loaded, rig.Applier.LastPublished);

        rig["a"].Before = null;
        hang.SetResult();
        await rig.WriteAsync(("a", "k4"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync(withoutHints: true));
        Assert.Equal((2L, 2L), (rig.Applier.LastPublished!.Entries, rig.Applier.LastPassEntries));
    }

    [Fact]
    public async Task SIMP1_a_rejoins_load_is_a_full_reload_with_a_cursor_that_is_still_valid_and_the_kept_generation_stays_until_it_publishes()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync();
        await rig.WriteAsync(("b", "k2"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        var kept = rig.Store.Current!;

        rig.Gate.ReportFatalFault("cache corrupt");
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced'");
        rig.Applier.Stop();
        rig.Gate.CompleteFence();
        rig.Gate.Apply(MemberTrigger.RejoinPermitted);
        await rig.JoinAsync();
        Assert.Equal(kept.Head, (await rig.RowCursorAsync()).Seq);
        IGeneration? during = null;
        rig["a"].Before = (_, _) =>
        {
            during = rig.Store.Current;
            return Task.CompletedTask;
        };

        Assert.Equal(LoadOutcome.Loaded, await rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None));

        Assert.Same(kept, during);
        Assert.NotSame(kept, rig.Store.Current);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.True(rig["b"].Calls.Last().FullLoad);
        Assert.Equal(new PublishedPass(await rig.HeadAsync(), 2, Full: true), rig.Applier.LastPublished);
        var begin = rig.Events.All.Last(e => e.Action == LogActions.PassBegin).State!;
        Assert.Equal(true, begin["full"]);
        Assert.Equal(true, begin["reload"]);
    }

    [Fact]
    public async Task I32_a_pass_on_another_timeline_than_the_joins_raises_the_fence_trigger_and_publishes_nothing()
    {
        var rig = await RigAsync(["a"]);
        await rig.WriteAsync(("a", "k"));
        await rig.JoinAsync(joinedTimeline: long.MaxValue);

        var outcome = await rig.Applier.LoadAsync(rig.Epoch, long.MaxValue, CancellationToken.None);

        Assert.Equal(LoadOutcome.Failed, outcome);
        Assert.Null(rig.Store.Current);
        Assert.Empty(rig["a"].Calls);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        Assert.Equal(FenceTrigger.Timeline, rig.Gate.CurrentFence!.Request.Trigger);
        Assert.Equal(Cursor.Empty, await rig.RowCursorAsync());
    }

    [Fact]
    public async Task I5_a_pass_checks_its_own_cursor_not_the_row_a_local_cursor_that_names_no_row_reloads_in_full()
    {
        var rig = await RigAsync(["a", "b"]);
        var survivor = await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("a", "k2"));
        await rig.LoadAsync();

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq > {survivor}");
        await rig.ExecuteAsync($"UPDATE reeve_cluster.member SET applied_seq = seq, applied_token = entry_token FROM reeve_cluster.change_log WHERE seq = {survivor}");
        await rig.WriteAsync(("b", "k3"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.True(rig["b"].Calls.Last().FullLoad);
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.True(rig.Applier.LastPublished!.Full);
        Assert.Equal(await rig.HeadAsync(), await rig.RowCursorAsync());
    }

    [Fact]
    public async Task I5_a_valid_local_cursor_applies_incrementally_whatever_the_row_says()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync();
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET applied_seq = 999, applied_token = gen_random_uuid()");
        await rig.WriteAsync(("b", "k2"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Single(rig["a"].Calls);
        Assert.False(rig["b"].Calls.Last().FullLoad);
        Assert.False(rig.Applier.LastPublished!.Full);
    }

    [Fact]
    public async Task Hints_are_the_keys_after_the_cursor_and_a_type_over_HintLimit_or_with_a_keyless_entry_runs_without_them()
    {
        var rig = await RigAsync(["a", "b", "c"], configure: o => o.HintLimit = 2);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "k1"), ("a", "k2"), ("a", "k2"), ("b", "k1"), ("b", "k2"), ("b", "k3"), ("c", null), ("c", "k1"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(["k1", "k2"], rig["a"].Calls.Last().Hints!.Order());
        Assert.Null(rig["b"].Calls.Last().Hints);
        Assert.Null(rig["c"].Calls.Last().Hints);
        Assert.Equal(8, rig.Applier.LastPublished!.Entries);
    }

    [Fact]
    public async Task The_reconcilers_run_in_declared_order_in_the_appliers_own_execution_context()
    {
        var rig = await RigAsync(["z", "a"]);
        var order = new List<string>();
        rig["z"].Before = (_, _) => { order.Add("z"); return Task.CompletedTask; };
        rig["a"].Before = (_, _) => { order.Add("a"); return Task.CompletedTask; };

        await rig.LoadAsync();

        Assert.Equal(["z", "a"], order);
        Assert.Equal(ClusterScopeKind.Applier, rig.State("z").Scope);
        Assert.Equal(ClusterScopeKind.Applier, rig.State("a").Scope);
        Assert.Null(rig.Log.Scopes.Current);
    }

    [Fact]
    public async Task A_lost_notice_is_covered_by_the_poll_and_a_rewind_with_no_new_write_is_found_by_it()
    {
        var rig = await RigAsync(["a"], configure: o => o.ApplyPollInterval = TimeSpan.FromMilliseconds(200));
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync(startLoop: true);

        var seq = await rig.WriteAsync(("a", "k2"));
        await UntilAsync(() => rig.Store.Current!.Head == seq);
        Assert.Equal(2, rig.State("a").Value);

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq = {seq}");
        var head = await rig.HeadAsync();
        await UntilAsync(() => rig.Store.Current!.Head == head.Seq);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        await UntilAsync(async () => await rig.RowCursorAsync() == head);
    }

    [Fact]
    public async Task A_wake_up_starts_a_pass_before_the_poll_would()
    {
        var clock = new PacedClock();
        var rig = await RigAsync(["a"], configure: o => o.ApplyPollInterval = TimeSpan.FromMinutes(5), time: clock);
        await rig.LoadAsync(startLoop: true);
        await Poll.UntilAsync(() => clock.IsPending(TimeSpan.FromMinutes(5)), "the loop's first poll ran, found nothing and parked");
        var applied = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = rig.Channel.Subscribe(Reeve.Cluster.Wakeup.WakeupKind.Applied, w => applied.TrySetResult(w.Seq));

        var seq = await rig.WriteAsync(("a", "k1"));
        await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Appended, seq));

        Assert.Equal(seq, await applied.Task.WaitAsync(Wait));
        Assert.Equal(seq, rig.Store.Current!.Head);
    }

    [Fact]
    public async Task Fence_step_6_stops_the_applier_its_pass_publishes_nothing_and_no_pass_of_the_epoch_runs_again()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var before = rig.Store.Current!;
        await rig.WriteAsync(("a", "k"));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig["a"].Before = (_, _) => { entered.TrySetResult(); return hang.Task; };

        var pass = rig.PassAsync();
        await entered.Task.WaitAsync(Wait);
        var connection = rig.PassConnection.Take();
        Assert.NotNull(connection);

        await new ApplierStopParticipant(rig.Applier).RunAsync(new FenceContext(new FenceRequest(FenceTrigger.FenceStart)), CancellationToken.None);

        Assert.Equal(PassOutcome.Abandoned, await pass.WaitAsync(Wait));
        Assert.Same(before, rig.Store.Current);
        await connection.DisposeAsync().AsTask().WaitAsync(Wait);
        hang.SetResult();

        rig["a"].Before = null;
        var calls = rig["a"].Calls.Count;
        Assert.Equal(PassOutcome.Abandoned, await rig.PassAsync());
        Assert.Equal(calls, rig["a"].Calls.Count);
        Assert.Same(before, rig.Store.Current);
    }

    private static Task UntilAsync(Func<bool> condition) => Poll.UntilAsync(condition, "the condition");

    private static Task UntilAsync(Func<Task<bool>> condition) => Poll.UntilAsync(condition, "the condition");
}
