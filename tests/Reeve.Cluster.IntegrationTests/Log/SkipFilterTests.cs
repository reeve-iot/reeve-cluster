using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class SkipFilterTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public SkipFilterTests(PostgresFixture postgres)
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

    [Fact]
    public async Task Filter_an_entry_in_an_open_range_counts_as_applied_and_leaves_its_type_clean_while_another_type_at_the_same_seqs_is_dirty()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.LoadAsync();
        var a = rig.State("a");
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("a", "k2"), ("b", "k3"));
        var head = await rig.HeadAsync();

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(head.Seq, rig.Store.Current!.Head);
        Assert.Equal(head, await rig.RowCursorAsync());
        Assert.Single(rig["a"].Calls);
        Assert.Same(a, rig.State("a"));
        Assert.Equal(new HashSet<string> { "k3" }, rig.State("b").Hints);
        var begin = LastPassBegin(rig);
        Assert.Equal(new[] { "b" }, (IEnumerable<string>)begin["dirty"]!);

        Assert.Equal(3L, begin["entries"]);
        Assert.Equal(3, rig.Applier.LastPassEntries);
    }

    [Fact]
    public async Task Filter_a_closed_range_covers_from_seq_up_to_its_repair_entry_which_is_not_covered_and_hints_come_from_uncovered_entries_only()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("a", "k2"));
        var repair = await rig.WriteAsync(("a", "k3"));
        await rig.ExecuteAsync($"""
            INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, until_seq, reason, opened_by, closed_by, closed_at)
            VALUES ('a', {from}, {repair}, 'test', 'operator', 'operator', clock_timestamp())
            """);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        var call = rig["a"].Calls.Last();
        Assert.False(call.FullLoad);
        Assert.Equal(new HashSet<string> { "k3" }, call.Hints);
        Assert.Equal(3L, LastPassBegin(rig)["entries"]);
    }

    [Fact]
    public async Task Filter_an_open_range_covers_from_its_from_seq_on_and_an_entry_of_the_type_below_it_keeps_the_type_dirty_with_only_its_key()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "below"));
        var from = await rig.WriteAsync(("a", "at"));
        await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("a", "above"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(new HashSet<string> { "below" }, rig["a"].Calls.Last().Hints);
    }

    [Fact]
    public async Task Filter_a_pass_whose_new_entries_are_all_covered_publishes_for_its_head_with_no_dirty_type_and_advances_the_cursor()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var loaded = rig.Store.Current!;
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("a", "k2"));
        var head = await rig.HeadAsync();

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.NotSame(loaded, rig.Store.Current);
        Assert.Equal(head.Seq, rig.Store.Current!.Head);
        Assert.Equal(head, await rig.RowCursorAsync());
        Assert.Single(rig["a"].Calls);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["dirty"]!);
        Assert.Equal(PassOutcome.NothingNew, await rig.PassAsync());
    }

    [Fact]
    public async Task Filter_a_skip_opened_during_a_stall_in_Serving_lets_the_retry_publish_without_the_type_and_the_replica_does_not_fence()
    {
        var clock = new PacedClock();
        var rig = await RigAsync(["a"], o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(200);
            o.ReconcileTimeout = TimeSpan.FromSeconds(1);
        }, time: clock);
        await rig.LoadAsync(startLoop: true);
        ToServing(rig);
        var createdAtAttempt = 0;
        rig["a"].Before = (_, _) =>
        {
            Volatile.Write(ref createdAtAttempt, clock.Created);
            throw new InvalidOperationException("a poison entry");
        };

        var seq = await rig.WriteAsync(("a", "poison"));
        await Poll.UntilAsync(
            async () =>
            {
                await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Appended, seq), CancellationToken.None);
                return rig.Events.All.Any(e => e.Action == LogActions.AttemptFail);
            },
            "the failed attempt");
        await clock.UntilCreatedAfterAsync(Volatile.Read(ref createdAtAttempt), "the applier parked on its retry");
        await OpenAsync(rig, "a", seq);

        clock.AdvanceToNext();
        await UntilAsync(() => rig.Store.Current!.Head == seq);
        clock.Advance(TimeSpan.FromSeconds(3.5));
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        Assert.Null(rig.Gate.CurrentFence);
        Assert.DoesNotContain(rig.Events.All, e => e.Action == MembershipActions.FenceStall);

        var later = await rig.WriteAsync(("a", "later"));
        await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Appended, later), CancellationToken.None);
        await UntilAsync(() => rig.Store.Current!.Head == later);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
    }

    [Fact]
    public async Task Filter_a_skip_committed_after_the_snapshot_is_not_seen_by_the_pass()
    {
        var points = new OnceAt();
        var rig = await RigAsync(["a"], points: points);
        await rig.LoadAsync();
        var seq = await rig.WriteAsync(("a", "k1"));
        points.Arm(LogFaultPoints.AfterSnapshotRead.Name, () => OpenAsync(rig, "a", seq));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.True(points.Fired, "the skip was inserted after the snapshot");
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.Equal(new HashSet<string> { "k1" }, rig["a"].Calls.Last().Hints);
    }

    [Fact]
    public async Task Filter_a_covered_keyless_entry_leaves_the_hints_of_the_uncovered_entries()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "below"));
        var from = await rig.WriteAsync(("a", null));
        await OpenAsync(rig, "a", from);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(new HashSet<string> { "below" }, rig["a"].Calls.Last().Hints);
    }

    [Fact]
    public async Task Filter_covered_keys_do_not_count_towards_HintLimit()
    {
        var rig = await RigAsync(["a"], o => o.HintLimit = 2);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "below"));
        var from = await rig.WriteAsync(("a", "c1"));
        await rig.WriteAsync(("a", "c2"));
        await rig.WriteAsync(("a", "c3"));
        await OpenAsync(rig, "a", from);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(new HashSet<string> { "below" }, rig["a"].Calls.Last().Hints);
    }

    [Fact]
    public async Task KeptOnReload_a_rejoins_reload_keeps_a_type_an_open_skip_covers_after_the_generations_cursor_and_reloads_the_others()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.LoadAsync();
        var a = rig.State("a");
        var from = await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("b", "k2"));
        await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("a", "k3"));
        var head = await rig.HeadAsync();

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.Equal(head.Seq, rig.Store.Current!.Head);
        Assert.Single(rig["a"].Calls);
        Assert.Same(a, rig.State("a"));
        Assert.True(rig["b"].Calls.Last().FullLoad);
        var begin = LastPassBegin(rig);
        Assert.Equal(true, begin["full"]);
        Assert.Equal(new[] { "b" }, (IEnumerable<string>)begin["dirty"]!);
        Assert.Equal(new[] { "a" }, (IEnumerable<string>)begin["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_with_no_open_skip_in_the_snapshot_the_rejoins_reload_reloads_the_type()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        var id = await OpenAsync(rig, "a", from);

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_skip WHERE id = {id}");

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_a_closed_skip_keeps_nothing()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        var repair = await rig.WriteAsync(("a", null));
        await rig.ExecuteAsync($"""
            INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, until_seq, reason, opened_by, closed_by, closed_at)
            VALUES ('a', {from}, {repair}, 'test', 'operator', 'operator', clock_timestamp())
            """);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        Assert.Equal(2, rig["a"].Calls.Count);
    }

    [Fact]
    public async Task KeptOnReload_a_first_start_has_no_generation_to_keep_and_loads_a_skipped_type()
    {
        var rig = await RigAsync(["a"]);
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);

        await rig.JoinAsync();
        Assert.Equal(LoadOutcome.Loaded, await rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None));

        Assert.True(Assert.Single(rig["a"].Calls).FullLoad);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_a_generation_whose_cursor_is_invalid_in_the_snapshot_reloads_the_type()
    {
        var rig = await RigAsync(["a"]);
        var lost = await rig.WriteAsync(("x", "k0"));
        await rig.LoadAsync();
        Assert.Equal(lost, rig.Store.Current!.Head);
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);

        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq = {lost}");

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_an_entry_of_the_type_between_the_generations_cursor_and_from_seq_reloads_the_type()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "uncovered"));
        var from = await rig.WriteAsync(("a", "covered"));
        await OpenAsync(rig, "a", from);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.True(rig["a"].Calls.Last().FullLoad);
    }

    [Fact]
    public async Task KeptOnReload_an_open_skip_from_above_the_snapshots_head_reloads_the_type()
    {
        var rig = await RigAsync(["a", "b"]);
        await rig.LoadAsync();
        var head = await rig.WriteAsync(("b", "k1"));
        await OpenAsync(rig, "a", head + 10);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_an_incremental_pass_keeps_nothing_and_filters_instead()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        var begin = LastPassBegin(rig);
        Assert.Equal(false, begin["full"]);
        Assert.Empty((IEnumerable<string>)begin["kept"]!);
        Assert.Single(rig["a"].Calls);
    }

    [Fact]
    public async Task KeptOnReload_a_skip_committed_after_the_reloads_snapshot_keeps_nothing()
    {
        var points = new OnceAt();
        var rig = await RigAsync(["a"], points: points);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        points.Arm(LogFaultPoints.BeforeFullReload.Name, () => OpenAsync(rig, "a", from));

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.True(points.Fired, "the skip was inserted after the snapshot");
        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_with_two_open_skips_the_lowest_from_seq_decides()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();
        var a = rig.State("a");
        var low = await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("a", "k2"));
        var high = await rig.WriteAsync(("a", "k3"));
        await OpenAsync(rig, "a", high);
        await OpenAsync(rig, "a", low);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.Single(rig["a"].Calls);
        Assert.Same(a, rig.State("a"));
        Assert.Equal(new[] { "a" }, (IEnumerable<string>)LastPassBegin(rig)["kept"]!);
    }

    [Fact]
    public async Task KeptOnReload_a_closed_skip_does_not_count_and_an_open_skip_from_below_every_entry_keeps_the_type()
    {
        var gap = await RigAsync(["a"]);
        await gap.LoadAsync();
        var f1 = await gap.WriteAsync(("a", "k1"));
        var repair = await gap.WriteAsync(("a", null));
        await InsertClosedAsync(gap, "a", f1, repair);
        await OpenAsync(gap, "a", repair);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(gap));
        Assert.Equal(2, gap["a"].Calls.Count);
        Assert.Empty((IEnumerable<string>)LastPassBegin(gap)["kept"]!);

        var covered = await RigAsync(["a"]);
        await covered.LoadAsync();
        var state = covered.State("a");
        var g1 = await covered.WriteAsync(("a", "k1"));
        var repair2 = await covered.WriteAsync(("a", null));
        await InsertClosedAsync(covered, "a", g1, repair2);
        await OpenAsync(covered, "a", g1);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(covered));
        Assert.Single(covered["a"].Calls);
        Assert.Same(state, covered.State("a"));
    }

    private static Task<PassOutcome> ReloadAsync(ApplierRig rig) =>
        rig.Applier.RunPassAsync(rig.Epoch, forceFull: true, withoutHints: false, CancellationToken.None);

    private static IReadOnlyDictionary<string, object?> LastPassBegin(ApplierRig rig) =>
        rig.Events.All.Last(e => e.Action == LogActions.PassBegin).State!;

    private static async Task<long> OpenAsync(ApplierRig rig, string type, long from)
    {
        await using var connection = await rig.Log.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            $"INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, reason, opened_by) VALUES ('{type}', {from}, 'test', 'operator') RETURNING id",
            connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static Task InsertClosedAsync(ApplierRig rig, string type, long from, long until) =>
        rig.ExecuteAsync($"""
            INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, until_seq, reason, opened_by, closed_by, closed_at)
            VALUES ('{type}', {from}, {until}, 'test', 'operator', 'operator', clock_timestamp())
            """);

    private static void ToServing(ApplierRig rig)
    {
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
    }

    private static Task UntilAsync(Func<bool> condition) => Poll.UntilAsync(condition, "the condition");

    private async Task<ApplierRig> RigAsync(string[] types, Action<ClusterOptions>? configure = null, IFaultPoints? points = null, TimeProvider? time = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, types, points, configure, time);
        rigs.Add(rig);
        return rig;
    }

    private sealed class OnceAt : IFaultPoints
    {
        private string? point;
        private Func<Task>? action;

        public bool Fired { get; private set; }

        public void Arm(string name, Func<Task> run)
        {
            point = name;
            action = run;
        }

        public async ValueTask ReachAsync(string name, CancellationToken cancellationToken = default)
        {
            if (action is { } run && name == point)
            {
                action = null;
                await run();
                Fired = true;
            }
        }
    }
}
