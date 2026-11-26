using System.Diagnostics;
using System.Runtime.CompilerServices;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

namespace Int.Log;

public sealed class ApplierFailureTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan Allowance = TimeSpan.FromSeconds(3);

    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public ApplierFailureTests(PostgresFixture postgres)
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
    public async Task Backoff_a_failing_reconciler_is_retried_after_ApplyPollInterval_then_twice_as_long_each_time_and_wake_ups_do_not_shorten_it()
    {
        var points = new PointProbe();
        var rig = await RigAsync(["a"], points, o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(200);
            o.ReconcileTimeout = TimeSpan.FromSeconds(2);
            o.ApplyStallLimit = TimeSpan.FromSeconds(30);
        });
        await rig.LoadAsync(startLoop: true);
        var clock = Stopwatch.StartNew();
        var starts = new List<TimeSpan>();
        rig["a"].Before = (_, _) =>
        {
            lock (starts)
            {
                starts.Add(clock.Elapsed);
                if (starts.Count <= 4)
                {
                    throw new InvalidOperationException($"failure {starts.Count}");
                }
            }

            return Task.CompletedTask;
        };

        using var stopWaking = new CancellationTokenSource();
        var waking = Task.Run(async () =>
        {
            while (!stopWaking.IsCancellationRequested)
            {
                await rig.Channel.PublishAsync(new Wakeup(WakeupKind.Appended, 0), CancellationToken.None);
                await Task.Delay(20);
            }
        });

        var seq = await rig.WriteAsync(("a", "k1"));
        await UntilAsync(() => rig.Store.Current!.Head == seq);
        await stopWaking.CancelAsync();
        await waking;

        TimeSpan[] seen;
        lock (starts)
        {
            seen = [.. starts];
        }

        Assert.Equal(5, seen.Length);
        double[] delays = [200, 400, 800, 1600];
        for (var i = 0; i < delays.Length; i++)
        {
            var gap = (seen[i + 1] - seen[i]).TotalMilliseconds;
            Assert.True(gap >= delays[i] - 50, $"retry {i + 1} came {gap} ms after its stall; the backoff is {delays[i]} ms");
        }

        Assert.Equal(4, points.Reached.Count(p => p == LogFaultPoints.BeforeBackoffRetry.Name));
        Assert.Equal(1, rig.State("a").Value);
    }

    [Theory]
    [InlineData("head-below-cursor")]
    [InlineData("reused-seq")]
    [InlineData("below-horizon")]
    public async Task A_rewound_log_with_no_new_write_is_found_by_the_poll_and_reloaded_in_full(string rewind)
    {
        var points = new PointProbe();
        var rig = await RigAsync(["a", "b"], points);
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "k1"));
        await rig.WriteAsync(("b", "k2"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        var cursor = rig.Applier.Cursor;
        Assert.False(await rig.Applier.PollAsync(CancellationToken.None));

        switch (rewind)
        {
            case "head-below-cursor":
                await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq = {cursor.Seq}");
                break;
            case "reused-seq":
                await rig.ExecuteAsync($"UPDATE reeve_cluster.change_log SET entry_token = gen_random_uuid() WHERE seq = {cursor.Seq}");
                break;
            default:
                await rig.WriteAsync(("a", "k3"));
                await rig.ExecuteAsync(
                    $"DELETE FROM reeve_cluster.change_log WHERE seq <= {cursor.Seq}; UPDATE reeve_cluster.cluster SET horizon_seq = {cursor.Seq + 1}");
                break;
        }

        var head = await rig.HeadAsync();
        Assert.True(await rig.Applier.PollAsync(CancellationToken.None));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        var begin = rig.Events.All.Last(e => e.Action == LogActions.PassBegin).State!;
        Assert.Equal(true, begin["full"]);
        Assert.Equal(false, begin["reload"]);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.True(rig["b"].Calls.Last().FullLoad);
        Assert.Contains(LogFaultPoints.BeforeFullReload.Name, points.Reached);
        Assert.Equal(head, rig.Applier.Cursor);
        Assert.Equal(head, await rig.RowCursorAsync());
        Assert.False(await rig.Applier.PollAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData(nameof(FenceTrigger.FenceStart))]
    [InlineData(nameof(FenceTrigger.ApplierStall))]
    [InlineData(nameof(FenceTrigger.FatalFault))]
    [InlineData(nameof(FenceTrigger.ZeroRowRenewal))]
    [InlineData(nameof(FenceTrigger.Timeline))]
    [InlineData(nameof(FenceTrigger.UnansweredActivation))]
    public async Task SIMP1_every_rejoin_reloads_in_full_after_a_fence_of_any_trigger_and_the_kept_generation_stays_current_until_it_publishes(string trigger)
    {
        var points = new PointProbe();
        points.Hold(LogFaultPoints.BeforeFullReload);
        var rig = await RigAsync(["a", "b"], points);
        await rig.WriteAsync(("a", "k1"));
        await rig.LoadAsync();
        await rig.WriteAsync(("b", "k2"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        var kept = rig.Store.Current!;

        rig.Gate.Apply(MemberTrigger.StateLoaded);
        if (trigger == nameof(FenceTrigger.ApplierStall))
        {
            rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        }

        Assert.Equal(FenceDecision.Fence, rig.Gate.RequestFence(new FenceRequest(Enum.Parse<FenceTrigger>(trigger), "a test")));
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET state = 'fenced'");
        rig.Applier.Stop();
        Assert.Equal(ReplicaPhase.Fenced, rig.Gate.CompleteFence());
        rig.Gate.Apply(MemberTrigger.RejoinPermitted);
        await rig.JoinAsync();
        Assert.Equal(kept.Head, (await rig.RowCursorAsync()).Seq);

        var loading = rig.Applier.LoadAsync(rig.Epoch, rig.Timeline, CancellationToken.None);
        await points.Arrived.WaitAsync(Wait);
        Assert.Same(kept, rig.Store.Current);
        await rig.WriteAsync(("a", "k3"));
        Assert.Same(kept, rig.Store.Current);
        points.Release();

        Assert.Equal(LoadOutcome.Loaded, await loading.WaitAsync(Wait));
        Assert.NotSame(kept, rig.Store.Current);
        var begin = rig.Events.All.Last(e => e.Action == LogActions.PassBegin).State!;
        Assert.Equal(true, begin["full"]);
        Assert.Equal(true, begin["reload"]);
        Assert.True(rig["a"].Calls.Last().FullLoad);
        Assert.True(rig["b"].Calls.Last().FullLoad);
    }

    [Fact]
    public async Task FenceStall_a_reconciler_that_keeps_failing_in_Serving_fences_at_the_stalled_pass_start_plus_the_allowance()
    {
        var clock = new PacedClock();
        var rig = await ShortRigAsync(["a"], clock);
        await rig.LoadAsync(startLoop: true);
        ToServing(rig);
        var attempts = new Attempts(clock);
        rig["a"].Before = (_, _) => attempts.Failed("a poison entry");

        var seq = await rig.WriteAsync(("a", "k1"));
        await DriveUntilAsync(rig, clock, attempts, seq, () => rig.Gate.Phase != ReplicaPhase.Serving);
        var fencedAt = clock.GetUtcNow();

        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        Assert.Equal(FenceTrigger.ApplierStall, rig.Gate.CurrentFence!.Request.Trigger);
        Assert.False(rig.Gate.ExitAfterFence);
        Assert.Equal(Allowance, fencedAt - attempts.FirstStart);
        Assert.Single(rig.Events.All, e => e.Action == MembershipActions.FenceStall);
        Assert.True(rig["a"].Calls.Count >= 4, $"attempts: {rig["a"].Calls.Count}");
    }

    [Fact]
    public async Task The_applier_status_reports_the_stall_with_its_failing_type_and_the_first_unapplied_seq_until_a_pass_publishes()
    {
        var rig = await RigAsync(["a"], configure: o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(200);
            o.ReconcileTimeout = TimeSpan.FromSeconds(1);
        });
        await rig.WriteAsync(("a", "k0"));
        await rig.LoadAsync(startLoop: true);
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        var loaded = rig.Applier.Status;
        Assert.Equal(1L, loaded.Cursor.Seq);
        Assert.Equal(1L, loaded.Generation);
        Assert.Null(loaded.StalledFor);
        Assert.Null(loaded.FailingType);
        Assert.Equal(2L, loaded.FirstUnappliedSeq);
        var failing = true;
        rig["a"].Before = (_, _) => Volatile.Read(ref failing) ? throw new InvalidOperationException("a poison entry") : Task.CompletedTask;

        var seq = await rig.WriteAsync(("a", "k1"));
        await UntilAsync(() => rig.Applier.Status.FailingType == "a");

        var stalled = rig.Applier.Status;
        Assert.NotNull(stalled.StalledFor);
        Assert.Equal(1L, stalled.Cursor.Seq);
        Assert.Equal(1L, stalled.Generation);
        Assert.Equal(2L, stalled.FirstUnappliedSeq);
        var section = (await new ApplierStatusSection(rig.Applier).CollectAsync(CancellationToken.None)).Values;
        Assert.Equal(true, section["stalled"]);
        Assert.Equal("a", section["failingType"]);
        Assert.Equal(2L, section["firstUnappliedSeq"]);

        Volatile.Write(ref failing, false);
        await UntilAsync(() => rig.Store.Current!.Head == seq);
        await UntilAsync(() => rig.Applier.Status.FailingType is null);
        var recovered = rig.Applier.Status;
        Assert.Null(recovered.StalledFor);
        Assert.Equal(seq, recovered.Cursor.Seq);
        Assert.Equal(seq + 1, recovered.FirstUnappliedSeq);
    }

    [Fact]
    public async Task A_reconciler_failing_in_CatchingUp_keeps_retrying_past_the_allowance_fences_nothing_and_applies_once_fixed()
    {
        var rig = await RigAsync(["a"], configure: o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(200);
            o.ReconcileTimeout = TimeSpan.FromSeconds(1);
        });
        await rig.WriteAsync(("a", "k0"));
        await rig.LoadAsync(startLoop: true);
        Assert.Equal(1, rig.Applier.LastPassEntries);
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        var failing = true;
        rig["a"].Before = (_, _) => Volatile.Read(ref failing) ? throw new InvalidOperationException("a poison entry") : Task.CompletedTask;

        var seq = await rig.WriteAsync(("a", "k1"));
        await Task.Delay(TimeSpan.FromSeconds(4));

        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
        Assert.Null(rig.Gate.CurrentFence);
        Assert.Equal(0, rig.Applier.LastPassEntries);
        Assert.DoesNotContain(rig.Events.All, e => e.Action == MembershipActions.FenceStall);

        Volatile.Write(ref failing, false);
        await UntilAsync(() => rig.Store.Current!.Head == seq);
        Assert.Equal(1, rig.Applier.LastPassEntries);
        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
    }

    [Fact]
    public async Task A_stall_in_Serving_that_a_retry_recovers_by_publishing_does_not_fence_at_its_first_pass_start_plus_the_allowance()
    {
        var clock = new PacedClock();
        var rig = await ShortRigAsync(["a"], clock);
        await rig.LoadAsync(startLoop: true);
        ToServing(rig);
        var attempts = new Attempts(clock);
        rig["a"].Before = (_, _) => attempts.Count == 0 ? attempts.Failed("one failure") : attempts.Passed();

        var seq = await rig.WriteAsync(("a", "k1"));
        await DriveUntilAsync(rig, clock, attempts, seq, () => rig.Store.Current!.Head == seq);
        Assert.Equal(TimeSpan.FromMilliseconds(200), clock.GetUtcNow() - attempts.FirstStart);

        clock.Advance(Allowance + TimeSpan.FromSeconds(1.5));
        await Task.Delay(TimeSpan.FromMilliseconds(300));

        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        Assert.Null(rig.Gate.CurrentFence);
        Assert.DoesNotContain(rig.Events.All, e => e.Action == MembershipActions.FenceStall);
    }

    [Fact]
    public async Task A_type_that_keeps_failing_beside_an_accepted_one_fences_at_the_first_stalled_pass_start_plus_the_allowance()
    {
        var clock = new PacedClock();
        var rig = await ShortRigAsync(["a", "b"], clock);
        await rig.LoadAsync(startLoop: true);
        ToServing(rig);
        var attempts = new Attempts(clock);
        rig["a"].Before = (_, _) => Task.CompletedTask;
        rig["b"].Before = (_, _) => attempts.Failed("b's poison entry");

        var seq = await rig.WriteAsync(("a", "k1"), ("b", "k2"));
        await DriveUntilAsync(rig, clock, attempts, seq, () => rig.Gate.Phase != ReplicaPhase.Serving);
        var fencedAt = clock.GetUtcNow();

        Assert.Equal(FenceTrigger.ApplierStall, rig.Gate.CurrentFence!.Request.Trigger);
        Assert.Equal(Allowance, fencedAt - attempts.FirstStart);
        Assert.True(rig["a"].Calls.Count >= 3, $"a's attempts: {rig["a"].Calls.Count}");
    }

    [Fact]
    public async Task A_stall_by_timeout_fences_at_its_first_pass_start_plus_the_allowance_not_at_its_timeout_plus_it()
    {
        var clock = new PacedClock();
        var rig = await ShortRigAsync(["a"], clock);
        await rig.LoadAsync();
        ToServing(rig);
        var attempts = new Attempts(clock);
        rig["a"].Before = async (_, _) =>
        {
            var returning = Task.Delay(TimeSpan.FromMilliseconds(1500), clock, CancellationToken.None);
            await attempts.Passed();
            await returning;
        };
        await rig.WriteAsync(("a", "k1"));

        var pass = rig.PassAsync();
        await Poll.UntilAsync(() => attempts.Count == 1, "the attempt");
        clock.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromMilliseconds(1));
        await Task.WhenAny(pass, Task.Delay(TimeSpan.FromMilliseconds(300)));
        Assert.False(pass.IsCompleted, "The pass ended before ReconcileTimeout.");
        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(PassOutcome.Stalled, await pass.WaitAsync(Wait));

        clock.Advance(Allowance - TimeSpan.FromSeconds(1) - TimeSpan.FromMilliseconds(1));
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await Poll.UntilAsync(() => rig.Gate.Phase == ReplicaPhase.Fencing, "the fence at the stall limit");

        Assert.Equal(FenceTrigger.ApplierStall, rig.Gate.CurrentFence!.Request.Trigger);
        Assert.Equal(Allowance, clock.GetUtcNow() - attempts.FirstStart);
    }

    [Fact]
    public async Task A_fault_that_clears_late_in_the_stall_is_retried_before_the_deadline_and_the_replica_does_not_fence()
    {
        var clock = new PacedClock();
        var rig = await ShortRigAsync(["a"], clock);
        await rig.LoadAsync(startLoop: true);
        ToServing(rig);
        var attempts = new Attempts(clock);
        rig["a"].Before = (_, _) =>
            clock.GetUtcNow() - (attempts.Count == 0 ? clock.GetUtcNow() : attempts.FirstStart) < TimeSpan.FromSeconds(1.7)
                ? attempts.Failed("a fault that clears")
                : attempts.Passed();

        var seq = await rig.WriteAsync(("a", "k1"));
        await DriveUntilAsync(rig, clock, attempts, seq, () => rig.Store.Current!.Head == seq);

        Assert.Equal(
            [TimeSpan.Zero, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(600), TimeSpan.FromMilliseconds(1400), TimeSpan.FromSeconds(2)],
            attempts.Starts.Select(at => at - attempts.FirstStart));

        clock.Advance(TimeSpan.FromSeconds(2));
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        Assert.Null(rig.Gate.CurrentFence);
    }

    private sealed class Attempts(PacedClock clock)
    {
        private readonly List<(DateTimeOffset At, int Created)> started = [];

        public int Count
        {
            get
            {
                lock (started)
                {
                    return started.Count;
                }
            }
        }

        public DateTimeOffset FirstStart
        {
            get
            {
                lock (started)
                {
                    return started[0].At;
                }
            }
        }

        public IReadOnlyList<DateTimeOffset> Starts
        {
            get
            {
                lock (started)
                {
                    return [.. started.Select(s => s.At)];
                }
            }
        }

        public int CreatedAt(int attempt)
        {
            lock (started)
            {
                return started[attempt - 1].Created;
            }
        }

        public Task Passed()
        {
            Record();
            return Task.CompletedTask;
        }

        public Task Failed(string reason)
        {
            Record();
            throw new InvalidOperationException(reason);
        }

        private void Record()
        {
            lock (started)
            {
                started.Add((clock.GetUtcNow(), clock.Created));
            }
        }
    }

    private static readonly TimeSpan DriveBound = TimeSpan.FromMinutes(2);

    private static async Task DriveUntilAsync(
        ApplierRig rig, PacedClock clock, Attempts attempts, long seq, Func<bool> done, [CallerArgumentExpression(nameof(done))] string? endCondition = null)
    {
        await Poll.UntilAsync(
            async () =>
            {
                await rig.Channel.PublishAsync(new Wakeup(WakeupKind.Appended, seq), CancellationToken.None);
                return attempts.Count >= 1;
            },
            "the first attempt");
        var seen = 1;
        var begun = clock.GetUtcNow();
        while (true)
        {
            Assert.True(
                clock.GetUtcNow() - begun <= DriveBound || done(),
                $"The end condition `{endCondition}` did not hold within {DriveBound} of the clock's time after {attempts.Count} attempts; the applier keeps retrying.");
            await Poll.UntilAsync(() => done() || attempts.Count > seen || clock.Created > attempts.CreatedAt(seen), "the applier parked on its retry");
            if (done())
            {
                return;
            }

            if (attempts.Count > seen)
            {
                seen = attempts.Count;
                continue;
            }

            clock.AdvanceToNext();
            await Poll.UntilAsync(() => done() || attempts.Count > seen, "the retry (or the end)");
            seen = attempts.Count;
        }
    }

    private static void ToServing(ApplierRig rig)
    {
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
    }

    private async Task<ApplierRig> RigAsync(string[] types, IFaultPoints? points = null, Action<ClusterOptions>? configure = null, TimeProvider? time = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, types, points, configure, time);
        rigs.Add(rig);
        return rig;
    }

    private Task<ApplierRig> ShortRigAsync(string[] types, TimeProvider? time = null) => RigAsync(types, configure: o =>
    {
        o.ApplyPollInterval = TimeSpan.FromMilliseconds(200);
        o.ReconcileTimeout = TimeSpan.FromSeconds(1);
    }, time: time);

    private static Task UntilAsync(Func<bool> condition) => UntilAsync(() => Task.FromResult(condition()));

    private static Task UntilAsync(Func<Task<bool>> condition) => Poll.UntilAsync(condition, "the condition");
}
