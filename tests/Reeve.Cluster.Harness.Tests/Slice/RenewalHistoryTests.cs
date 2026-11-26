using System.Diagnostics;
using System.Globalization;
using System.Net;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Slice;

[Collection(StackCollection.Name)]
public sealed class RenewalHistoryTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(90);
    private static readonly SemaphoreSlim CalibrationGate = new(1, 1);
    private static TimeSpan? restartCycle;

    private static void LibraryDefaults(ClusterOptions o)
    {
        TestProfile.ApplyLibraryDefaults(o);
        o.ApplyStallLimit = TestProfile.TolerantApplyStallLimit;
    }

    private async Task<TimeSpan> RestartCycleAsync()
    {
        await CalibrationGate.WaitAsync();
        try
        {
            if (restartCycle is null)
            {
                var timer = Stopwatch.StartNew();
                await stack.Stack.Database.RestartAsync(TimeSpan.Zero);
                restartCycle = timer.Elapsed;
            }

            return restartCycle.Value;
        }
        finally
        {
            CalibrationGate.Release();
        }
    }

    [Theory]
    [InlineData(14, false)]
    [InlineData(16, false)]
    [InlineData(18, false)]
    [InlineData(20, false)]
    [InlineData(22, false)]
    [InlineData(16, true)]
    [InlineData(20, true)]
    public async Task A_database_restart_fences_where_the_tolerance_says_and_not_before(int seconds, bool late)
    {
        var cycle = await RestartCycleAsync();
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", SliceApp.Spec(LibraryDefaults, o => o.RejoinBackoff = TimeSpan.FromSeconds(2)));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var options = r1.Options;
        var stalls = rig.Run.Stalls.Begin();
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Successful(rig.Run, r1).Count >= 2), "two renewals on record");

        if (late)
        {
            var lastT0 = Renewals.Seconds(r1, Renewals.Successful(rig.Run, r1)[^1].T0);
            while (Renewals.Seconds(r1, r1.Clock.GetTimestamp()) < lastT0 + options.RenewInterval.TotalSeconds - 0.5)
            {
                await Task.Delay(20);
            }
        }

        var outage = Stopwatch.StartNew();
        var stopRequested = r1.Clock.GetTimestamp();
        await stack.Stack.Database.StopAsync();

        var stopped = r1.Clock.GetTimestamp();
        var before = Renewals.Successful(rig.Run, r1).Last(r => r.Exec < stopped);
        output.WriteLine($"database stop returned {(Renewals.Seconds(r1, stopped) - Renewals.Seconds(r1, stopRequested)):0.0} s after the request");
        var downtime = TimeSpan.FromSeconds(seconds) - cycle;
        if (downtime > TimeSpan.Zero)
        {
            await Task.Delay(downtime);
        }

        await stack.Stack.Database.StartAsync();
        var ready = r1.Clock.GetTimestamp();
        var outageLength = outage.Elapsed;
        output.WriteLine($"outage of {seconds} s: stop to accepting connections took {outageLength.TotalSeconds:0.0} s; last renewal before it at t0 {Renewals.Seconds(r1, before.T0):0.0} s");

        var fenceStart = TimingWindows.FenceStart(options);
        await SliceRig.UntilAsync(
            () => Task.FromResult(
                Fencing(rig, r1) is not null
                || (Renewals.Successful(rig.Run, r1).Any(r => r.Exec > ready) && Renewals.Seconds(r1, r1.Clock.GetTimestamp()) > Renewals.Seconds(r1, before.T0) + fenceStart.Latest.TotalSeconds + 3)),
            "the history to settle",
            Limit);

        var readyOffset = Renewals.Seconds(r1, ready) - Renewals.Seconds(r1, before.T0);
        var lastAttempt = Math.Floor(((fenceStart.Earliest.TotalSeconds - 0.3) / options.RenewInterval.TotalSeconds)) * options.RenewInterval.TotalSeconds;
        output.WriteLine($"database accepting connections {readyOffset:0.0} s after that t0; the last attempt that can land begins at {lastAttempt:0.0} s");
        var fenced = Fencing(rig, r1);
        if (fenced is not null)
        {
            var offset = Renewals.Seconds(r1, fenced.Timestamp) - Renewals.Seconds(r1, before.T0);
            var onTime = TimingWindows.FenceStart(options, TimeSpan.FromSeconds(2)).AssertContains(TimeSpan.FromSeconds(offset), stalls, output.WriteLine);
            Assert.DoesNotContain(Renewals.Successful(rig.Run, r1), r => r.Exec > before.Exec && r.Counted);
            if (onTime && !stalls.Exceeds(TimingWindows.DefaultSlack))
            {
                Assert.True(readyOffset > lastAttempt - options.RenewInterval.TotalSeconds - TimingWindows.DefaultSlack.TotalSeconds, $"fenced although the database answered {readyOffset:0.0} s after t0");
            }
            else if (onTime)
            {
                Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the replica fenced in its window, but whether the database answered in time for it to have been rescued was not checked");
            }

            await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
            Assert.Equal(2, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
            Assert.Equal(HttpStatusCode.OK, (await rig.Client(r1).GetAsync("ready")).StatusCode);
        }
        else
        {
            var rescue = Renewals.Successful(rig.Run, r1).First(r => r.Exec > ready);
            var rescueOffset = Renewals.Seconds(r1, rescue.T0) - Renewals.Seconds(r1, before.T0);
            output.WriteLine($"the renewal that rescued the replica began {rescueOffset:0.0} s after that t0; fence start is {fenceStart.Earliest.TotalSeconds:0.0} s");
            Assert.True(rescueOffset < options.LeaseTtl.TotalSeconds, $"the renewal that rescued the replica began {rescueOffset:0.0} s after t0, past the lease");
            if (rescueOffset >= fenceStart.Earliest.TotalSeconds)
            {
                Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the renewal that rescued the replica began {rescueOffset - fenceStart.Earliest.TotalSeconds:0.0} s after fence start (a late schedule and fence timer), so its start before fence start was not checked");
            }

            if (stalls.Exceeds(TimingWindows.DefaultSlack))
            {
                Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the replica was ridden out, but whether the database answered before the last attempt that could land was not checked");
            }
            else
            {
                Assert.True(readyOffset < lastAttempt + TimingWindows.DefaultSlack.TotalSeconds, $"not fenced although the database answered {readyOffset:0.0} s after t0");
            }

            Assert.Equal(1, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
            Assert.Empty(await rig.Run.Observer.FenceHistoryAsync("r1"));
            Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r1));
        }
    }

    [Fact]
    public async Task A_process_paused_across_fence_start_and_resumed_before_its_fence_timer_runs_admits_no_request_and_fences()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);

        var r1 = await rig.StartAsync("r1", SliceApp.Spec(configure: o =>
        {
            o.StopBudget = TimeSpan.FromSeconds(8);
            o.RenewInterval = TimeSpan.FromSeconds(1);
            o.ApplyStallLimit = TimeSpan.FromSeconds(30);
        }));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var options = r1.Options;
        var http = rig.Client(r1);
        var calls = r1.Services.GetRequiredService<SliceCalls>();
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Successful(rig.Run, r1).Count >= 2), "two renewals on record");

        using var sends = r1.Faults.Hold(MembershipFaultPoints.RenewSend);
        await sends.WaitReachedAsync(timeout: Limit);
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Answered(rig.Run, r1)), "the attempts begun to be answered");
        using var timer = r1.Faults.HoldNext(MembershipFaultPoints.FenceBeforeStep0);
        await r1.PauseAsync();
        var last = Renewals.Successful(rig.Run, r1)[^1];
        var deadline = Renewals.Seconds(r1, last.T0) + (options.LeaseTtl - options.SafetyMargin).TotalSeconds;
        var fenceStart = Renewals.Seconds(r1, last.T0) + (options.LeaseTtl - options.SafetyMargin - options.StopBudget).TotalSeconds;
        var resumeAt = fenceStart + 1;
        var stalls = rig.Run.Stalls.Begin();
        while (Renewals.Seconds(r1, r1.Clock.GetTimestamp()) < resumeAt)
        {
            await Task.Delay(50);
        }

        await r1.ResumeAsync();
        await timer.WaitReachedAsync(timeout: Limit);

        if (Renewals.Seconds(r1, r1.Clock.GetTimestamp()) >= deadline)
        {
            Assert.True(stalls.Exceeds(TimeSpan.FromSeconds(1)), "the resume came after the local deadline, and the process was not stalled");
            Degraded.Report(output.WriteLine, $"{stalls.Describe()}: the resume came after the local deadline, so the request's own check against fence start was not made");
            sends.Release();
            timer.Release();
            return;
        }

        Assert.Equal(ReplicaPhase.Serving, SliceRig.PhaseOf(r1));
        using var read = await http.GetAsync("items/widget/a");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, read.StatusCode);
        Assert.Contains(await SliceRig.ErrorCodeAsync(read), new[] { "replica_fenced", "replica_not_serving" });
        Assert.Contains(SliceRig.PhaseOf(r1), new[] { ReplicaPhase.Fencing, ReplicaPhase.Fenced });
        Assert.Equal(1, timer.HeldNow);
        Assert.Empty(calls.All);

        timer.Release();
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Phases.Path("r1").Contains(ReplicaPhase.Fenced)), "the fence to end");
        sends.Release();
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal(2, (await rig.Run.Observer.MemberAsync("r1"))!.Epoch);
    }

    [Fact]
    public async Task A_replica_whose_clock_is_at_the_slowest_the_assumptions_allow_starts_its_fence_before_the_database_expires_its_lease()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);

        var rate = TimingWindows.SlowestClockRate(TestProfile.Options("x", "x", profile: null));
        var r1 = await rig.StartAsync("r1", SliceApp.Spec(clockRate: rate));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var options = r1.Options;
        var stalls = rig.Run.Stalls.Begin();
        Assert.True(options.StopBudget > options.SafetyMargin / 2);
        await SliceRig.UntilAsync(() => Task.FromResult(Renewals.Successful(rig.Run, r1).Count >= 2), "two renewals on record");

        await r1.Link!.CutAsync();
        var (members, _) = await rig.Run.Observer.MembersAsync();
        var expiry = members.Single().LeaseExpiresAt;
        var before = Renewals.Successful(rig.Run, r1)[^1];

        await SliceRig.UntilAsync(() => Task.FromResult(Fencing(rig, r1) is not null), "the fence to start", Limit);

        var fenced = Fencing(rig, r1)!;
        var offset = Renewals.Seconds(r1, fenced.Timestamp) - Renewals.Seconds(r1, before.T0);
        TimingWindows.FenceStart(options, TimeSpan.FromSeconds(2)).AssertContains(TimeSpan.FromSeconds(offset), stalls, output.WriteLine);

        var readAt = r1.Clock.GetTimestamp();
        var databaseNow = (await rig.Run.Observer.MembersAsync()).DatabaseNow;
        var sinceFence = TimeSpan.FromSeconds((readAt - fenced.Timestamp) / (double)r1.Clock.TimestampFrequency / r1.Clock.Rate);
        var databaseNowAtFence = databaseNow - sinceFence;
        Assert.True(databaseNowAtFence < expiry, $"fence start was at {databaseNowAtFence:O} on the database's clock, after the database's expiry {expiry:O}");
    }

    private static PhaseChange? Fencing(SliceRig rig, Replica replica) =>
        rig.Run.Phases.Of(replica.Id).FirstOrDefault(c => c.To == ReplicaPhase.Fencing);
}

internal readonly record struct SuccessfulRenewal(long T0, long Exec, bool Counted = false);

internal static class Renewals
{
    public static IReadOnlyList<SuccessfulRenewal> Successful(HarnessRun run, Replica replica)
    {
        var renewals = new List<SuccessfulRenewal>();
        long? sent = null;
        var awaitingAnswer = false;
        foreach (var e in run.Events.Snapshot().Where(e => e.Replica == replica.Id))
        {
            if (e.Action == MembershipActions.RenewSend)
            {
                awaitingAnswer = false;
                sent = e.Timestamp;
            }
            else if (e.Action == MembershipActions.RenewExec && sent is { } t0 && e.State is not null
                && Convert.ToInt64(e.State["rows"], CultureInfo.InvariantCulture) == 1)
            {
                renewals.Add(new SuccessfulRenewal(t0, e.Timestamp));
                awaitingAnswer = true;
            }
            else if (e.Action == MembershipActions.RenewAnswer && awaitingAnswer && renewals.Count > 0
                && Equals(e.State?["res"], "one"))
            {
                renewals[^1] = renewals[^1] with { Counted = true };
                awaitingAnswer = false;
            }
        }

        return renewals;
    }

    public static bool Answered(HarnessRun run, Replica replica)
    {
        var events = run.Events.Snapshot().Where(e => e.Replica == replica.Id).ToList();
        return events.Count(e => e.Action == MembershipActions.RenewSend) == events.Count(e => e.Action == MembershipActions.RenewExec);
    }

    public static double Seconds(Replica replica, long timestamp) => timestamp / (double)replica.Clock.TimestampFrequency;
}
