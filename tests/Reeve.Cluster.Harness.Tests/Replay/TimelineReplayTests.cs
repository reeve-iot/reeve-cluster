using System.Net;
using Harness.Failover;
using Harness.Slice;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Replicas;
using Xunit.Abstractions;

namespace Harness.Replay.Membership;

[Collection("harness-failover")]
public sealed class TimelineReplayTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = FailoverRig.Limit;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Replay_Membership_timeline_a_restore_on_a_new_timeline_with_unchanged_epochs_fences_each_replica_once_and_the_rejoin_loads_in_full(int replicas)
    {
        await using var rig = await FailoverRig.CreateAsync(output.WriteLine);
        var fleet = new List<Replica>();
        for (var i = 1; i <= replicas; i++)
        {
            fleet.Add(await rig.StartAsync($"r{i}"));
        }

        await rig.PutAsync(fleet[0], "kept", "1");
        var epochs = await FailoverTests.EpochsAsync(rig, [.. fleet]);

        await rig.Stack.ReplicationLink.CutAsync();
        await rig.WaitStandbyGoneAsync();
        long lost;
        await using (rig.CancelSyncWaits())
        {
            lost = await rig.LostAppendAsync(fleet[^1], "lost", 1);
            foreach (var replica in fleet)
            {
                await SliceRig.UntilAsync(() => Task.FromResult(FailoverApp.HeadOf(replica) >= lost), $"{replica.Id} to apply the lost entry", Limit);
                Assert.Contains("s:lost0", FailoverApp.StateOf(replica).Keys);
            }
        }

        var before = rig.Run.Sequence.Next();
        var stalls = rig.Run.Stalls.Begin();
        var outage = System.Diagnostics.Stopwatch.StartNew();
        await rig.Stack.FailOverAsync();

        var rides = fleet[0].Options.LeaseTtl - fleet[0].Options.SafetyMargin - fleet[0].Options.StopBudget - fleet[0].Options.RenewInterval - fleet[0].Options.RenewTimeout;
        if (outage.Elapsed > rides && stalls.Covers(outage.Elapsed - rides))
        {
            Reeve.Cluster.Harness.Timing.Degraded.Report(output.WriteLine, $"the outage from the cut to the restore took {outage.Elapsed.TotalSeconds:0.#} s, more than the {rides.TotalSeconds:0.#} s the fleet is sure to ride out, and {stalls.Describe()} accounts for the overrun: the replicas may fence by lease before the timeline, which was not asserted");
            return;
        }

        foreach (var replica in fleet)
        {
            await SliceRig.UntilAsync(
                () => Task.FromResult(rig.Run.Events.Of(replica.Id).Any(e => e.Sequence > before && e.Action == MembershipActions.TimelineFence
                    || (e.Sequence > before && e.Action == MembershipActions.RenewAnswer && Equals(e.State!["res"], "other")))),
                $"{replica.Id} to meet the new timeline",
                Limit);
        }

        await FailoverTests.WaitForRejoinAsync(rig, epochs, [.. fleet]);

        foreach (var replica in fleet)
        {
            FailoverTests.AssertFencedOnceOnTheTimeline(rig, replica, before);
            FailoverTests.AssertLoadedInFullAfter(rig, replica, before, epochs[replica.Id]);
            Assert.DoesNotContain(MembershipActions.RenewZeroL, rig.Run.Events.Of(replica.Id).Where(e => e.Sequence > before).Select(e => e.Action));
            Assert.Equal(2, FailoverTests.JoinedTimeline(replica));
            Assert.Equal(["s:kept"], FailoverApp.StateOf(replica).Keys.ToArray());

            using var gone = await rig.Client(replica).GetAsync("items/lost0");
            Assert.Equal(HttpStatusCode.NotFound, gone.StatusCode);
            using var kept = await rig.Client(replica).GetAsync("items/kept");
            Assert.Equal(HttpStatusCode.OK, kept.StatusCode);
        }
    }
}
