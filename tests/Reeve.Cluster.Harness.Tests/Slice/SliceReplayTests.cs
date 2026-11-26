using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Slice;

[Collection(StackCollection.Name)]
public sealed class SliceReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task Replay_M1_S10_a_write_in_Loading_is_not_awaited_and_the_replica_goes_on_to_serve_it()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.PrepareAsync("r1");
        using var load = r1.Faults.HoldNext(LogFaultPoints.InsideFirstLoad);
        await r1.StartAsync();
        await load.WaitReachedAsync(timeout: Limit);
        Assert.Equal(ReplicaPhase.Loading, SliceRig.PhaseOf(r1));

        var scopes = r1.Services.GetRequiredService<IClusterScopeAccessor>();
        var ack = r1.Services.GetRequiredService<IChangeAck>();
        AckOutcome outcome;
        long seq;
        using (scopes.BeginBackground())
        {
            var written = await SliceApp.WriteAsync(r1.Services.GetRequiredService<IChangeLog>(), r1.Options, SliceApp.Widget, "init", "v", CancellationToken.None);
            seq = written.Seq!.Value;

            outcome = await ack.WaitAppliedAsync(seq).WaitAsync(r1.Options.AckTimeout - TimeSpan.FromSeconds(3));
        }

        Assert.Equal(new AckOutcome.NotAwaited(NotAwaitedReason.Joining), outcome);
        var counted = rig.Run.Events.WaitSets().Where(w => w.AwaitedSeq == seq).ToList();
        Assert.Equal(["AckBegin"], counted.Select(w => w.Action).Distinct());
        Assert.All(counted, w => Assert.Empty(w.Members));

        load.Release();
        await r1.Started!.WaitAsync(Limit);
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        Assert.Equal("v", (await SliceRig.ItemAsync(await rig.Client(r1).GetAsync("items/widget/init"))).Value);
    }

    [Fact]
    public async Task Replay_M1_F5_a_second_append_waits_for_the_first_write_and_no_generation_is_labelled_past_an_uncommitted_seq()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var r1 = await rig.StartAsync("r1", SliceApp.Spec(configure: o =>
            o.ConnectionString = new NpgsqlConnectionStringBuilder(o.ConnectionString) { ApplicationName = "r1" }.ConnectionString));
        await SliceRig.WaitForPhaseAsync(r1, ReplicaPhase.Serving);

        await using var first = await HarnessOperations.StartWrite(r1);
        await using var second = await HarnessOperations.StartWrite(r1);
        var seq1 = await first.AppendAsync([new ChangeEntry(SliceApp.Widget, "k1")]);

        var append2 = second.AppendAsync([new ChangeEntry(SliceApp.Gadget, "k2")]);
        await rig.WaitForLockWaitAsync("r1", append2);
        Assert.False(append2.IsCompleted, "the second append returned while the first write was open: seqs are not assigned in commit order");

        await first.CommitAsync();
        var seq2 = await append2.WaitAsync(Limit);
        Assert.True(seq2 > seq1);
        await second.CommitAsync();
        await SliceRig.UntilAsync(() => Task.FromResult(rig.Run.Events.Publications().Any(p => p.Replica == "r1" && p.Head >= seq2)), "a generation at the second write's seq");

        var commits = rig.Run.Events.Snapshot().Where(e => e.Action == LogActions.WCommit).Select(e => (e.Sequence, Seq: Convert.ToInt64(e.State![ObservationKeys.Seq]))).ToList();
        foreach (var publication in rig.Run.Events.Publications())
        {
            Assert.All(commits.Where(c => c.Seq <= publication.Head), c => Assert.True(c.Sequence < publication.Sequence, $"generation {publication.Generation} (head {publication.Head}) came before the commit of seq {c.Seq}"));
        }

        var gen = rig.Run.Events.Publications().Last(p => p.Replica == "r1");
        Assert.True(gen.Head >= seq2);
    }
}
