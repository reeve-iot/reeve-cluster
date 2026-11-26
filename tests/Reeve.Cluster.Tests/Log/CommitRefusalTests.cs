using Reeve.Cluster;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Testing;

namespace Unit.Log;

public sealed class CommitRefusalTests
{
    private readonly EventGate gate = new();
    private readonly FenceSteps steps = new();

    private static readonly FenceContext Fence = new(new FenceRequest(FenceTrigger.FenceStart));

    private ClusterScope Scope(ClusterScopeKind kind) => new(kind, null, steps.Count);

    [Fact]
    public void The_participant_runs_in_step_2()
    {
        using var refusal = new CommitRefusal(gate, steps);

        Assert.Equal(FenceStep.RefuseCommits, new AppendRefusalParticipant(refusal).Step);
    }

    [Fact]
    public void Before_step_2_nothing_is_refused()
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);

        Assert.False(refusal.Refusing);
        Assert.False(refusal.Refuses(Scope(ClusterScopeKind.ClientRequest)));
    }

    [Fact]
    public async Task After_step_2_a_client_requests_commit_is_refused()
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);

        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);

        Assert.True(refusal.Refusing);
        Assert.True(refusal.Refuses(Scope(ClusterScopeKind.ClientRequest)));
    }

    [Theory]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public async Task After_step_2_every_other_scope_commits(ClusterScopeKind kind)
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);

        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);

        Assert.False(refusal.Refuses(Scope(kind)));
        Assert.False(refusal.Refuses(null));
    }

    [Theory]
    [InlineData(ReplicaPhase.Fenced)]
    [InlineData(ReplicaPhase.Exited)]
    public void The_refusal_lasts_the_fence(ReplicaPhase next)
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        refusal.Start();

        gate.MoveTo(next);
        gate.RaiseDrainStep4();

        Assert.True(refusal.Refuses(Scope(ClusterScopeKind.ClientRequest)));
    }

    [Fact]
    public void A_rejoin_lifts_the_refusal_and_the_next_fence_refuses_again_only_from_its_own_step_2()
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        refusal.Start();
        gate.MoveTo(ReplicaPhase.Fenced);

        gate.MoveTo(ReplicaPhase.Starting);
        Assert.False(refusal.Refuses(Scope(ClusterScopeKind.ClientRequest)));

        foreach (var phase in new[] { ReplicaPhase.Loading, ReplicaPhase.CatchingUp, ReplicaPhase.Activated, ReplicaPhase.Serving, ReplicaPhase.Fencing })
        {
            gate.MoveTo(phase);
            Assert.False(refusal.Refusing, $"refusing in {phase}");
        }

        refusal.Start();
        Assert.True(refusal.Refuses(Scope(ClusterScopeKind.ClientRequest)));
    }

    [Fact]
    public void Disposing_the_refusal_unsubscribes_it_from_the_gate()
    {
        var refusal = new CommitRefusal(gate, steps);
        Assert.Equal(1, gate.Subscribers);

        refusal.Dispose();

        Assert.Equal(0, gate.Subscribers);
    }

    [Fact]
    public async Task After_step_2_background_work_in_a_flow_that_outlived_its_request_is_not_refused()
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);
        var source = new ClusterScopeSource(steps);
        var requestEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<bool> loop;

        using (source.Begin(ClusterScopeKind.ClientRequest))
        {
            loop = Task.Run(async () =>
            {
                await requestEnded.Task;
                using var work = source.BeginBackground();
                return refusal.Refuses(source.CurrentWriter);
            });
        }

        requestEnded.SetResult();

        Assert.False(await loop.WaitAsync(Bounds.Safety));
    }

    [Fact]
    public async Task Refuses_is_false_for_a_client_request_scope_that_has_ended()
    {
        using var refusal = new CommitRefusal(gate, steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);
        var scope = Scope(ClusterScopeKind.ClientRequest);

        scope.End();

        Assert.False(refusal.Refuses(scope));
    }

    [Fact]
    public async Task A_client_request_running_at_step_2_stays_refused_past_the_rejoin_until_it_ends()
    {
        using var refusal = new CommitRefusal(gate, steps);
        var source = new ClusterScopeSource(steps);
        var request = source.Begin(ClusterScopeKind.ClientRequest);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);

        gate.MoveTo(ReplicaPhase.Fenced);
        gate.MoveTo(ReplicaPhase.Starting);
        gate.MoveTo(ReplicaPhase.Serving);

        Assert.False(refusal.Refusing);
        Assert.True(refusal.Refuses(source.CurrentWriter));
        Assert.True(refusal.RanThroughStep2(source.CurrentWriter));

        var writer = (IClusterScopeWriter)request.Scope;
        request.Dispose();
        Assert.False(refusal.Refuses(writer));
        Assert.False(refusal.RanThroughStep2(writer));
    }

    [Fact]
    public async Task A_client_request_begun_after_the_rejoin_is_not_refused()
    {
        using var refusal = new CommitRefusal(gate, steps);
        var source = new ClusterScopeSource(steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);
        gate.MoveTo(ReplicaPhase.Fenced);
        gate.MoveTo(ReplicaPhase.Starting);

        using var request = source.Begin(ClusterScopeKind.ClientRequest);

        Assert.False(refusal.Refuses(source.CurrentWriter));
        Assert.False(refusal.RanThroughStep2(source.CurrentWriter));
    }

    [Fact]
    public async Task A_client_request_begun_during_the_fence_after_step_2_is_refused_until_the_rejoin_but_did_not_run_through_it()
    {
        using var refusal = new CommitRefusal(gate, steps);
        var source = new ClusterScopeSource(steps);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);

        using var request = source.Begin(ClusterScopeKind.ClientRequest);
        Assert.True(refusal.Refuses(source.CurrentWriter));
        Assert.False(refusal.RanThroughStep2(source.CurrentWriter));

        gate.MoveTo(ReplicaPhase.Fenced);
        gate.MoveTo(ReplicaPhase.Starting);
        Assert.False(refusal.Refuses(source.CurrentWriter));
    }

    [Theory]
    [InlineData(ClusterScopeKind.ExemptRequest)]
    [InlineData(ClusterScopeKind.UnitOfWork)]
    [InlineData(ClusterScopeKind.Background)]
    [InlineData(ClusterScopeKind.Applier)]
    public async Task Only_a_client_request_stays_refused_past_the_rejoin(ClusterScopeKind kind)
    {
        using var refusal = new CommitRefusal(gate, steps);
        var source = new ClusterScopeSource(steps);
        using var scope = source.Begin(kind);
        gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(refusal).RunAsync(Fence, CancellationToken.None);
        gate.MoveTo(ReplicaPhase.Fenced);
        gate.MoveTo(ReplicaPhase.Starting);

        Assert.False(refusal.Refuses(source.CurrentWriter));
        Assert.False(refusal.RanThroughStep2(source.CurrentWriter));
    }
}
