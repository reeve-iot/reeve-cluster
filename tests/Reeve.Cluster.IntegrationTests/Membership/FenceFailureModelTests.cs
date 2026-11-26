using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Int.Membership;

[CollectionDefinition(Name)]
public sealed class MembershipStackCollection : ICollectionFixture<StackFixture>
{
    public const string Name = "membership-stack";
}

[Collection(MembershipStackCollection.Name)]
public sealed class FenceFailureModelTests(StackFixture fixture, ITestOutputHelper output)
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly ClusterStack stack = fixture.Stack;

    private static async Task AwaitAsync(LocalPhaseGate gate, Func<LocalPhaseGate, bool> reached)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (reached(gate))
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private static Task AwaitJoinedAsync(LocalPhaseGate gate, long after = 0) =>
        AwaitAsync(gate, g => g.Phase is ReplicaPhase.Loading or ReplicaPhase.CatchingUp or ReplicaPhase.Activated or ReplicaPhase.Serving && g.Epoch > after);

    private static async Task<List<(long Epoch, string Trigger, TimeSpan Span)>> ReadHistoryAsync(HarnessRun run)
    {
        await using var connection = await run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT epoch, trigger, span FROM reeve_cluster.fence_history ORDER BY epoch", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(long, string, TimeSpan)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetTimeSpan(2)));
        }

        return rows;
    }

    private static TaskCompletionSource<(FenceTrigger Trigger, TimeSpan At, TimeSpan Deadline)> OnFence(LocalPhaseGate gate, out IDisposable subscription)
    {
        var fenced = new TaskCompletionSource<(FenceTrigger, TimeSpan, TimeSpan)>(TaskCreationOptions.RunContinuationsAsynchronously);
        subscription = gate.OnFenceRequested(r => fenced.TrySetResult((r.Trigger, gate.Now, gate.Deadline ?? TimeSpan.Zero)));
        return fenced;
    }

    [Fact]
    public async Task Failure_model_a_replica_that_loses_PostgreSQL_fences_at_its_fence_start_and_rejoins_and_catches_up_when_it_is_back()
    {
        await using var run = await HarnessRun.BeginAsync(output.WriteLine, stack);
        await run.MigrateAsync();
        var replica = await run.StartReplicaAsync("r1");
        var gate = replica.Services.GetRequiredService<LocalPhaseGate>();
        await AwaitJoinedAsync(gate);
        var first = gate.Epoch!.Value;
        var fenced = OnFence(gate, out var subscription);
        using var _ = subscription;

        var stalls = run.Stalls.Begin();
        await replica.Link!.Postgres.CutAsync();

        var (trigger, at, deadline) = await fenced.Task.WaitAsync(Wait);
        Assert.Equal(FenceTrigger.FenceStart, trigger);

        TimingWindows.FenceStart(replica.Options).Widen(TimeSpan.FromMilliseconds(50), TimeSpan.Zero)
            .AssertContains(at - (deadline - (replica.Options.LeaseTtl - replica.Options.SafetyMargin)), stalls, output.WriteLine);
        await AwaitAsync(gate, g => g.Phase == ReplicaPhase.Fenced);

        await Task.Delay(TestProfile.RejoinBackoff + TestProfile.RenewInterval);
        Assert.Equal(ReplicaPhase.Fenced, gate.Phase);

        await replica.Link.Postgres.RestoreAsync();
        await AwaitAsync(gate, g => g.Phase is ReplicaPhase.CatchingUp or ReplicaPhase.Activated or ReplicaPhase.Serving && g.Epoch > first);

        Assert.All(await ReadHistoryAsync(run), fence => Assert.Equal((first, "fence-start"), (fence.Epoch, fence.Trigger)));
    }

    [Fact]
    public async Task Failure_model_a_replica_whose_network_to_PostgreSQL_flaps_fences_and_rejoins_after_RejoinBackoff_each_time()
    {
        var backoff = TimeSpan.FromSeconds(5);
        await using var run = await HarnessRun.BeginAsync(output.WriteLine, stack);
        await run.MigrateAsync();
        var replica = await run.StartReplicaAsync("r1", new ReplicaSpec { Configure = o => o.RejoinBackoff = backoff });
        var gate = replica.Services.GetRequiredService<LocalPhaseGate>();
        var epochs = new List<long>();

        await AwaitJoinedAsync(gate);
        for (var flap = 0; flap < 2; flap++)
        {
            var epoch = gate.Epoch!.Value;
            epochs.Add(epoch);
            var fenced = OnFence(gate, out var subscription);
            using (subscription)
            {
                var stalls = run.Stalls.Begin();
                await replica.Link!.Postgres.CutAsync();
                var (trigger, step0, _) = await fenced.Task.WaitAsync(Wait);
                Assert.Equal(FenceTrigger.FenceStart, trigger);
                await AwaitAsync(gate, g => g.Phase == ReplicaPhase.Fenced);
                await replica.Link.Postgres.RestoreAsync();

                var starting = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
                using var rejoin = gate.Subscribe(e =>
                {
                    if (e.To == ReplicaPhase.Starting)
                    {
                        starting.TrySetResult(gate.Now);
                    }
                });
                var rejoinedAfter = await starting.Task.WaitAsync(Wait) - step0;
                new Window("rejoin after RejoinBackoff", backoff - TimeSpan.FromMilliseconds(50), backoff + replica.Options.RenewInterval + replica.Options.RenewTimeout + TimingWindows.DefaultSlack)
                    .AssertContains(rejoinedAfter, stalls, output.WriteLine);
                await AwaitJoinedAsync(gate, after: epoch);
            }
        }

        var history = await ReadHistoryAsync(run);
        Assert.Subset(epochs.ToHashSet(), history.Select(h => h.Epoch).ToHashSet());
        Assert.All(history, h => Assert.Equal("fence-start", h.Trigger));
        Assert.All(history, h => Assert.True(h.Span < replica.Options.LeaseTtl, $"epoch {h.Epoch} lasted {h.Span}"));
    }
}
