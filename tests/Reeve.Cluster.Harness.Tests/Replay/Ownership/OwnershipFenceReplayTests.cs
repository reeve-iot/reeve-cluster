using Harness.Owned;
using Harness.Placer;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;

namespace Harness.Replay.Ownership;

[Collection(StackCollection.Name)]
public sealed class OwnershipFenceReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Kind = "k0";
    private static readonly EligibilityFilter Hosting = EligibilityFilter.Any.Require(PlacerRig.WidgetHost);

    private static void NoRebalance(ClusterOptions o) => o.StabilisationWindow = TimeSpan.FromMinutes(10);

    [Fact]
    public async Task Replay_M2_N1_a_release_waits_behind_the_old_owners_open_fenced_transaction_and_its_writes_commit_before_the_new_owners()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services => services.AddClusterUnit<UninterruptedWriter>(Kind, o => o.Eligibility = Hosting),
            configure: o =>
            {
                NoRebalance(o);

                o.FencedTransactionTimeout = TimeSpan.FromSeconds(30);
            });
        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await PlacerRig.HostWidgetsAsync(r2);
        var old = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r2" }, "the unit on r2");

        using var open = r2.Faults.HoldNext(OwnedFaultPoints.FencedAfterAssert);
        await open.WaitReachedAsync(timeout: OwnedRig.Limit);

        await PlacerRig.HostWidgetsAsync(r1);
        Assert.True(await PlacerRig.Placer(r1).RequestHandoverAsync(Kind, Kind, CancellationToken.None), "the release request did not store its token");

        await OwnedRig.UntilAsync(
            async () => await WaitingForALockAsync(rig) || rig.Log.Starts.Any(s => s is { Kind: Kind, Replica: "r1" }),
            "the owner's release to wait behind its fenced transaction");
        Assert.DoesNotContain(rig.Log.Starts, s => s is { Kind: Kind, Replica: "r1" });
        var row = await rig.RowAsync(Kind, Kind);
        Assert.Equal("r2", row?.Owner);
        Assert.Equal(old.Token, row?.Token);

        Assert.True(open.Release());
        var moved = await rig.Log.StartAsync(s => s is { Kind: Kind, Replica: "r1" }, "the unit to start on r1");
        Assert.True(moved.Token > old.Token);
        await rig.AwaitTenureAsync(Kind, Kind, old.Token, 2);
        var effects = await rig.EffectsAsync();
        var lastOld = effects.Where(e => e.Tenure == old.Token).Max(e => e.N);
        var firstNew = effects.Where(e => e.Tenure == moved.Token).Min(e => e.N);
        Assert.True(lastOld < firstNew, $"tenure {old.Token} committed n = {lastOld} after tenure {moved.Token}'s first write n = {firstNew}");
        OwnedRig.AssertNoInterleave([.. effects.Select(e => e.Tenure)], "the fenced effects in commit order");
    }

    [Fact]
    public async Task Replay_M2_S3_a_unit_that_ignores_its_stop_runs_beside_its_successor_and_only_the_successors_fenced_writes_land()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        using var stubborn = new StubbornState();
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(stubborn);
                services.AddClusterUnit<StubbornWriter>(Kind, o => o.Eligibility = Hosting);
            },
            configure: NoRebalance);

        var r2 = await rig.StartAsync("r2", spec);
        var r1 = await rig.StartAsync("r1", spec);
        await PlacerRig.HostWidgetsAsync(r1);
        var first = await rig.AwaitTenureAsync(Kind, Kind, 0, 2);
        Assert.Equal("r1", first.Replica);

        using var beforeStep6 = r1.Faults.HoldNext(MembershipFaultPoints.FenceBeforeStep6);
        r1.Services.GetRequiredService<LocalPhaseGate>().RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE replica_id = 'r1'");
        await PlacerRig.HostWidgetsAsync(r2);
        var second = await rig.AwaitTenureAsync(Kind, Kind, first.Token, 2);
        Assert.Equal("r2", second.Replica);

        Assert.True(stubborn.Running(first.Token), "the old tenure's code ended; it should be running still");
        Assert.True(stubborn.Running(second.Token));
        Assert.Equal(2, stubborn.MostRunning);

        var refusals = stubborn.Refusals(first.Token);
        await OwnedRig.UntilAsync(() => Task.FromResult(stubborn.Refusals(first.Token) >= refusals + 2), "two more refused writes of the old tenure");
        var timeline = await rig.ScalarAsync("SELECT reeve_cluster.server_timeline()");
        Assert.Equal("RC001", await rig.AssertOwnerStateAsync(Kind, Kind, first.Token, timeline));
        Assert.Null(await rig.AssertOwnerStateAsync(Kind, Kind, second.Token, timeline));

        var effects = await rig.EffectsAsync();
        Assert.All(effects.SkipWhile(e => e.Tenure == first.Token), e => Assert.Equal(second.Token, e.Tenure));
        OwnedRig.AssertNoInterleave([.. effects.Select(e => e.Tenure)], "the fenced effects in commit order");
    }

    private static async Task<bool> WaitingForALockAsync(OwnedRig rig) =>
        await rig.ScalarAsync("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%reeve_cluster.owned%'") > 0;

    private const string EffectSql =
        """
        WITH next AS (UPDATE placer_history SET n = n + 1 RETURNING n)
        INSERT INTO placer_effect SELECT n, @tenure, @replica FROM next
        """;

    internal sealed class UninterruptedWriter(OwnedLog log, IOptions<ClusterOptions> options) : IOwnedUnit
    {
        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            log.Started(new OwnedStart(options.Value.ReplicaId!, context.Kind, context.Key, context.Token, context, null));
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await WriteOnceAsync(context, options.Value);
                }
                catch (OwnershipLostException)
                {
                    return;
                }
                catch (Exception)
                {
                }

                try
                {
                    await Task.Delay(50, stop);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }

    internal sealed class StubbornState : IDisposable
    {
        private readonly TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Lock gate = new();
        private readonly HashSet<long> running = [];
        private readonly Dictionary<long, int> refusals = [];
        private int mostRunning;

        public Task Done => done.Task;

        public int MostRunning
        {
            get
            {
                lock (gate)
                {
                    return mostRunning;
                }
            }
        }

        public bool Running(long tenure)
        {
            lock (gate)
            {
                return running.Contains(tenure);
            }
        }

        public int Refusals(long tenure)
        {
            lock (gate)
            {
                return refusals.GetValueOrDefault(tenure);
            }
        }

        public void Entered(long tenure)
        {
            lock (gate)
            {
                running.Add(tenure);
                mostRunning = Math.Max(mostRunning, running.Count);
            }
        }

        public void Left(long tenure)
        {
            lock (gate)
            {
                running.Remove(tenure);
            }
        }

        public void Refused(long tenure)
        {
            lock (gate)
            {
                refusals[tenure] = refusals.GetValueOrDefault(tenure) + 1;
            }
        }

        public void Dispose() => done.TrySetResult();
    }

    internal sealed class StubbornWriter(OwnedLog log, StubbornState state, IOptions<ClusterOptions> options) : IOwnedUnit
    {
        public async Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            log.Started(new OwnedStart(options.Value.ReplicaId!, context.Kind, context.Key, context.Token, context, null));
            state.Entered(context.Token);
            try
            {
                while (!state.Done.IsCompleted)
                {
                    try
                    {
                        await WriteOnceAsync(context, options.Value);
                    }
                    catch (OwnershipLostException)
                    {
                        state.Refused(context.Token);
                    }
                    catch (Exception)
                    {
                    }

                    await Task.WhenAny(Task.Delay(50, CancellationToken.None), state.Done);
                }
            }
            finally
            {
                state.Left(context.Token);
            }
        }

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }

    private static async Task WriteOnceAsync(OwnedContext context, ClusterOptions options)
    {
        await using var connection = new NpgsqlConnection(options.ConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        await using var fenced = await context.BeginFencedTransactionAsync(connection, cancellationToken: CancellationToken.None);
        await using (var effect = new NpgsqlCommand(EffectSql, connection, fenced.Transaction))
        {
            effect.Parameters.AddWithValue("tenure", context.Token);
            effect.Parameters.AddWithValue("replica", options.ReplicaId!);
            await effect.ExecuteNonQueryAsync(CancellationToken.None);
        }

        await fenced.CommitAsync(CancellationToken.None);
    }
}
