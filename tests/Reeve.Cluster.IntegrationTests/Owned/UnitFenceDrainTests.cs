using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Int.Owned;

public sealed class UnitFenceDrainTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly ProbeUnit unit = new();
    private OwnedDatabase db = null!;
    private IHost host = null!;

    public UnitFenceDrainTests(PostgresFixture postgres) => this.postgres = postgres;

    private OwnedStore Store => host.Services.GetRequiredService<OwnedStore>();

    private LocalPhaseGate Gate => host.Services.GetRequiredService<LocalPhaseGate>();

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddClusterKeyedResource<ProbeUnit>("widget", o => o.Policy = AdoptionPolicy.None);
        builder.Services.AddClusterKeyedResource<ProbeUnit>("sweep", o =>
        {
            o.Policy = AdoptionPolicy.None;
            o.Convergent = true;
        });
        host = builder.Build();
        await host.StartAsync().WaitAsync(Wait);
        await AwaitAsync(g => g.Phase == ReplicaPhase.Serving);
    }

    public async Task DisposeAsync()
    {
        unit.ReleaseAll();
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(bound.Token);
        host.Dispose();
    }

    private async Task AwaitAsync(Func<LocalPhaseGate, bool> condition)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var change = Gate.NextPhaseChange;
            if (condition(Gate))
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private async Task<long> AssignAsync(string kind, string key)
    {
        var epoch = Gate.Epoch!.Value;
        var read = await Store.ReadAsync(kind, key, CancellationToken.None);
        var token = read.Row is { } row
            ? await Store.TakeAsync(kind, key, row, "r1", epoch, CancellationToken.None)
            : await Store.InsertAsync(kind, key, "r1", epoch, CancellationToken.None);
        return token ?? throw new InvalidOperationException($"{kind}/{key} could not be assigned");
    }

    private async Task<OwnedRead> AwaitConditionAsync(string kind, string key, OwnedCondition condition)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var read = await Store.ReadAsync(kind, key, bound.Token);
            if (read.Condition == condition)
            {
                return read;
            }

            await Task.Delay(50, bound.Token);
        }
    }

    [Fact]
    public async Task A_drain_reaches_free_rows_without_a_placer_and_leaves_its_convergent_rows_to_be_taken_over()
    {
        var a = await AssignAsync("widget", "a");
        var b = await AssignAsync("widget", "b");
        await AssignAsync("sweep", "s");
        for (var i = 0; i < 3; i++)
        {
            await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        }

        await host.StopAsync().WaitAsync(Wait);

        Assert.Equal(ReplicaPhase.Exited, Gate.Phase);
        Assert.Equal((OwnedCondition.Free, a), await ReadAsync("widget", "a"));
        Assert.Equal((OwnedCondition.Free, b), await ReadAsync("widget", "b"));
        Assert.Equal(["a", "b", "s"], unit.LostKeys.Order());

        Assert.Equal(OwnedCondition.Orphaned, (await ReadAsync("sweep", "s")).Condition);
    }

    private async Task<(OwnedCondition Condition, long Token)> ReadAsync(string kind, string key)
    {
        var read = await Store.ReadAsync(kind, key, CancellationToken.None);
        return (read.Condition, read.Row!.Value.Token);
    }

    [Fact]
    public async Task A_units_stop_that_hangs_at_a_fence_holds_neither_the_fence_nor_the_key_on_another_tenure()
    {
        await AssignAsync("widget", "a");
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");
        var first = Gate.Epoch!.Value;

        Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart, "the test's fence"));
        Assert.True(started.Stop.IsCancellationRequested);
        await unit.LostStarted("a").WaitAsync(Wait);
        await AwaitAsync(g => g.Phase == ReplicaPhase.Serving && g.Epoch > first);

        var again = await AssignAsync("widget", "a");
        Assert.Equal(again, (await AwaitConditionAsync("widget", "a", OwnedCondition.Free)).Row!.Value.Token);
        Assert.False(unit.Acquired.Reader.TryRead(out _));

        unit.ReleaseLost("a");
        await unit.LostReturned("a").WaitAsync(Wait);
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var token = await AssignAsync("widget", "a");
            var read = await AwaitAnyAsync("a", token, bound.Token);
            if (read is { } next)
            {
                Assert.Equal(token, next.Token);
                return;
            }
        }
    }

    private async Task<(string Kind, string Key, long Token, CancellationToken Stop)?> AwaitAnyAsync(string key, long token, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (unit.Acquired.Reader.TryRead(out var started))
            {
                return started;
            }

            var read = await Store.ReadAsync("widget", key, cancellationToken);
            if (read.Condition == OwnedCondition.Free && read.Row!.Value.Token == token)
            {
                return null;
            }

            await Task.Delay(50, cancellationToken);
        }
    }
}
