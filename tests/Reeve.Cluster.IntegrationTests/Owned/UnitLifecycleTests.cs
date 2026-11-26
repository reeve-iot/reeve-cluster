using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Int.Membership;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Int.Owned;

public sealed class UnitLifecycleTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string Kind = "widget";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly ProbeUnit unit = new();
    private TimeSpan startupBound = TimeSpan.Zero;
    private OwnedDatabase db = null!;
    private IHost host = null!;

    public UnitLifecycleTests(PostgresFixture postgres) => this.postgres = postgres;

    private OwnedStore Store => host.Services.GetRequiredService<OwnedStore>();

    private long Epoch => host.Services.GetRequiredService<LocalPhaseGate>().Epoch!.Value;

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            startupBound = StartupBound.For(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddClusterKeyedResource<ProbeUnit>(Kind, o => o.Policy = AdoptionPolicy.None);
        host = builder.Build();
        await host.StartAsync().WaitAsync(startupBound);
        await AwaitServingAsync(host.Services.GetRequiredService<LocalPhaseGate>());
    }

    public async Task DisposeAsync()
    {
        unit.ReleaseAll();
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(bound.Token);
        host.Dispose();
    }

    private async Task AwaitServingAsync(LocalPhaseGate gate)
    {
        using var bound = new CancellationTokenSource(startupBound);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (gate.Phase == ReplicaPhase.Serving)
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    private async Task<long> AssignAsync(string key, string kind = Kind)
    {
        var read = await Store.ReadAsync(kind, key, CancellationToken.None);
        var token = read.Row is { } row
            ? await Store.TakeAsync(kind, key, row, "r1", Epoch, CancellationToken.None)
            : await Store.InsertAsync(kind, key, "r1", Epoch, CancellationToken.None);
        return token ?? throw new InvalidOperationException($"{kind}/{key} could not be assigned");
    }

    private async Task<OwnedRead> AwaitConditionAsync(string key, OwnedCondition condition, string kind = Kind)
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
    public async Task The_join_advertises_the_kind_and_the_poll_starts_an_assigned_unit()
    {
        await using var connection = await db.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand("SELECT capabilities FROM reeve_cluster.member WHERE replica_id = 'r1'", connection);
        Assert.Contains("unit:widget", (string[])(await command.ExecuteScalarAsync())!);

        var token = await AssignAsync("a");

        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal((Kind, "a", token), (started.Kind, started.Key, started.Token));
    }

    [Fact]
    public async Task A_release_request_stops_the_unit_runs_OnLostAsync_and_frees_the_row_with_its_request()
    {
        var token = await AssignAsync("a");
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        Assert.True(await Store.RequestReleaseAsync(Kind, "a", token, CancellationToken.None));

        var freed = await AwaitConditionAsync("a", OwnedCondition.Free);
        Assert.True(started.Stop.IsCancellationRequested);
        Assert.Contains("a", unit.LostKeys);
        Assert.Equal(token, freed.Row!.Value.Token);
        Assert.Null(freed.RequestToken);
    }

    [Fact]
    public async Task I26_a_hung_OnLostAsync_frees_the_row_at_StopBudget_and_holds_the_key_back_until_it_returns()
    {
        var stopBudget = host.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<ClusterOptions>>().Value.StopBudget;
        var token = await AssignAsync("a");
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");

        await Store.RequestReleaseAsync(Kind, "a", token, CancellationToken.None);
        var lostAt = await unit.LostStarted("a").WaitAsync(Wait);
        await AwaitConditionAsync("a", OwnedCondition.Free);

        Assert.True(lostAt.Elapsed >= stopBudget / 2, $"released {lostAt.Elapsed} after OnLostAsync started");

        var again = await AssignAsync("a");
        var released = await AwaitConditionAsync("a", OwnedCondition.Free);
        Assert.Equal(again, released.Row!.Value.Token);
        Assert.False(unit.Acquired.Reader.TryRead(out _));

        unit.ReleaseLost("a");
        await unit.LostReturned("a").WaitAsync(Wait);
        using var bound = new CancellationTokenSource(Wait);
        long? started = null;
        while (started is null)
        {
            started = await StartedOrReleasedAsync("a", await AssignAsync("a"), bound.Token);
        }
    }

    private async Task<long?> StartedOrReleasedAsync(string key, long token, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (unit.Acquired.Reader.TryRead(out var started))
            {
                Assert.Equal((key, token), (started.Key, started.Token));
                return started.Token;
            }

            if ((await Store.ReadAsync(Kind, key, cancellationToken)).Condition == OwnedCondition.Free)
            {
                return null;
            }

            await Task.Delay(50, cancellationToken);
        }
    }

    [Fact]
    public async Task A_row_no_longer_this_tenures_stops_the_unit_as_lost_and_its_new_tenure_is_released_at_once()
    {
        var token = await AssignAsync("a");
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await using (var connection = await db.OpenAsync())
        {
            await using var command = new Npgsql.NpgsqlCommand(
                "UPDATE reeve_cluster.owned SET token = token + 1 WHERE kind = 'widget' AND key = 'a'", connection);
            await command.ExecuteNonQueryAsync();
        }

        var freed = await AwaitConditionAsync("a", OwnedCondition.Free);
        Assert.Equal(token + 1, freed.Row!.Value.Token);
        Assert.True(started.Stop.IsCancellationRequested);
        await unit.LostReturned("a").WaitAsync(Wait);
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task A_row_of_a_kind_not_registered_here_is_released()
    {
        await AssignAsync("a", kind: "gizmo");

        await AwaitConditionAsync("a", OwnedCondition.Free, kind: "gizmo");
        Assert.False(unit.Acquired.Reader.TryRead(out _));
    }

    [Fact]
    public async Task The_poll_reads_a_request_only_for_the_rows_tenure()
    {
        var first = await AssignAsync("a");
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var second = await AssignAsync("b");
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await Store.RequestReleaseAsync(Kind, "b", second - 1, CancellationToken.None);
        await Store.RequestReleaseAsync(Kind, "a", first, CancellationToken.None);

        var held = await Store.ReadHeldAsync("r1", Epoch, CancellationToken.None);
        Assert.Contains(held, r => r.Kind == Kind && r.Key == "b" && r.Token == second && !r.ReleaseRequested);
        Assert.DoesNotContain(held, r => r.Key == "a" && !r.ReleaseRequested);
        Assert.Empty(await Store.ReadHeldAsync("r1", Epoch + 1, CancellationToken.None));
    }
}
