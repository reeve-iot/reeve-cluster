using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Int.Membership;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class KeyedResourceTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string Adopted = "conn";
    private const string Waiting = "sess";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly ContextUnit unit = new();
    private OwnedDatabase db = null!;
    private IHost host = null!;

    public KeyedResourceTests(PostgresFixture postgres) => this.postgres = postgres;

    private OwnedStore Store => host.Services.GetRequiredService<OwnedStore>();

    private IKeyedResources Creator => host.Services.GetRequiredService<IKeyedResources>();

    private UnitRuntime Runtime => host.Services.GetRequiredService<UnitRuntime>();

    private long Epoch => host.Services.GetRequiredService<LocalPhaseGate>().Epoch!.Value;

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);

        builder.Services.AddClusterKeyedResource<FreeRowSweep>(FreeRowSweep.Kind, o =>
        {
            o.Convergent = true;
            o.Policy = AdoptionPolicy.None;
        });
        var startupBound = TimeSpan.Zero;
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            startupBound = StartupBound.For(o);
            o.ReplicaId = "r1";
            o.ConnectionString = db.ConnectionString;
            o.OwnedRowRetention = TimeSpan.FromMilliseconds(1);
        });
        builder.Services.AddClusterKeyedResource<ContextUnit>(Adopted, k => k.Policy = AdoptionPolicy.AnyEligible);
        builder.Services.AddClusterKeyedResource<ContextUnit>(Waiting, k => k.Policy = AdoptionPolicy.None);
        host = builder.Build();
        await host.StartAsync().WaitAsync(startupBound);
        await AwaitServingAsync(host.Services.GetRequiredService<LocalPhaseGate>(), startupBound);
    }

    public async Task DisposeAsync()
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(bound.Token);
        host.Dispose();
    }

    private static async Task AwaitServingAsync(LocalPhaseGate gate, TimeSpan bound)
    {
        using var cancellation = new CancellationTokenSource(bound);
        while (true)
        {
            var change = gate.NextPhaseChange;
            if (gate.Phase == ReplicaPhase.Serving)
            {
                return;
            }

            await change.WaitAsync(cancellation.Token);
        }
    }

    private async Task<OwnedRead> AwaitAsync(string kind, string key, Func<OwnedRead, bool> state)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var read = await Store.ReadAsync(kind, key, bound.Token);
            if (state(read))
            {
                return read;
            }

            await Task.Delay(50, bound.Token);
        }
    }

    private Task<OwnedRead> AwaitAsync(string kind, string key, OwnedCondition condition) => AwaitAsync(kind, key, r => r.Condition == condition);

    private async Task<(OwnedContext Context, CancellationToken Stop)> StartedAsync(string kind, string key)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var started = await unit.Acquired.Reader.ReadAsync(bound.Token);
            if ((started.Context.Kind, started.Context.Key) == (kind, key))
            {
                return started;
            }
        }
    }

    private async Task PollsAsync(int count)
    {
        var polls = Runtime.Polls;
        using var bound = new CancellationTokenSource(Wait);
        while (Runtime.Polls < polls + count)
        {
            Runtime.Nudge();
            await Task.Delay(20, bound.Token);
        }
    }

    private async Task AwaitStopEndedAsync(string kind, string key)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (Runtime.Holds(kind, key))
        {
            await Task.Delay(20, bound.Token);
        }
    }

    private async Task<long> OrphanOfR0Async(string kind, string key)
    {
        if (!await MemberExistsAsync("r0"))
        {
            await db.AddMemberAsync("r0");
        }

        await using var connection = await db.OpenAsync();
        await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + interval '1 minute' WHERE replica_id = 'r0'");
        var token = await db.CreateOwnedAsync(kind, key, "r0");
        await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'r0'");
        return token;
    }

    private async Task<bool> MemberExistsAsync(string replica)
    {
        await using var connection = await db.OpenAsync();
        return await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM reeve_cluster.member WHERE replica_id = @r)", ("r", replica));
    }

    [Fact]
    public async Task O01_a_creation_inserts_the_key_and_starts_it_and_a_second_one_is_AlreadyOwned()
    {
        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Adopted, "a"));

        var started = await StartedAsync(Adopted, "a");
        var read = await Store.ReadAsync(Adopted, "a", CancellationToken.None);
        Assert.Equal(new OwnedRow("r1", Epoch, started.Context.Token), read.Row);
        Assert.Equal(CreateOutcome.AlreadyOwned, await Creator.CreateAsync(Adopted, "a"));
        Assert.Equal(read.Row, (await Store.ReadAsync(Adopted, "a", CancellationToken.None)).Row);
    }

    [Fact]
    public async Task O02_a_creators_insert_on_an_orphaned_row_takes_it_over()
    {
        var old = await OrphanOfR0Async(Waiting, "a");

        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Waiting, "a"));

        var started = await StartedAsync(Waiting, "a");
        Assert.True(started.Context.Token > old);
        Assert.Equal(new OwnedRow("r1", Epoch, started.Context.Token), (await Store.ReadAsync(Waiting, "a", CancellationToken.None)).Row);
    }

    [Fact]
    public async Task I10_the_tokens_of_one_key_increase_across_every_end_sweep_and_re_creation()
    {
        var tokens = new List<long>();
        async Task CreateAndEndAsync()
        {
            Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Waiting, "a"));
            var started = await StartedAsync(Waiting, "a");
            tokens.Add(started.Context.Token);

            await started.Context.EndAsync();

            Assert.True(started.Stop.IsCancellationRequested);
            Assert.Equal(new OwnedRow(null, null, started.Context.Token), (await Store.ReadAsync(Waiting, "a", CancellationToken.None)).Row);
            await unit.LostReturned(started.Context.Token).WaitAsync(Wait);
            await AwaitStopEndedAsync(Waiting, "a");
        }

        await CreateAndEndAsync();
        await CreateAndEndAsync();
        await AwaitAsync(Waiting, "a", r => r.Row is { OwnerReplica: null });
        var sweep = host.Services.GetRequiredService<FreeRowSweep>();
        await AwaitSweptAsync(sweep, Waiting, "a");
        await CreateAndEndAsync();

        Assert.Equal(3, tokens.Count);
        Assert.True(tokens[0] < tokens[1] && tokens[1] < tokens[2], string.Join(", ", tokens));
    }

    private async Task AwaitSweptAsync(FreeRowSweep sweep, string kind, string key)
    {
        using var bound = new CancellationTokenSource(Wait);
        while ((await Store.ReadAsync(kind, key, bound.Token)).Condition != OwnedCondition.Absent)
        {
            await sweep.PassAsync(bound.Token);
            await Task.Delay(20, bound.Token);
        }
    }

    [Fact]
    public async Task O08_an_AnyEligible_orphan_is_adopted_and_started_and_a_None_orphan_stays()
    {
        var waiting = await OrphanOfR0Async(Waiting, "a");
        var orphaned = await OrphanOfR0Async(Adopted, "a");

        var started = await StartedAsync(Adopted, "a");

        Assert.True(started.Context.Token > orphaned);
        Assert.Equal(new OwnedRow("r1", Epoch, started.Context.Token), (await Store.ReadAsync(Adopted, "a", CancellationToken.None)).Row);
        await PollsAsync(2);

        var stays = await Store.ReadAsync(Waiting, "a", CancellationToken.None);
        Assert.Equal((OwnedCondition.Orphaned, new OwnedRow("r0", stays.Row!.Value.OwnerEpoch, waiting)), (stays.Condition, stays.Row));
    }

    [Fact]
    public async Task O10_the_clean_up_frees_an_orphaned_row_and_leaves_a_held_one()
    {
        var token = await OrphanOfR0Async(Waiting, "a");
        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Waiting, "b"));

        Assert.True(await Creator.CleanUpAsync(Waiting, "a"));
        Assert.False(await Creator.CleanUpAsync(Waiting, "b"));

        Assert.Equal(new OwnedRow(null, null, token), (await Store.ReadAsync(Waiting, "a", CancellationToken.None)).Row);
        Assert.Equal(OwnedCondition.Owned, (await Store.ReadAsync(Waiting, "b", CancellationToken.None)).Condition);
        await Assert.ThrowsAsync<ArgumentException>(() => Creator.CleanUpAsync(Adopted, "a"));
    }

    [Fact]
    public async Task O12_the_sweep_on_a_hand_assigned_row_deletes_free_rows_past_the_retention()
    {
        var freed = await OrphanOfR0Async("gone", "x");
        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'gone'");
            await ExecAsync(connection, "INSERT INTO reeve_cluster.release_request VALUES ('gone', 'x', @t, clock_timestamp())", ("t", freed));
        }

        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Adopted, "kept"));

        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, "SELECT pg_sleep(0.05)");
        }

        Assert.NotNull(await Store.InsertAsync(FreeRowSweep.Kind, FreeRowSweep.Kind, "r1", Epoch, CancellationToken.None));

        await AwaitAsync("gone", "x", OwnedCondition.Absent);
        await using (var connection = await db.OpenAsync())
        {
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.release_request"));
        }

        Assert.Equal(OwnedCondition.Owned, (await Store.ReadAsync(Adopted, "kept", CancellationToken.None)).Condition);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_sweep_waits_for_an_open_statement_that_drew_a_token_and_deletes_only_after_its_rollback(bool commit)
    {
        var token = await OrphanOfR0Async(Waiting, "a");
        Assert.True(await Creator.CleanUpAsync(Waiting, "a"));
        await AwaitAsync(Waiting, "a", r => r.Row is { OwnerReplica: null });
        await using var adopter = await db.OpenAsync();
        var adoption = await adopter.BeginTransactionAsync();
        var drawn = await AdoptAsync(adopter, Waiting, "a", token, "r0");

        var sweep = Store.SweepAsync(Waiting, "a", token, CancellationToken.None);
        await db.WaitForALockWaiterAsync();
        Assert.False(sweep.IsCompleted);
        if (commit)
        {
            await adoption.CommitAsync();
        }
        else
        {
            await adoption.RollbackAsync();
        }

        Assert.Equal(!commit, await sweep.WaitAsync(Wait));
        var after = await Store.ReadAsync(Waiting, "a", CancellationToken.None);
        Assert.Equal(commit ? new OwnedRow("r0", after.Row?.OwnerEpoch, drawn!.Value) : (OwnedRow?)null, after.Row);
    }

    [Fact]
    public async Task O08_the_tombstone_of_an_ended_AnyEligible_key_is_not_adopted_and_an_orphan_beside_it_is()
    {
        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Adopted, "t"));
        var started = await StartedAsync(Adopted, "t");
        await started.Context.EndAsync();
        await unit.LostReturned(started.Context.Token).WaitAsync(Wait);
        await AwaitStopEndedAsync(Adopted, "t");

        await PollsAsync(3);

        Assert.Equal(new OwnedRow(null, null, started.Context.Token), (await Store.ReadAsync(Adopted, "t", CancellationToken.None)).Row);
        var orphan = await OrphanOfR0Async(Adopted, "o");
        var adopted = await StartedAsync(Adopted, "o");
        Assert.True(adopted.Context.Token > orphan);
        Assert.Equal(new OwnedRow(null, null, started.Context.Token), (await Store.ReadAsync(Adopted, "t", CancellationToken.None)).Row);
    }

    [Theory]
    [InlineData("draining")]
    [InlineData("rejoined")]
    public async Task O08_the_row_of_a_draining_owner_or_of_an_owners_earlier_epoch_is_adopted(string owner)
    {
        await db.AddMemberAsync("r7");
        var old = await db.CreateOwnedAsync(Adopted, "d", "r7");
        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, owner == "draining"
                ? "UPDATE reeve_cluster.member SET state = 'draining' WHERE replica_id = 'r7'"
                : "UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'r7'");
        }

        var started = await StartedAsync(Adopted, "d");

        Assert.True(started.Context.Token > old);
        Assert.Equal(new OwnedRow("r1", Epoch, started.Context.Token), (await Store.ReadAsync(Adopted, "d", CancellationToken.None)).Row);
    }

    [Fact]
    public async Task O11_an_end_called_with_the_units_own_stop_token_ends_the_resource()
    {
        Assert.Equal(CreateOutcome.Created, await Creator.CreateAsync(Adopted, "s"));
        var started = await StartedAsync(Adopted, "s");

        await started.Context.EndAsync(started.Stop);

        Assert.Equal(new OwnedRow(null, null, started.Context.Token), (await Store.ReadAsync(Adopted, "s", CancellationToken.None)).Row);
    }

    internal sealed class ContextUnit : IOwnedUnit
    {
        private readonly ConcurrentDictionary<long, TaskCompletionSource> returned = new();

        public Channel<(OwnedContext Context, CancellationToken Stop)> Acquired { get; } = Channel.CreateUnbounded<(OwnedContext, CancellationToken)>();

        public Task LostReturned(long token) => Of(token).Task;

        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            Acquired.Writer.TryWrite((context, stop));
            return Task.Delay(Timeout.Infinite, stop);
        }

        public Task OnLostAsync(OwnedContext context)
        {
            Of(context.Token).TrySetResult();
            return Task.CompletedTask;
        }

        private TaskCompletionSource Of(long token) =>
            returned.GetOrAdd(token, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}

public sealed class KeyedResourceDrainTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string Kind = "conn";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;
    private readonly KeyedResourceTests.ContextUnit unit1 = new();
    private readonly KeyedResourceTests.ContextUnit unit2 = new();
    private OwnedDatabase db = null!;
    private IHost host1 = null!;
    private IHost host2 = null!;
    private bool host1Stopped;

    public KeyedResourceDrainTests(PostgresFixture postgres) => this.postgres = postgres;

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        host1 = await StartAsync("r1", unit1);
        host2 = await StartAsync("r2", unit2);
    }

    public async Task DisposeAsync()
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        if (!host1Stopped)
        {
            await host1.StopAsync(bound.Token);
        }

        await host2.StopAsync(bound.Token);
        host1.Dispose();
        host2.Dispose();
    }

    private async Task<IHost> StartAsync(string replica, KeyedResourceTests.ContextUnit unit)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton(unit);
        builder.Services.AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), o =>
        {
            TestProfile.ApplyStallTolerant(o);
            o.ReplicaId = replica;
            o.ConnectionString = db.ConnectionString;
        });
        builder.Services.AddClusterKeyedResource<KeyedResourceTests.ContextUnit>(Kind, k => k.Policy = AdoptionPolicy.AnyEligible);
        var host = builder.Build();
        await host.StartAsync().WaitAsync(Wait);
        var gate = host.Services.GetRequiredService<LocalPhaseGate>();
        using var bound = new CancellationTokenSource(Wait);
        while (true)
        {
            var changed = gate.NextPhaseChange;
            if (gate.Phase == ReplicaPhase.Serving)
            {
                break;
            }

            await changed.WaitAsync(bound.Token);
        }

        return host;
    }

    [Fact]
    public async Task Q1_a_drained_owners_AnyEligible_row_is_adopted_by_another_member_within_the_failover_bound_with_no_creation()
    {
        Assert.Equal(CreateOutcome.Created, await host1.Services.GetRequiredService<IKeyedResources>().CreateAsync(Kind, "a"));
        var first = await unit1.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var options = host2.Services.GetRequiredService<IOptions<ClusterOptions>>().Value;
        var bound = host2.Services.GetServices<UnitKind>().Single(k => k.Kind == Kind).FailoverBound(options);

        var sinceDrain = Stopwatch.StartNew();
        using (var stop = new CancellationTokenSource(Wait))
        {
            await host1.StopAsync(stop.Token);
            host1Stopped = true;
        }

        var adopted = await unit2.Acquired.Reader.ReadAsync().AsTask().WaitAsync(bound + Wait);
        Assert.True(sinceDrain.Elapsed <= bound, $"Adopted after {sinceDrain.Elapsed}, past the failover bound {bound}.");
        Assert.Equal(("a", true), (adopted.Context.Key, adopted.Context.Token > first.Context.Token));
        await unit1.LostReturned(first.Context.Token).WaitAsync(Wait);
        var store = host2.Services.GetRequiredService<OwnedStore>();
        var epoch2 = host2.Services.GetRequiredService<LocalPhaseGate>().Epoch!.Value;
        Assert.Equal(new OwnedRow("r2", epoch2, adopted.Context.Token), (await store.ReadAsync(Kind, "a", CancellationToken.None)).Row);
    }
}

public sealed class CreatorInsertBoundTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan OwnershipTimeout = Ftt + TimeSpan.FromSeconds(1);

    private readonly PostgresFixture postgres;

    public CreatorInsertBoundTests(PostgresFixture postgres) => this.postgres = postgres;

    [Fact]
    public async Task O5_a_creators_insert_held_open_past_its_bound_is_ended_by_the_database_and_the_sweeps_delete_then_proceeds()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        var epoch = await db.AddMemberAsync("a");
        var options = Options.Create(new ClusterOptions
        {
            ConnectionString = db.ConnectionString,
            FencedTransactionTimeout = Ftt,
            OwnedRowRetention = TimeSpan.FromMilliseconds(1),
        });
        await using var data = new OwnedDataSource(options);
        var store = new OwnedStore(data, options);

        await using var holder = await db.OpenAsync();
        var held = await holder.BeginTransactionAsync();
        var first = await ScalarAsync<long>(
            holder, "INSERT INTO reeve_cluster.owned (kind, key, token) VALUES ('conn', 'k', reeve_cluster.draw_token()) RETURNING token");

        var elapsed = Stopwatch.StartNew();
        var insert = store.InsertAsync("conn", "k", "a", epoch, CancellationToken.None);
        await db.WaitForALockWaiterAsync();
        var drawn = await db.LastTokenAsync();
        Assert.True(drawn > SequenceValueOf(first));
        var ended = await Record.ExceptionAsync(() => insert.WaitAsync((5 * OwnershipTimeout) + TimeSpan.FromSeconds(10)));
        Assert.Equal("25P04", (ended as PostgresException ?? ended?.InnerException as PostgresException)?.SqlState);
        Assert.InRange(elapsed.Elapsed, OwnershipTimeout - TimeSpan.FromMilliseconds(100), (5 * OwnershipTimeout) + TimeSpan.FromSeconds(10));

        await held.CommitAsync();
        using (var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        {
            while (!await store.SweepAsync("conn", "k", first, bound.Token))
            {
                await Task.Delay(20, bound.Token);
            }
        }

        var recreated = await store.InsertAsync("conn", "k", "a", epoch, CancellationToken.None);
        Assert.True(SequenceValueOf(recreated!.Value) > drawn, $"{recreated} <= {drawn}");
        Assert.Equal(new OwnedRow("a", epoch, recreated!.Value), (await store.ReadAsync("conn", "k", CancellationToken.None)).Row);
    }
}
