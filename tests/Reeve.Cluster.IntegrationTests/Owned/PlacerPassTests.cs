using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;

namespace Int.Owned;

public sealed class PlacerPassTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private readonly PostgresFixture postgres;

    public PlacerPassTests(PostgresFixture postgres) => this.postgres = postgres;

    [Fact]
    public async Task A_member_that_released_at_once_is_not_assigned_the_key_again_within_the_window()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["widget"], "a", "b");
        rig.Start();

        await rig.Gate.RunAsync(0);
        var first = await rig.RowAsync("widget");
        Assert.Equal("a", first?.OwnerReplica);
        Assert.True(await rig.Store.ReleaseAsync("widget", "widget", "a", rig.Epochs["a"], first!.Value.Token, CancellationToken.None));

        await rig.Gate.RunAsync(1);
        var second = await rig.RowAsync("widget");
        Assert.Equal("b", second?.OwnerReplica);
        Assert.True(await rig.Store.ReleaseAsync("widget", "widget", "b", rig.Epochs["b"], second!.Value.Token, CancellationToken.None));

        await rig.Gate.RunAsync(2);
        Assert.Null((await rig.RowAsync("widget"))?.OwnerReplica);
        await rig.Gate.RunAsync(3);
        Assert.Null((await rig.RowAsync("widget"))?.OwnerReplica);

        rig.Time.Shift(Window);
        await rig.Gate.RunAsync(4);
        Assert.Equal("a", (await rig.RowAsync("widget"))?.OwnerReplica);
    }

    [Fact]
    public async Task A_pass_that_assigns_does_not_rebalance_and_the_next_one_does()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["k0", "k1", "k2", "k3"], "a", "b");
        foreach (var k in new[] { "k0", "k1", "k2" })
        {
            await rig.Db.CreateOwnedAsync(k, k, "a");
        }

        var k3 = await rig.Db.CreateOwnedAsync("k3", "k3", "b");
        rig.Start();

        await rig.Gate.RunAsync(0);
        Assert.Equal(0, await rig.RequestsAsync());

        rig.Time.Shift(Window);
        Assert.True(await rig.Store.ReleaseAsync("k3", "k3", "b", rig.Epochs["b"], k3, CancellationToken.None));
        await rig.Gate.RunAsync(1);
        Assert.Equal("b", (await rig.RowAsync("k3"))?.OwnerReplica);
        Assert.Equal(0, await rig.RequestsAsync());

        await rig.Gate.RunAsync(2);
        Assert.Equal(1, await rig.RequestsAsync());
    }

    [Fact]
    public async Task The_pass_reports_overdue_requests_and_the_end_of_the_tenure_empties_the_report()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["widget"], "a");
        var token = await rig.Db.CreateOwnedAsync("widget", "widget", "a");
        await rig.ExecAsync($"INSERT INTO reeve_cluster.release_request VALUES ('widget', 'widget', {token}, clock_timestamp() - interval '1 minute')");
        rig.Start();

        await rig.Gate.RunAsync(0);
        Assert.Equal(token, Assert.Single(rig.Report.Overdue).Token);

        await rig.Stop.CancelAsync();
        await rig.Running!.WaitAsync(Limit);
        Assert.Empty(rig.Report.Overdue);
    }

    [Fact]
    public async Task A_failed_rebalance_request_is_forgotten_and_the_next_pass_requests_again()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["k0", "k1"], "a", "b");
        await rig.Db.CreateOwnedAsync("k0", "k0", "a");
        await rig.Db.CreateOwnedAsync("k1", "k1", "a");
        rig.Start();

        await rig.Gate.RunAsync(0);
        rig.Time.Shift(Window);
        rig.Placer.Refusal = new OwnershipLostException("test: the request fails before the database");
        await rig.Gate.RunAsync(1);
        Assert.Equal(0, await rig.RequestsAsync());

        rig.Placer.Refusal = null;
        await rig.Gate.RunAsync(2);
        Assert.Equal(1, await rig.RequestsAsync());
    }

    [Fact]
    public async Task A_held_assignment_bars_nobody()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["widget"], "a");
        rig.Start();

        await rig.Gate.RunAsync(0);
        Assert.Equal("a", (await rig.RowAsync("widget"))?.OwnerReplica);
        await rig.Gate.RunAsync(1);

        await rig.ExecAsync("UPDATE reeve_cluster.member SET epoch = epoch + 100 WHERE replica_id = 'a'");
        await rig.Gate.RunAsync(2);
        var row = await rig.RowAsync("widget");
        Assert.Equal(("a", rig.Epochs["a"] + 100), (row?.OwnerReplica, row?.OwnerEpoch));
    }

    [Fact]
    public async Task An_assignment_nudges_its_target()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["widget"], "a");
        rig.Start();

        await rig.Gate.RunAsync(0);
        Assert.Equal("a", (await rig.RowAsync("widget"))?.OwnerReplica);
        Assert.Equal(["a"], rig.Nudges);
    }

    [Fact]
    public async Task A_stored_release_request_nudges_the_owner()
    {
        await using var rig = await Rig.CreateAsync(postgres, ["k0", "k1"], "a", "b");
        await rig.Db.CreateOwnedAsync("k0", "k0", "a");
        await rig.Db.CreateOwnedAsync("k1", "k1", "a");
        rig.Start();

        await rig.Gate.RunAsync(0);
        Assert.Empty(rig.Nudges);
        rig.Time.Shift(Window);
        await rig.Gate.RunAsync(1);
        Assert.Equal(1, await rig.RequestsAsync());
        Assert.Equal(["a"], rig.Nudges);
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required OwnedDatabase Db { get; init; }

        public required OwnedStore Store { get; init; }

        public required FencedOwner Placer { get; init; }

        public required PlacerUnit Unit { get; init; }

        public required ReleaseOverdueReport Report { get; init; }

        public required PassGate Gate { get; init; }

        public required ShiftedTime Time { get; init; }

        public required Dictionary<string, long> Epochs { get; init; }

        public required ConcurrentQueue<string?> Nudges { get; init; }

        public CancellationTokenSource Stop { get; } = new();

        public Task? Running { get; private set; }

        public static async Task<Rig> CreateAsync(PostgresFixture postgres, string[] kinds, params string[] members)
        {
            var db = await OwnedDatabase.CreateAsync(postgres);

            await db.AddMemberAsync("p");
            var epochs = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var member in members)
            {
                epochs[member] = await db.AddMemberAsync(member);
            }

            var capabilities = "{" + string.Join(",", kinds.Select(k => "unit:" + k)) + "}";
            await using (var connection = await db.OpenAsync())
            {
                await OwnedDatabase.ExecAsync(connection, $"UPDATE reeve_cluster.member SET serving_since = clock_timestamp(), capabilities = '{capabilities}' WHERE replica_id <> 'p'");
            }

            var placerToken = await db.CreateOwnedAsync(Placement.PlacerKind, Placement.PlacerKind, "p");
            var options = Options.Create(new ClusterOptions
            {
                ConnectionString = db.ConnectionString,
                FencedTransactionTimeout = TimeSpan.FromSeconds(30),
                PlacementInterval = TimeSpan.FromMilliseconds(100),
                StabilisationWindow = Window,
            });
            var store = new OwnedStore(new OwnedDataSource(options), options);
            var gate = new PassGate();
            var time = new ShiftedTime();
            var report = new ReleaseOverdueReport(NullLogger<ReleaseOverdueReport>.Instance);
            var wakeups = new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance);
            var nudges = new ConcurrentQueue<string?>();
            wakeups.Subscribe(WakeupKind.OwnerNudge, w => nudges.Enqueue(w.Target));
            var registered = kinds.Select(k => new UnitKind(k, typeof(object), new UnitKindOptions { Policy = AdoptionPolicy.Placer })).ToList();
            return new Rig
            {
                Db = db,
                Store = store,
                Placer = new FencedOwner(Placement.PlacerKind, Placement.PlacerKind, placerToken, db.Timeline),
                Unit = new PlacerUnit(registered, store, wakeups, gate, options, time, report, NullLogger<PlacerUnit>.Instance),
                Report = report,
                Gate = gate,
                Time = time,
                Epochs = epochs,
                Nudges = nudges,
            };
        }

        public void Start() => Running = Task.Run(() => Unit.OnAcquiredAsync(Placer, Stop.Token));

        public async Task<OwnedRow?> RowAsync(string key) => (await Store.ReadAsync(key, key, CancellationToken.None)).Row;

        public async Task<long> RequestsAsync()
        {
            await using var connection = await Db.OpenAsync();
            return await OwnedDatabase.ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.release_request");
        }

        public async Task ExecAsync(string sql)
        {
            await using var connection = await Db.OpenAsync();
            await OwnedDatabase.ExecAsync(connection, sql);
        }

        public async ValueTask DisposeAsync()
        {
            await Stop.CancelAsync();
            if (Running is { } running)
            {
                await running.WaitAsync(Limit);
            }

            Report.Dispose();
            Stop.Dispose();
        }
    }

    private sealed class PassGate : IFaultPoints
    {
        private readonly object sync = new();
        private readonly List<TaskCompletionSource> reached = [];
        private readonly List<TaskCompletionSource> released = [];
        private int passes;

        public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
        {
            if (point != OwnedFaultPoints.PlacerPassBeforeRead.Name)
            {
                return ValueTask.CompletedTask;
            }

            Task release;
            lock (sync)
            {
                var n = passes++;
                Ensure(n);
                reached[n].TrySetResult();
                release = released[n].Task;
            }

            return new ValueTask(release.WaitAsync(cancellationToken));
        }

        public async Task RunAsync(int n)
        {
            await Reached(n).WaitAsync(Limit);
            lock (sync)
            {
                Ensure(n);
                released[n].TrySetResult();
            }

            await Reached(n + 1).WaitAsync(Limit);
        }

        private Task Reached(int n)
        {
            lock (sync)
            {
                Ensure(n);
                return reached[n].Task;
            }
        }

        private void Ensure(int n)
        {
            while (reached.Count <= n)
            {
                reached.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                released.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            }
        }
    }

    private sealed class ShiftedTime : TimeProvider
    {
        private long shift;

        public void Shift(TimeSpan by) => Interlocked.Add(ref shift, (long)(by.TotalSeconds * TimestampFrequency));

        public override long GetTimestamp() => base.GetTimestamp() + Interlocked.Read(ref shift);
    }
}
