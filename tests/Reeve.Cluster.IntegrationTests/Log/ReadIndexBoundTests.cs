using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ReadIndexBoundTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan InTime = TimeSpan.FromSeconds(20);

    private static readonly TimeSpan Pending = TimeSpan.FromMilliseconds(300);

    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public ReadIndexBoundTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            try
            {
                await item.DisposeAsync();
            }
            catch (Exception)
            {
            }
        }
    }

    private async Task<ApplierRig> ServingRigAsync(Action<ClusterOptions>? configure = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, configure, new FakeTimeProvider());
        owned.Add(rig);
        await rig.LoadAsync();
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        return rig;
    }

    private ReadIndex Index(ApplierRig rig, IReadIndexStore store, IModelEventSink events)
    {
        var index = new ReadIndex(
            rig.Gate, rig.Store, store, rig.Channel, Options.Create(rig.Options), rig.Time,
            NoFaultPoints.Instance, events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        return index;
    }

    private static ClusterScope Scope() => new(ClusterScopeKind.ClientRequest);

    private (HoldingProxy Proxy, ClusterDataSource Data) Proxied(ApplierRig rig, bool noResetOnClose = false)
    {
        var server = new NpgsqlConnectionStringBuilder(rig.Options.ConnectionString);
        var proxy = new HoldingProxy(server.Host!, server.Port);
        owned.Add(proxy);
        var proxied = new NpgsqlConnectionStringBuilder(rig.Options.ConnectionString)
        {
            Host = "127.0.0.1",
            Port = proxy.Port,
            Timeout = 60,
            NoResetOnClose = noResetOnClose,
        }.ConnectionString;
        var data = new ClusterDataSource(new ClusterOptions { ConnectionString = proxied });
        owned.Add(data);
        return (proxy, data);
    }

    private static async Task<string> TimeSilentDatabaseAsync(ReadIndex index, HoldingProxy proxy, FakeTimeProvider time, Func<bool> reached)
    {
        proxy.Hold();
        var entering = index.EnterAsync(Scope(), wait: true, CancellationToken.None);
        await Poll.UntilAsync(reached, "the read at the stopped database");
        Assert.False(entering.IsCompleted, "The request answered before its bound.");
        time.Advance(Bound - TimeSpan.FromMilliseconds(1));
        await Task.WhenAny(entering, Task.Delay(Pending));
        Assert.False(entering.IsCompleted, "The request answered before its bound.");
        time.Advance(TimeSpan.FromMilliseconds(1));
        var done = await Task.WhenAny(entering, Task.Delay(InTime)) == entering;
        var opened = proxy.AcceptedWhileHeld;
        proxy.Release();
        string outcome;
        try
        {
            outcome = (await entering.WaitAsync(Wait)).Outcome.ToString();
        }
        catch (Exception e)
        {
            outcome = e.GetType().Name;
        }

        var verdict = done && outcome == nameof(ReadIndexOutcome.Behind) ? "IN TIME" : "LATE";
        return $"{verdict}: ReadIndexTimeout {Bound.TotalMilliseconds:F0} ms; answered within {InTime.TotalSeconds:F0} s of the bound: {done}; "
            + $"answer {outcome}; connections the proxy accepted while it held, an open's or a cancel request's: {opened}";
    }

    [Fact]
    public async Task Behind_comes_at_the_bound_when_the_database_stops_answering_a_pooled_connection()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = Bound);
        var (proxy, data) = Proxied(rig);
        var index = Index(rig, new ReadIndexStore(data), new RecordedEvents());
        Assert.Equal(ReadIndexOutcome.Pinned, (await index.EnterAsync(Scope(), wait: true, CancellationToken.None).WaitAsync(Wait)).Outcome);

        var report = await TimeSilentDatabaseAsync(index, proxy, (FakeTimeProvider)rig.Time, () => proxy.BytesHeld || proxy.AcceptedWhileHeld > 0);

        Assert.True(report.StartsWith("IN TIME", StringComparison.Ordinal), report);
    }

    [Fact]
    public async Task Behind_comes_at_the_bound_when_the_database_stops_answering_an_open()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = Bound);
        var (proxy, data) = Proxied(rig);
        var index = Index(rig, new ReadIndexStore(data), new RecordedEvents());

        var report = await TimeSilentDatabaseAsync(index, proxy, (FakeTimeProvider)rig.Time, () => proxy.AcceptedWhileHeld > 0);

        Assert.True(report.StartsWith("IN TIME", StringComparison.Ordinal), report);
    }

    [Fact]
    public async Task Behind_comes_at_the_bound_when_a_live_server_holds_the_head_read_on_a_lock()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = Bound);
        var index = Index(rig, new ReadIndexStore(rig.Data), new RecordedEvents());
        await using var locker = await rig.Log.OpenAsync();
        await using var transaction = await locker.BeginTransactionAsync();
        await using (var lockTable = new NpgsqlCommand("LOCK TABLE reeve_cluster.change_log IN ACCESS EXCLUSIVE MODE", locker, transaction))
        {
            await lockTable.ExecuteNonQueryAsync();
        }

        var entering = index.EnterAsync(Scope(), wait: true, CancellationToken.None);
        await Poll.UntilAsync(async () => await rig.Log.ScalarAsync<long>(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'") > 0, "the head read waiting for the lock");
        ((FakeTimeProvider)rig.Time).Advance(Bound);
        var done = await Task.WhenAny(entering, Task.Delay(InTime)) == entering;
        await transaction.RollbackAsync();
        var result = await entering.WaitAsync(Wait);

        Assert.True(
            done && result.Outcome == ReadIndexOutcome.Behind,
            $"ReadIndexTimeout {Bound.TotalMilliseconds:F0} ms; answered {result.Outcome}, within {InTime.TotalSeconds:F0} s of the bound: {done}");
    }

    [Fact]
    public async Task The_harness_reads_the_generation_the_request_read_from_its_QRead()
    {
        var rig = await ServingRigAsync();
        var log = new ModelEventLog(new RunSequence());
        var index = Index(rig, new ReadIndexStore(rig.Data), new ReplicaModelEvents("r1", TimeProvider.System, log, () => true));
        var scope = Scope();

        var result = await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);

        var read = Assert.Single(log.GenerationsRead());
        Assert.Equal(scope.Id, read.Request);
        Assert.Equal(result.Generation!.Head, read.Head);
        Assert.Equal(((Generation)result.Generation).Id, read.Generation);
        foreach (var e in log.Snapshot().Where(e => e.Action.StartsWith('Q')))
        {
            var request = e.State?.GetValueOrDefault(ObservationKeys.Request);
            Assert.True(
                Equals(request, scope.Id),
                $"{e.Action} carries request {request ?? "(none)"} ({request?.GetType().Name ?? "null"}), not the scope's {scope.Id}");
        }
    }

    [Fact]
    public async Task An_append_in_flight_is_not_waited_for_and_a_request_after_its_commit_waits_for_it()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = TimeSpan.FromSeconds(10));
        var events = new RecordedEvents();
        var index = Index(rig, new ReadIndexStore(rig.Data), events);
        var before = rig.Store.Current!;
        await using var writer = await rig.Log.OpenAsync();
        await using var transaction = await writer.BeginTransactionAsync();
        long inFlight;
        await using (var append = new NpgsqlCommand(
            "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('a', 'k') RETURNING seq", writer, transaction))
        {
            inFlight = (long)(await append.ExecuteScalarAsync())!;
        }

        var first = await index.EnterAsync(Scope(), wait: true, CancellationToken.None).WaitAsync(Wait);
        Assert.Equal(ReadIndexOutcome.Pinned, first.Outcome);
        Assert.Same(before, first.Generation);
        Assert.True(before.Head < inFlight);

        await transaction.CommitAsync();
        var second = index.EnterAsync(Scope(), wait: true, CancellationToken.None);
        await Poll.UntilAsync(() => events.All.Count(e => e.Action == "QStart") == 2, "the second request's head read");
        await Task.WhenAny(second, Task.Delay(Pending));
        Assert.False(second.IsCompleted, "a request that started after the commit did not wait for it");
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        var result = await second.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.True(result.Generation!.Head >= inFlight, $"pinned head {result.Generation.Head}, the committed append {inFlight}");
    }

    [Fact]
    public async Task A_generation_whose_head_entry_is_gone_is_not_pinned()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = Bound);
        var events = new RecordedEvents();
        var index = Index(rig, new ReadIndexStore(rig.Data), events);
        await rig.WriteAsync(("a", "k"));
        var lost = await rig.WriteAsync(("a", "k"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal(lost, rig.Store.Current!.Head);
        await rig.ExecuteAsync($"DELETE FROM reeve_cluster.change_log WHERE seq = {lost}");

        var scope = Scope();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await Poll.UntilAsync(() => events.All.Count == 1, "the head read (QStart)");
        ((FakeTimeProvider)rig.Time).Advance(Bound);
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Behind, result.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(["QStart", "QTimeout"], events.All.Select(e => e.Action));
    }

    [Fact]
    public async Task A_read_index_pool_exhausted_by_stalled_head_reads_does_not_block_the_main_pool()
    {
        var rig = await ServingRigAsync(o => o.ReadIndexTimeout = Bound);
        var server = new NpgsqlConnectionStringBuilder(rig.Options.ConnectionString);
        var proxy = new HoldingProxy(server.Host!, server.Port);
        owned.Add(proxy);
        var readIndexOptions = new ClusterOptions
        {
            ConnectionString = new NpgsqlConnectionStringBuilder(rig.Options.ConnectionString)
            {
                Host = "127.0.0.1",
                Port = proxy.Port,
                Timeout = 60,
                Pooling = true,
            }.ConnectionString,
        };
        var source = new ReadIndexDataSource(Options.Create(readIndexOptions));
        owned.Add(source);
        var index = Index(rig, new ReadIndexStore(source.Source), new RecordedEvents());

        proxy.Hold();
        var requests = Enumerable.Range(0, ReadIndexDataSource.MaxPoolSize + 4)
            .Select(_ => index.EnterAsync(Scope(), wait: true, CancellationToken.None))
            .ToArray();

        await Poll.UntilAsync(() => proxy.AcceptedWhileHeld >= ReadIndexDataSource.MaxPoolSize, "every slot of the read index's pool taken");
        await Task.Delay(Pending);
        var opened = proxy.AcceptedWhileHeld;

        await using (var connection = await rig.Data.DataSource.OpenConnectionAsync().AsTask().WaitAsync(Wait))
        await using (var command = new NpgsqlCommand("SELECT 1", connection))
        {
            Assert.Equal(1, (int)(await command.ExecuteScalarAsync().WaitAsync(Wait))!);
        }

        Assert.All(requests, r => Assert.False(r.IsCompleted, "A request answered before its bound."));
        ((FakeTimeProvider)rig.Time).Advance(Bound);
        var outcomes = await Task.WhenAll(requests).WaitAsync(InTime);
        proxy.Release();

        Assert.All(outcomes, o => Assert.Equal(ReadIndexOutcome.Behind, o.Outcome));
        Assert.True(opened == ReadIndexDataSource.MaxPoolSize, $"{opened} connections opened for {requests.Length} requests");
    }

    private sealed class Disposing(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
