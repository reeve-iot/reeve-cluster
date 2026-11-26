using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ReadIndexTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public ReadIndexTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            await item.DisposeAsync();
        }
    }

    private async Task<ApplierRig> ServingRigAsync(
        IFaultPoints? applierPoints = null, bool startLoop = false, Action<ClusterOptions>? configure = null, TimeProvider? time = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], applierPoints, configure, time);
        owned.Add(rig);
        await rig.LoadAsync(startLoop);
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, rig.Gate.Phase);
        return rig;
    }

    private ReadIndex Index(ApplierRig rig, IFaultPoints? points = null)
    {
        var index = new ReadIndex(
            rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), rig.Time,
            points ?? NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        return index;
    }

    private static ClusterScope Scope() => new(ClusterScopeKind.ClientRequest);

    private static (long A, long B) Read(IGeneration generation) =>
        (generation.Get<ProbeState>("a").Value, generation.Get<ProbeState>("b").Value);

    private static Task AppendBareAsync(ApplierRig rig) =>
        rig.ExecuteAsync("INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('a', 'bare')");

    [Fact]
    public async Task The_statement_reads_the_head_and_the_validity_of_the_cursor()
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a"]);
        owned.Add(rig);
        var store = new ReadIndexStore(rig.Data);

        Assert.Equal(new ReadIndexRead(0, CursorValid: true), await store.ReadAsync(Cursor.Empty, CancellationToken.None));

        await rig.WriteAsync(("a", "k"));
        var first = await rig.HeadAsync();
        await rig.WriteAsync(("a", "k"));
        var head = await rig.HeadAsync();

        Assert.Equal(new ReadIndexRead(head.Seq, CursorValid: true), await store.ReadAsync(first, CancellationToken.None));
        Assert.Equal(new ReadIndexRead(head.Seq, CursorValid: false), await store.ReadAsync(first with { Token = Guid.NewGuid() }, CancellationToken.None));
        Assert.Equal(new ReadIndexRead(head.Seq, CursorValid: false), await store.ReadAsync(new Cursor(head.Seq + 1, Guid.NewGuid()), CancellationToken.None));
    }

    [Fact]
    public async Task A_paused_request_reads_both_new_when_the_write_committed_before_the_pass_snapshot()
    {
        var rig = await ServingRigAsync();
        var points = new PointProbe();
        points.Hold(LogFaultPoints.BetweenWaitAndPin);
        var index = Index(rig, points);
        var scope = Scope();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Arrived.WaitAsync(Wait);

        await rig.WriteAsync(("a", "k"), ("b", "k"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        points.Release();
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(rig.Store.Current, scope.PinnedGeneration);
        Assert.Equal((1L, 1L), Read(scope.PinnedGeneration!));
        Assert.Equal(
            ["QStart", "QPin", "QRead"],
            rig.Events.All.Select(e => e.Action).Where(a => a.StartsWith('Q')));
    }

    [Fact]
    public async Task A_paused_request_reads_both_old_when_the_write_committed_after_the_pass_snapshot()
    {
        var applierPoints = new PointProbe();
        var rig = await ServingRigAsync(applierPoints);
        var points = new PointProbe();
        points.Hold(LogFaultPoints.BetweenWaitAndPin);
        var index = Index(rig, points);
        var scope = Scope();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Arrived.WaitAsync(Wait);

        await AppendBareAsync(rig);
        applierPoints.Hold(LogFaultPoints.AfterSnapshotRead);
        var pass = rig.PassAsync();
        await applierPoints.Arrived.WaitAsync(Wait);
        await rig.WriteAsync(("a", "k"), ("b", "k"));
        applierPoints.Release();
        Assert.Equal(PassOutcome.Published, await pass.WaitAsync(Wait));
        var published = rig.Store.Current!;

        points.Release();
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(published, scope.PinnedGeneration);
        Assert.Equal((0L, 0L), Read(scope.PinnedGeneration!));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal((1L, 1L), Read(rig.Store.Current!));
        Assert.Equal((0L, 0L), Read(scope.PinnedGeneration!));
    }

    [Fact]
    public async Task A_publication_between_the_pin_and_the_read_does_not_move_the_pin()
    {
        var rig = await ServingRigAsync();
        var points = new PointProbe();
        points.Hold(LogFaultPoints.BetweenPinAndRead);
        var index = Index(rig, points);
        var scope = Scope();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await points.Arrived.WaitAsync(Wait);
        var pinned = scope.PinnedGeneration!;

        await rig.WriteAsync(("a", "k"), ("b", "k"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        points.Release();
        var result = await entering.WaitAsync(Wait);

        Assert.Same(pinned, result.Generation);
        Assert.Equal((0L, 0L), Read(pinned));
        Assert.Equal((1L, 1L), Read(rig.Store.Current!));
    }

    [Fact]
    public async Task A_write_committed_before_the_request_is_read_once_the_applier_has_applied_it()
    {
        var rig = await ServingRigAsync(startLoop: true);
        var index = Index(rig);
        var seq = await rig.WriteAsync(("a", "k"), ("b", "k"));

        var scope = Scope();
        var result = await index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.True(scope.PinnedGeneration!.Head >= seq);
        Assert.Equal((1L, 1L), Read(scope.PinnedGeneration));
    }

    [Fact]
    public async Task Behind_comes_within_ReadIndexTimeout_while_the_applier_does_not_apply()
    {
        var timeout = TimeSpan.FromMilliseconds(500);
        var time = new FakeTimeProvider();
        var rig = await ServingRigAsync(configure: o => o.ReadIndexTimeout = timeout, time: time);
        var index = Index(rig);
        await rig.WriteAsync(("a", "k"), ("b", "k"));

        var scope = Scope();
        var entering = index.EnterAsync(scope, wait: true, CancellationToken.None);
        await Poll.UntilAsync(() => rig.Events.All.Any(e => e.Action == "QStart"), "the head read (QStart)");

        time.Advance(timeout - TimeSpan.FromMilliseconds(1));
        await Task.WhenAny(entering, Task.Delay(TimeSpan.FromMilliseconds(300)));
        Assert.False(entering.IsCompleted, "The request answered before ReadIndexTimeout.");
        time.Advance(TimeSpan.FromMilliseconds(1));
        var result = await entering.WaitAsync(Wait);

        Assert.Equal(ReadIndexOutcome.Behind, result.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(
            ["QStart", "QTimeout"],
            rig.Events.All.Select(e => e.Action).Where(a => a.StartsWith('Q')));
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
