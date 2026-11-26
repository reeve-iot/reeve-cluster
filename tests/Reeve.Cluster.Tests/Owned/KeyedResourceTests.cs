using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Membership;
using Unit.Testing;

namespace Unit.Owned;

public sealed class KeyedResourceTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly FakeTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly ScriptedUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly List<UnitRuntime> runtimes = [];

    private readonly Unit.Generations.HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);

    public KeyedResourceTests()
    {
        gate = new LocalPhaseGate(Options.Create(options), time, new RecordingModelEvents(), fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        fenceTimer.Release();
        foreach (var runtime in runtimes)
        {
            runtime.Dispose();
        }

        gate.Dispose();
    }

    private static UnitKind Kind(string kind, AdoptionPolicy policy, EligibilityFilter? eligibility = null, bool keyed = true) =>
        new(kind, typeof(RecordingUnit), new UnitKindOptions { Policy = policy, Eligibility = eligibility ?? EligibilityFilter.Any }) { Keyed = keyed };

    private UnitRuntime Runtime(params UnitKind[] kinds) => Runtime(rows, kinds);

    private UnitRuntime Runtime(IUnitRows unitRows, params UnitKind[] kinds)
    {
        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var runtime = new UnitRuntime(
            kinds, services, unitRows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, wakeups, new NoTenureFence(), Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        runtimes.Add(runtime);
        return runtime;
    }

    private void ToServing()
    {
        Assert.True(gate.Joined(Epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private static OrphanedRow Orphan(string kind, string key, long token) => new(kind, key, new OwnedRow("r0", 3, token));

    [Fact]
    public async Task O08_a_serving_eligible_member_adopts_an_orphaned_row_of_an_AnyEligible_kind_and_starts_it()
    {
        var runtime = Runtime(
            Kind("conn", AdoptionPolicy.AnyEligible),
            Kind("gpu-conn", AdoptionPolicy.AnyEligible, EligibilityFilter.Any.Require("gpu")),
            Kind("sess", AdoptionPolicy.None),
            Kind("widget", AdoptionPolicy.Placer, keyed: false));
        ToServing();
        rows.Orphaned = [Orphan("conn", "a", 10)];

        await runtime.PassAsync(CancellationToken.None);

        Assert.Equal(["conn"], Assert.Single(rows.OrphanScans));
        var take = await rows.Takes.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("conn", "a", new OwnedRow("r0", 3, 10), "r1", Epoch), take);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("conn", "a", 11L), (started.Kind, started.Key, started.Token));
    }

    [Fact]
    public async Task A_replica_with_no_AnyEligible_kind_it_passes_does_not_scan()
    {
        var runtime = Runtime(Kind("gpu-conn", AdoptionPolicy.AnyEligible, EligibilityFilter.Any.Require("gpu")), Kind("sess", AdoptionPolicy.None));
        ToServing();

        await runtime.PassAsync(CancellationToken.None);

        Assert.Empty(rows.OrphanScans);
    }

    [Fact]
    public async Task A_key_whose_stop_has_not_ended_here_is_not_adopted()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");
        rows.Held = [];
        await runtime.PassAsync(CancellationToken.None);
        Assert.True(runtime.Holds("conn", "a"));

        rows.Orphaned = [Orphan("conn", "a", 10)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(rows.Takes.Reader.TryRead(out _));
    }

    [Fact]
    public async Task I1_no_adoption_once_fence_start_passed_and_its_serving_check_fences()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Orphaned = [Orphan("conn", "a", 10)];

        fenceTimer.Arm();
        time.Advance(options.LeaseTtl);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(rows.Takes.Reader.TryRead(out var take), $"Adopted {take} after fence start.");
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Fact]
    public async Task A_drain_releases_a_Placer_kinds_row_but_not_an_AnyEligible_kinds()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible), Kind("widget", AdoptionPolicy.Placer, keyed: false));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false), new HeldRow("widget", "widget", 11, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await runtime.StopAllAsync(drain: true, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(("widget", 11L), await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait) is var r ? (r.Kind, r.Token) : default);
        await unit.LostReturned("a").WaitAsync(Wait);
        Assert.False(rows.Releases.Reader.TryRead(out var other), $"Released {other}.");
    }

    [Fact]
    public async Task O11_the_end_stops_the_unit_frees_the_row_once_and_runs_OnLostAsync()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await started.Context.EndAsync();

        Assert.True(started.Stop.IsCancellationRequested);
        Assert.Throws<OwnershipLostException>(started.Context.EnsureOwner);
        Assert.Equal(("conn", "a", "r1", Epoch, 10L), await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait));
        await unit.LostReturned("a").WaitAsync(Wait);
        rows.Held = [];
        await rows.UntilAsync(() => Task.FromResult(!runtime.Holds("conn", "a"))).WaitAsync(Wait);
        Assert.False(rows.Releases.Reader.TryRead(out _));
        await Assert.ThrowsAsync<OwnershipLostException>(() => started.Context.EndAsync());
    }

    [Fact]
    public async Task O11_the_stop_has_begun_when_the_ends_statement_runs()
    {
        var observing = new ObservingRows(rows);
        var runtime = Runtime(observing, Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        observing.Probe = () => started.Stop.IsCancellationRequested;

        await started.Context.EndAsync();

        Assert.True(observing.Observed.TryDequeue(out var stoppedDuringStatement));
        Assert.True(stoppedDuringStatement, "The end's statement ran before the stop began.");
    }

    [Fact]
    public async Task An_end_called_with_the_units_own_stop_token_frees_the_row_before_OnLostAsync()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");

        await started.Context.EndAsync(started.Stop);

        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.False(unit.LostReturned("a").IsCompleted);
    }

    [Fact]
    public async Task An_end_whose_retry_got_no_answer_either_is_released_by_a_later_poll_and_never_restarted()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        rows.FailReleases = true;

        await Assert.ThrowsAsync<TimeoutException>(() => started.Context.EndAsync());
        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        await unit.LostReturned("a").WaitAsync(Wait);

        rows.FailReleases = false;
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return rows.Releases.Reader.TryRead(out var again) && again.Token == 10;
        }).WaitAsync(Wait);
        Assert.False(unit.Acquired.Reader.TryRead(out var restarted), $"The ended unit started again: {restarted}.");

        rows.Held = [];
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return !runtime.Holds("conn", "a");
        }).WaitAsync(Wait);
    }

    [Fact]
    public async Task An_end_whose_releases_keep_getting_no_answer_is_released_at_each_poll_and_never_restarted()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        rows.FailReleases = true;

        await Assert.ThrowsAsync<TimeoutException>(() => started.Context.EndAsync());
        Assert.Equal(10L, await NextReleaseAsync());
        Assert.Equal(10L, await NextReleaseAsync());
        await unit.LostReturned("a").WaitAsync(Wait);
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return rows.Releases.Reader.TryRead(out var again) && again.Token == 10;
        }).WaitAsync(Wait);

        for (var i = 0; i < 3; i++)
        {
            await runtime.PassAsync(CancellationToken.None);
            Assert.False(unit.Acquired.Reader.TryRead(out var restarted), $"Poll {i}: the ended unit started again under {restarted.Token}.");
            Assert.Equal(10L, await NextReleaseAsync());
        }

        Assert.True(runtime.Holds("conn", "a"));
    }

    [Fact]
    public async Task A_hung_OnLostAsync_keeps_the_guard_across_polls_that_no_longer_see_the_row()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("a");

        rows.Held = [];
        await runtime.PassAsync(CancellationToken.None);
        await runtime.PassAsync(CancellationToken.None);
        await runtime.PassAsync(CancellationToken.None);
        Assert.True(runtime.Holds("conn", "a"), "The guard ended while OnLostAsync still hangs.");

        rows.Orphaned = [Orphan("conn", "a", 10)];
        rows.Held = [new HeldRow("conn", "a", 11, false)];
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(rows.Takes.Reader.TryRead(out var take), $"Adopted {take} while OnLostAsync hangs.");
        Assert.Equal(11L, await NextReleaseAsync());
        Assert.False(unit.Acquired.Reader.TryRead(out var started), $"Started {started.Token} while OnLostAsync hangs.");
    }

    [Fact]
    public async Task A_new_assignment_after_a_kept_stops_OnLostAsync_returned_starts()
    {
        var runtime = Runtime(Kind("widget", AdoptionPolicy.Placer, keyed: false));
        ToServing();
        rows.Held = [new HeldRow("widget", "widget", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        rows.FailReleases = true;
        rows.Held = [new HeldRow("widget", "widget", 10, true)];
        await runtime.PassAsync(CancellationToken.None);
        Assert.Equal(10L, await NextReleaseAsync());
        await unit.LostReturned("widget").WaitAsync(Wait);
        rows.FailReleases = false;

        rows.Held = [new HeldRow("widget", "widget", 11, false)];
        await rows.UntilAsync(async () =>
        {
            await runtime.PassAsync(CancellationToken.None);
            return unit.Acquired.Reader.TryRead(out var started) && started.Token == 11;
        }).WaitAsync(Wait);
    }

    [Fact]
    public async Task A_drain_releases_a_None_kinds_row_and_waits_for_an_AnyEligible_units_stop_until_StopBudget()
    {
        var runtime = Runtime(Kind("sess", AdoptionPolicy.None), Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("sess", "a", 10, false), new HeldRow("conn", "b", 11, false)];
        await runtime.PassAsync(CancellationToken.None);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        unit.HangLost("b");

        var drain = runtime.StopAllAsync(drain: true, CancellationToken.None);
        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("sess", 10L), (release.Kind, release.Token));
        Assert.False(drain.IsCompleted, "Drain step 2 ended before the AnyEligible unit's OnLostAsync returned or StopBudget passed.");

        time.Advance(options.StopBudget);
        await drain.WaitAsync(Wait);
        Assert.False(rows.Releases.Reader.TryRead(out var other), $"Released {other}.");
    }

    [Fact]
    public async Task A_unit_kind_cannot_end_its_row()
    {
        var runtime = Runtime(Kind("widget", AdoptionPolicy.Placer, keyed: false));
        ToServing();
        rows.Held = [new HeldRow("widget", "widget", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await Assert.ThrowsAsync<InvalidOperationException>(() => started.Context.EndAsync());
        Assert.False(started.Stop.IsCancellationRequested);
    }

    [Fact]
    public void A_keyed_kind_with_the_Placer_policy_is_refused()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddClusterKeyedResource<RecordingUnit>("conn", _ => { }));
        Assert.Throws<ArgumentException>(() => services.AddClusterKeyedResource<RecordingUnit>("conn", k => k.Policy = AdoptionPolicy.Placer));
        Assert.DoesNotContain(services, d => d.ImplementationInstance is UnitKind);

        services.AddClusterKeyedResource<RecordingUnit>("conn", k => k.Policy = AdoptionPolicy.AnyEligible);
        services.AddClusterUnit<RecordingUnit>("widget");
        var kinds = services.Select(d => d.ImplementationInstance).OfType<UnitKind>().ToDictionary(k => k.Kind, k => k.Keyed);

        Assert.Equal(new Dictionary<string, bool> { ["conn"] = true, ["widget"] = false, [Placement.PlacerKind] = false }, kinds);
    }

    [Theory]
    [InlineData(AdoptionPolicy.AnyEligible)]
    [InlineData(AdoptionPolicy.None)]
    public void A_unit_with_the_AnyEligible_or_None_policy_is_refused(AdoptionPolicy policy)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddClusterUnit<RecordingUnit>("widget", o => o.Policy = policy));
        Assert.DoesNotContain(services, d => d.ImplementationInstance is UnitKind);

        services.AddClusterUnit<RecordingUnit>("widget", o => o.Policy = AdoptionPolicy.Placer);
        services.AddClusterKeyedResource<RecordingUnit>("conn", k => k.Policy = policy);
        Assert.Contains(services, d => d.ImplementationInstance is UnitKind { Kind: "widget", Keyed: false });
    }

    private async Task<long> NextReleaseAsync() => (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token;

    [Fact]
    public async Task An_end_that_finds_the_row_no_longer_the_tenures_throws_and_still_stops_the_unit()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        rows.ReleaseMatches = false;

        await Assert.ThrowsAsync<OwnershipLostException>(() => started.Context.EndAsync());

        Assert.True(started.Stop.IsCancellationRequested);
        await unit.LostReturned("a").WaitAsync(Wait);
    }

    [Fact]
    public async Task An_end_with_no_answer_throws_and_the_stop_releases_the_row_again()
    {
        var runtime = Runtime(Kind("conn", AdoptionPolicy.AnyEligible));
        ToServing();
        rows.Held = [new HeldRow("conn", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        rows.FailReleases = true;

        await Assert.ThrowsAsync<TimeoutException>(() => started.Context.EndAsync());

        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        Assert.Equal(10L, (await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Token);
        await unit.LostReturned("a").WaitAsync(Wait);
    }

    private KeyedResources Creator(ScriptedCreatorRows creatorRows, Action? nudge = null, Func<string, string, bool>? holds = null) =>
        new(
            [
                Kind("conn", AdoptionPolicy.AnyEligible),
                Kind("sess", AdoptionPolicy.None),
                Kind("guarded", AdoptionPolicy.AnyEligible, EligibilityFilter.Any.Forbid("degraded-mode")),
                Kind("widget", AdoptionPolicy.Placer, keyed: false),
            ],
            creatorRows,
            gate,
            nudge ?? (() => { }),
            holds ?? ((_, _) => false),
            () => capabilities.Current,
            Options.Create(options),
            time,
            NullLogger<KeyedResources>.Instance);

    private static OwnedRead Absent() => new(null, null, null);

    private static OwnedRead Held(long token) => new(new OwnedRow("r2", 7, token), new OwnerMember(7, MemberRowState.Active, true), null);

    private static OwnedRead Orphaned(long token) => new(new OwnedRow("r0", 3, token), null, null);

    private static OwnedRead Free(long token) => new(new OwnedRow(null, null, token), null, null);

    [Fact]
    public async Task O01_the_creator_inserts_an_absent_key_and_reads_again_after_a_conflict()
    {
        ToServing();
        var creatorRows = new ScriptedCreatorRows { Reads = { Absent(), Absent(), Held(20) }, Inserts = { 10, null } };
        var nudges = 0;
        var creator = Creator(creatorRows, () => nudges++);

        Assert.Equal(CreateOutcome.Created, await creator.CreateAsync("conn", "a"));
        Assert.Equal(1, nudges);
        Assert.Equal(CreateOutcome.AlreadyOwned, await creator.CreateAsync("conn", "b"));
        Assert.Equal(["insert conn/a r1 5", "insert conn/b r1 5"], creatorRows.Writes);
    }

    [Fact]
    public async Task O02_the_creator_takes_a_free_or_orphaned_row_and_inserts_again_after_a_sweep()
    {
        ToServing();
        var creatorRows = new ScriptedCreatorRows { Reads = { Orphaned(10), Free(11), Absent() }, Takes = { 12, null }, Inserts = { 13 } };
        var creator = Creator(creatorRows);

        Assert.Equal(CreateOutcome.Created, await creator.CreateAsync("conn", "a"));
        Assert.Equal(CreateOutcome.Created, await creator.CreateAsync("conn", "b"));
        Assert.Equal(["take conn/a r0:3:10 r1 5", "take conn/b -:-:11 r1 5", "insert conn/b r1 5"], creatorRows.Writes);
    }

    [Fact]
    public async Task O02_a_release_requested_row_is_already_owned()
    {
        ToServing();
        var requested = Held(20) with { RequestToken = 20 };
        var creatorRows = new ScriptedCreatorRows { Reads = { requested } };

        Assert.Equal(CreateOutcome.AlreadyOwned, await Creator(creatorRows).CreateAsync("conn", "a"));
        Assert.Empty(creatorRows.Writes);
    }

    [Fact]
    public async Task I1_a_replica_that_is_not_serving_creates_nothing()
    {
        var creatorRows = new ScriptedCreatorRows { Reads = { Absent() } };

        Assert.Equal(CreateOutcome.NotServing, await Creator(creatorRows).CreateAsync("conn", "a"));
        Assert.Empty(creatorRows.Writes);
    }

    [Fact]
    public async Task I1_a_replica_that_is_not_serving_cleans_up_nothing()
    {
        var creatorRows = new ScriptedCreatorRows { Reads = { Orphaned(10) } };

        Assert.False(await Creator(creatorRows).CleanUpAsync("sess", "a"));
        Assert.Empty(creatorRows.Writes);
    }

    [Fact]
    public async Task A_key_still_stopping_here_is_not_created_again()
    {
        ToServing();
        var creatorRows = new ScriptedCreatorRows { Reads = { Free(10) } };

        Assert.Equal(CreateOutcome.Stopping, await Creator(creatorRows, holds: (kind, key) => (kind, key) == ("conn", "a")).CreateAsync("conn", "a"));
        Assert.Empty(creatorRows.Writes);
    }

    [Fact]
    public async Task A_creator_that_fails_the_kinds_filter_creates_nothing()
    {
        ToServing();
        await capabilities.SetAsync(["degraded-mode"]);
        var creatorRows = new ScriptedCreatorRows { Reads = { Absent(), Held(20), Free(21) } };
        var creator = Creator(creatorRows);

        Assert.Equal(CreateOutcome.NotEligible, await creator.CreateAsync("guarded", "a"));
        Assert.Equal(CreateOutcome.AlreadyOwned, await creator.CreateAsync("guarded", "b"));
        Assert.Equal(CreateOutcome.NotEligible, await creator.CreateAsync("guarded", "c"));
        Assert.Empty(creatorRows.Writes);
    }

    [Fact]
    public async Task A_replica_that_fails_the_filter_and_is_not_serving_answers_NotEligible()
    {
        await capabilities.SetAsync(["degraded-mode"]);
        var creatorRows = new ScriptedCreatorRows { Reads = { Absent() } };

        Assert.Equal(CreateOutcome.NotEligible, await Creator(creatorRows).CreateAsync("guarded", "a"));
        Assert.Empty(creatorRows.Writes);
    }

    [Theory]
    [InlineData(AdoptionPolicy.AnyEligible)]
    [InlineData(AdoptionPolicy.None)]
    public async Task An_owner_of_a_keyed_kind_that_stops_passing_the_filter_keeps_the_key(AdoptionPolicy policy)
    {
        var runtime = Runtime(Kind("guarded", policy, EligibilityFilter.Any.Forbid("degraded-mode")));
        ToServing();
        rows.Held = [new HeldRow("guarded", "a", 10, false)];
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        await capabilities.SetAsync(["degraded-mode"]);
        await runtime.PassAsync(CancellationToken.None);
        await runtime.PassAsync(CancellationToken.None);

        Assert.False(started.Stop.IsCancellationRequested);
        Assert.False(rows.Releases.Reader.TryRead(out var release), $"Released {release}.");
        Assert.True(runtime.Holds("guarded", "a"));
    }

    [Fact]
    public async Task A_kind_not_registered_here_as_a_keyed_resource_is_refused()
    {
        ToServing();
        var creator = Creator(new ScriptedCreatorRows());

        await Assert.ThrowsAsync<ArgumentException>(() => creator.CreateAsync("gizmo", "a"));
        await Assert.ThrowsAsync<ArgumentException>(() => creator.CleanUpAsync("gizmo", "a"));
        await Assert.ThrowsAsync<ArgumentException>(() => creator.CreateAsync("widget", "widget"));
    }

    [Fact]
    public async Task O10_the_creator_cleans_up_an_orphaned_row_of_a_None_kind_only()
    {
        ToServing();
        var creatorRows = new ScriptedCreatorRows { Reads = { Orphaned(10), Orphaned(11), Held(12), Free(13) }, Clears = { false, true } };
        var creator = Creator(creatorRows);

        await Assert.ThrowsAsync<ArgumentException>(() => creator.CleanUpAsync("conn", "a"));
        Assert.True(await creator.CleanUpAsync("sess", "a"));
        Assert.False(await creator.CleanUpAsync("sess", "b"));
        Assert.False(await creator.CleanUpAsync("sess", "c"));
        Assert.Equal(["clear sess/a r0:3:10", "clear sess/a r0:3:11"], creatorRows.Writes);
    }

    private sealed class ObservingRows(ScriptedUnitRows inner) : IUnitRows
    {
        public Func<bool>? Probe { get; set; }

        public ConcurrentQueue<bool> Observed { get; } = new();

        public Task<IReadOnlyList<HeldRow>> ReadHeldAsync(string owner, long epoch, CancellationToken cancellationToken) =>
            inner.ReadHeldAsync(owner, epoch, cancellationToken);

        public Task<IReadOnlyList<OrphanedRow>> ReadOrphanedAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken) =>
            inner.ReadOrphanedAsync(kinds, cancellationToken);

        public Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken) =>
            inner.TakeAsync(kind, key, expected, owner, epoch, cancellationToken);

        public Task<bool> ReleaseAsync(string kind, string key, string owner, long epoch, long token, CancellationToken cancellationToken)
        {
            if (Probe is { } probe)
            {
                Observed.Enqueue(probe());
            }

            return inner.ReleaseAsync(kind, key, owner, epoch, token, cancellationToken);
        }
    }

    private sealed class ScriptedCreatorRows : ICreatorRows
    {
        private readonly ConcurrentQueue<string> writes = new();

        public List<OwnedRead> Reads { get; } = [];

        public List<long?> Inserts { get; } = [];

        public List<long?> Takes { get; } = [];

        public List<bool> Clears { get; } = [];

        public IReadOnlyList<string> Writes => [.. writes];

        public Task<OwnedRead> ReadAsync(string kind, string key, CancellationToken cancellationToken) => Task.FromResult(Next(Reads));

        public Task<long?> InsertAsync(string kind, string key, string owner, long epoch, CancellationToken cancellationToken)
        {
            writes.Enqueue($"insert {kind}/{key} {owner} {epoch}");
            return Task.FromResult(Next(Inserts));
        }

        public Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken)
        {
            writes.Enqueue($"take {kind}/{key} {Of(expected)} {owner} {epoch}");
            return Task.FromResult(Next(Takes));
        }

        public Task<bool> ClearAsync(string kind, string key, OwnedRow expected, CancellationToken cancellationToken)
        {
            writes.Enqueue($"clear {kind}/{key} {Of(expected)}");
            return Task.FromResult(Next(Clears));
        }

        private static string Of(OwnedRow row) => $"{row.OwnerReplica ?? "-"}:{row.OwnerEpoch?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-"}:{row.Token}";

        private static T Next<T>(List<T> answers)
        {
            Assert.NotEmpty(answers);
            var next = answers[0];
            answers.RemoveAt(0);
            return next;
        }
    }
}
