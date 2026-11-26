using Int.Log;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class ActivationPhaseTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly List<ActivationRig> rigs = [];

    public ActivationPhaseTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var rig in rigs)
        {
            await rig.DisposeAsync();
        }
    }

    private async Task<ActivationRig> CreateAsync(IFaultPoints? points = null, Action<ClusterOptions>? configure = null)
    {
        var rig = await ActivationRig.CreateAsync(postgres, points, configure);
        rigs.Add(rig);
        return rig;
    }

    [Fact]
    public async Task Phases_a_delay_between_activation_and_Serving_keeps_readiness_false_and_passes_no_serving_check_until_the_cursor_reaches_H()
    {
        var probe = new PointProbe();
        var rig = await CreateAsync(probe);
        await rig.Applier.WriteAsync(("a", "k"), ("a", "k"), ("a", "k"));
        await rig.LoadAsync();
        probe.Hold(LogFaultPoints.AfterSnapshotRead);
        var head = await rig.Applier.WriteAsync(("a", "k1"), ("a", "k2"));
        await probe.Arrived.WaitAsync(Wait);

        rig.Run();
        await rig.AwaitPhaseAsync(ReplicaPhase.Activated);
        var row = await rig.ReadRowAsync();
        Assert.Equal(("active", head, (DateTime?)null), (row.State, row.ActivatedAtSeq, row.ServingSince));
        Assert.True(rig.Applier.Applier.Cursor.Seq < head);
        await Task.Delay(500);
        Assert.Equal(ReplicaPhase.Activated, rig.Gate.Phase);
        Assert.Equal(ServingCheckOutcome.NotServing, rig.Gate.CheckServing().Outcome);
        Assert.Null((await rig.ReadRowAsync()).ServingSince);

        probe.Release();
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
        Assert.True(rig.Applier.Applier.Cursor.Seq >= head);
        Assert.Equal(ServingCheckOutcome.Serving, rig.Gate.CheckServing().Outcome);
        Assert.NotNull((await rig.ReadRowAsync()).ServingSince);
        Assert.Equal(
            [MembershipActions.Activate, MembershipActions.EnterServing],
            rig.Applier.Events.All.Select(e => e.Action).Where(a => a is MembershipActions.Activate or MembershipActions.EnterServing));
    }

    [Fact]
    public async Task Phases_a_delay_at_the_point_between_the_commit_and_the_applier_reaching_H_holds_Serving_back()
    {
        var probe = new PointProbe();
        var rig = await CreateAsync(probe);
        await rig.LoadAsync();
        probe.Hold(MembershipFaultPoints.ActivateCommittedBeforeApplied);

        rig.Run();
        await rig.AwaitPhaseAsync(ReplicaPhase.Activated);
        await probe.Arrived.WaitAsync(Wait);
        await Task.Delay(300);
        Assert.Equal(ReplicaPhase.Activated, rig.Gate.Phase);
        Assert.Equal(ServingCheckOutcome.NotServing, rig.Gate.CheckServing().Outcome);
        Assert.Null((await rig.ReadRowAsync()).ServingSince);

        probe.Release();
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
    }

    [Theory]
    [InlineData("membership.activate.before-lock")]
    [InlineData("membership.activate.after-head-read")]
    [InlineData("membership.activate.before-update")]
    [InlineData("membership.activate.after-update")]
    [InlineData("membership.activate.before-commit")]
    public async Task Phases_a_fence_trigger_during_the_activation_transaction_leaves_the_row_fenced_never_active(string point)
    {
        var probe = new PointProbe();
        var rig = await CreateAsync(probe);
        await MemberAudit.InstallAsync(rig.Applier);
        await rig.LoadAsync();
        probe.Hold(FaultPointCatalogue.Find(point)!.Value);

        rig.Run();
        await probe.Arrived.WaitAsync(Wait);
        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault, "a fence during the activation"));
        var step3 = rig.ReleaseLeaseAsync();
        await Task.Delay(100);
        probe.Release();

        Assert.True(await step3.WaitAsync(Wait), "step 3 must find the row this process's");
        await rig.StopAsync();
        var row = await rig.ReadRowAsync();
        Assert.Equal("fenced", row.State);
        Assert.Null(row.ServingSince);
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        Assert.DoesNotContain(rig.Applier.Events.All, e => e.Action == MembershipActions.Activate);
        Assert.Equal(["joining->fenced"], await MemberAudit.ReadAsync(rig.Applier));
    }

    [Theory]
    [InlineData("version")]
    [InlineData("protocol")]
    public async Task I14_a_member_below_a_floor_stays_joining_with_its_named_reason_and_activates_once_the_floor_is_rolled_back(string floor)
    {
        var rig = await CreateAsync();
        await rig.LoadAsync();
        await rig.Applier.ExecuteAsync(floor == "version"
            ? "UPDATE reeve_cluster.cluster SET version_floor = '1.10.0'"
            : $"UPDATE reeve_cluster.cluster SET protocol_floor = {MemberIdentity.CurrentProtocol + 1}");
        var reason = floor == "version" ? ActivationRules.VersionFloorReason : ActivationRules.ProtocolFloorReason;

        rig.Run();
        await rig.EventuallyAsync(() => rig.Gate.Readiness.Current.Any(r => r.Code == reason), $"the {reason} reason");
        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
        Assert.Equal("joining", (await rig.ReadRowAsync()).State);

        await rig.Applier.ExecuteAsync("UPDATE reeve_cluster.cluster SET version_floor = NULL, protocol_floor = 0");
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
        Assert.Empty(rig.Gate.Readiness.Current);
    }

    [Fact]
    public async Task The_first_activation_waits_until_the_entries_left_number_no_more_than_the_full_load_applied()
    {
        var probe = new PointProbe();
        var rig = await CreateAsync(probe);
        await rig.Applier.WriteAsync(("a", "k"), ("a", "k"));
        await rig.LoadAsync();
        Assert.Equal(2, rig.Applier.Applier.LastPublished!.Entries);
        probe.Hold(LogFaultPoints.AfterSnapshotRead);
        await rig.Applier.WriteAsync(("a", "1"), ("a", "2"), ("a", "3"));
        await probe.Arrived.WaitAsync(Wait);

        rig.Run();
        await Task.Delay(500);
        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
        Assert.Equal("joining", (await rig.ReadRowAsync()).State);

        probe.Release();
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
    }

    [Theory]
    [InlineData("fails")]
    [InlineData("times-out")]
    public async Task Phases_a_reconciler_that_stalls_during_catch_up_keeps_the_replica_joining_until_a_pass_publishes(string stall)
    {
        var rig = await CreateAsync(configure: stall == "times-out"
            ? o =>
            {
                o.ReconcileTimeout = TimeSpan.FromMilliseconds(300);
                o.ApplyStallLimit = TimeSpan.FromSeconds(60);
            }
            : null);
        await rig.Applier.WriteAsync(("a", "k1"), ("a", "k2"), ("a", "k3"));
        await rig.LoadAsync();
        Assert.Equal(3, rig.Applier.Applier.LastPublished!.Entries);
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Applier["a"].Before = stall == "fails"
            ? (_, _) => throw new InvalidOperationException("the test's reconciler fails from here on")
            : (_, _) => hang.Task;
        var stalled = await rig.Applier.WriteAsync(("a", "k4"));
        await rig.EventuallyAsync(() => rig.Applier["a"].Calls.Count(c => c.Head >= stalled) >= 2, "two stalled attempts at the new entry");

        var attempts = rig.Applier["a"].Calls.Count(c => c.Head >= stalled);
        rig.Run();
        await rig.EventuallyAsync(() => rig.Applier["a"].Calls.Count(c => c.Head >= stalled) >= attempts + 3, "three more stalled attempts after the rounds began");
        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
        var row = await rig.ReadRowAsync();
        Assert.Equal(("joining", (long?)null), (row.State, row.ActivatedAtSeq));
        Assert.True(rig.Applier.Applier.Cursor.Seq < stalled);
        Assert.DoesNotContain(rig.Applier.Events.All, e => e.Action == MembershipActions.Activate);

        rig.Applier["a"].Before = null;
        hang.TrySetResult();
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
        Assert.True(rig.Applier.Applier.Cursor.Seq >= stalled);
        Assert.Equal("active", (await rig.ReadRowAsync()).State);
    }

    [Fact]
    public async Task Under_a_continuous_stream_of_writes_the_replica_still_activates_and_serves()
    {
        var rig = await CreateAsync();
        await rig.LoadAsync();
        using var stop = new CancellationTokenSource();
        var writer = Task.Run(async () =>
        {
            var writes = 0;
            while (!stop.IsCancellationRequested)
            {
                await rig.Applier.WriteAsync(("a", $"k{writes++ % 10}"));
            }

            return writes;
        });

        rig.Run();
        await rig.AwaitPhaseAsync(ReplicaPhase.Serving);
        await stop.CancelAsync();
        var written = await writer.WaitAsync(Wait);

        var row = await rig.ReadRowAsync();
        Assert.Equal("active", row.State);
        Assert.NotNull(row.ServingSince);
        Assert.True(written > 0);
        Assert.True(row.ActivatedAtSeq <= (await rig.Applier.HeadAsync()).Seq);
    }
}

internal static class MemberAudit
{
    public static async Task InstallAsync(ApplierRig rig)
    {
        await rig.ExecuteAsync("CREATE TABLE public.member_audit (id bigserial PRIMARY KEY, epoch bigint NOT NULL, old_state text NOT NULL, new_state text NOT NULL, activated_at_seq bigint)");
        await rig.ExecuteAsync("""
            CREATE FUNCTION public.member_audit_row() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD.state IS DISTINCT FROM NEW.state THEN
                    INSERT INTO public.member_audit (epoch, old_state, new_state, activated_at_seq)
                    VALUES (NEW.epoch, OLD.state, NEW.state, NEW.activated_at_seq);
                END IF;
                RETURN NULL;
            END
            $$
            """);
        await rig.ExecuteAsync("CREATE TRIGGER member_audit AFTER UPDATE ON reeve_cluster.member FOR EACH ROW EXECUTE FUNCTION public.member_audit_row()");
    }

    public static async Task<string[]> ReadAsync(ApplierRig rig)
    {
        await using var connection = await rig.Log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT old_state, new_state FROM public.member_audit ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            rows.Add($"{reader.GetString(0)}->{reader.GetString(1)}");
        }

        return [.. rows];
    }
}

internal sealed class ActivationRig : IAsyncDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly CancellationTokenSource stopping = new();
    private readonly ActivationStore store;
    private Task? run;

    private ActivationRig(ApplierRig applier, IFaultPoints points)
    {
        Applier = applier;
        var options = Microsoft.Extensions.Options.Options.Create(applier.Options);
        Data = new ClusterDataSource(applier.Options);
        Writes = new MemberWriteDataSource(options);
        Renewals = new RenewalDataSource(options);
        store = new ActivationStore(Data, Writes, options, points);
        Fences = new FenceStore(Data, Renewals, Writes);
        Activation = new MemberActivation(
            applier.Gate, store, applier.Applier, new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []), applier.Channel,
            options, TimeProvider.System, points, NullLogger<MemberActivation>.Instance, Reeve.Cluster.Floors.NoUpgradePathGuard.Instance);
    }

    public ApplierRig Applier { get; }

    public LocalPhaseGate Gate => Applier.Gate;

    public MemberActivation Activation { get; }

    public FenceStore Fences { get; }

    private ClusterDataSource Data { get; }

    private MemberWriteDataSource Writes { get; }

    private RenewalDataSource Renewals { get; }

    public static async Task<ActivationRig> CreateAsync(PostgresFixture postgres, IFaultPoints? points = null, Action<ClusterOptions>? configure = null)
    {
        points ??= NoFaultPoints.Instance;
        var applier = await ApplierRig.CreateAsync(postgres, ["a"], points, o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
            o.RenewInterval = TimeSpan.FromMilliseconds(200);
            o.RenewTimeout = TimeSpan.FromSeconds(10);
            configure?.Invoke(o);
        });
        return new ActivationRig(applier, points);
    }

    public async Task LoadAsync()
    {
        await Applier.LoadAsync(startLoop: true);
        Assert.Equal(ReplicaPhase.CatchingUp, Gate.Apply(MemberTrigger.StateLoaded).To);
    }

    public void Run() => run = Task.Run(async () =>
    {
        while (!stopping.IsCancellationRequested)
        {
            var changed = Gate.NextPhaseChange;
            var phase = Gate.Phase;
            if (phase is ReplicaPhase.CatchingUp or ReplicaPhase.Activated && Gate.Epoch is { } epoch)
            {
                await Activation.RunAsync(phase, epoch, changed, stopping.Token);
            }
            else
            {
                await changed.WaitAsync(stopping.Token);
            }
        }
    });

    public async Task StopAsync()
    {
        await stopping.CancelAsync();
        if (run is { } r)
        {
            try
            {
                await r.WaitAsync(Wait);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    public Task<bool> ReleaseLeaseAsync() => Fences.ReleaseLeaseAsync("r1", Applier.Epoch, CancellationToken.None);

    public async Task AwaitPhaseAsync(ReplicaPhase wanted)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (Gate.Phase != wanted)
        {
            var change = Gate.NextPhaseChange;
            if (Gate.Phase == wanted)
            {
                return;
            }

            await change.WaitAsync(bound.Token);
        }
    }

    public async Task EventuallyAsync(Func<bool> condition, string what)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(clock.Elapsed < Wait, $"{what} did not happen within {Wait}");
            await Task.Delay(20);
        }
    }

    public async Task<(string State, long? ActivatedAtSeq, DateTime? ServingSince)> ReadRowAsync()
    {
        await using var connection = await Applier.Log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT state, activated_at_seq, serving_since FROM reeve_cluster.member WHERE replica_id = 'r1'", connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetDateTime(2));
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        Activation.Dispose();
        await Applier.DisposeAsync();
        await Writes.DisposeAsync();
        await Renewals.DisposeAsync();
        await Data.DisposeAsync();
    }
}
