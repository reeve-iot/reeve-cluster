using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Log;

public sealed class ApplierGuardTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan InTime = TimeSpan.FromSeconds(20);
    private static readonly FenceContext Step6 = new(new FenceRequest(FenceTrigger.FenceStart));

    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public ApplierGuardTests(PostgresFixture postgres)
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

    [Fact]
    public async Task Step_6s_participant_returns_at_once_while_a_pass_statement_waits_for_a_database_that_does_not_answer()
    {
        var server = new NpgsqlConnectionStringBuilder(postgres.ConnectionString);
        await using var proxy = new HoldingProxy(server.Host!, server.Port);
        var probe = new PointProbe();
        var rig = await ApplierRig.CreateAsync(postgres, ["a"], probe, o =>
        {
            o.ApplyPollInterval = TimeSpan.FromMilliseconds(100);
            o.ConnectionString = new NpgsqlConnectionStringBuilder(o.ConnectionString)
            {
                Host = "127.0.0.1",
                Port = proxy.Port,
                Timeout = 60,
            }.ConnectionString;
        });
        try
        {
            await rig.LoadAsync(startLoop: true);
            probe.Hold(LogFaultPoints.AfterSnapshotRead);
            await rig.WriteAsync(("a", "k1"));
            await probe.Arrived.WaitAsync(Wait);

            proxy.Hold();
            probe.Release();
            await proxy.UntilBytesHeldAsync(Wait);

            var stop = Task.Run(async () => await new ApplierStopParticipant(rig.Applier).RunAsync(Step6, CancellationToken.None));
            var returned = await Task.WhenAny(stop, Task.Delay(InTime)) == stop;
            var opened = proxy.AcceptedWhileHeld;
            proxy.Release();
            await stop.WaitAsync(Wait);

            Assert.True(
                returned,
                $"step 6's participant did not return within {InTime.TotalSeconds:F0} s (the connect Timeout is 60 s); connections opened to the "
                + $"database while it did not answer, the cancel request's among them: {opened}.");
        }
        finally
        {
            proxy.Release();
            await rig.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_pass_that_read_its_snapshot_before_step_6_begins_nothing_after_it()
    {
        var probe = new PointProbe();
        var rig = await RigAsync(["a"], probe);
        await rig.LoadAsync();
        var before = rig.Store.Current!;
        await rig.WriteAsync(("a", "k"));
        var calls = rig["a"].Calls.Count;
        var passBegins = PassBegins(rig);
        probe.Hold(LogFaultPoints.AfterSnapshotRead);

        var pass = rig.PassAsync();
        await probe.Arrived.WaitAsync(Wait);
        await new ApplierStopParticipant(rig.Applier).RunAsync(Step6, CancellationToken.None);
        probe.Release();

        Assert.Equal(PassOutcome.Abandoned, await pass.WaitAsync(Wait));
        Assert.Same(before, rig.Store.Current);
        Assert.Equal(calls, rig["a"].Calls.Count);
        Assert.Equal(passBegins, PassBegins(rig));
    }

    [Fact]
    public async Task The_cursor_statement_of_an_earlier_epoch_writes_nothing_once_the_row_has_a_later_epoch()
    {
        var probe = new PointProbe();
        var rig = await RigAsync(["a"], probe);
        await rig.LoadAsync();
        var row = await rig.RowCursorAsync();
        await rig.WriteAsync(("a", "k"));
        probe.Hold(LogFaultPoints.BetweenPublishAndCursor);

        var pass = rig.PassAsync();
        await probe.Arrived.WaitAsync(Wait);
        await rig.ExecuteAsync("UPDATE reeve_cluster.member SET epoch = epoch + 1 WHERE replica_id = 'r1'");
        probe.Release();

        Assert.Equal(PassOutcome.Published, await pass.WaitAsync(Wait));
        Assert.Equal(row, await rig.RowCursorAsync());
        var write = rig.Events.All.Last(e => e.Action == LogActions.CursorWrite);
        Assert.Equal((object)0, write.State!["rows"]);
    }

    [Fact]
    public async Task The_pass_transaction_and_an_importers_are_read_only_repeatable_read()
    {
        var rig = await RigAsync(["a"]);
        var seen = "the reconciler did not run";
        rig["a"].Before = async (context, ct) =>
        {
            try
            {
                var pass = await SettingsAsync((NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction, ct);
                await using var session = await context.OpenSnapshotSessionAsync(ct);
                var importer = await SettingsAsync((NpgsqlConnection)session.Connection, (NpgsqlTransaction)session.Transaction, ct);
                seen = $"pass: {pass}; importer: {importer}";
            }
            catch (Exception exception)
            {
                seen = $"threw {exception.GetType().Name}: {exception.Message}";
            }
        };

        await rig.LoadAsync();

        Assert.Equal("pass: on, repeatable read; importer: on, repeatable read", seen);
    }

    [Fact]
    public async Task S3_a_publish_whose_rollback_fails_keeps_its_threshold_and_its_cursor_reaches_the_row_without_a_new_write()
    {
        var probe = new PointProbe();
        var rig = await RigAsync(["a"], probe);
        await rig.LoadAsync();
        var seq = await rig.WriteAsync(("a", "k"));
        probe.Hold(LogFaultPoints.BeforeReconcilerReturns);

        var pass = rig.PassAsync();
        await probe.Arrived.WaitAsync(Wait);
        await TerminateOtherSessionsAsync(rig);
        probe.Release();

        Assert.Equal(PassOutcome.Published, await pass.WaitAsync(Wait));
        Assert.Equal(seq, rig.Store.Current!.Head);
        Assert.Equal(seq, rig.Applier.LastPublished!.Head.Seq);
        Assert.Equal(1, rig.Applier.LastPassEntries);
        Assert.Equal(0, (await rig.RowCursorAsync()).Seq);

        rig.Applier.StartApplying(rig.Epoch);
        await UntilRowCursorAsync(rig, seq);
        Assert.Equal(1, rig.Applier.LastPassEntries);
    }

    [Fact]
    public async Task S3_a_cursor_statement_that_fails_after_the_publish_is_written_without_a_new_write()
    {
        var probe = new PointProbe();
        var rig = await RigAsync(["a"], probe);
        await rig.LoadAsync();
        var seq = await rig.WriteAsync(("a", "k"));
        probe.Hold(LogFaultPoints.BetweenPublishAndCursor);

        var pass = rig.PassAsync();
        await probe.Arrived.WaitAsync(Wait);
        await TerminateOtherSessionsAsync(rig);
        probe.Release();

        Assert.Equal(PassOutcome.Published, await pass.WaitAsync(Wait));
        Assert.Equal(seq, rig.Store.Current!.Head);
        Assert.Equal(1, rig.Applier.LastPassEntries);
        Assert.Equal(0, (await rig.RowCursorAsync()).Seq);

        rig.Applier.StartApplying(rig.Epoch);
        await UntilRowCursorAsync(rig, seq);
        Assert.Equal(seq, rig.Store.Current!.Head);
        Assert.Equal(1, rig.Applier.LastPassEntries);
    }

    [Fact]
    public async Task The_loops_retry_after_a_stall_runs_without_hints()
    {
        var rig = await RigAsync(["a"], configure: o => o.ApplyPollInterval = TimeSpan.FromMilliseconds(200));
        await rig.LoadAsync(startLoop: true);
        rig["a"].Before = (_, _) =>
        {
            rig["a"].Before = null;
            throw new InvalidOperationException("a stall, once");
        };

        var seq = await rig.WriteAsync(("a", "k1"));
        await UntilAsync(() => rig.Store.Current!.Head == seq);

        var calls = rig["a"].Calls.ToArray();
        Assert.True(calls.Length >= 3, $"calls: {calls.Length}");
        Assert.Equal(["k1"], calls[^2].Hints!.Order());
        Assert.Null(calls[^1].Hints);
    }

    [Fact]
    public async Task A_wake_up_after_the_loops_first_poll_starts_a_pass()
    {
        var clock = new PacedClock();
        var rig = await RigAsync(["a"], configure: o => o.ApplyPollInterval = TimeSpan.FromMinutes(5), time: clock);
        await rig.LoadAsync(startLoop: true);
        await Poll.UntilAsync(() => clock.IsPending(TimeSpan.FromMinutes(5)), "the loop's first poll ran, found nothing and parked");
        var applied = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = rig.Channel.Subscribe(Reeve.Cluster.Wakeup.WakeupKind.Applied, w => applied.TrySetResult(w.Seq));

        var seq = await rig.WriteAsync(("a", "k1"));
        await rig.Channel.PublishAsync(new Reeve.Cluster.Wakeup.Wakeup(Reeve.Cluster.Wakeup.WakeupKind.Appended, seq));

        Assert.Equal(seq, await applied.Task.WaitAsync(Wait));
    }

    [Fact]
    public async Task The_gate_emits_LoadDone_with_the_epoch_when_the_load_is_applied()
    {
        var rig = await RigAsync(["a"]);
        await rig.LoadAsync();

        rig.Gate.Apply(MemberTrigger.StateLoaded);

        Assert.Equal(ReplicaPhase.CatchingUp, rig.Gate.Phase);
        var done = Assert.Single(rig.Events.All, e => e.Action == LogActions.LoadDone);
        Assert.Equal((object)rig.Epoch, done.State!["epoch"]);
    }

    private static int PassBegins(ApplierRig rig) => rig.Events.All.Count(e => e.Action == LogActions.PassBegin);

    private static Task TerminateOtherSessionsAsync(ApplierRig rig) =>
        rig.ExecuteAsync(
            "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity "
            + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'");

    private static async Task<string> SettingsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT current_setting('transaction_read_only') || ', ' || current_setting('transaction_isolation')", connection, transaction);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task UntilRowCursorAsync(ApplierRig rig, long seq)
    {
        var deadline = DateTime.UtcNow + Wait;
        while ((await rig.RowCursorAsync()).Seq != seq)
        {
            Assert.True(DateTime.UtcNow < deadline, $"The row cursor did not reach {seq} in time.");
            await Task.Delay(50);
        }
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition did not hold in time.");
            await Task.Delay(20);
        }
    }

    private async Task<ApplierRig> RigAsync(string[] types, IFaultPoints? points = null, Action<ClusterOptions>? configure = null, TimeProvider? time = null)
    {
        var rig = await ApplierRig.CreateAsync(postgres, types, points, configure, time);
        rigs.Add(rig);
        return rig;
    }
}
