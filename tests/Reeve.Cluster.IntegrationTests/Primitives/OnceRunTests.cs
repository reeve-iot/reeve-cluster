using Int.Claims;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class OnceRunTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public OnceRunTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_run_on_a_free_key_runs_the_effect_names_its_executor_and_completes_the_record()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        var result = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtLeastOnce, Minute);

        Assert.Equal(new OnceResult.Completed(), result);
        Assert.Equal([("send", "k")], rig.Ran);
        var row = Assert.IsType<OnceRow>(await rig.RowAsync("k"));
        Assert.Equal(("a", a.Epoch, "at_least_once", "send", true), (row.Holder, row.Epoch, row.Mode, row.Effect, row.Completed));
    }

    [Fact]
    public async Task The_mode_of_the_record_is_the_calls()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.Equal("at_most_once", (await rig.RowAsync("k"))!.Mode);
    }

    [Fact]
    public async Task A_plain_claim_and_a_once_run_on_the_same_key_never_meet()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var once = rig.OnceOf(a);

        var plain = await b.WinAsync("k");
        Assert.Equal(new OnceResult.Completed(), await once.RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
        Assert.Equal(2, await rig.Claims.CountAsync());

        Assert.True(await b.Claims.ReleaseAsync("k", plain));
        var onceRow = await rig.RowAsync("k");
        Assert.True(onceRow!.Completed);
        await a.WinAsync("k");
        Assert.Equal(onceRow, await rig.RowAsync("k"));
        Assert.Equal(new OnceResult.AlreadyCompleted(), await once.RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
    }

    [Fact]
    public async Task A_plain_claim_on_the_once_key_space_is_refused_and_writes_nothing()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        var before = await rig.RowAsync("k");
        var stored = Reeve.Cluster.Claims.OnceKeys.Store("k");

        await Assert.ThrowsAsync<ArgumentException>(() => a.Claims.ClaimAsync(stored, Minute));
        await Assert.ThrowsAsync<ArgumentException>(() => a.Claims.ReleaseAsync(stored, before!.Token));

        Assert.Equal(before, await rig.RowAsync("k"));
    }

    [Fact]
    public async Task A_second_call_after_completion_is_already_completed_on_any_replica_and_runs_nothing()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.Equal(new OnceResult.AlreadyCompleted(), await rig.OnceOf(b).RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
        Assert.Equal(new OnceResult.AlreadyCompleted(), await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtLeastOnce, Minute));

        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task A_completed_run_stays_completed_after_its_executors_lease_and_its_time_to_live_ended()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await rig.Claims.EndLeaseAsync("a");
        await rig.ExpireAsync("k");
        var before = await rig.RowAsync("k");

        Assert.Equal(new OnceResult.AlreadyCompleted(), await rig.OnceOf(b).RunAsync("k", "send", OnceMode.AtMostOnce, Minute));

        Assert.Equal(before, await rig.RowAsync("k"));
        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task A_call_while_another_executors_run_is_valid_is_running_and_runs_nothing()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        rig.Body = async (_, _) =>
        {
            started.TrySetResult();
            await finish.Task;
        };
        var first = rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await started.Task.WaitAsync(Poll.Bound);

        var second = await rig.OnceOf(b).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        finish.SetResult();

        Assert.Equal(new OnceResult.Running("a"), second);
        Assert.Equal(new OnceResult.Completed(), await first);
        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task Callers_racing_one_key_run_the_effect_once()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var replicas = new List<Replica>();
        for (var i = 0; i < 6; i++)
        {
            replicas.Add(await rig.Claims.AddReplicaAsync($"r{i}"));
        }

        var results = await Task.WhenAll(replicas.Select(r => rig.OnceOf(r).RunAsync("k", "send", OnceMode.AtMostOnce, Minute)));

        Assert.Single(rig.Ran);
        Assert.Single(results, r => r is OnceResult.Completed);
        Assert.All(results.Where(r => r is not OnceResult.Completed), r => Assert.True(r is OnceResult.Running or OnceResult.AlreadyCompleted));
    }

    [Fact]
    public async Task I16_a_replica_that_does_not_serve_starts_no_run_and_writes_no_record()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        a.Serving = false;

        var result = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.IsType<OnceResult.NotServing>(result);
        Assert.Empty(rig.Ran);
        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_starts_no_run()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.OnceBeforeInsert);

        var run = rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        hold.Release();

        Assert.IsType<OnceResult.NotServing>(await run);
        Assert.Empty(rig.Ran);
        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task AtMostOnce_a_run_whose_executors_lease_ended_during_the_effect_has_an_unknown_completion_and_is_in_doubt_for_later_calls()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        var b = await rig.Claims.AddReplicaAsync("b");
        rig.Body = (_, _) => rig.Claims.EndLeaseAsync("a");

        var first = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.Equal(new OnceResult.CompletionUnknown(), first);
        Assert.False((await rig.RowAsync("k"))!.Completed);

        await rig.ExpireAsync("k");
        Assert.Equal(new OnceResult.InDoubt(), await rig.OnceOf(b).RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task AtMostOnce_a_run_whose_time_to_live_passed_during_the_effect_has_an_unknown_completion()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        rig.Body = (key, _) => rig.ExpireAsync(key);

        var result = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);

        Assert.Equal(new OnceResult.CompletionUnknown(), result);
        Assert.False((await rig.RowAsync("k"))!.Completed);
    }

    [Fact]
    public async Task An_effect_that_throws_leaves_an_uncompleted_record_and_the_next_call_reports_it_in_doubt()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");
        rig.Body = (_, _) => throw new InvalidOperationException("the effect failed");

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
        await rig.ExpireAsync("k");
        rig.Body = null;

        Assert.Equal(new OnceResult.InDoubt(), await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute));
        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task An_effect_that_is_not_registered_is_refused_and_writes_no_record()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var a = await rig.Claims.AddReplicaAsync("a");

        await Assert.ThrowsAsync<ArgumentException>(() => rig.OnceOf(a).RunAsync("k", "unknown", OnceMode.AtMostOnce, Minute));

        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task PostgreSQL_failing_the_grant_fails_closed_the_effect_does_not_run_and_no_record_is_left()
    {
        await using var rig = await OnceRig.CreateAsync(postgres, TimeSpan.FromSeconds(1));
        var a = await rig.Claims.AddReplicaAsync("a");
        await using var locker = await rig.Claims.Db.OpenAsync();
        await using var holding = await locker.BeginTransactionAsync();
        await Int.Owned.OwnedDatabase.ExecAsync(locker, "LOCK TABLE reeve_cluster.claim IN ACCESS EXCLUSIVE MODE");

        OnceResult? result = null;
        try
        {
            result = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        }
        catch (Exception)
        {
        }
        await holding.RollbackAsync();

        Assert.True(result is null or OnceResult.Unknown);
        Assert.Empty(rig.Ran);
        Assert.Equal(0, await rig.Claims.CountAsync());
    }

    [Fact]
    public async Task PostgreSQL_failing_the_completion_fails_closed_the_run_is_in_doubt_and_not_completed()
    {
        await using var rig = await OnceRig.CreateAsync(postgres, TimeSpan.FromSeconds(1));
        var a = await rig.Claims.AddReplicaAsync("a");
        await using var locker = await rig.Claims.Db.OpenAsync();
        NpgsqlTransaction? holding = null;
        rig.Body = async (_, token) =>
        {
            holding = await locker.BeginTransactionAsync(token);
            await Int.Owned.OwnedDatabase.ExecAsync(locker, "LOCK TABLE reeve_cluster.claim IN ACCESS EXCLUSIVE MODE");
        };

        var result = await rig.OnceOf(a).RunAsync("k", "send", OnceMode.AtMostOnce, Minute);
        await holding!.RollbackAsync();
        await holding.DisposeAsync();

        Assert.Equal(new OnceResult.CompletionUnknown(), result);
        Assert.False((await rig.RowAsync("k"))!.Completed);
        Assert.Single(rig.Ran);
    }
}
