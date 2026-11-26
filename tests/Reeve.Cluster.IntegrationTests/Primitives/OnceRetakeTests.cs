using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class OnceRetakeTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public OnceRetakeTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_record_whose_executors_lease_ended_is_retaken_by_the_sweeper_and_its_effect_repeated_there()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var old = await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch);
        await rig.Claims.EndLeaseAsync("dead");

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal([("send", "k")], rig.Ran);
        var row = Assert.IsType<OnceRow>(await rig.RowAsync("k"));
        Assert.Equal(("sweeper", sweeper.Epoch, true), (row.Holder, row.Epoch, row.Completed));
        Assert.NotEqual(old, row.Token);
    }

    [Fact]
    public async Task A_record_whose_time_to_live_passed_is_retaken_although_its_executor_lives()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var slow = await rig.Claims.AddReplicaAsync("slow");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        await rig.SeedAsync("k", "send", "at_least_once", "slow", slow.Epoch, expired: true);

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal("sweeper", (await rig.RowAsync("k"))!.Holder);
    }

    [Fact]
    public async Task A_retaken_record_is_valid_under_the_sweeper_for_the_time_to_live_the_call_gave()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true, ttl: "2 hours");
        OnceRow? observed = null;
        rig.Body = async (key, _) =>
        {
            observed = await rig.RowAsync(key);
        };

        await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Single(rig.Ran);
        Assert.True(observed!.Valid);
        Assert.Equal("sweeper", observed.Holder);
        Assert.True(await rig.Claims.ScalarAsync<bool>(
            "SELECT expires_at > clock_timestamp() + interval '1 hour' FROM reeve_cluster.claim WHERE key = @key", ("key", OnceKeys.Store("k"))));
    }

    [Fact]
    public async Task A_record_whose_executor_is_alive_and_whose_claim_is_valid_is_left_alone()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var alive = await rig.Claims.AddReplicaAsync("alive");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var token = await rig.SeedAsync("k", "send", "at_least_once", "alive", alive.Epoch);

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Equal(token, (await rig.RowAsync("k"))!.Token);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task A_complete_record_and_an_AtMostOnce_record_are_never_retaken_whatever_their_validity()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var done = await rig.SeedAsync("done", "send", "at_least_once", "dead", dead.Epoch, expired: true, completed: true);
        var most = await rig.SeedAsync("most", "send", "at_most_once", "dead", dead.Epoch, expired: true);
        await rig.Claims.EndLeaseAsync("dead");

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Equal(done, (await rig.RowAsync("done"))!.Token);
        Assert.Equal(most, (await rig.RowAsync("most"))!.Token);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task A_plain_claim_with_no_once_mode_is_never_retaken()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var holder = await rig.Claims.AddReplicaAsync("holder");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var token = await holder.WinAsync("plain", TimeSpan.FromMinutes(1));
        await rig.Claims.ExpireClaimAsync("plain");

        await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(token, (await rig.Claims.RowAsync("plain"))!.Token);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task A_record_of_an_effect_the_sweeper_does_not_have_is_left_for_one_that_does_and_does_not_starve_the_others()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var foreign = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            foreign.Add(await rig.SeedAsync($"foreign{i}", "elsewhere", "at_least_once", "dead", dead.Epoch, expired: true));
        }

        await rig.SeedAsync("mine", "send", "at_least_once", "dead", dead.Epoch, expired: true);

        var count = await rig.SweeperOf(sweeper, passLimit: 2).PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.Equal([("send", "mine")], rig.Ran);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(foreign[i], (await rig.RowAsync($"foreign{i}"))!.Token);
        }
    }

    [Fact]
    public async Task Two_sweepers_racing_one_retake_run_the_effect_once()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var s1 = await rig.Claims.AddReplicaAsync("s1");
        var s2 = await rig.Claims.AddReplicaAsync("s2");
        await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        var hold1 = s1.Points.Hold(ClaimsFaultPoints.RetakeBeforeStatement);
        var hold2 = s2.Points.Hold(ClaimsFaultPoints.RetakeBeforeStatement);

        var pass1 = rig.SweeperOf(s1).PassAsync(CancellationToken.None);
        var pass2 = rig.SweeperOf(s2).PassAsync(CancellationToken.None);
        await Task.WhenAll(hold1.Arrived, hold2.Arrived).WaitAsync(Poll.Bound);
        hold1.Release();
        hold2.Release();
        var counts = await Task.WhenAll(pass1, pass2);

        Assert.Equal(1, counts.Sum());
        Assert.Single(rig.Ran);
        Assert.True((await rig.RowAsync("k"))!.Completed);
    }

    [Fact]
    public async Task A_retake_that_met_another_sweepers_retake_is_refused_by_the_token_compare()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var s1 = await rig.Claims.AddReplicaAsync("s1");
        var s2 = await rig.Claims.AddReplicaAsync("s2");
        await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        var hold = s1.Points.Hold(ClaimsFaultPoints.RetakeBeforeStatement);

        var slow = rig.SweeperOf(s1).PassAsync(CancellationToken.None);
        await hold.Arrived.WaitAsync(Poll.Bound);
        Assert.Equal(1, await rig.SweeperOf(s2).PassAsync(CancellationToken.None));
        hold.Release();

        Assert.Equal(0, await slow);
        Assert.Single(rig.Ran);
    }

    [Fact]
    public async Task I1_a_sweeper_that_does_not_serve_retakes_nothing()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var old = await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        sweeper.Serving = false;

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Equal(old, (await rig.RowAsync("k"))!.Token);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task I1_a_sweeper_paused_before_its_retake_across_fence_start_retakes_nothing()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        var old = await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        var hold = sweeper.Points.Hold(ClaimsFaultPoints.RetakeBeforeStatement);

        var pass = rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);
        await hold.Arrived.WaitAsync(Poll.Bound);
        sweeper.Serving = false;
        hold.Release();

        Assert.Equal(0, await pass);
        Assert.Equal(old, (await rig.RowAsync("k"))!.Token);
        Assert.Empty(rig.Ran);
    }

    [Fact]
    public async Task An_effect_that_throws_on_the_sweeper_leaves_the_record_retaken_not_complete_and_the_pass_goes_on()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        await rig.SeedAsync("bad", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        await rig.SeedAsync("good", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        rig.Body = (key, _) => key == "bad" ? throw new InvalidOperationException("the effect failed") : Task.CompletedTask;

        var count = await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None);

        Assert.Equal(1, count);
        Assert.False((await rig.RowAsync("bad"))!.Completed);
        Assert.True((await rig.RowAsync("good"))!.Completed);
    }

    [Fact]
    public async Task A_retaken_record_whose_completion_was_refused_is_a_candidate_again_once_its_new_claim_ends()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var sweeper = await rig.Claims.AddReplicaAsync("sweeper");
        await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);
        rig.Body = (key, _) => rig.ExpireAsync(key);

        Assert.Equal(1, await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None));
        Assert.False((await rig.RowAsync("k"))!.Completed);
        rig.Body = null;

        Assert.Equal(1, await rig.SweeperOf(sweeper).PassAsync(CancellationToken.None));
        Assert.Equal(2, rig.Ran.Count);
        Assert.True((await rig.RowAsync("k"))!.Completed);
    }

    [Fact]
    public async Task A_caller_that_finds_an_AtLeastOnce_record_not_valid_reports_it_in_doubt_and_the_sweeper_then_repeats_it()
    {
        await using var rig = await OnceRig.CreateAsync(postgres);
        var dead = await rig.Claims.AddReplicaAsync("dead");
        var caller = await rig.Claims.AddReplicaAsync("caller");
        await rig.SeedAsync("k", "send", "at_least_once", "dead", dead.Epoch, expired: true);

        Assert.Equal(new OnceResult.InDoubt(), await rig.OnceOf(caller).RunAsync("k", "send", OnceMode.AtLeastOnce, TimeSpan.FromMinutes(1)));
        Assert.Empty(rig.Ran);

        await rig.SweeperOf(caller).PassAsync(CancellationToken.None);
        Assert.Equal([("send", "k")], rig.Ran);
        Assert.Equal(new OnceResult.AlreadyCompleted(), await rig.OnceOf(caller).RunAsync("k", "send", OnceMode.AtLeastOnce, TimeSpan.FromMinutes(1)));
    }
}
