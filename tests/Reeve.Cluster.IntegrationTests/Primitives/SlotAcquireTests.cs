using System.Transactions;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class SlotAcquireTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public SlotAcquireTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I35_concurrent_acquires_from_several_connections_never_pass_the_limit()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var replicas = new[] { await rig.AddReplicaAsync("a"), await rig.AddReplicaAsync("b"), await rig.AddReplicaAsync("c") };

        var results = await Task.WhenAll(Enumerable.Range(0, 18).Select(i => Task.Run(() => replicas[i % 3].AcquireAsync("k", $"id{i}", limit: 4))));

        Assert.Equal(4, results.Count(r => r.IsAcquired));
        Assert.Equal(14, results.Count(r => r is SlotAcquireResult.Full));
        Assert.Equal(4L, await rig.CountAsync("k"));
        Assert.Equal(4L, await rig.ValidCountAsync("k"));
    }

    [Fact]
    public async Task A_mutex_is_a_slot_with_a_limit_of_one_and_its_release_frees_it()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "run-1");

        Assert.IsType<SlotAcquireResult.Full>(await b.AcquireAsync("k", "run-2"));
        Assert.True(await a.Slots.ReleaseAsync("k", "run-1", token));

        Assert.True((await b.AcquireAsync("k", "run-2")).IsAcquired);
    }

    [Fact]
    public async Task I4_an_acquire_a_renewal_and_a_release_are_transactions_of_their_own_and_never_enlist_in_the_callers_ambient_transaction()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");

        Guid token;
        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromSeconds(10));
        }

        Assert.NotNull(await rig.RowAsync("k", "i"));
        Assert.True(await rig.ValidAsync("k", "i", token));

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i", token, TimeSpan.FromMinutes(5)));
        }

        Assert.True(await rig.Claims.ScalarAsync<bool>("SELECT expires_at > clock_timestamp() + interval '4 minutes' FROM reeve_cluster.slot WHERE key = 'k'"));

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            Assert.True(await a.Slots.ReleaseAsync("k", "i", token));
        }

        Assert.Null(await rig.RowAsync("k", "i"));
    }

    [Fact]
    public async Task The_re_acquire_with_the_same_id_answers_the_slot_without_comparing_the_limit_and_extends_it()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", limit: 1);

        var again = await a.AcquireAsync("k", "i", limit: 1, ttl: TimeSpan.FromMinutes(10));

        Assert.Equal(new SlotAcquireResult.Acquired(token), again);
        Assert.Equal(1L, await rig.CountAsync("k"));
        Assert.True(await rig.Claims.ScalarAsync<bool>("SELECT expires_at > clock_timestamp() + interval '5 minutes' FROM reeve_cluster.slot WHERE key = 'k'"));
        Assert.IsType<SlotAcquireResult.Full>(await a.AcquireAsync("k", "other", limit: 1));
    }

    [Fact]
    public async Task A_re_acquire_with_the_id_of_a_valid_slot_answers_that_slot_whoever_acquired_it()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");

        Assert.Equal(new SlotAcquireResult.Acquired(token), await b.AcquireAsync("k", "i"));
        Assert.Equal("a", (await rig.RowAsync("k", "i"))!.Holder);
    }

    [Fact]
    public async Task A_lowered_limit_ends_no_slot_refuses_new_ones_and_still_answers_a_re_acquire()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var tokens = new[] { await a.TakeAsync("k", "i0", limit: 3), await a.TakeAsync("k", "i1", limit: 3), await a.TakeAsync("k", "i2", limit: 3) };

        Assert.IsType<SlotAcquireResult.Full>(await a.AcquireAsync("k", "i3", limit: 2));
        Assert.Equal(new SlotAcquireResult.Acquired(tokens[0]), await a.AcquireAsync("k", "i0", limit: 1));
        Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i1", tokens[1]));
        Assert.Equal(3L, await rig.ValidCountAsync("k"));

        Assert.True(await a.Slots.ReleaseAsync("k", "i0", tokens[0]));
        Assert.IsType<SlotAcquireResult.Full>(await a.AcquireAsync("k", "i3", limit: 2));
        Assert.True(await a.Slots.ReleaseAsync("k", "i1", tokens[1]));
        Assert.True((await a.AcquireAsync("k", "i3", limit: 2)).IsAcquired);
    }

    [Fact]
    public async Task An_acquire_deletes_the_keys_invalid_rows_and_takes_the_place_of_an_expired_slot()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "old");
        await a.TakeAsync("other", "stays");
        await rig.ExpireAsync("k", "old");

        Assert.True((await b.AcquireAsync("k", "new")).IsAcquired);

        Assert.Null(await rig.RowAsync("k", "old"));
        Assert.Equal(1L, await rig.CountAsync("k"));
        Assert.Equal(1L, await rig.CountAsync("other"));
    }

    [Fact]
    public async Task A_crashed_holders_slot_frees_at_its_time_to_live()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var crashed = await rig.AddReplicaAsync("crashed");
        var b = await rig.AddReplicaAsync("b");
        var token = await crashed.TakeAsync("k", "i", ttl: TimeSpan.FromSeconds(3.5));

        Assert.IsType<SlotAcquireResult.Full>(await b.AcquireAsync("k", "j"));
        await Poll.UntilAsync(async () => !await rig.ValidAsync("k", "i", token), "the slot's time to live to pass");

        Assert.True((await b.AcquireAsync("k", "j")).IsAcquired);
    }

    [Fact]
    public async Task A_slot_and_a_claim_under_one_key_are_unrelated()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var c = await rig.Claims.AddReplicaAsync("c");

        Assert.IsType<ClaimResult.Won>(await c.Claims.ClaimAsync("k", TimeSpan.FromMinutes(1)));
        Assert.True((await a.AcquireAsync("k", "i")).IsAcquired);
    }

    [Fact]
    public async Task I1_a_replica_that_does_not_serve_acquires_nothing_and_writes_nothing()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        a.Serving = false;

        Assert.IsType<SlotAcquireResult.NotServing>(await a.AcquireAsync("k", "i"));
        Assert.Equal(0L, await rig.CountAsync("k"));
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_acquire_across_fence_start_writes_nothing()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var hold = a.Points.Hold(SlotsFaultPoints.AcquireBeforeStatement);

        var acquire = a.AcquireAsync("k", "i");
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        hold.Release();

        Assert.IsType<SlotAcquireResult.NotServing>(await acquire);
        Assert.Equal(0L, await rig.CountAsync("k"));
    }

    [Fact]
    public async Task I32_an_acquire_on_another_timeline_than_the_join_answers_not_serving_and_starts_the_fence()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        a.Timeline = rig.Db.PreviousTimeline;

        Assert.IsType<SlotAcquireResult.NotServing>(await a.AcquireAsync("k", "i"));

        Assert.Equal([a.Epoch], a.OtherTimelines);
        Assert.Equal(0L, await rig.CountAsync("k"));
    }

    [Fact]
    public async Task A_grant_is_not_checked_again_after_its_commit_so_Acquired_can_come_back_after_the_fence_started()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var hold = a.Points.Hold(SlotsFaultPoints.AcquireAfterCommit);

        var acquire = a.AcquireAsync("k", "i");
        await hold.Arrived.WaitAsync(Poll.Bound);
        a.Serving = false;
        a.Holder = false;
        hold.Release();

        Assert.True((await acquire).IsAcquired);
        Assert.Equal(1L, await rig.ValidCountAsync("k"));
    }
}
