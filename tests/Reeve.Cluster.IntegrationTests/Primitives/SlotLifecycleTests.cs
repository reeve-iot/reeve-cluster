using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class SlotLifecycleTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public SlotLifecycleTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_renewal_extends_a_valid_slot_to_the_new_time_to_live()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromSeconds(10));

        Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i", token, TimeSpan.FromMinutes(5)));

        Assert.True(await rig.Claims.ScalarAsync<bool>("SELECT expires_at > clock_timestamp() + interval '4 minutes' FROM reeve_cluster.slot WHERE key = 'k'"));
    }

    [Fact]
    public async Task The_renewal_of_a_lapsed_slot_is_ended_and_does_not_make_it_valid_again()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await rig.ExpireAsync("k", "i");

        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));

        Assert.False((await rig.RowAsync("k", "i"))!.Valid);
    }

    [Fact]
    public async Task A_stale_tokens_renewal_release_and_fenced_effect_are_refused_and_the_current_tenure_is_untouched()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var stale = await a.TakeAsync("k", "i");
        await rig.ExpireAsync("k", "i");
        var current = await b.TakeAsync("k", "i");
        Assert.NotEqual(stale, current);
        await using var connection = await rig.Db.OpenAsync();

        var refused = await Assert.ThrowsAsync<SlotLostException>(() => a.Slots.BeginSlotFencedTransactionAsync("k", "i", stale, connection));
        Assert.Equal("RC004", SqlStateIn(refused));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", stale));
        Assert.False(await a.Slots.ReleaseAsync("k", "i", stale));

        Assert.Equal(new SlotRow("b", b.Epoch, current, Live: true, Valid: true), await rig.RowAsync("k", "i"));
    }

    [Fact]
    public async Task A_token_that_is_not_in_this_processs_registry_is_ended_and_a_wrong_token_extends_nothing()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");

        Assert.Equal(SlotRenewResult.Ended, await b.RenewAsync("k", "i", token));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", Guid.NewGuid()));
        Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i", token));
    }

    [Fact]
    public async Task A_release_with_a_token_ends_only_that_tokens_slot_and_one_without_ends_whatever_the_id_has()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");

        Assert.False(await a.Slots.ReleaseAsync("k", "i", Guid.NewGuid()));
        Assert.NotNull(await rig.RowAsync("k", "i"));
        Assert.True(await a.Slots.ReleaseAsync("k", "i", null));
        Assert.Null(await rig.RowAsync("k", "i"));
        Assert.False(await a.Slots.ReleaseAsync("k", "i", token));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));
    }

    [Fact]
    public async Task B1_a_draining_members_slot_outlives_its_member_lease_while_it_is_renewed_and_no_peer_takes_it()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");

        await rig.Claims.SetStateAsync("a", "draining");
        await rig.Claims.EndLeaseAsync("a");
        a.Serving = false;
        Assert.True(await rig.ValidAsync("k", "i", token));
        Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i", token));
        Assert.IsType<SlotAcquireResult.Full>(await b.AcquireAsync("k", "j"));

        Assert.True(await a.Slots.ReleaseAsync("k", "i", token));
        Assert.True((await b.AcquireAsync("k", "j")).IsAcquired);
    }

    [Fact]
    public async Task B1_a_draining_member_that_stops_renewing_loses_its_slot_at_the_slots_own_time_to_live()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");
        await rig.Claims.SetStateAsync("a", "draining");
        await rig.ExpireAsync("k", "i");

        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));
        Assert.True((await b.AcquireAsync("k", "j")).IsAcquired);
    }

    [Fact]
    public async Task A_fenced_members_slot_is_invalid_from_step_3_its_renewal_is_refused_and_a_peer_takes_the_place()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i", limit: 2, ttl: TimeSpan.FromMinutes(10));
        var second = await a.TakeAsync("k", "i2", limit: 2, ttl: TimeSpan.FromMinutes(10));

        await rig.FenceMemberAsync("a");
        Assert.False(await rig.ValidAsync("k", "i", token));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));
        a.Holder = false;
        Assert.Equal(SlotRenewResult.NotServing, await a.RenewAsync("k", "i2", second));

        Assert.True((await b.AcquireAsync("k", "j", limit: 2)).IsAcquired);
        Assert.Equal(1L, await rig.ValidCountAsync("k"));
        Assert.Equal(1L, await rig.CountAsync("k"));
    }

    [Fact]
    public async Task A_slot_whose_member_was_fenced_stays_invalid_after_the_member_rejoins_under_a_new_epoch()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        await rig.FenceMemberAsync("a");

        var rejoined = await rig.RejoinAsync("a");

        Assert.NotEqual(a.Epoch, rejoined);
        Assert.False(await rig.ValidAsync("k", "i", token));
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));
        Assert.False((await rig.RowAsync("k", "i"))!.Valid);
    }

    [Fact]
    public async Task A_member_that_joined_again_without_a_fence_ends_its_earlier_epochs_slots()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));

        await rig.RejoinAsync("a");

        Assert.False(await rig.ValidAsync("k", "i", token));
    }

    [Fact]
    public async Task A_peers_acquire_deletes_the_slot_of_a_member_that_joined_again_without_a_fence_and_is_granted()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        await rig.RejoinAsync("a");

        var result = await b.AcquireAsync("k", "j", limit: 1);

        Assert.True(result.IsAcquired);
        Assert.Equal(1L, await rig.CountAsync("k"));
        Assert.Null(await rig.RowAsync("k", "i"));
    }

    [Fact]
    public async Task A_same_id_acquire_after_a_rejoin_grants_a_new_slot_and_does_not_re_acquire_the_old_epochs_row()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var old = await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        var rejoined = new SlotReplica(rig, "a", await rig.RejoinAsync("a"));

        var fresh = await rejoined.TakeAsync("k", "i");

        Assert.NotEqual(old, fresh);
        Assert.Equal(new SlotRow("a", rejoined.Epoch, fresh, Live: true, Valid: true), await rig.RowAsync("k", "i"));
        Assert.Equal(1L, await rig.CountAsync("k"));
    }

    [Theory]
    [InlineData("row gone")]
    [InlineData("stale token")]
    [InlineData("expired")]
    public async Task D2_a_renewal_on_the_previous_timeline_that_reaches_no_live_row_answers_Ended_and_does_not_fence(string case_)
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");
        switch (case_)
        {
            case "row gone":
                await rig.Claims.ExecAsync("DELETE FROM reeve_cluster.slot WHERE key = 'k'");
                break;
            case "stale token":
                await rig.ExpireAsync("k", "i");
                await b.TakeAsync("k", "i");
                break;
            default:
                await rig.ExpireAsync("k", "i");
                break;
        }

        a.Timeline = rig.Db.PreviousTimeline;

        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", token));
        Assert.Empty(a.OtherTimelines);
    }

    [Fact]
    public async Task A_deleted_member_row_ends_its_slots()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));

        await rig.Claims.ExecAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");

        Assert.False(await rig.ValidAsync("k", "i", token));
    }

    [Fact]
    public async Task I32_a_renewal_on_another_timeline_answers_not_serving_and_starts_the_fence()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        a.Timeline = rig.Db.PreviousTimeline;

        Assert.Equal(SlotRenewResult.NotServing, await a.RenewAsync("k", "i", token));

        Assert.Equal([a.Epoch], a.OtherTimelines);
        Assert.True((await rig.RowAsync("k", "i"))!.Valid);
    }

    [Fact]
    public async Task A_renewal_whose_holder_check_refused_sends_nothing()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromSeconds(10));
        await rig.SetTtlAsync("k", "i", "5 seconds");
        a.Holder = false;

        Assert.Equal(SlotRenewResult.NotServing, await a.RenewAsync("k", "i", token, TimeSpan.FromMinutes(5)));

        Assert.True(await rig.Claims.ScalarAsync<bool>("SELECT expires_at < clock_timestamp() + interval '10 seconds' FROM reeve_cluster.slot WHERE key = 'k'"));
    }

    private static string? SqlStateIn(Exception? exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is Npgsql.PostgresException pg)
            {
                return pg.SqlState;
            }
        }

        return null;
    }
}
