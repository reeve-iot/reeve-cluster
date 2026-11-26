using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class SlotHolderReadTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public SlotHolderReadTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_valid_slot_is_read_on_a_peer_with_its_id_holder_replica_and_epoch()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i");

        var holders = await b.Slots.ReadHoldersAsync("k");

        Assert.Equal([new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }], holders);
    }

    [Fact]
    public async Task A_key_without_a_slot_and_another_keys_slot_read_as_no_holder()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await a.TakeAsync("other", "i");

        Assert.Empty(await a.Slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task A_slot_whose_time_to_live_passed_by_the_database_clock_is_not_read_although_its_row_is_left()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await a.TakeAsync("k", "i");
        await rig.ExpireAsync("k", "i");

        Assert.Empty(await a.Slots.ReadHoldersAsync("k"));
        Assert.Equal(1L, await rig.CountAsync("k"));
    }

    [Fact]
    public async Task A_fenced_members_slot_is_not_read_from_fence_step_3()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));

        await rig.FenceMemberAsync("a");

        Assert.Empty(await b.Slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task B1_a_draining_members_slot_is_read_while_it_is_valid_without_a_member_lease()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i");

        await rig.Claims.SetStateAsync("a", "draining");
        await rig.Claims.EndLeaseAsync("a");

        Assert.Equal([new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }], await b.Slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task An_active_members_slot_is_read_after_its_member_lease_ended_because_validity_has_no_lease_arm()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await a.TakeAsync("k", "i");

        await rig.Claims.EndLeaseAsync("a");

        Assert.Equal([new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }], await a.Slots.ReadHoldersAsync("k"));
    }

    [Theory]
    [InlineData("fenced first")]
    [InlineData("without a fence")]
    public async Task A_slot_of_an_earlier_epoch_is_not_read_after_its_member_joined_again(string case_)
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        if (case_ == "fenced first")
        {
            await rig.FenceMemberAsync("a");
        }

        await rig.RejoinAsync("a");

        Assert.Empty(await a.Slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task A_slot_whose_member_row_was_deleted_is_not_read()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));

        await rig.Claims.ExecAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");

        Assert.Empty(await b.Slots.ReadHoldersAsync("k"));
    }

    [Theory]
    [InlineData("active", false, true)]
    [InlineData("active", true, false)]
    [InlineData("draining", false, true)]
    [InlineData("draining", true, false)]
    [InlineData("fenced", false, false)]
    [InlineData("fenced", true, false)]
    [InlineData("joining", false, false)]
    [InlineData("joining", true, false)]
    [InlineData("deleted", false, false)]
    [InlineData("deleted", true, false)]
    public async Task The_read_answers_a_slot_exactly_while_slot_valid_holds_for_it(string memberState, bool expired, bool held)
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        if (expired)
        {
            await rig.ExpireAsync("k", "i");
        }

        if (memberState == "deleted")
        {
            await rig.Claims.ExecAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");
        }
        else
        {
            await rig.Claims.SetStateAsync("a", memberState);
        }

        var holders = await b.Slots.ReadHoldersAsync("k");

        SlotHolder[] expected = held ? [new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }] : [];
        Assert.Equal(held, (await rig.RowAsync("k", "i"))!.Valid);
        Assert.Equal(await rig.ValidCountAsync("k"), (long)holders.Count);
        Assert.Equal(expected, holders);
    }

    [Fact]
    public async Task A_key_held_under_a_limit_above_one_reads_every_valid_holder_ordered_by_id()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "j", limit: 3);
        await b.TakeAsync("k", "i", limit: 3);
        await a.TakeAsync("k", "h", limit: 3);
        await rig.ExpireAsync("k", "h");

        var holders = await b.Slots.ReadHoldersAsync("k");

        Assert.Equal(
            [new SlotHolder { Id = "i", ReplicaId = "b", Epoch = b.Epoch }, new SlotHolder { Id = "j", ReplicaId = "a", Epoch = a.Epoch }],
            holders);
    }

    [Fact]
    public async Task A_replica_that_neither_serves_nor_holds_reads_the_holder()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b", state: "fenced");
        await a.TakeAsync("k", "i");
        b.Serving = false;
        b.Holder = false;

        Assert.Equal([new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }], await b.Slots.ReadHoldersAsync("k"));
    }

    [Fact]
    public async Task A_read_waits_for_neither_the_keys_acquire_lock_nor_an_uncommitted_delete_and_answers_the_committed_holder()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        await a.TakeAsync("k", "i");
        await using var connection = await rig.Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand("SELECT reeve_cluster.slot_lock('k', @timeline)", connection, transaction))
        {
            hold.Parameters.AddWithValue("timeline", rig.Db.Timeline);
            await hold.ExecuteNonQueryAsync();
        }

        await using (var delete = new NpgsqlCommand("DELETE FROM reeve_cluster.slot WHERE key = 'k'", connection, transaction))
        {
            Assert.Equal(1, await delete.ExecuteNonQueryAsync());
        }

        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var holders = await b.Slots.ReadHoldersAsync("k", bound.Token);

        Assert.Equal([new SlotHolder { Id = "i", ReplicaId = "a", Epoch = a.Epoch }], holders);
        await transaction.RollbackAsync();
    }
}
