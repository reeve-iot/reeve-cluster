using Int.Owned;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Slots;

namespace Int.Primitives;

public sealed class SlotFenceTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;

    public SlotFenceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I11_a_slot_fenced_transaction_holds_its_row_FOR_KEY_SHARE_which_a_renewal_does_not_wait_for_and_a_delete_does()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await using var connection = await rig.Db.OpenAsync();

        await using var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection);

        Assert.True(await rig.CanLockAsync("k", "i", "FOR NO KEY UPDATE"));
        Assert.True(await rig.CanLockAsync("k", "i", "FOR KEY SHARE"));
        Assert.False(await rig.CanLockAsync("k", "i", "FOR UPDATE"));
        Assert.Equal(SlotRenewResult.Renewed, await a.RenewAsync("k", "i", token).WaitAsync(Poll.Bound));
    }

    [Fact]
    public async Task A_slot_fenced_transaction_runs_under_FencedTransactionTimeout_and_commits_its_effect()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY, who text NOT NULL)");

        await using (var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection))
        {
            Assert.Equal("30s", await ScalarAsync<string>(fenced, "SHOW transaction_timeout"));
            await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1, 'a')");
            await fenced.CommitAsync();
        }

        Assert.Equal(1L, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.True(await rig.CanLockAsync("k", "i", "FOR UPDATE"));
    }

    [Fact]
    public async Task A_release_or_an_acquire_inside_an_open_scope_of_this_process_is_refused_until_the_transaction_ends()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", limit: 2);
        await using var connection = await rig.Db.OpenAsync();

        await using (await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => a.Slots.ReleaseAsync("k", "i", token));
            await Assert.ThrowsAsync<InvalidOperationException>(() => a.AcquireAsync("k", "other", limit: 2));
            Assert.False(await a.Slots.ReleaseAsync("k", "unrelated", Guid.NewGuid()));
        }

        Assert.True(await a.Slots.ReleaseAsync("k", "i", token).WaitAsync(Poll.Bound));
    }

    [Theory]
    [InlineData("expired")]
    [InlineData("fenced member")]
    [InlineData("rejoined member")]
    [InlineData("deleted row")]
    [InlineData("wrong token")]
    public async Task A_begin_is_refused_with_RC004_unless_the_slot_is_valid_and_holds_no_lock(string reason)
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromMinutes(10));
        var asked = token;
        switch (reason)
        {
            case "expired":
                await rig.ExpireAsync("k", "i");
                break;
            case "fenced member":
                await rig.FenceMemberAsync("a");
                break;
            case "rejoined member":
                await rig.RejoinAsync("a");
                break;
            case "deleted row":
                await rig.Claims.ExecAsync("DELETE FROM reeve_cluster.slot WHERE key = 'k'");
                break;
            default:
                asked = Guid.NewGuid();
                break;
        }

        await using var connection = await rig.Db.OpenAsync();
        var refused = await Assert.ThrowsAsync<SlotLostException>(() => a.Slots.BeginSlotFencedTransactionAsync("k", "i", asked, connection));

        Assert.Equal(reason == "wrong token" ? null : "RC004", SqlStateIn(refused));
        if (reason != "deleted row")
        {
            Assert.True(await rig.CanLockAsync("k", "i", "FOR UPDATE"));
        }

        await using var again = await connection.BeginTransactionAsync();
    }

    [Fact]
    public async Task B1_a_draining_holders_effect_is_fenced_by_its_slot_which_stays_valid_without_a_member_lease()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await rig.Claims.SetStateAsync("a", "draining");
        await rig.Claims.EndLeaseAsync("a");
        a.Serving = false;
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY)");

        await using (var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection))
        {
            await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1)");
            await fenced.CommitAsync();
        }

        Assert.Equal(1L, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
    }

    [Fact]
    public async Task A_fenced_process_whose_holder_check_refuses_begins_nothing()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        a.Holder = false;
        await using var connection = await rig.Db.OpenAsync();

        var refused = await Assert.ThrowsAsync<SlotLostException>(() => a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection));

        Assert.Null(SqlStateIn(refused));
        Assert.True(await rig.CanLockAsync("k", "i", "FOR UPDATE"));
    }

    [Fact]
    public async Task I32_a_begin_on_another_timeline_than_the_join_is_refused_as_SlotLostException_and_starts_the_fence()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await using var connection = await rig.Db.OpenAsync();
        a.Timeline = rig.Db.PreviousTimeline;

        var refused = await Assert.ThrowsAsync<SlotLostException>(() => a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection));

        Assert.Equal("RC002", SqlStateIn(refused));
        Assert.Equal([a.Epoch], a.OtherTimelines);
        Assert.True(await rig.CanLockAsync("k", "i", "FOR UPDATE"));
    }

    [Fact]
    public async Task A_grant_waits_for_an_open_slot_fenced_transaction_on_an_invalid_row_and_the_effect_lands_before_the_grant()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "i");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY, who text NOT NULL)");
        var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, connection);
        await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1, 'a')");

        await rig.ExpireAsync("k", "i");
        var grant = b.AcquireAsync("k", "j");
        await rig.Db.WaitForALockWaiterAsync();
        Assert.False(grant.IsCompleted);
        Assert.Null(await rig.RowAsync("k", "j"));

        await fenced.CommitAsync();
        await fenced.DisposeAsync();
        var result = await grant.WaitAsync(Poll.Bound);

        Assert.True(result.IsAcquired);
        Assert.Equal(1L, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
        Assert.Null(await rig.RowAsync("k", "i"));
    }

    [Fact]
    public async Task C2_a_renewal_held_uncommitted_across_the_end_of_the_slot_while_an_acquire_deletes_before_it_and_counts_after_it_answers_Full()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "x");

        await using var renewing = await rig.Db.OpenAsync();
        await using var renewal = await renewing.BeginTransactionAsync();
        await using (var update = new NpgsqlCommand(SlotStore.RenewSql, renewing, renewal))
        {
            update.Parameters.AddWithValue("key", "k");
            update.Parameters.AddWithValue("id", "x");
            update.Parameters.AddWithValue("token", token);
            update.Parameters.AddWithValue("timeline", rig.Db.Timeline);
            update.Parameters.AddWithValue("ttl", TimeSpan.FromMinutes(10));
            Assert.Equal(token, await update.ExecuteScalarAsync());
        }

        var hold = b.Points.Hold(SlotsFaultPoints.AcquireAfterDelete);
        var acquire = b.AcquireAsync("k", "y", limit: 1);
        await hold.Arrived.WaitAsync(Poll.Bound);
        await rig.FenceMemberAsync("a");
        await Poll.UntilAsync(async () => !await rig.ValidAsync("k", "x", token), "the committed version of the slot to end");
        hold.Release();

        Assert.IsType<SlotAcquireResult.Full>(await acquire.WaitAsync(Poll.Bound));
        await rig.Claims.SetStateAsync("a", "active");
        await renewal.CommitAsync();
        Assert.True(await rig.ValidAsync("k", "x", token));
        Assert.True(await rig.Claims.ScalarAsync<bool>("SELECT expires_at > clock_timestamp() + interval '5 minutes' FROM reeve_cluster.slot WHERE key = 'k' AND id = 'x'"));
        Assert.Equal(1L, await rig.ValidCountAsync("k"));
        Assert.Null(await rig.RowAsync("k", "y"));
    }

    [Fact]
    public async Task C2_an_acquire_whose_delete_waits_for_a_renewal_that_is_then_rolled_back_deletes_the_row_and_is_granted()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "x");

        await using var renewing = await rig.Db.OpenAsync();
        await using var renewal = await renewing.BeginTransactionAsync();
        await using (var update = new NpgsqlCommand(SlotStore.RenewSql, renewing, renewal))
        {
            update.Parameters.AddWithValue("key", "k");
            update.Parameters.AddWithValue("id", "x");
            update.Parameters.AddWithValue("token", token);
            update.Parameters.AddWithValue("timeline", rig.Db.Timeline);
            update.Parameters.AddWithValue("ttl", TimeSpan.FromMinutes(1));
            Assert.Equal(token, await update.ExecuteScalarAsync());
        }

        await rig.FenceMemberAsync("a");
        await Poll.UntilAsync(async () => !await rig.ValidAsync("k", "x", token), "the committed version of the slot to end");
        var acquire = b.AcquireAsync("k", "y", limit: 1);
        await rig.Db.WaitForALockWaiterAsync();
        Assert.False(acquire.IsCompleted);

        await renewal.RollbackAsync();

        Assert.True((await acquire.WaitAsync(Poll.Bound)).IsAcquired);
        Assert.Equal(1L, await rig.CountAsync("k"));
        Assert.Null(await rig.RowAsync("k", "x"));
    }

    [Fact]
    public async Task C2_with_a_slot_fenced_transaction_open_on_the_slot_the_grant_waits_for_it_and_the_effect_commits_before_any_grant()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.TakeAsync("k", "x");
        await using var connection = await rig.Db.OpenAsync();
        await OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY)");
        var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "x", token, connection);
        await OwnedDatabase.ExecAsync(connection, "INSERT INTO public.effect VALUES (1)");

        var hold = b.Points.Hold(SlotsFaultPoints.AcquireAfterDelete);
        var acquire = b.AcquireAsync("k", "y", limit: 1);
        await hold.Arrived.WaitAsync(Poll.Bound);
        await rig.ExpireAsync("k", "x");
        await Poll.UntilAsync(async () => !await rig.ValidAsync("k", "x", token), "the slot to expire");
        hold.Release();

        Assert.IsType<SlotAcquireResult.Full>(await acquire.WaitAsync(Poll.Bound));
        Assert.Equal(0L, await rig.ValidCountAsync("k"));

        var next = b.AcquireAsync("k", "y", limit: 1);
        await rig.Db.WaitForALockWaiterAsync();
        Assert.False(next.IsCompleted);
        await fenced.CommitAsync();
        await fenced.DisposeAsync();

        Assert.True((await next.WaitAsync(Poll.Bound)).IsAcquired);
        Assert.Equal(1L, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
    }

    private static async Task<T> ScalarAsync<T>(SlotFencedTransaction fenced, string sql)
    {
        await using var command = new NpgsqlCommand(sql, fenced.Connection, fenced.Transaction);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private static string? SqlStateIn(Exception? exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is PostgresException pg)
            {
                return pg.SqlState;
            }
        }

        return null;
    }
}
