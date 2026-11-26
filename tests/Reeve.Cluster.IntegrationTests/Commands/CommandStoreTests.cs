using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Harness.Containers;
using Int.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Commands;

public sealed class CommandStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private CommandsRig rig = null!;

    public CommandStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => rig = await CommandsRig.CreateAsync(postgres);

    public async Task DisposeAsync() => await rig.DisposeAsync();

    [Fact]
    public async Task The_insert_writes_a_pending_command_with_no_token_and_a_second_insert_of_the_key_writes_nothing()
    {
        var first = await rig.InsertAsync("i1");

        Assert.Equal(("pending", (long?)null), await rig.RowAsync(first.Id));
        Assert.Null(await rig.Store.InsertAsync(CommandsRig.Kind, CommandsRig.Key, "i1", [9], CancellationToken.None));
        var read = await rig.Store.ReadAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);
        Assert.Equal(first.Id, read!.Id);
        Assert.Equal(new byte[] { 1, 2, 3 }, read.Payload);
    }

    [Fact]
    public async Task The_resend_pends_an_expired_command_again_and_touches_no_other_state()
    {
        var command = await rig.InsertAsync("i1");
        Assert.False(await rig.Store.ResendAsync(command.Id, CancellationToken.None));

        await rig.SetStateAsync(command.Id, "expired");
        Assert.True(await rig.Store.ResendAsync(command.Id, CancellationToken.None));

        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task The_table_refuses_a_pending_command_with_a_token_and_a_taken_one_without()
    {
        var command = await rig.InsertAsync("i1");
        await using var connection = await rig.Db.OpenAsync();

        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET taken_token = 5, taken_at = now() WHERE id = @id", ("id", command.Id)));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET state = 'taken' WHERE id = @id", ("id", command.Id)));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET state = 'running' WHERE id = @id", ("id", command.Id)));
    }

    [Fact]
    public async Task The_table_refuses_a_completion_time_on_an_active_command_and_a_second_taken_command_of_one_key()
    {
        var first = await rig.InsertAsync("i1");
        var second = await rig.InsertAsync("i2");
        await using var connection = await rig.Db.OpenAsync();

        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET completed_at = now() WHERE id = @id", ("id", first.Id)));
        await rig.SetStateAsync(first.Id, "taken", rig.Token);
        await Assert.ThrowsAsync<PostgresException>(() => rig.SetStateAsync(second.Id, "taken", rig.Token));
    }

    [Fact]
    public async Task The_active_keys_and_the_active_read_list_the_taken_commands_and_the_oldest_pending_one_in_order()
    {
        var first = await rig.InsertAsync("i1");
        var second = await rig.InsertAsync("i2");
        await rig.InsertAsync("i3");
        await rig.InsertAsync("i4", key: "k2");
        var finished = await rig.InsertAsync("i5");
        await rig.SetStateAsync(finished.Id, "done", rig.Token);

        var keys = await rig.Store.ListActiveKeysAsync([CommandsRig.Kind], CancellationToken.None);
        var active = await rig.Store.ReadActiveAsync(CommandsRig.Kind, CommandsRig.Key, CancellationToken.None);

        Assert.Equal([(CommandsRig.Kind, "k1"), (CommandsRig.Kind, "k2")], keys.OrderBy(k => k.Key).ToArray());
        Assert.Equal([first.Id], active.Select(r => r.Id));
        await rig.SetStateAsync(first.Id, "taken", rig.Token);
        active = await rig.Store.ReadActiveAsync(CommandsRig.Kind, CommandsRig.Key, CancellationToken.None);
        Assert.Equal([first.Id, second.Id], active.Select(r => r.Id));
        Assert.Equal(CommandState.Taken, active[0].State);
    }

    [Fact]
    public async Task A_fenced_take_marks_the_command_taken_under_the_tenures_token_and_returns_its_payload()
    {
        var command = await rig.InsertAsync("i1", [7, 8]);

        var taken = await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);

        Assert.Equal(TakeAction.Run, taken.Action);
        Assert.Equal(new byte[] { 7, 8 }, taken.Row!.Payload);
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_second_command_of_the_key_is_not_taken_while_one_is_taken()
    {
        var first = await rig.InsertAsync("i1");
        var second = await rig.InsertAsync("i2");
        Assert.Equal(TakeAction.Run, (await rig.Store.TakeAsync(rig.Owner, first.Id, CancellationToken.None)).Action);

        var again = await rig.Store.TakeAsync(rig.Owner, second.Id, CancellationToken.None);

        Assert.Equal(TakeAction.NotTaken, again.Action);
        Assert.Equal(("pending", (long?)null), await rig.RowAsync(second.Id));
    }

    [Fact]
    public async Task A_command_that_is_not_pending_is_not_taken()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "expired");

        Assert.Equal(TakeAction.NotTaken, (await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None)).Action);
    }

    [Fact]
    public async Task A_take_under_a_token_the_row_no_longer_carries_is_refused_by_the_database_and_changes_nothing()
    {
        var command = await rig.InsertAsync("i1");

        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.Store.TakeAsync(rig.OwnerWith(rig.Token + 1), command.Id, CancellationToken.None));

        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_completion_under_the_taking_token_marks_the_command_done_with_its_result()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);

        var action = await rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(new byte[] { 5 }), CancellationToken.None);

        Assert.Equal(CompleteAction.Completed, action);
        Assert.Equal(("done", rig.Token), await rig.RowAsync(command.Id));
        await using var connection = await rig.Db.OpenAsync();
        Assert.Equal(new byte[] { 5 }, await ScalarAsync<byte[]>(connection, "SELECT result FROM reeve_cluster.command WHERE id = @id", ("id", command.Id)));
    }

    [Fact]
    public async Task I18_a_completion_of_a_command_taken_under_an_older_token_is_refused_and_the_row_keeps_its_token()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 1);

        var action = await rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(new byte[] { 5 }), CancellationToken.None);

        Assert.Equal(CompleteAction.Refused, action);
        Assert.Equal(("taken", rig.Token - 1), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_completion_under_a_token_the_owned_row_no_longer_carries_is_refused_by_the_database_even_though_the_command_is_taken_under_it()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token + 1);

        await Assert.ThrowsAsync<OwnershipLostException>(
            () => rig.Store.CompleteAsync(rig.OwnerWith(rig.Token + 1), command.Id, CommandOutcome.Returned(new byte[] { 5 }), CancellationToken.None));

        Assert.Equal(("taken", rig.Token + 1), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_completion_after_the_owners_lease_ended_is_refused_by_the_database_and_the_command_stays_taken()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
        await using (var connection = await rig.Db.OpenAsync())
        {
            await ExecAsync(connection, "UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'");
        }

        await Assert.ThrowsAsync<OwnershipLostException>(
            () => rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(new byte[] { 5 }), CancellationToken.None));

        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_completion_by_a_tenure_of_another_timeline_is_refused_by_the_database_and_the_owner_is_told()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
        var stale = new FencedOwner(CommandsRig.Kind, CommandsRig.Key, rig.Token, rig.Db.PreviousTimeline);

        await Assert.ThrowsAsync<OwnershipLostException>(
            () => rig.Store.CompleteAsync(stale, command.Id, CommandOutcome.Returned(new byte[] { 5 }), CancellationToken.None));

        Assert.Equal(1, stale.OtherTimelines);
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_completion_of_a_command_that_is_not_taken_is_refused()
    {
        var command = await rig.InsertAsync("i1");

        Assert.Equal(CompleteAction.Refused, await rig.Store.CompleteAsync(rig.Owner, command.Id, default, CancellationToken.None));
        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_fenced_take_holds_the_owned_row_share_lock_until_its_commit_and_takes_the_command_row_after_it()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);

        Assert.True(await rig.Db.CanLockOwnedAsync(CommandsRig.Kind, CommandsRig.Key, "FOR UPDATE"));
        Assert.True(await rig.Db.CanLockAsync("command", "id = @id", "FOR UPDATE", ("id", command.Id)));
    }
}
