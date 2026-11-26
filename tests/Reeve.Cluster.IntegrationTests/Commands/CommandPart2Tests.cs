using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Commands;

public sealed class CommandPart2Tests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private CommandsRig rig = null!;

    public CommandPart2Tests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => rig = await CommandsRig.CreateAsync(postgres);

    public async Task DisposeAsync() => await rig.DisposeAsync();

    private async Task BackdateAsync(long id, string created = "11 minutes", string? completed = null)
    {
        await using var connection = await rig.Db.OpenAsync();
        await ExecAsync(
            connection,
            $"UPDATE reeve_cluster.command SET created_at = clock_timestamp() - interval '{created}', completed_at = CASE WHEN @completed::text IS NULL THEN completed_at ELSE clock_timestamp() - @completed::text::interval END WHERE id = @id",
            ("id", id), ("completed", (object?)completed ?? DBNull.Value));
    }

    [Fact]
    public async Task A_completion_with_an_error_marks_the_command_done_with_the_error_and_no_result()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);

        var action = await rig.Store.CompleteAsync(rig.Owner, command.Id, new CommandOutcome(default, "boom"), CancellationToken.None);

        Assert.Equal(CompleteAction.Completed, action);
        Assert.Equal(("done", rig.Token), await rig.RowAsync(command.Id));
        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);
        Assert.Equal("boom", status!.Value.Error);
        Assert.Null(status.Value.Result);
    }

    [Fact]
    public async Task A_completion_without_an_error_keeps_its_result_and_reads_as_completed()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
        await rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(new byte[] { 4, 2 }), CancellationToken.None);

        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);

        Assert.Null(status!.Value.Error);
        Assert.Equal(new byte[] { 4, 2 }, status.Value.Result);
        Assert.Equal(DurableCommandOutcome.Completed, WaitDecisions.Finished(status)!.Outcome);
    }

    [Fact]
    public async Task The_table_refuses_an_error_on_a_command_that_is_not_done_and_an_error_beside_a_result()
    {
        var command = await rig.InsertAsync("i1");
        await using var connection = await rig.Db.OpenAsync();

        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET error = 'x' WHERE id = @id", ("id", command.Id)));
        await rig.SetStateAsync(command.Id, "done", rig.Token);
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE reeve_cluster.command SET error = 'x', result = '\\x01' WHERE id = @id", ("id", command.Id)));
        Assert.Equal(1, await ExecAsync(connection, "UPDATE reeve_cluster.command SET error = 'x' WHERE id = @id", ("id", command.Id)));
    }

    [Fact]
    public async Task The_database_refuses_a_NUL_and_the_driver_a_lone_surrogate_in_the_error_and_the_command_stays_taken()
    {
        var command = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);

        var nul = await Assert.ThrowsAsync<PostgresException>(
            () => rig.Store.CompleteAsync(rig.Owner, command.Id, new CommandOutcome(default, "a\0b"), CancellationToken.None));
        var surrogate = await Assert.ThrowsAnyAsync<Exception>(
            () => rig.Store.CompleteAsync(rig.Owner, command.Id, new CommandOutcome(default, "a" + '\uD800'), CancellationToken.None));

        Assert.Equal("22021", nul.SqlState);
        Assert.True(CommandText.IsPermanent(nul));
        Assert.True(CommandText.IsPermanent(surrogate), $"{surrogate.GetType()} is not told apart from a lost connection");
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task An_error_made_storable_by_the_normaliser_completes_the_command_and_reads_back_as_failed()
    {
        var command = await rig.InsertAsync("i1");
        var next = await rig.InsertAsync("i2");
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
        var error = CommandText.ErrorOf(new FormatException("bad value 'a\0b' " + '\uD800' + " and \uD83D\uDE00"));

        var action = await rig.Store.CompleteAsync(rig.Owner, command.Id, new CommandOutcome(default, error), CancellationToken.None);

        Assert.Equal(CompleteAction.Completed, action);
        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);
        Assert.Equal("bad value 'a\uFFFDb' \uFFFD and \uD83D\uDE00", status!.Value.Error);
        Assert.Equal(DurableCommandOutcome.Failed, WaitDecisions.Finished(status)!.Outcome);
        Assert.Equal(TakeAction.Run, (await rig.Store.TakeAsync(rig.Owner, next.Id, CancellationToken.None)).Action);
    }

    [Fact]
    public async Task An_error_of_the_cap_length_and_the_fixed_errors_are_stored_as_they_are()
    {
        var long_ = await rig.InsertAsync("i1");
        await rig.Store.TakeAsync(rig.Owner, long_.Id, CancellationToken.None);
        var error = CommandText.ErrorOf(new InvalidOperationException(new string('x', 50_000)));

        await rig.Store.CompleteAsync(rig.Owner, long_.Id, new CommandOutcome(default, error), CancellationToken.None);

        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);
        Assert.Equal(CommandText.MaxErrorLength, status!.Value.Error!.Length);
        foreach (var (idempotency, fixedError) in new[] { ("i2", CommandText.Unstorable), ("i3", CommandText.Cancelled) })
        {
            var command = await rig.InsertAsync(idempotency);
            await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
            await rig.Store.CompleteAsync(rig.Owner, command.Id, new CommandOutcome(default, fixedError), CancellationToken.None);
            Assert.Equal(fixedError, (await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, idempotency, CancellationToken.None))!.Value.Error);
        }
    }

    [Fact]
    public async Task A_payload_with_every_byte_value_round_trips_and_a_NUL_in_the_idempotency_key_is_a_data_exception()
    {
        var bytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        var command = await rig.InsertAsync("i1", bytes);
        await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None);
        await rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(bytes), CancellationToken.None);

        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);

        Assert.Equal(bytes, status!.Value.Result);
        var refused = await Assert.ThrowsAsync<PostgresException>(() => rig.InsertAsync("i\0"));
        Assert.Equal("22021", refused.SqlState);
    }

    [Fact]
    public async Task A_fenced_retake_takes_a_command_of_an_older_token_again_under_the_tenures_token()
    {
        var command = await rig.InsertAsync("i1", [7]);
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 1);

        var retaken = await rig.Store.RetakeAsync(rig.Owner, command.Id, rig.Token - 1, CancellationToken.None);

        Assert.Equal(TakeAction.Run, retaken.Action);
        Assert.Equal(new byte[] { 7 }, retaken.Row!.Payload);
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_retake_that_names_another_old_token_than_the_rows_matches_nothing()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 2);

        var retaken = await rig.Store.RetakeAsync(rig.Owner, command.Id, rig.Token - 1, CancellationToken.None);

        Assert.Equal(TakeAction.NotTaken, retaken.Action);
        Assert.Equal(("taken", rig.Token - 2), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_retake_of_a_command_taken_under_the_own_or_a_newer_token_matches_nothing()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token);

        Assert.Equal(TakeAction.NotTaken, (await rig.Store.RetakeAsync(rig.Owner, command.Id, rig.Token, CancellationToken.None)).Action);
        await rig.SetStateAsync(command.Id, "taken", rig.Token + 5);
        Assert.Equal(TakeAction.NotTaken, (await rig.Store.RetakeAsync(rig.Owner, command.Id, rig.Token + 5, CancellationToken.None)).Action);
    }

    [Fact]
    public async Task A_retake_under_a_token_the_owned_row_no_longer_carries_is_refused_by_the_database_and_changes_nothing()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 1);

        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.Store.RetakeAsync(rig.OwnerWith(rig.Token + 1), command.Id, rig.Token - 1, CancellationToken.None));

        Assert.Equal(("taken", rig.Token - 1), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_fenced_in_doubt_verdict_records_a_command_of_an_older_token_in_doubt_and_keeps_that_token()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 1);

        var action = await rig.Store.MarkInDoubtAsync(rig.Owner, command.Id, rig.Token - 1, CancellationToken.None);

        Assert.Equal(MarkAction.Marked, action);
        Assert.Equal(("in_doubt", rig.Token - 1), await rig.RowAsync(command.Id));
        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);
        Assert.Equal(DurableCommandOutcome.InDoubt, WaitDecisions.Finished(status)!.Outcome);
    }

    [Fact]
    public async Task An_in_doubt_verdict_under_a_token_the_owned_row_no_longer_carries_is_refused_by_the_database_and_changes_nothing()
    {
        var command = await rig.InsertAsync("i1");
        await rig.SetStateAsync(command.Id, "taken", rig.Token - 1);

        await Assert.ThrowsAsync<OwnershipLostException>(() => rig.Store.MarkInDoubtAsync(rig.OwnerWith(rig.Token + 1), command.Id, rig.Token - 1, CancellationToken.None));

        Assert.Equal(("taken", rig.Token - 1), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task An_in_doubt_verdict_matches_nothing_for_a_pending_command_the_own_token_or_another_old_token()
    {
        var pending = await rig.InsertAsync("i1");
        Assert.Equal(MarkAction.Refused, await rig.Store.MarkInDoubtAsync(rig.Owner, pending.Id, rig.Token - 1, CancellationToken.None));

        await rig.SetStateAsync(pending.Id, "taken", rig.Token);
        Assert.Equal(MarkAction.Refused, await rig.Store.MarkInDoubtAsync(rig.Owner, pending.Id, rig.Token, CancellationToken.None));
        await rig.SetStateAsync(pending.Id, "taken", rig.Token - 3);
        Assert.Equal(MarkAction.Refused, await rig.Store.MarkInDoubtAsync(rig.Owner, pending.Id, rig.Token - 1, CancellationToken.None));
        Assert.Equal(("taken", rig.Token - 3), await rig.RowAsync(pending.Id));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    [InlineData("expired")]
    public async Task The_statements_of_the_old_tenures_command_agree_with_the_state_machine_in_every_state(string stateSql)
    {
        var state = CommandStates.Parse(stateSql);
        long? token = state is CommandState.Pending or CommandState.Expired ? null : rig.Token - 1;

        async Task<long> FreshAsync(string idempotency)
        {
            await using (var connection = await rig.Db.OpenAsync())
            {
                await ExecAsync(connection, "UPDATE reeve_cluster.command SET state = 'expired', taken_token = NULL, taken_at = NULL, completed_at = now() WHERE state = 'taken'");
            }

            var command = await rig.InsertAsync(idempotency);
            if (state != CommandState.Pending)
            {
                await rig.SetStateAsync(command.Id, stateSql, token);
            }

            return command.Id;
        }

        var row = new CommandRow(state, token);
        var retake = await rig.Store.RetakeAsync(rig.Owner, await FreshAsync("r"), rig.Token - 1, CancellationToken.None);
        Assert.Equal(CommandStateMachine.Decide(CommandWriter.Retake, row, rig.Token) is not null, retake.Action == TakeAction.Run);

        var mark = await rig.Store.MarkInDoubtAsync(rig.Owner, await FreshAsync("m"), rig.Token - 1, CancellationToken.None);
        Assert.Equal(CommandStateMachine.Decide(CommandWriter.MarkInDoubt, row, rig.Token) is not null, mark == MarkAction.Marked);

        var ownRow = new CommandRow(state, state is CommandState.Pending or CommandState.Expired ? null : rig.Token);
        var complete = await FreshAsync("c");
        if (ownRow.TakenToken is { } own)
        {
            await rig.SetStateAsync(complete, stateSql, own);
        }

        var completed = await rig.Store.CompleteAsync(rig.Owner, complete, default, CancellationToken.None);
        Assert.Equal(CommandStateMachine.Decide(CommandWriter.Complete, ownRow, rig.Token) is not null, completed == CompleteAction.Completed);
    }

    [Fact]
    public async Task Tokens_a_command_retained_with_the_old_tenures_token_is_the_old_tenures_after_the_key_is_created_again()
    {
        var command = await rig.InsertAsync("i1");
        Assert.Equal(TakeAction.Run, (await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None)).Action);
        await using (var connection = await rig.Db.OpenAsync())
        {
            await ExecAsync(connection, "DELETE FROM reeve_cluster.owned WHERE kind = @kind AND key = @key", ("kind", CommandsRig.Kind), ("key", CommandsRig.Key));
        }

        var recreated = await rig.Db.CreateOwnedAsync(CommandsRig.Kind, CommandsRig.Key, "a");
        var owner = rig.OwnerWith(recreated);

        Assert.True(recreated > rig.Token);
        var active = await rig.Store.ReadActiveAsync(CommandsRig.Kind, CommandsRig.Key, CancellationToken.None);
        Assert.Equal(new PickupStep(PickupAction.ClassifyOlder, command.Id), PickupDecisions.Next([.. active.Select(r => r.Active)], recreated, flight: null));
        Assert.Equal(CompleteAction.Refused, await rig.Store.CompleteAsync(owner, command.Id, default, CancellationToken.None));
        Assert.Equal(MarkAction.Marked, await rig.Store.MarkInDoubtAsync(owner, command.Id, rig.Token, CancellationToken.None));
        Assert.Equal(("in_doubt", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task The_expire_lists_and_expires_a_pending_command_older_than_the_ttl_whose_key_has_no_row()
    {
        var old = await rig.InsertAsync("old", key: "k2");
        var young = await rig.InsertAsync("young", key: "k3");
        await BackdateAsync(old.Id);

        var listed = await rig.Store.ListExpirableAsync(100, CancellationToken.None);

        Assert.Equal([old.Id], listed.Select(c => c.Id));
        Assert.True(await rig.Store.ExpireAsync(old.Id, CancellationToken.None));
        Assert.False(await rig.Store.ExpireAsync(young.Id, CancellationToken.None));
        Assert.Equal(("expired", (long?)null), await rig.RowAsync(old.Id));
        Assert.Equal(("pending", (long?)null), await rig.RowAsync(young.Id));
        await using var connection = await rig.Db.OpenAsync();
        Assert.True(await ScalarAsync<bool>(connection, "SELECT completed_at IS NOT NULL FROM reeve_cluster.command WHERE id = @id", ("id", old.Id)));
    }

    [Fact]
    public async Task A_slow_but_present_owners_old_backlog_is_not_listed_and_not_expired()
    {
        var command = await rig.InsertAsync("slow");
        await BackdateAsync(command.Id, "3 hours");

        Assert.Empty(await rig.Store.ListExpirableAsync(100, CancellationToken.None));
        Assert.False(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));
        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_key_owned_by_a_member_whose_lease_ended_is_orphaned_and_its_old_command_expires()
    {
        await rig.Db.AddMemberAsync("dead", lease: "-1 minute");
        await rig.Db.CreateOwnedAsync(CommandsRig.Kind, "orphan", "dead");
        var command = await rig.InsertAsync("o", key: "orphan");
        await BackdateAsync(command.Id);

        Assert.Equal([command.Id], (await rig.Store.ListExpirableAsync(100, CancellationToken.None)).Select(c => c.Id));
        Assert.True(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));
    }

    [Fact]
    public async Task A_key_adopted_between_the_sweeps_read_and_its_update_keeps_its_command()
    {
        var command = await rig.InsertAsync("adopted", key: "k4");
        await BackdateAsync(command.Id);
        var listed = await rig.Store.ListExpirableAsync(100, CancellationToken.None);
        Assert.Equal([command.Id], listed.Select(c => c.Id));

        await rig.Db.CreateOwnedAsync(CommandsRig.Kind, "k4", "a");

        Assert.False(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));
        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
    }

    [Theory]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    public async Task Only_a_pending_command_expires(string stateSql)
    {
        var command = await rig.InsertAsync("x", key: "k2");
        await rig.SetStateAsync(command.Id, stateSql, rig.Token);
        await BackdateAsync(command.Id);

        Assert.False(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));
        Assert.Equal((stateSql, (long?)rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task A_resend_after_expiry_pends_the_command_again_with_a_fresh_age()
    {
        var command = await rig.InsertAsync("e", key: "k2");
        await BackdateAsync(command.Id);
        Assert.True(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));

        Assert.True(await rig.Store.ResendAsync(command.Id, CancellationToken.None));

        Assert.Equal(("pending", (long?)null), await rig.RowAsync(command.Id));
        Assert.Empty(await rig.Store.ListExpirableAsync(100, CancellationToken.None));
        Assert.False(await rig.Store.ExpireAsync(command.Id, CancellationToken.None));
    }

    [Fact]
    public async Task Retention_deletes_finished_commands_completed_more_than_the_retention_ago_and_nothing_else()
    {
        var done = await rig.InsertAsync("done", key: "k2");
        var inDoubt = await rig.InsertAsync("doubt", key: "k2");
        var expired = await rig.InsertAsync("exp", key: "k2");
        var recent = await rig.InsertAsync("recent", key: "k2");
        var pending = await rig.InsertAsync("pending", key: "k2");
        var taken = await rig.InsertAsync("taken", key: "k3");
        await rig.SetStateAsync(done.Id, "done", rig.Token);
        await rig.SetStateAsync(inDoubt.Id, "in_doubt", rig.Token);
        await rig.SetStateAsync(expired.Id, "expired");
        await rig.SetStateAsync(recent.Id, "done", rig.Token);
        await rig.SetStateAsync(taken.Id, "taken", rig.Token);
        foreach (var old in new[] { done.Id, inDoubt.Id, expired.Id })
        {
            await BackdateAsync(old, "3 days", completed: "25 hours");
        }

        await BackdateAsync(recent.Id, "3 days", completed: "23 hours");
        await BackdateAsync(pending.Id, "3 days");
        await BackdateAsync(taken.Id, "3 days");

        Assert.Equal(3, await rig.Store.DeleteRetainedAsync(100, CancellationToken.None));

        await using var connection = await rig.Db.OpenAsync();
        Assert.Equal(3, await ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.command"));
        Assert.Equal(0, await ScalarAsync<long>(connection, "SELECT count(*) FROM reeve_cluster.command WHERE id = ANY(@ids)", ("ids", new[] { done.Id, inDoubt.Id, expired.Id })));
    }

    [Fact]
    public async Task Retention_keeps_an_active_command_by_its_state_even_where_the_table_would_let_it_carry_a_completion_time()
    {
        await using (var connection = await rig.Db.OpenAsync())
        {
            await ExecAsync(connection, "ALTER TABLE reeve_cluster.command DROP CONSTRAINT command_completed_at_shape");
        }

        var pending = await rig.InsertAsync("pending", key: "k2");
        var taken = await rig.InsertAsync("taken", key: "k3");
        var done = await rig.InsertAsync("done", key: "k2");
        await rig.SetStateAsync(taken.Id, "taken", rig.Token);
        await rig.SetStateAsync(done.Id, "done", rig.Token);
        foreach (var id in new[] { pending.Id, taken.Id, done.Id })
        {
            await BackdateAsync(id, "3 days", completed: "30 hours");
        }

        Assert.Equal(1, await rig.Store.DeleteRetainedAsync(100, CancellationToken.None));

        Assert.Equal(("pending", (long?)null), await rig.RowAsync(pending.Id));
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(taken.Id));
    }

    [Fact]
    public async Task Retention_deletes_at_most_its_limit()
    {
        for (var i = 0; i < 3; i++)
        {
            var command = await rig.InsertAsync($"d{i}", key: "k2");
            await rig.SetStateAsync(command.Id, "done", rig.Token);
            await BackdateAsync(command.Id, "3 days", completed: "30 hours");
        }

        Assert.Equal(2, await rig.Store.DeleteRetainedAsync(2, CancellationToken.None));
        Assert.Equal(1, await rig.Store.DeleteRetainedAsync(2, CancellationToken.None));
    }

    [Fact]
    public async Task The_waiting_callers_read_finds_the_command_by_idempotency_key_and_nothing_for_an_unknown_one()
    {
        var command = await rig.InsertAsync("i1");

        var status = await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "i1", CancellationToken.None);

        Assert.Equal(new CommandStatus(command.Id, CommandState.Pending, null, null), status);
        Assert.Null(await rig.Store.ReadStatusAsync(CommandsRig.Kind, CommandsRig.Key, "nope", CancellationToken.None));
        Assert.Null(WaitDecisions.Finished(status));
    }
}
