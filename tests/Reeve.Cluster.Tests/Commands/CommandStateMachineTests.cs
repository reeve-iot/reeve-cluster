using Reeve.Cluster.Commands;

namespace Unit.Commands;

public sealed class CommandStateMachineTests
{
    private static readonly CommandState[] States = Enum.GetValues<CommandState>();

    private static CommandRow Row(CommandState state, long? token = null) =>
        new(state, state is CommandState.Pending or CommandState.Expired ? null : token ?? 5);

    [Fact]
    public void A_send_with_no_row_inserts_a_pending_command_with_no_token()
    {
        Assert.Equal(new CommandStep(CommandState.Pending, null), CommandStateMachine.Decide(CommandWriter.Send, null));
    }

    [Fact]
    public void A_send_never_overwrites_a_row()
    {
        foreach (var state in States)
        {
            Assert.Null(CommandStateMachine.Decide(CommandWriter.Send, Row(state)));
        }
    }

    [Fact]
    public void Only_an_expired_command_is_pended_again_and_it_keeps_no_token()
    {
        foreach (var state in States)
        {
            var step = CommandStateMachine.Decide(CommandWriter.Resend, Row(state));
            Assert.Equal(state == CommandState.Expired ? new CommandStep(CommandState.Pending, null) : null, step);
        }

        Assert.Null(CommandStateMachine.Decide(CommandWriter.Resend, null));
    }

    [Fact]
    public void Only_a_pending_command_is_taken_and_it_takes_the_owners_token()
    {
        foreach (var state in States)
        {
            var step = CommandStateMachine.Decide(CommandWriter.Take, Row(state), actor: 9);
            Assert.Equal(state == CommandState.Pending ? new CommandStep(CommandState.Taken, 9) : null, step);
        }

        Assert.Null(CommandStateMachine.Decide(CommandWriter.Take, null, actor: 9));
    }

    [Fact]
    public void A_completion_needs_the_command_taken_by_exactly_the_completers_token()
    {
        Assert.Equal(new CommandStep(CommandState.Done, 5), CommandStateMachine.Decide(CommandWriter.Complete, Row(CommandState.Taken, 5), actor: 5));
    }

    [Theory]
    [InlineData(4L)]
    [InlineData(6L)]
    public void A_completion_with_another_token_is_refused_whether_it_is_older_or_newer(long actor)
    {
        Assert.Null(CommandStateMachine.Decide(CommandWriter.Complete, Row(CommandState.Taken, 5), actor));
    }

    [Fact]
    public void A_completion_of_a_command_that_is_not_taken_is_refused()
    {
        foreach (var state in States.Where(s => s != CommandState.Taken))
        {
            Assert.Null(CommandStateMachine.Decide(CommandWriter.Complete, Row(state, 5), actor: 5));
        }
    }

    [Fact]
    public void A_retake_moves_a_command_taken_by_an_older_token_to_the_new_one()
    {
        Assert.Equal(new CommandStep(CommandState.Taken, 9), CommandStateMachine.Decide(CommandWriter.Retake, Row(CommandState.Taken, 5), actor: 9));
    }

    [Theory]
    [InlineData(9L)]
    [InlineData(8L)]
    public void A_retake_by_the_same_or_a_lower_token_is_refused(long actor)
    {
        Assert.Null(CommandStateMachine.Decide(CommandWriter.Retake, Row(CommandState.Taken, 9), actor));
    }

    [Fact]
    public void An_in_doubt_verdict_keeps_the_old_token_and_needs_an_older_one()
    {
        Assert.Equal(new CommandStep(CommandState.InDoubt, 5), CommandStateMachine.Decide(CommandWriter.MarkInDoubt, Row(CommandState.Taken, 5), actor: 9));
        Assert.Null(CommandStateMachine.Decide(CommandWriter.MarkInDoubt, Row(CommandState.Taken, 5), actor: 5));
        Assert.Null(CommandStateMachine.Decide(CommandWriter.MarkInDoubt, Row(CommandState.Taken, 9), actor: 5));
    }

    [Fact]
    public void Retake_and_in_doubt_act_on_taken_commands_only()
    {
        foreach (var state in States.Where(s => s != CommandState.Taken))
        {
            Assert.Null(CommandStateMachine.Decide(CommandWriter.Retake, Row(state, 5), actor: 9));
            Assert.Null(CommandStateMachine.Decide(CommandWriter.MarkInDoubt, Row(state, 5), actor: 9));
        }
    }

    [Fact]
    public void Only_a_pending_command_expires()
    {
        foreach (var state in States)
        {
            var step = CommandStateMachine.Decide(CommandWriter.Expire, Row(state));
            Assert.Equal(state == CommandState.Pending ? new CommandStep(CommandState.Expired, null) : null, step);
        }
    }

    [Fact]
    public void Retention_deletes_only_finished_commands()
    {
        foreach (var state in States)
        {
            var step = CommandStateMachine.Decide(CommandWriter.Retention, Row(state));
            var finished = state is CommandState.Done or CommandState.InDoubt or CommandState.Expired;
            Assert.Equal(finished ? new CommandStep(null, null) : null, step);
        }
    }

    [Fact]
    public void The_sql_names_of_the_states_round_trip()
    {
        foreach (var state in States)
        {
            Assert.Equal(state, CommandStates.Parse(CommandStates.ToSql(state)));
        }

        Assert.Equal("in_doubt", CommandStates.ToSql(CommandState.InDoubt));
        Assert.Throws<ArgumentException>(() => CommandStates.Parse("nope"));
    }
}
