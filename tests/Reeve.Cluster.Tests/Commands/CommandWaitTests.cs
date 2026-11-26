using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Wakeup;
using Unit.Testing;

namespace Unit.Commands;

public sealed class CommandWaitTests
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private readonly FakeCommandRows rows = new();
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly FakeTimeProvider time = new();
    private readonly ClusterCommands commands;

    public CommandWaitTests()
    {
        commands = new ClusterCommands(
            rows, [new CommandHandlerRegistration(Kind, typeof(ScriptedCommandHandler), InDoubtPolicy.ReRun)], wakeups,
            Options.Create(new ClusterOptions { ReplicaId = "a", FencedTransactionTimeout = TimeSpan.FromSeconds(1), PlacementInterval = TimeSpan.FromSeconds(2) }),
            time, NullLogger<ClusterCommands>.Instance);
    }

    private Task<DurableCommandResult> Wait(string idempotency, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        commands.WaitAsync(Kind, Key, idempotency, timeout ?? TimeSpan.FromMinutes(1), cancellationToken);

    private async Task<T> RunClockAsync<T>(Task<T> task, TimeSpan step)
    {
        var deadline = DateTime.UtcNow + Bounds.Safety;
        while (!task.IsCompleted)
        {
            Assert.True(DateTime.UtcNow < deadline, "the wait did not end");
            time.Advance(step);
            await Task.Delay(5);
        }

        return await task;
    }

    private async Task UntilWaitingAsync(int reads)
    {
        var deadline = DateTime.UtcNow + Bounds.Safety;
        while (rows.StatusReads < reads)
        {
            Assert.True(DateTime.UtcNow < deadline, "the wait did not read the row");
            await Task.Delay(5);
        }
    }

    [Fact]
    public async Task A_done_command_ends_the_wait_at_once_with_its_result()
    {
        var command = rows.Add(token: 5, state: CommandState.Done, idempotency: "a");
        command.Result = [1, 2];

        var result = await Wait("a");

        Assert.Equal(DurableCommandOutcome.Completed, result.Outcome);
        Assert.Equal(command.Id, result.CommandId);
        Assert.Equal(DurableCommandState.Done, result.State);
        Assert.Equal(new byte[] { 1, 2 }, result.Result.ToArray());
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task A_done_command_with_an_error_ends_the_wait_as_failed_with_the_handlers_message()
    {
        var command = rows.Add(token: 5, state: CommandState.Done, idempotency: "a");
        command.Error = "boom";

        var result = await Wait("a");

        Assert.Equal(DurableCommandOutcome.Failed, result.Outcome);
        Assert.Equal("boom", result.Error);
        Assert.True(result.Result.IsEmpty);
        Assert.Equal(DurableCommandState.Done, result.State);
    }

    [Fact]
    public async Task An_in_doubt_command_ends_the_wait_as_in_doubt()
    {
        rows.Add(token: 5, state: CommandState.InDoubt, idempotency: "a");

        var result = await Wait("a");

        Assert.Equal(DurableCommandOutcome.InDoubt, result.Outcome);
        Assert.Equal(DurableCommandState.InDoubt, result.State);
    }

    [Fact]
    public async Task An_expired_command_ends_the_wait_as_no_owner_and_it_never_ran()
    {
        rows.Add(state: CommandState.Expired, idempotency: "a");

        var result = await Wait("a");

        Assert.Equal(DurableCommandOutcome.NoOwner, result.Outcome);
        Assert.Equal(DurableCommandState.Expired, result.State);
        Assert.True(result.SafeToRetry);
    }

    [Fact]
    public async Task A_command_that_does_not_exist_ends_the_wait_as_not_found()
    {
        var result = await Wait("nope");

        Assert.Equal(DurableCommandOutcome.NotFound, result.Outcome);
        Assert.False(result.SafeToRetry);
    }

    [Theory]
    [InlineData("pending", DurableCommandState.Pending)]
    [InlineData("taken", DurableCommandState.Taken)]
    public async Task A_command_still_active_when_the_callers_timeout_passes_ends_the_wait_unfinished_with_its_state(string sql, DurableCommandState expected)
    {
        var state = CommandStates.Parse(sql);
        rows.Add(token: state == CommandState.Taken ? 5 : 0, state: state, idempotency: "a");

        var result = await RunClockAsync(Wait("a", TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(1));

        Assert.Equal(DurableCommandOutcome.Unfinished, result.Outcome);
        Assert.Equal(expected, result.State);
        Assert.False(result.SafeToRetry);
    }

    [Fact]
    public async Task A_completion_nudge_for_the_kind_wakes_the_wait_before_the_poll()
    {
        var command = rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        var wait = Wait("a");
        await UntilWaitingAsync(1);

        command.State = CommandState.Done;
        command.Result = [9];
        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, CommandNudges.Completed(Kind, Key)));

        var result = await wait.WaitAsync(Bounds.Safety);
        Assert.Equal(DurableCommandOutcome.Completed, result.Outcome);
    }

    [Fact]
    public async Task A_nudge_for_another_kind_or_another_key_or_for_the_owner_does_not_wake_the_wait()
    {
        rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        var wait = Wait("a");
        await UntilWaitingAsync(1);

        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, CommandNudges.Completed("other", Key)));
        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, CommandNudges.Completed(Kind, "k2")));
        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, Kind));
        await Task.Delay(50);

        Assert.Equal(1, rows.StatusReads);
        Assert.False(wait.IsCompleted);
        await RunClockAsync(wait, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task A_read_that_fails_on_the_connection_is_read_again_by_the_poll_and_the_wait_ends_with_the_outcome()
    {
        var command = rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        var failures = 2;
        rows.StatusFailure = () => failures-- > 0 ? new NpgsqlException("connection refused", new IOException("refused")) : null;
        var wait = Wait("a");
        await UntilWaitingAsync(1);

        command.State = CommandState.Done;
        command.Result = [9];
        var result = await RunClockAsync(wait, TimeSpan.FromSeconds(2));

        Assert.Equal(DurableCommandOutcome.Completed, result.Outcome);
        Assert.True(rows.StatusReads >= 3);
    }

    [Fact]
    public async Task A_read_that_keeps_failing_ends_the_wait_unfinished_at_its_deadline_and_does_not_throw()
    {
        rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        rows.StatusFailure = () => new NpgsqlException("connection refused", new IOException("refused"));

        var result = await RunClockAsync(Wait("a", TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(2));

        Assert.Equal(DurableCommandOutcome.Unfinished, result.Outcome);
    }

    [Fact]
    public async Task A_read_the_database_refuses_for_its_content_is_not_read_again_for_ever()
    {
        rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        rows.StatusFailure = () => new PostgresException("invalid", "ERROR", "ERROR", "22021");

        await Assert.ThrowsAsync<PostgresException>(() => Wait("a").WaitAsync(Bounds.Safety));
    }

    [Fact]
    public async Task A_read_that_fails_on_something_other_than_the_database_or_the_connection_is_a_defect_and_is_thrown()
    {
        rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        rows.StatusFailure = () => new InvalidOperationException("defect");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Wait("a").WaitAsync(Bounds.Safety));
    }

    [Fact]
    public async Task A_key_or_idempotency_key_the_table_cannot_hold_is_refused_before_any_read()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => commands.WaitAsync(Kind, "k\0", "a", TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentException>(() => commands.WaitAsync(Kind, Key, "a" + '\uD800', TimeSpan.FromSeconds(1)));
        Assert.Equal(0, rows.StatusReads);
    }

    [Fact]
    public async Task With_no_nudge_the_poll_finds_the_completion()
    {
        var command = rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        var wait = Wait("a");
        await UntilWaitingAsync(1);

        command.State = CommandState.InDoubt;
        var result = await RunClockAsync(wait, TimeSpan.FromSeconds(1));

        Assert.Equal(DurableCommandOutcome.InDoubt, result.Outcome);
        Assert.True(rows.StatusReads >= 2);
    }

    [Fact]
    public async Task Cancelling_the_wait_ends_it_as_the_timeout_does_and_the_command_is_left_alone()
    {
        var command = rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        using var cancel = new CancellationTokenSource();
        var wait = Wait("a", cancellationToken: cancel.Token);
        await UntilWaitingAsync(1);

        await cancel.CancelAsync();

        var result = await wait.WaitAsync(Bounds.Safety);
        Assert.Equal(DurableCommandOutcome.Unfinished, result.Outcome);
        Assert.Equal(CommandState.Taken, command.State);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_timeout_that_is_not_positive_is_refused(int seconds)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Wait("a", TimeSpan.FromSeconds(seconds)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Wait("a", Timeout.InfiniteTimeSpan));
    }

    [Fact]
    public async Task A_caller_can_come_back_later_by_idempotency_key_after_its_wait_ended_unfinished()
    {
        var command = rows.Add(token: 5, state: CommandState.Taken, idempotency: "a");
        var first = await RunClockAsync(Wait("a", TimeSpan.FromSeconds(5)), TimeSpan.FromSeconds(1));
        Assert.Equal(DurableCommandOutcome.Unfinished, first.Outcome);

        command.State = CommandState.Done;
        var second = await Wait("a");

        Assert.Equal(DurableCommandOutcome.Completed, second.Outcome);
    }
}
